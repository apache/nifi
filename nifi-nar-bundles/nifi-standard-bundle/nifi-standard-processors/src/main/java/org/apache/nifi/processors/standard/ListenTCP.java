/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.AbstractListenEventProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.handler.socket.SocketChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a line separator " +
        "as the message demarcator. The default behavior is for each message to produce a single FlowFile, however this can " +
        "be controlled by increasing the Batch Size to a larger value for higher throughput. The Receive Buffer Size must be " +
        "set as large as the largest messages expected to be received, meaning if every 100kb there is a line separator, then " +
        "the Receive Buffer Size must be greater than 100kb.")
@WritesAttributes({
        @WritesAttribute(attribute="tcp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="tcp.port", description="The sending port the messages were received.")
})
public class ListenTCP extends AbstractListenEventProcessor<ListenTCP.TCPEvent> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue(SSLContextService.ClientAuth.REQUIRED.name())
            .build();

    // it is only the array reference that is volatile - not the contents.
    private volatile byte[] messageDemarcatorBytes;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                MAX_CONNECTIONS,
                MAX_BATCH_SIZE,
                MESSAGE_DELIMITER,
                SSL_CONTEXT_SERVICE,
                CLIENT_AUTH
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        final String msgDemarcator = context.getProperty(MESSAGE_DELIMITER).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<TCPEvent> events)
            throws IOException {

        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());

        // initialize the buffer pool based on max number of connections and the buffer size
        final LinkedBlockingQueue<ByteBuffer> bufferPool = new LinkedBlockingQueue<>(maxConnections);
        for (int i = 0; i < maxConnections; i++) {
            bufferPool.offer(ByteBuffer.allocate(bufferSize));
        }

        final EventFactory<TCPEvent> eventFactory = new TCPEventFactory();

        // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
        SSLContext sslContext = null;
        SslContextFactory.ClientAuth clientAuth = null;

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuthValue));
            clientAuth = SslContextFactory.ClientAuth.valueOf(clientAuthValue);
        }

        final ChannelHandlerFactory<TCPEvent<SocketChannel>, AsyncChannelDispatcher> handlerFactory = new SocketChannelHandlerFactory<>();
        return new SocketChannelDispatcher(eventFactory, handlerFactory, bufferPool, events, getLogger(), maxConnections, sslContext, clientAuth, charSet);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final Map<String,FlowFileEventBatch> batches = getBatches(session, maxBatchSize, messageDemarcatorBytes);

        // if the size is 0 then there was nothing to process so return
        // we don't need to yield here because we have a long poll in side of getBatches
        if (batches.size() == 0) {
            return;
        }

        for (Map.Entry<String,FlowFileEventBatch> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<TCPEvent> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.size() == 0) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", new Object[] {entry.getKey()});
                continue;
            }

            // the sender and command will be the same for all events based on the batch key
            final String sender = events.get(0).getSender();

            final Map<String,String> attributes = new HashMap<>(3);
            attributes.put("tcp.sender", sender);
            attributes.put("tcp.port", String.valueOf(port));
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", new Object[] {flowFile});
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            // create a provenance receive event
            final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
            final String transitUri = new StringBuilder().append("tcp").append("://").append(senderHost).append(":")
                    .append(port).toString();
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    /**
     * Event implementation for TCP.
     */
    static class TCPEvent<C extends SelectableChannel> extends StandardEvent<C> {

        public TCPEvent(String sender, byte[] data, ChannelResponder<C> responder) {
            super(sender, data, responder);
        }
    }

    /**
     * Factory implementation for TCPEvents.
     */
    static final class TCPEventFactory implements EventFactory<TCPEvent> {

        @Override
        public TCPEvent create(byte[] data, Map<String, String> metadata, ChannelResponder responder) {
            String sender = null;
            if (metadata != null && metadata.containsKey(EventFactory.SENDER_KEY)) {
                sender = metadata.get(EventFactory.SENDER_KEY);
            }
            return new TCPEvent(sender, data, responder);
        }
    }
}
