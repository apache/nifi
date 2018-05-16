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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.standard.relp.event.RELPEvent;
import org.apache.nifi.processors.standard.relp.event.RELPEventFactory;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.handler.RELPSocketChannelHandlerFactory;
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "relp", "tcp", "logs"})
@CapabilityDescription("Listens for RELP messages being sent to a given port over TCP. Each message will be " +
        "acknowledged after successfully writing the message to a FlowFile. Each FlowFile will contain data " +
        "portion of one or more RELP frames. In the case where the RELP frames contain syslog messages, the " +
        "output of this processor can be sent to a ParseSyslog processor for further processing.")
@WritesAttributes({
        @WritesAttribute(attribute="relp.command", description="The command of the RELP frames."),
        @WritesAttribute(attribute="relp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="relp.port", description="The sending port the messages were received over."),
        @WritesAttribute(attribute="relp.txnr", description="The transaction number of the message. Only included if <Batch Size> is 1."),
        @WritesAttribute(attribute="mime.type", description="The mime.type of the content which is text/plain")
    })
@SeeAlso({ParseSyslog.class})
public class ListenRELP extends AbstractListenEventBatchingProcessor<RELPEvent> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue(SSLContextService.ClientAuth.REQUIRED.name())
            .build();

    private volatile RELPEncoder relpEncoder;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(MAX_CONNECTIONS, SSL_CONTEXT_SERVICE, CLIENT_AUTH);
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        // wanted to ensure charset was already populated here
        relpEncoder = new RELPEncoder(charset);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        // Validate CLIENT_AUTH
        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<RELPEvent> events) throws IOException {
        final EventFactory<RELPEvent> eventFactory = new RELPEventFactory();
        final ChannelHandlerFactory<RELPEvent,AsyncChannelDispatcher> handlerFactory = new RELPSocketChannelHandlerFactory<>();

        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());

        // initialize the buffer pool based on max number of connections and the buffer size
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(maxConnections, bufferSize);

        // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
        SSLContext sslContext = null;
        SslContextFactory.ClientAuth clientAuth = null;

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuthValue));
            clientAuth = SslContextFactory.ClientAuth.valueOf(clientAuthValue);

        }

        // if we decide to support SSL then get the context and pass it in here
        return new SocketChannelDispatcher<>(eventFactory, handlerFactory, bufferPool, events,
                getLogger(), maxConnections, sslContext, clientAuth, charSet);
    }

    @Override
    protected String getBatchKey(RELPEvent event) {
        return event.getSender() + "_" + event.getCommand();
    }

    @Override
    protected void postProcess(final ProcessContext context, final ProcessSession session, final List<RELPEvent> events) {
        // first commit the session so we guarantee we have all the events successfully
        // written to FlowFiles and transferred to the success relationship
        session.commit();

        // respond to each event to acknowledge successful receipt
        for (final RELPEvent event : events) {
            respond(event, RELPResponse.ok(event.getTxnr()));
        }
    }

    protected void respond(final RELPEvent event, final RELPResponse relpResponse) {
        final ChannelResponse response = new RELPChannelResponse(relpEncoder, relpResponse);

        final ChannelResponder responder = event.getResponder();
        responder.addResponse(response);
        try {
            responder.respond();
        } catch (IOException e) {
            getLogger().error("Error sending response for transaction {} due to {}",
                    new Object[] {event.getTxnr(), e.getMessage()}, e);
        }
    }

    @Override
    protected Map<String, String> getAttributes(FlowFileEventBatch batch) {
        final List<RELPEvent> events = batch.getEvents();

        // the sender and command will be the same for all events based on the batch key
        final String sender = events.get(0).getSender();
        final String command = events.get(0).getCommand();

        final int numAttributes = events.size() == 1 ? 5 : 4;

        final Map<String,String> attributes = new HashMap<>(numAttributes);
        attributes.put(RELPAttributes.COMMAND.key(), command);
        attributes.put(RELPAttributes.SENDER.key(), sender);
        attributes.put(RELPAttributes.PORT.key(), String.valueOf(port));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        // if there was only one event then we can pass on the transaction
        // NOTE: we could pass on all the transaction ids joined together
        if (events.size() == 1) {
            attributes.put(RELPAttributes.TXNR.key(), String.valueOf(events.get(0).getTxnr()));
        }
        return attributes;
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("relp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    public enum RELPAttributes implements FlowFileAttributeKey {
        TXNR("relp.txnr"),
        COMMAND("relp.command"),
        SENDER("relp.sender"),
        PORT("relp.port");

        private final String key;

        RELPAttributes(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }
}
