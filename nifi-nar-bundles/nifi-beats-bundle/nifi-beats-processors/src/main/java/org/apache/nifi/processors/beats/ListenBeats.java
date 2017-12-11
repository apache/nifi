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
package org.apache.nifi.processors.beats;

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

import javax.net.ssl.SSLContext;

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
import org.apache.nifi.processors.beats.event.BeatsEvent;
import org.apache.nifi.processors.beats.event.BeatsEventFactory;
import org.apache.nifi.processors.beats.frame.BeatsEncoder;
import org.apache.nifi.processors.beats.handler.BeatsSocketChannelHandlerFactory;
import org.apache.nifi.processors.beats.response.BeatsChannelResponse;
import org.apache.nifi.processors.beats.response.BeatsResponse;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "beats", "tcp", "logs"})
@CapabilityDescription("Listens for messages sent by libbeat compatible clients (e.g. filebeats, metricbeats, etc) using Libbeat's 'output.logstash', writing its JSON formatted payload " +
        "to the content of a FlowFile." +
        "This processor replaces the now deprecated ListenLumberjack")
@WritesAttributes({
    @WritesAttribute(attribute = "beats.sender", description = "The sending host of the messages."),
    @WritesAttribute(attribute = "beats.port", description = "The sending port the messages were received over."),
    @WritesAttribute(attribute = "beats.sequencenumber", description = "The sequence number of the message. Only included if <Batch Size> is 1."),
    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the content which is application/json")
})
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.ParseSyslog"})
public class ListenBeats extends AbstractListenEventBatchingProcessor<BeatsEvent> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL_CONTEXT_SERVICE")
        .displayName("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
            "messages will be received over a secure connection.")
        // Nearly all Lumberjack v1 implementations require TLS to work. v2 implementations (i.e. beats) have TLS as optional
        .required(false)
        .identifiesControllerService(RestrictedSSLContextService.class)
        .build();

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
            MAX_CONNECTIONS,
            SSL_CONTEXT_SERVICE
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && sslContextService.isTrustStoreConfigured() == false) {
            results.add(new ValidationResult.Builder()
                .explanation("The context service must have a truststore  configured for the beats forwarder client to work correctly")
                .valid(false).subject(SSL_CONTEXT_SERVICE.getName()).build());
        }

        return results;
    }

    private volatile BeatsEncoder beatsEncoder;


    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        // wanted to ensure charset was already populated here
        beatsEncoder = new BeatsEncoder();
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<BeatsEvent> events) throws IOException {
        final EventFactory<BeatsEvent> eventFactory = new BeatsEventFactory();
        final ChannelHandlerFactory<BeatsEvent, AsyncChannelDispatcher> handlerFactory = new BeatsSocketChannelHandlerFactory<>();

        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());

        // initialize the buffer pool based on max number of connections and the buffer size
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(maxConnections, bufferSize);

        // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
        SSLContext sslContext = null;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
        }

        // if we decide to support SSL then get the context and pass it in here
        return new SocketChannelDispatcher<>(eventFactory, handlerFactory, bufferPool, events,
            getLogger(), maxConnections, sslContext, charSet);
    }


    @Override
    protected String getBatchKey(BeatsEvent event) {
        return event.getSender();
    }

    protected void respond(final BeatsEvent event, final BeatsResponse beatsResponse) {
        final ChannelResponse response = new BeatsChannelResponse(beatsEncoder, beatsResponse);

        final ChannelResponder responder = event.getResponder();
        responder.addResponse(response);
        try {
            responder.respond();
        } catch (IOException e) {
            getLogger().error("Error sending response for transaction {} due to {}",
                new Object[]{event.getSeqNumber(), e.getMessage()}, e);
        }
    }

    protected void postProcess(final ProcessContext context, final ProcessSession session, final List<BeatsEvent> events) {
        // first commit the session so we guarantee we have all the events successfully
        // written to FlowFiles and transferred to the success relationship
        session.commit();
        // respond to each event to acknowledge successful receipt
        for (final BeatsEvent event : events) {
            respond(event, BeatsResponse.ok(event.getSeqNumber()));
        }
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("beats").append("://").append(senderHost).append(":")
            .append(port).toString();
        return transitUri;
    }

    @Override
    protected Map<String, String> getAttributes(FlowFileEventBatch batch) {
        final List<BeatsEvent> events = batch.getEvents();
        // the sender and command will be the same for all events based on the batch key
        final String sender = events.get(0).getSender();
        final int numAttributes = events.size() == 1 ? 5 : 4;
        final Map<String, String> attributes = new HashMap<>(numAttributes);
        attributes.put(beatsAttributes.SENDER.key(), sender);
        attributes.put(beatsAttributes.PORT.key(), String.valueOf(port));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        // if there was only one event then we can pass on the transaction
        // NOTE: we could pass on all the transaction ids joined together
        if (events.size() == 1) {
            attributes.put(beatsAttributes.SEQNUMBER.key(), String.valueOf(events.get(0).getSeqNumber()));
        }
        return attributes;
    }

    public enum beatsAttributes implements FlowFileAttributeKey {
        SENDER("beats.sender"),
        PORT("beats.port"),
        SEQNUMBER("beats.sequencenumber");

        private final String key;

        beatsAttributes(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }
}
