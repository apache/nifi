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
package org.apache.nifi.processors.lumberjack;

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
import org.apache.nifi.processors.lumberjack.event.LumberjackEvent;
import org.apache.nifi.processors.lumberjack.event.LumberjackEventFactory;
import org.apache.nifi.processors.lumberjack.frame.LumberjackEncoder;
import org.apache.nifi.processors.lumberjack.handler.LumberjackSocketChannelHandlerFactory;
import org.apache.nifi.processors.lumberjack.response.LumberjackChannelResponse;
import org.apache.nifi.processors.lumberjack.response.LumberjackResponse;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import com.google.gson.Gson;

@Deprecated
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "lumberjack", "tcp", "logs"})
@CapabilityDescription("This processor is deprecated and may be removed in the near future. Listens for Lumberjack messages being sent to a given port over TCP. Each message will be " +
    "acknowledged after successfully writing the message to a FlowFile. Each FlowFile will contain data " +
    "portion of one or more Lumberjack frames. In the case where the Lumberjack frames contain syslog messages, the " +
    "output of this processor can be sent to a ParseSyslog processor for further processing. ")
@WritesAttributes({
    @WritesAttribute(attribute = "lumberjack.sender", description = "The sending host of the messages."),
    @WritesAttribute(attribute = "lumberjack.port", description = "The sending port the messages were received over."),
    @WritesAttribute(attribute = "lumberjack.sequencenumber", description = "The sequence number of the message. Only included if <Batch Size> is 1."),
    @WritesAttribute(attribute = "lumberjack.*", description = "The keys and respective values as sent by the lumberjack producer. Only included if <Batch Size> is 1."),
    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the content which is text/plain")
})
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.ParseSyslog"})
/**
 * @deprecated  As of release 1.2.0, replaced by {@link org.apache.nifi.processors.beats.ListenBeats}
 * */
public class ListenLumberjack extends AbstractListenEventBatchingProcessor<LumberjackEvent> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
            "messages will be received over a secure connection. Note that as Lumberjack client requires" +
            "two-way SSL authentication, the controller MUST have a truststore and a keystore to work" +
            "properly.")
        .required(true)
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
                .explanation("The context service must have a truststore  configured for the lumberjack forwarder client to work correctly")
                .valid(false).subject(SSL_CONTEXT_SERVICE.getName()).build());
        }

        return results;
    }

    private volatile LumberjackEncoder lumberjackEncoder;


    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        // wanted to ensure charset was already populated here
        lumberjackEncoder = new LumberjackEncoder();
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<LumberjackEvent> events) throws IOException {
        final EventFactory<LumberjackEvent> eventFactory = new LumberjackEventFactory();
        final ChannelHandlerFactory<LumberjackEvent, AsyncChannelDispatcher> handlerFactory = new LumberjackSocketChannelHandlerFactory<>();

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
    protected String getBatchKey(LumberjackEvent event) {
        return event.getSender();
    }

    protected void respond(final LumberjackEvent event, final LumberjackResponse lumberjackResponse) {
        final ChannelResponse response = new LumberjackChannelResponse(lumberjackEncoder, lumberjackResponse);

        final ChannelResponder responder = event.getResponder();
        responder.addResponse(response);
        try {
            responder.respond();
        } catch (IOException e) {
            getLogger().error("Error sending response for transaction {} due to {}",
                new Object[]{event.getSeqNumber(), e.getMessage()}, e);
        }
    }

    protected void postProcess(final ProcessContext context, final ProcessSession session, final List<LumberjackEvent> events) {
        // first commit the session so we guarantee we have all the events successfully
        // written to FlowFiles and transferred to the success relationship
        session.commit();
        // respond to each event to acknowledge successful receipt
        for (final LumberjackEvent event : events) {
            respond(event, LumberjackResponse.ok(event.getSeqNumber()));
        }
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("lumberjack").append("://").append(senderHost).append(":")
            .append(port).toString();
        return transitUri;
    }

    @Override
    protected Map<String, String> getAttributes(FlowFileEventBatch batch) {
        final List<LumberjackEvent> events = batch.getEvents();
        // the sender and command will be the same for all events based on the batch key
        final String sender = events.get(0).getSender();
        final int numAttributes = events.size() == 1 ? 5 : 4;
        final Map<String, String> attributes = new HashMap<>(numAttributes);
        attributes.put(LumberjackAttributes.SENDER.key(), sender);
        attributes.put(LumberjackAttributes.PORT.key(), String.valueOf(port));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
        // if there was only one event then we can pass on the transaction
        // NOTE: we could pass on all the transaction ids joined together
        if (events.size() == 1) {
            attributes.put(LumberjackAttributes.SEQNUMBER.key(), String.valueOf(events.get(0).getSeqNumber()));

            // Convert the serialized fields from JSON
            String serialFields = String.valueOf(events.get(0).getFields());
            Gson jsonObject = new Gson();

            Map<String, String> fields = jsonObject.fromJson(serialFields, Map.class);

            for (Map.Entry<String, String> entry : fields.entrySet()) {
                attributes.put(LumberjackAttributes.FIELDS.key().concat(".").concat(entry.getKey()), entry.getValue());
            }
        }
        return attributes;
    }

    public enum LumberjackAttributes implements FlowFileAttributeKey {
        SENDER("lumberjack.sender"),
        PORT("lumberjack.port"),
        SEQNUMBER("lumberjack.sequencenumber"),
        FIELDS("lumberjack.fields");

        private final String key;

        LumberjackAttributes(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }
}
