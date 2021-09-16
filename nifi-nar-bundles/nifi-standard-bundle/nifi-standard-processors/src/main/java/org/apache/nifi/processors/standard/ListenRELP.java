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
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventSenderFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.AbstractListenEventProcessor;
import org.apache.nifi.processor.util.listen.EventBatcher;
import org.apache.nifi.processor.util.listen.FlowFileNettyEventBatch;
import org.apache.nifi.processors.standard.relp.event.RELPNettyEvent;
import org.apache.nifi.processors.standard.relp.handler.RELPNettyEventSenderFactory;
import org.apache.nifi.processors.standard.relp.handler.RELPNettyEventServerFactory;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

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
public class ListenRELP extends AbstractProcessor {

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
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    protected List<PropertyDescriptor> descriptors;
    protected Set<Relationship> relationships;
    protected volatile int port;
    protected volatile Charset charset;
    protected volatile BlockingQueue<RELPNettyEvent> events;
    protected volatile BlockingQueue<RELPNettyEvent> errorEvents;
    protected volatile String hostname;
    protected EventServer eventServer;
    protected EventSender eventSender;
    protected volatile byte[] messageDemarcatorBytes;

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        final String nicIPAddressStr = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final InetAddress nicIPAddress = AbstractListenEventProcessor.getNICIPAddress(nicIPAddressStr);

        hostname = nicIPAddress.getHostName();
        charset = Charset.forName(context.getProperty(AbstractListenEventProcessor.CHARSET).getValue());
        port = context.getProperty(AbstractListenEventProcessor.PORT).evaluateAttributeExpressions().asInteger();
        events = new LinkedBlockingQueue<>(context.getProperty(AbstractListenEventProcessor.MAX_MESSAGE_QUEUE_SIZE).asInteger());
        errorEvents = new LinkedBlockingQueue<>();

        final String msgDemarcator = context.getProperty(AbstractListenEventBatchingProcessor.MESSAGE_DELIMITER)
                                            .getValue()
                                            .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);

        initializeRELPServer(context);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(AbstractListenEventProcessor.PORT);
        descriptors.add(AbstractListenEventProcessor.RECV_BUFFER_SIZE);
        descriptors.add(AbstractListenEventProcessor.MAX_MESSAGE_QUEUE_SIZE);
        descriptors.add(AbstractListenEventProcessor.MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(AbstractListenEventProcessor.CHARSET);
        descriptors.add(AbstractListenEventProcessor.MAX_CONNECTIONS);
        descriptors.add(AbstractListenEventBatchingProcessor.MAX_BATCH_SIZE);
        descriptors.add(AbstractListenEventBatchingProcessor.MESSAGE_DELIMITER);
        descriptors.add(NETWORK_INTF_NAME);
        descriptors.add(CLIENT_AUTH);
        descriptors.add(SSL_CONTEXT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
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

    private void initializeRELPServer(final ProcessContext context) throws IOException {
        final NettyEventServerFactory eventFactory = getNettyEventServerFactory();
        final NettyEventSenderFactory eventSenderFactory = getNettyEventSenderFactory();

        final int maxConnections = context.getProperty(AbstractListenEventProcessor.MAX_CONNECTIONS).asInteger();
        final int bufferSize = context.getProperty(AbstractListenEventProcessor.RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        eventFactory.setSocketReceiveBuffer(bufferSize);
        eventSenderFactory.setSocketSendBufferSize(bufferSize);
        eventSenderFactory.setMaxConnections(maxConnections);

        // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
        SSLContext sslContext;
        ClientAuth clientAuth;

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createContext();
            clientAuth = ClientAuth.valueOf(clientAuthValue);

            eventFactory.setSslContext(sslContext);
            eventFactory.setClientAuth(clientAuth);
            eventSenderFactory.setSslContext(sslContext);
        }

        eventServer = eventFactory.getEventServer();
        eventSender = eventSenderFactory.getEventSender();
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected Map<String, String> getAttributes(FlowFileNettyEventBatch batch) {
        final List<RELPNettyEvent> events = batch.getEvents();

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

    protected String getTransitUri(FlowFileNettyEventBatch batch) {
        final List<RELPNettyEvent> events = batch.getEvents();
        final String sender = events.get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("relp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        EventBatcher eventBatcher = new EventBatcher<RELPNettyEvent>(getLogger(), events, errorEvents) {
            @Override
            protected String getBatchKey(RELPNettyEvent event) {
                return getRELPBatchKey(event);
            }
        };

        final int batchSize = context.getProperty(AbstractListenEventBatchingProcessor.MAX_BATCH_SIZE).asInteger();
        Map<String, FlowFileNettyEventBatch> batches = eventBatcher.getBatches(session, batchSize, messageDemarcatorBytes);

        final List<RELPNettyEvent> eventsAwaitingResponse = new ArrayList<>();
        getEventsAwaitingResponse(session, batches, eventsAwaitingResponse);
        respondToEvents(session, eventsAwaitingResponse);
    }


    private void getEventsAwaitingResponse(final ProcessSession session, final Map<String, FlowFileNettyEventBatch> batches, final List<RELPNettyEvent> allEvents) {
        for (Map.Entry<String, FlowFileNettyEventBatch> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<RELPNettyEvent> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.size() == 0) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", new Object[] {entry.getKey()});
                continue;
            }

            final Map<String,String> attributes = getAttributes(entry.getValue());
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", new Object[] {flowFile});
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            // the sender and command will be the same for all events based on the batch key
            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);

            allEvents.addAll(events);
        }
    }

    /**
     * Respond to RELP events awaiting response.
     * @param session
     * @param events
     */
    protected void respondToEvents(final ProcessSession session, final List<RELPNettyEvent> events) {
        // first commit the session so we guarantee we have all the events successfully
        // written to FlowFiles and transferred to the success relationship
        session.commitAsync(() -> {
            // respond to each event to acknowledge successful receipt
            for (final RELPNettyEvent event : events) {
                eventSender.sendEvent(RELPResponse.ok(event.getTxnr()));
            }
        });
    }

    private String getRELPBatchKey(RELPNettyEvent event) {
        return event.getSender() + "_" + event.getCommand();
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

    private NettyEventServerFactory getNettyEventServerFactory() {
        return new RELPNettyEventServerFactory(getLogger(), hostname, port, charset, events);
    }

    private NettyEventSenderFactory getNettyEventSenderFactory() {
        return new RELPNettyEventSenderFactory(getLogger(), hostname, port, TransportProtocol.TCP, charset);
    }
}
