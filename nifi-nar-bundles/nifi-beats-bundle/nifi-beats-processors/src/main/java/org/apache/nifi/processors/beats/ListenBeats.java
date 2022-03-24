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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
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
import org.apache.nifi.processor.util.listen.EventBatcher;
import org.apache.nifi.processor.util.listen.FlowFileEventBatch;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processors.beats.netty.BeatsMessage;
import org.apache.nifi.processors.beats.netty.BeatsMessageServerFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
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

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "beats", "tcp", "logs"})
@CapabilityDescription("Listens for messages sent by libbeat compatible clients (e.g. filebeats, metricbeats, etc) using Libbeat's 'output.logstash', writing its JSON formatted payload " +
        "to the content of a FlowFile." +
        "This processor replaces the now deprecated/removed ListenLumberjack")
@WritesAttributes({
    @WritesAttribute(attribute = "beats.sender", description = "The sending host of the messages."),
    @WritesAttribute(attribute = "beats.port", description = "The sending port the messages were received over."),
    @WritesAttribute(attribute = "beats.sequencenumber", description = "The sequence number of the message. Only included if <Batch Size> is 1."),
    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the content which is application/json")
})
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.ParseSyslog"})
public class ListenBeats extends AbstractProcessor {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL_CONTEXT_SERVICE")
        .displayName("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
            "messages will be received over a secure connection.")
        // Nearly all Lumberjack v1 implementations require TLS to work. v2 implementations (i.e. beats) have TLS as optional
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
    protected volatile BlockingQueue<BeatsMessage> events;
    protected volatile BlockingQueue<BeatsMessage> errorEvents;
    protected volatile EventServer eventServer;
    protected volatile byte[] messageDemarcatorBytes;
    protected volatile EventBatcher<BeatsMessage> eventBatcher;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ListenerProperties.NETWORK_INTF_NAME);
        descriptors.add(ListenerProperties.PORT);
        descriptors.add(ListenerProperties.RECV_BUFFER_SIZE);
        descriptors.add(ListenerProperties.MAX_MESSAGE_QUEUE_SIZE);
        // Deprecated
        descriptors.add(ListenerProperties.MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(ListenerProperties.CHARSET);
        descriptors.add(ListenerProperties.MAX_BATCH_SIZE);
        descriptors.add(ListenerProperties.MESSAGE_DELIMITER);
        descriptors.add(ListenerProperties.WORKER_THREADS);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && !sslContextService.isTrustStoreConfigured()) {
            results.add(new ValidationResult.Builder()
                .explanation("SSL Context Service requires a truststore for the Beats forwarder client to work correctly")
                .valid(false).subject(SSL_CONTEXT_SERVICE.getName()).build());
        }

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int workerThreads = context.getProperty(ListenerProperties.WORKER_THREADS).asInteger();
        final int bufferSize = context.getProperty(ListenerProperties.RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String networkInterface = context.getProperty(ListenerProperties.NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final InetAddress address = NetworkUtils.getInterfaceAddress(networkInterface);
        final Charset charset = Charset.forName(context.getProperty(ListenerProperties.CHARSET).getValue());
        port = context.getProperty(ListenerProperties.PORT).evaluateAttributeExpressions().asInteger();
        events = new LinkedBlockingQueue<>(context.getProperty(ListenerProperties.MAX_MESSAGE_QUEUE_SIZE).asInteger());
        errorEvents = new LinkedBlockingQueue<>();
        final String msgDemarcator = getMessageDemarcator(context);
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);

        final NettyEventServerFactory eventFactory = new BeatsMessageServerFactory(getLogger(), address, port, charset, events);

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            ClientAuth clientAuth = ClientAuth.valueOf(clientAuthValue);
            SSLContext sslContext = sslContextService.createContext();
            eventFactory.setSslContext(sslContext);
            eventFactory.setClientAuth(clientAuth);
        }

        eventFactory.setSocketReceiveBuffer(bufferSize);
        eventFactory.setWorkerThreads(workerThreads);
        eventFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));

        try {
            eventServer = eventFactory.getEventServer();
        } catch (EventException e) {
            getLogger().error("Failed to bind to [{}:{}]", address, port, e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        EventBatcher<BeatsMessage> eventBatcher = getEventBatcher();

        final int batchSize = context.getProperty(ListenerProperties.MAX_BATCH_SIZE).asInteger();
        Map<String, FlowFileEventBatch<BeatsMessage>> batches = eventBatcher.getBatches(session, batchSize, messageDemarcatorBytes);
        processEvents(session, batches);
    }

    @OnStopped
    public void stopped() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
        eventBatcher = null;
    }

    private void processEvents(final ProcessSession session, final Map<String, FlowFileEventBatch<BeatsMessage>> batches) {
        for (Map.Entry<String, FlowFileEventBatch<BeatsMessage>> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<BeatsMessage> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.size() == 0) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", entry.getKey());
                continue;
            }

            final Map<String,String> attributes = getAttributes(entry.getValue());
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            // the sender and command will be the same for all events based on the batch key
            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);

        }
        session.commitAsync();
    }

    protected String getTransitUri(FlowFileEventBatch<BeatsMessage> batch) {
        final List<BeatsMessage> events = batch.getEvents();
        final String sender = events.get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        return String.format("beats://%s:%d", senderHost, port);
    }

    protected Map<String, String> getAttributes(FlowFileEventBatch<BeatsMessage> batch) {
        final List<BeatsMessage> events = batch.getEvents();
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

    private String getMessageDemarcator(final ProcessContext context) {
        return context.getProperty(ListenerProperties.MESSAGE_DELIMITER)
                .getValue()
                .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
    }

    private EventBatcher<BeatsMessage> getEventBatcher() {
        if (eventBatcher == null) {
            eventBatcher = new EventBatcher<BeatsMessage>(getLogger(), events, errorEvents) {
                @Override
                protected String getBatchKey(BeatsMessage event) {
                    return event.getSender();
                }
            };
        }
        return eventBatcher;
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
