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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.EventBatcher;
import org.apache.nifi.processor.util.listen.FlowFileEventBatch;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processors.beats.protocol.BatchMessage;
import org.apache.nifi.processors.beats.server.BeatsMessageServerFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"beats", "logstash", "elasticsearch", "log"})
@CapabilityDescription("Receive messages encoded using the Elasticsearch Beats protocol and write decoded JSON")
@WritesAttributes({
    @WritesAttribute(attribute = "beats.sender", description = "Internet Protocol address of the message sender"),
    @WritesAttribute(attribute = "beats.port", description = "TCP port on which the Processor received messages"),
    @WritesAttribute(attribute = "beats.sequencenumber", description = "The sequence number of the message included for batches containing single messages"),
    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the content which is application/json")
})
public class ListenBeats extends AbstractProcessor {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL_CONTEXT_SERVICE")
        .displayName("SSL Context Service")
        .description("SSL Context Service is required to enable TLS for socket connections")
        .required(false)
        .identifiesControllerService(RestrictedSSLContextService.class)
        .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
        .name("Client Auth")
        .displayName("Client Authentication")
        .description("Client authentication policy when TLS is enabled")
        .required(false)
        .dependsOn(SSL_CONTEXT_SERVICE)
        .allowableValues(ClientAuth.values())
        .defaultValue(ClientAuth.REQUIRED.name())
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            ListenerProperties.NETWORK_INTF_NAME,
            ListenerProperties.PORT,
            ListenerProperties.RECV_BUFFER_SIZE,
            ListenerProperties.MAX_MESSAGE_QUEUE_SIZE,
            ListenerProperties.MAX_SOCKET_BUFFER_SIZE,
            ListenerProperties.CHARSET,
            ListenerProperties.MAX_BATCH_SIZE,
            ListenerProperties.MESSAGE_DELIMITER,
            ListenerProperties.WORKER_THREADS,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    protected volatile BlockingQueue<BatchMessage> events;
    protected volatile BlockingQueue<BatchMessage> errorEvents;
    protected volatile EventServer eventServer;
    protected volatile byte[] messageDemarcatorBytes;
    protected volatile EventBatcher<BatchMessage> eventBatcher;

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int workerThreads = context.getProperty(ListenerProperties.WORKER_THREADS).asInteger();
        final int socketBufferSize = context.getProperty(ListenerProperties.MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String networkInterface = context.getProperty(ListenerProperties.NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final InetAddress address = NetworkUtils.getInterfaceAddress(networkInterface);
        final Charset charset = Charset.forName(context.getProperty(ListenerProperties.CHARSET).getValue());
        final int port = context.getProperty(ListenerProperties.PORT).evaluateAttributeExpressions().asInteger();
        events = new LinkedBlockingQueue<>(context.getProperty(ListenerProperties.MAX_MESSAGE_QUEUE_SIZE).asInteger());
        errorEvents = new LinkedBlockingQueue<>();
        final String msgDemarcator = getMessageDemarcator(context);
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);

        final NettyEventServerFactory eventFactory = new BeatsMessageServerFactory(getLogger(), address, port, events);

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            ClientAuth clientAuth = ClientAuth.valueOf(clientAuthValue);
            SSLContext sslContext = sslContextService.createContext();
            eventFactory.setSslContext(sslContext);
            eventFactory.setClientAuth(clientAuth);
        }

        eventFactory.setSocketReceiveBuffer(socketBufferSize);
        eventFactory.setWorkerThreads(workerThreads);
        eventFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));
        eventFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        eventFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());

        try {
            eventServer = eventFactory.getEventServer();
        } catch (final EventException e) {
            getLogger().error("Failed to bind to [{}:{}]", address, port, e);
        }
    }

    public int getListeningPort() {
        return eventServer.getListeningPort();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        EventBatcher<BatchMessage> eventBatcher = getEventBatcher();

        final int batchSize = context.getProperty(ListenerProperties.MAX_BATCH_SIZE).asInteger();
        Map<String, FlowFileEventBatch<BatchMessage>> batches = eventBatcher.getBatches(session, batchSize, messageDemarcatorBytes);
        processEvents(session, batches);
    }

    @OnStopped
    public void shutdown() {
        if (eventServer == null) {
            getLogger().warn("Event Server not configured");
        } else {
            eventServer.shutdown();
        }
        eventBatcher = null;
    }

    private void processEvents(final ProcessSession session, final Map<String, FlowFileEventBatch<BatchMessage>> batches) {
        for (final Map.Entry<String, FlowFileEventBatch<BatchMessage>> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<BatchMessage> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.size() == 0) {
                session.remove(flowFile);
                continue;
            }

            final Map<String,String> attributes = getAttributes(entry.getValue());
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, REL_SUCCESS);

            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    private String getTransitUri(final FlowFileEventBatch<BatchMessage> batch) {
        final List<BatchMessage> events = batch.getEvents();
        final String sender = events.get(0).getSender();
        return String.format("beats://%s:%d", sender, getListeningPort());
    }

    private Map<String, String> getAttributes(final FlowFileEventBatch<BatchMessage> batch) {
        final List<BatchMessage> events = batch.getEvents();

        final String sender = events.get(0).getSender();
        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(BeatsAttributes.SENDER.key(), sender);
        attributes.put(BeatsAttributes.PORT.key(), String.valueOf(getListeningPort()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        if (events.size() == 1) {
            attributes.put(BeatsAttributes.SEQUENCE_NUMBER.key(), String.valueOf(events.get(0).getSequenceNumber()));
        }
        return attributes;
    }

    private String getMessageDemarcator(final ProcessContext context) {
        return context.getProperty(ListenerProperties.MESSAGE_DELIMITER)
                .getValue()
                .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
    }

    private EventBatcher<BatchMessage> getEventBatcher() {
        if (eventBatcher == null) {
            eventBatcher = new EventBatcher<BatchMessage>(getLogger(), events, errorEvents) {
                @Override
                protected String getBatchKey(final BatchMessage event) {
                    return event.getSender();
                }
            };
        }
        return eventBatcher;
    }

    private enum BeatsAttributes implements FlowFileAttributeKey {
        SENDER("beats.sender"),
        PORT("beats.port"),
        SEQUENCE_NUMBER("beats.sequencenumber");

        private final String key;

        BeatsAttributes(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }
    }
}
