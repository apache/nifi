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
package org.apache.nifi.processors.opentelemetry;

import com.google.protobuf.Message;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.EventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryAttributeName;
import org.apache.nifi.processors.opentelemetry.io.RequestCallback;
import org.apache.nifi.processors.opentelemetry.io.RequestCallbackProvider;
import org.apache.nifi.processors.opentelemetry.server.HttpServerFactory;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DefaultSchedule(period = "25 ms")
@Tags({"OpenTelemetry", "OTel", "OTLP", "telemetry", "metrics", "traces", "logs"})
@CapabilityDescription(
        "Collect OpenTelemetry messages over HTTP or gRPC. " +
        "Supports standard Export Service Request messages for logs, metrics, and traces. " +
        "Implements OpenTelemetry OTLP Specification 1.0.0 with OTLP/gRPC and OTLP/HTTP. " +
        "Provides protocol detection using the HTTP Content-Type header."
)
@WritesAttributes({
        @WritesAttribute(attribute = TelemetryAttributeName.MIME_TYPE, description = "Content-Type set to application/json"),
        @WritesAttribute(attribute = TelemetryAttributeName.RESOURCE_TYPE, description = "OpenTelemetry Resource Type: LOGS, METRICS, or TRACES"),
        @WritesAttribute(attribute = TelemetryAttributeName.RESOURCE_COUNT, description = "Count of resource elements included in messages"),
})
public class ListenOTLP extends AbstractProcessor {

    static final PropertyDescriptor ADDRESS = new PropertyDescriptor.Builder()
            .name("Address")
            .displayName("Address")
            .description("Internet Protocol Address on which to listen for OTLP Export Service Requests. The default value enables listening on all addresses.")
            .required(true)
            .defaultValue("0.0.0.0")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .displayName("Port")
            .description("TCP port number on which to listen for OTLP Export Service Requests over HTTP and gRPC")
            .required(true)
            .defaultValue("4317")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("SSL Context Service enables TLS communication for HTTPS")
            .required(true)
            .identifiesControllerService(SSLContextProvider.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor CLIENT_AUTHENTICATION = new PropertyDescriptor.Builder()
            .name("Client Authentication")
            .displayName("Client Authentication")
            .description("Client authentication policy for TLS communication with HTTPS")
            .required(true)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.WANT.name())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor WORKER_THREADS = new PropertyDescriptor.Builder()
            .name("Worker Threads")
            .displayName("Worker Threads")
            .description("Number of threads responsible for decoding and queuing incoming OTLP Export Service Requests")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor QUEUE_CAPACITY = new PropertyDescriptor.Builder()
            .name("Queue Capacity")
            .displayName("Queue Capacity")
            .description("Maximum number of OTLP request resource elements that can be received and queued")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Maximum number of OTLP request resource elements included in each FlowFile produced")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Export Service Requests containing OTLP Telemetry")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(SUCCESS);

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            ADDRESS,
            PORT,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTHENTICATION,
            WORKER_THREADS,
            QUEUE_CAPACITY,
            BATCH_SIZE
    );

    private static final String TRANSIT_URI_FORMAT = "https://%s:%d";

    private Iterator<RequestCallback> requestCallbackProvider;

    private EventServer server;

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws UnknownHostException {
        final EventServerFactory eventServerFactory = createEventServerFactory(context);
        server = eventServerFactory.getEventServer();
    }

    @OnStopped
    public void onStopped() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        while (requestCallbackProvider.hasNext()) {
            final RequestCallback requestCallback = requestCallbackProvider.next();
            processRequestCallback(session, requestCallback);
        }
    }

    int getPort() {
        return server.getListeningPort();
    }

    private void processRequestCallback(final ProcessSession session, final RequestCallback requestCallback) {
        final String transitUri = requestCallback.getTransitUri();
        FlowFile flowFile = session.create();
        try {
            flowFile = session.write(flowFile, requestCallback);
            flowFile = session.putAllAttributes(flowFile, requestCallback.getAttributes());
            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, SUCCESS);
        } catch (final Exception e) {
            getLogger().warn("Request Transit URI [{}] processing failed {}", transitUri, flowFile, e);
            session.remove(flowFile);
        }
    }

    private EventServerFactory createEventServerFactory(final ProcessContext context) throws UnknownHostException {
        final String address = context.getProperty(ADDRESS).getValue();
        final InetAddress serverAddress = InetAddress.getByName(address);
        final int port = context.getProperty(PORT).asInteger();
        final URI transitBaseUri = URI.create(String.format(TRANSIT_URI_FORMAT, serverAddress.getCanonicalHostName(), port));

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final int queueCapacity = context.getProperty(QUEUE_CAPACITY).asInteger();
        final BlockingQueue<Message> messages = new LinkedBlockingQueue<>(queueCapacity);
        requestCallbackProvider = new RequestCallbackProvider(transitBaseUri, batchSize, messages);

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final SSLContext sslContext = sslContextProvider.createContext();

        final NettyEventServerFactory eventServerFactory = new HttpServerFactory(getLogger(), messages, serverAddress, port, sslContext);
        eventServerFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));
        final int workerThreads = context.getProperty(WORKER_THREADS).asInteger();
        eventServerFactory.setWorkerThreads(workerThreads);
        final ClientAuth clientAuth = ClientAuth.valueOf(context.getProperty(CLIENT_AUTHENTICATION).getValue());
        eventServerFactory.setClientAuth(clientAuth);

        return eventServerFactory;
    }
}
