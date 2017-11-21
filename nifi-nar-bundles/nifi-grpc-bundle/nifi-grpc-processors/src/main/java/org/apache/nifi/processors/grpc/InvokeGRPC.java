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
package org.apache.nifi.processors.grpc;

import com.google.protobuf.ByteString;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;

@SupportsBatching
@Tags({"grpc", "rpc", "client"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends FlowFiles, optionally with content, to a configurable remote gRPC service endpoint. The remote gRPC service must abide by the service IDL defined in NiFi. " +
        " gRPC isn't intended to carry large payloads,  so this processor should be used only when FlowFile" +
        " sizes are on the order of megabytes. The default maximum message size is 4MB.")
@WritesAttributes({
        @WritesAttribute(attribute = "invokegrpc.response.code", description = "The response code that is returned (0 = ERROR, 1 = SUCCESS, 2 = RETRY)"),
        @WritesAttribute(attribute = "invokegrpc.response.body", description = "The response message that is returned"),
        @WritesAttribute(attribute = "invokegrpc.service.host", description = "The remote gRPC service hostname"),
        @WritesAttribute(attribute = "invokegrpc.service.port", description = "The remote gRPC service port"),
        @WritesAttribute(attribute = "invokegrpc.java.exception.class", description = "The Java exception class raised when the processor fails"),
        @WritesAttribute(attribute = "invokegrpc.java.exception.message", description = "The Java exception message raised when the processor fails"),
})
public class InvokeGRPC extends AbstractProcessor {
    public static final String RESPONSE_CODE = "invokegrpc.response.code";
    public static final String RESPONSE_BODY = "invokegrpc.response.body";
    public static final String SERVICE_HOST = "invokegrpc.service.host";
    public static final String SERVICE_PORT = "invokegrpc.service.port";
    public static final String EXCEPTION_CLASS = "invokegrpc.java.exception.class";
    public static final String EXCEPTION_MESSAGE = "invokegrpc.java.exception.message";

    // properties
    public static final PropertyDescriptor PROP_SERVICE_HOST = new PropertyDescriptor.Builder()
            .name("Remote gRPC service hostname")
            .description("Remote host which will be connected to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_SERVICE_PORT = new PropertyDescriptor.Builder()
            .name("Remote gRPC service port")
            .description("Remote port which will be connected to")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Message Size")
            .description("The maximum size of FlowFiles that this processor will allow to be received." +
                    " The default is 4MB. If FlowFiles exceed this size, you should consider using another transport mechanism" +
                    " as gRPC isn't designed for heavy payloads.")
            .defaultValue("4MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_USE_SECURE = new PropertyDescriptor.Builder()
            .name("Use SSL/TLS")
            .description("Whether or not to use SSL/TLS to send the contents of the gRPC messages.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor PROP_SEND_CONTENT = new PropertyDescriptor.Builder()
            .name("Send FlowFile Content")
            .description("Whether or not to include the FlowFile content in the FlowFileRequest to the gRPC service.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor PROP_PENALIZE_NO_RETRY = new PropertyDescriptor.Builder()
            .name("Penalize on \"No Retry\"")
            .description("Enabling this property will penalize FlowFiles that are routed to the \"No Retry\" relationship.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor PROP_OUTPUT_RESPONSE_REGARDLESS = new PropertyDescriptor.Builder()
            .name("Always Output Response")
            .description("Will force a response FlowFile to be generated and routed to the 'Response' relationship regardless of what the server status code received is "
                    + "or if the processor is configured to put the server response body in the request attribute. In the later configuration a request FlowFile with the "
                    + "response body in the attribute and a typical response FlowFile will be emitted to their respective relationships.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_SERVICE_HOST,
            PROP_SERVICE_PORT,
            PROP_MAX_MESSAGE_SIZE,
            PROP_USE_SECURE,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_SEND_CONTENT,
            PROP_OUTPUT_RESPONSE_REGARDLESS,
            PROP_PENALIZE_NO_RETRY
    ));

    // relationships
    public static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
            .name("Original")
            .description("The original FlowFile will be routed upon success. It will have new attributes detailing the "
                    + "success of the request.")
            .build();
    public static final Relationship REL_RESPONSE = new Relationship.Builder()
            .name("Response")
            .description("A Response FlowFile will be routed upon success. If the 'Output Response Regardless' property "
                    + "is true then the response will be sent to this relationship regardless of the status code received.")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("Retry")
            .description("The original FlowFile will be routed on any status code that can be retried. It will have new "
                    + "attributes detailing the request.")
            .build();
    public static final Relationship REL_NO_RETRY = new Relationship.Builder()
            .name("No Retry")
            .description("The original FlowFile will be routed on any status code that should NOT be retried.  "
                    + "It will have new attributes detailing the request.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The original FlowFile will be routed on any type of connection failure, timeout or general exception. "
                    + "It will have new attributes detailing the request.")
            .build();
    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS_REQ,
            REL_NO_RETRY,
            REL_RESPONSE,
            REL_RETRY,
            REL_FAILURE
    )));

    private static final String USER_AGENT_PREFIX = "NiFi_invokeGRPC";
    // NOTE: you may need to add the sources generated after running `maven clean compile` to your IDE
    // configured source directories. Otherwise, the classes generated when the proto is compiled won't
    // be accessible from here. For IntelliJ, open this module's settings and mark the following as source directories:
    //
    // * target/generated-sources/protobuf/grpc-java
    // * target/generated-sources/protobuf/java
    private final AtomicReference<FlowFileServiceGrpc.FlowFileServiceBlockingStub> blockingStubReference = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> channelReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Whenever this processor is triggered, we need to construct a client in order to communicate
     * with the configured gRPC service.
     *
     * @param context the processor context
     */
    @OnScheduled
    public void initializeClient(final ProcessContext context) throws Exception {

        channelReference.set(null);
        blockingStubReference.set(null);
        final ComponentLog logger = getLogger();

        final String host = context.getProperty(PROP_SERVICE_HOST).getValue();
        final int port = context.getProperty(PROP_SERVICE_PORT).asInteger();
        final Integer maxMessageSize = context.getProperty(PROP_MAX_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        String userAgent = USER_AGENT_PREFIX;
        try {
            userAgent += "_" + InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            logger.warn("Unable to determine local hostname. Defaulting gRPC user agent to {}.", new Object[]{USER_AGENT_PREFIX}, e);
        }

        final NettyChannelBuilder nettyChannelBuilder = NettyChannelBuilder.forAddress(host, port)
                // supports both gzip and plaintext, but will compress by default.
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .maxInboundMessageSize(maxMessageSize)
                .userAgent(userAgent);

        // configure whether or not we're using secure comms
        final boolean useSecure = context.getProperty(PROP_USE_SECURE).asBoolean();
        final SSLContextService sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslContextService == null ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);

        if (useSecure && sslContext != null) {
            SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
            if(StringUtils.isNotBlank(sslContextService.getKeyStoreFile())) {
                final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm(),
                        sslContext.getProvider());
                final KeyStore keyStore = KeyStore.getInstance(sslContextService.getKeyStoreType());
                try (final InputStream is = new FileInputStream(sslContextService.getKeyStoreFile())) {
                    keyStore.load(is, sslContextService.getKeyStorePassword().toCharArray());
                }
                keyManagerFactory.init(keyStore, sslContextService.getKeyStorePassword().toCharArray());
                sslContextBuilder.keyManager(keyManagerFactory);
            }

            if(StringUtils.isNotBlank(sslContextService.getTrustStoreFile())) {
                final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(),
                        sslContext.getProvider());
                final KeyStore trustStore = KeyStore.getInstance(sslContextService.getTrustStoreType());
                try (final InputStream is = new FileInputStream(sslContextService.getTrustStoreFile())) {
                    trustStore.load(is, sslContextService.getTrustStorePassword().toCharArray());
                }
                trustManagerFactory.init(trustStore);
                sslContextBuilder.trustManager(trustManagerFactory);
            }
            nettyChannelBuilder.sslContext(sslContextBuilder.build());

        } else {
            nettyChannelBuilder.usePlaintext(true);
        }

        final ManagedChannel channel = nettyChannelBuilder.build();
        final FlowFileServiceGrpc.FlowFileServiceBlockingStub blockingStub = FlowFileServiceGrpc.newBlockingStub(channel);
        channelReference.set(channel);
        blockingStubReference.set(blockingStub);
    }

    /**
     * Perform cleanup prior to JVM shutdown
     *
     * @param context the processor context
     * @throws InterruptedException if there's an issue cleaning up
     */
    @OnShutdown
    public void shutdown(final ProcessContext context) throws InterruptedException {
        // close the channel
        final ManagedChannel channel = channelReference.get();
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final FlowFileServiceGrpc.FlowFileServiceBlockingStub blockingStub = blockingStubReference.get();
        final String host = context.getProperty(PROP_SERVICE_HOST).getValue();
        final String port = context.getProperty(PROP_SERVICE_PORT).getValue();
        fileToProcess = session.putAttribute(fileToProcess, SERVICE_HOST, host);
        fileToProcess = session.putAttribute(fileToProcess, SERVICE_PORT, port);
        FlowFile responseFlowFile = null;
        try {
            final FlowFileRequest.Builder requestBuilder = FlowFileRequest.newBuilder()
                    .setId(fileToProcess.getId())
                    .putAllAttributes(fileToProcess.getAttributes());

            // if the processor is configured to send the content, turn the content into bytes
            // and add it to the request.
            final boolean sendContent = context.getProperty(PROP_SEND_CONTENT).asBoolean();
            if (sendContent) {
                try (final InputStream contents = session.read(fileToProcess)) {
                    requestBuilder.setContent(ByteString.readFrom(contents));
                }
                // emit provenance event
                session.getProvenanceReporter().send(fileToProcess, getRemote(host, port), true);
            }
            final FlowFileRequest flowFileRequest = requestBuilder.build();
            logRequest(logger, host, port, flowFileRequest);

            final FlowFileReply flowFileReply = blockingStub.send(flowFileRequest);
            logReply(logger, host, port, flowFileReply);

            final FlowFileReply.ResponseCode responseCode = flowFileReply.getResponseCode();
            final String body = flowFileReply.getBody();

            fileToProcess = session.putAttribute(fileToProcess, RESPONSE_CODE, String.valueOf(responseCode));
            fileToProcess = session.putAttribute(fileToProcess, RESPONSE_BODY, body);

            responseFlowFile = session.create(fileToProcess);
            route(fileToProcess, responseFlowFile, session, context, responseCode);

        } catch (final Exception e) {
            // penalize or yield
            if (fileToProcess != null) {
                logger.error("Routing to {} due to exception: {}", new Object[]{REL_FAILURE.getName(), e}, e);
                fileToProcess = session.penalize(fileToProcess);
                fileToProcess = session.putAttribute(fileToProcess, EXCEPTION_CLASS, e.getClass().getName());
                fileToProcess = session.putAttribute(fileToProcess, EXCEPTION_MESSAGE, e.getMessage());
                // transfer original to failure
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                logger.error("Yielding processor due to exception encountered as a source processor: {}", e);
                context.yield();
            }

            // cleanup
            try {
                if (responseFlowFile != null) {
                    session.remove(responseFlowFile);
                }
            } catch (final Exception e1) {
                logger.error("Could not cleanup response flowfile due to exception: {}", new Object[]{e1}, e1);
            }
        }
    }

    /**
     * Route the {@link FlowFile} request and response appropriately, depending on the gRPC service
     * response code.
     *
     * @param request      the flowfile request
     * @param response     the flowfile response
     * @param session      the processor session
     * @param context      the processor context
     * @param responseCode the gRPC service response code
     */
    private void route(FlowFile request, FlowFile response, final ProcessSession session, final ProcessContext context, final FlowFileReply.ResponseCode responseCode) {
        boolean responseSent = false;
        if (context.getProperty(PROP_OUTPUT_RESPONSE_REGARDLESS).asBoolean()) {
            session.transfer(response, REL_RESPONSE);
            responseSent = true;
        }

        switch (responseCode) {
            // if the rpc failed, transfer flowfile to no retry relationship and penalize flowfile


            // if the rpc succeeded, transfer the request and response flowfiles
            case SUCCESS:
                session.transfer(request, REL_SUCCESS_REQ);
                if (!responseSent) {
                    session.transfer(response, REL_RESPONSE);
                }
                break;

            // if the gRPC service responded requesting a retry, then penalize the request and
            // transfer it to the retry relationship. The flowfile contains attributes detailing this
            // rpc request.
            case RETRY:
                request = session.penalize(request);
                session.transfer(request, REL_RETRY);
                // if we haven't sent the response by this point, clean it up.
                if (!responseSent) {
                    session.remove(response);
                }
                break;

            case ERROR:
            case UNRECOGNIZED: // unrecognized response code returned from gRPC service
            default:
                final boolean penalize = context.getProperty(PROP_PENALIZE_NO_RETRY).asBoolean();
                if (penalize) {
                    request = session.penalize(request);
                }
                session.transfer(request, REL_NO_RETRY);
                // if we haven't sent the response by this point, clean it up.
                if (!responseSent) {
                    session.remove(response);
                }
                break;
        }
    }

    private String getRemote(final String host, final String port) {
        return host + ":" + port;
    }

    private void logRequest(final ComponentLog logger, final String host, final String port, final FlowFileRequest flowFileRequest) {
        logger.debug("\nRequest to remote service:\n\t{}\n{}",
                new Object[]{getRemote(host, port), flowFileRequest.toString()});
    }

    private void logReply(final ComponentLog logger, final String host, final String port, final FlowFileReply flowFileReply) {
        logger.debug("\nResponse from remote service:\n\t{}\n{}",
                new Object[]{getRemote(host, port), flowFileReply.toString()});
    }
}
