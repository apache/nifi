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

import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Starts a gRPC server and listens on the given port to transform the incoming messages into FlowFiles." +
        " The message format is defined by the standard gRPC protobuf IDL provided by NiFi. gRPC isn't intended to carry large payloads," +
        " so this processor should be used only when FlowFile sizes are on the order of megabytes. The default maximum message size is 4MB.")
@Tags({"ingest", "grpc", "rpc", "listen"})
@WritesAttributes({
        @WritesAttribute(attribute = "listengrpc.remote.user.dn", description = "The DN of the user who sent the FlowFile to this NiFi"),
        @WritesAttribute(attribute = "listengrpc.remote.host", description = "The IP of the client who sent the FlowFile to this NiFi")
})
public class ListenGRPC extends AbstractSessionFactoryProcessor {
    public static final String REMOTE_USER_DN = "listengrpc.remote.user.dn";
    public static final String REMOTE_HOST = "listengrpc.remote.host";

    // properties
    public static final PropertyDescriptor PROP_SERVICE_PORT = new PropertyDescriptor.Builder()
            .name("Local gRPC service port")
            .displayName("Local gRPC Service Port")
            .description("The local port that the gRPC service will listen on.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_USE_SECURE = new PropertyDescriptor.Builder()
            .name("Use TLS")
            .displayName("Use TLS")
            .description("Whether or not to use TLS to send the contents of the gRPC messages.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS (https) connections.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();
    public static final PropertyDescriptor PROP_FLOW_CONTROL_WINDOW = new PropertyDescriptor.Builder()
            .name("Flow Control Window")
            .displayName("Flow Control Window")
            .description("The initial HTTP/2 flow control window for both new streams and overall connection." +
                    " Flow-control schemes ensure that streams on the same connection do not destructively interfere with each other." +
                    " The default is 1MB.")
            .defaultValue("1MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Message Size")
            .displayName("Maximum Message Size")
            .description("The maximum size of FlowFiles that this processor will allow to be received." +
                    " The default is 4MB. If FlowFiles exceed this size, you should consider using another transport mechanism" +
                    " as gRPC isn't designed for heavy payloads.")
            .defaultValue("4MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_AUTHORIZED_DN_PATTERN = new PropertyDescriptor.Builder()
            .name("Authorized DN Pattern")
            .displayName("Authorized DN Pattern")
            .description("A Regular Expression to apply against the Distinguished Name of incoming connections. If the Pattern does not match the DN, the connection will be refused.")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_SERVICE_PORT,
            PROP_USE_SECURE,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_FLOW_CONTROL_WINDOW,
            PROP_AUTHORIZED_DN_PATTERN,
            PROP_MAX_MESSAGE_SIZE
    ));

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The FlowFile was received successfully.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(Sets.newHashSet(Arrays.asList(
            REL_SUCCESS
    )));
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private volatile Server server = null;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }


    @OnScheduled
    public void startServer(final ProcessContext context) throws NoSuchAlgorithmException, IOException, KeyStoreException, CertificateException, UnrecoverableKeyException {
        final ComponentLog logger = getLogger();
        // gather configured properties
        final Integer port = context.getProperty(PROP_SERVICE_PORT).asInteger();
        final Boolean useSecure = context.getProperty(PROP_USE_SECURE).asBoolean();
        final Integer flowControlWindow = context.getProperty(PROP_FLOW_CONTROL_WINDOW).asDataSize(DataUnit.B).intValue();
        final Integer maxMessageSize = context.getProperty(PROP_MAX_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        final SSLContextService sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslContextService == null ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
        final Pattern authorizedDnPattern = Pattern.compile(context.getProperty(PROP_AUTHORIZED_DN_PATTERN).getValue());
        final FlowFileIngestServiceInterceptor callInterceptor = new FlowFileIngestServiceInterceptor(getLogger());
        callInterceptor.enforceDNPattern(authorizedDnPattern);

        final FlowFileIngestService flowFileIngestService = new FlowFileIngestService(getLogger(),
                sessionFactoryReference,
                context);
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(flowFileIngestService, callInterceptor))
                // default (de)compressor registries handle both plaintext and gzip compressed messages
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .flowControlWindow(flowControlWindow)
                .maxMessageSize(maxMessageSize);

        if (useSecure && sslContext != null) {
            // construct key manager
            if (StringUtils.isBlank(sslContextService.getKeyStoreFile())) {
                throw new IllegalStateException("SSL is enabled, but no keystore has been configured. You must configure a keystore.");
            }

            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm(),
                    sslContext.getProvider());
            final KeyStore keyStore = KeyStore.getInstance(sslContextService.getKeyStoreType());
            try (final InputStream is = new FileInputStream(sslContextService.getKeyStoreFile())) {
                keyStore.load(is, sslContextService.getKeyStorePassword().toCharArray());
            }
            keyManagerFactory.init(keyStore, sslContextService.getKeyStorePassword().toCharArray());

            SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory);

            // if the trust store is configured, then client auth is required.
            if (StringUtils.isNotBlank(sslContextService.getTrustStoreFile())) {
                final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(),
                        sslContext.getProvider());
                final KeyStore trustStore = KeyStore.getInstance(sslContextService.getTrustStoreType());
                try (final InputStream is = new FileInputStream(sslContextService.getTrustStoreFile())) {
                    trustStore.load(is, sslContextService.getTrustStorePassword().toCharArray());
                }
                trustManagerFactory.init(trustStore);
                sslContextBuilder = sslContextBuilder.trustManager(trustManagerFactory);
                sslContextBuilder = sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
            } else {
                sslContextBuilder = sslContextBuilder.clientAuth(ClientAuth.NONE);
            }
            sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder);
            serverBuilder = serverBuilder.sslContext(sslContextBuilder.build());
        }
        logger.info("Starting gRPC server on port: {}", new Object[]{port.toString()});
        this.server = serverBuilder.build().start();
    }

    @OnStopped
    public void stopServer(final ProcessContext context) {
        if (this.server != null) {
            try {
                this.server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().warn("Unable to cleanly shutdown embedded gRPC server due to {}", new Object[]{e});
                this.server = null;
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        context.yield();
    }
}
