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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.nifi.ssl.StandardSSLContextService;

import io.grpc.BindableService;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Generic gRPC test server to assist with unit tests that require a server to be present.
 *
 * @param <T> the gRPC service implementation
 */
public class TestGRPCServer<T extends BindableService> {
    public static final String HOST = "localhost";
    public static final String NEED_CLIENT_AUTH = "needClientAuth";
    private final Class<T> clazz;
    private Server server;
    private Map<String, String> sslProperties;

    /**
     * Create a gRPC server
     *
     * @param clazz the gRPC service implementation
     */
    public TestGRPCServer(final Class<T> clazz) {
        this(clazz, null);
    }


    /**
     * Create a gRPC server
     *
     * @param clazz         the gRPC service implementation
     * @param sslProperties the keystore and truststore properties for SSL communications
     */
    public TestGRPCServer(final Class<T> clazz, final Map<String, String> sslProperties) {
        this.clazz = clazz;
        this.sslProperties = sslProperties;
    }

    /**
     * Can be used by clients to grab a random port in a range of ports
     *
     * @return a port to use for client/server comms
     */
    public static int randomPort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        socket.setReuseAddress(true);
        final int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    /**
     * Starts the gRPC server @localhost:port.
     */
    public void start(final int port) throws Exception {
        final NettyServerBuilder nettyServerBuilder = NettyServerBuilder
                .forPort(port)
                .directExecutor()
                .addService(clazz.newInstance())
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance());

        if (this.sslProperties != null) {
            if (sslProperties.get(StandardSSLContextService.KEYSTORE.getName()) == null) {
                throw new RuntimeException("You must configure a keystore in order to use SSL with gRPC.");
            }

            final KeyManagerFactory keyManager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            final KeyStore keyStore = KeyStore.getInstance(sslProperties.get(StandardSSLContextService.KEYSTORE_TYPE.getName()));
            final String keyStoreFile = sslProperties.get(StandardSSLContextService.KEYSTORE.getName());
            final String keyStorePassword = sslProperties.get(StandardSSLContextService.KEYSTORE_PASSWORD.getName());
            try (final InputStream is = new FileInputStream(keyStoreFile)) {
                keyStore.load(is, keyStorePassword.toCharArray());
            }
            keyManager.init(keyStore, keyStorePassword.toCharArray());
            SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(keyManager);

            if (sslProperties.get(StandardSSLContextService.TRUSTSTORE.getName()) != null) {
                final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                final KeyStore trustStore = KeyStore.getInstance(sslProperties.get(StandardSSLContextService.TRUSTSTORE_TYPE.getName()));
                final String trustStoreFile = sslProperties.get(StandardSSLContextService.TRUSTSTORE.getName());
                final String trustStorePassword = sslProperties.get(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName());
                try (final InputStream is = new FileInputStream(trustStoreFile)) {
                    trustStore.load(is, trustStorePassword.toCharArray());
                }
                trustManagerFactory.init(trustStore);
                sslContextBuilder = sslContextBuilder.trustManager(trustManagerFactory);
            }

            final String clientAuth = sslProperties.get(NEED_CLIENT_AUTH);
            if (clientAuth == null) {
                sslContextBuilder = sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
            } else {
                sslContextBuilder = sslContextBuilder.clientAuth(ClientAuth.valueOf(clientAuth));
            }
            sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder);
            nettyServerBuilder.sslContext(sslContextBuilder.build());
        }

        server = nettyServerBuilder.build().start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                TestGRPCServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    /**
     * Stop the server.
     */
    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
