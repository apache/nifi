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

import org.apache.nifi.ssl.StandardSSLContextService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

import static org.apache.nifi.processors.grpc.TestGRPCServer.NEED_CLIENT_AUTH;

/**
 * Generic gRPC channel builder for use in unit testing. Consumers should use the channel built here
 * to construct the desired stubs to communicate with a gRPC service.
 */
public class TestGRPCClient {
    // Used to represent the ephemeral port range.
    private static final int PORT_START = 49152;
    private static final int PORT_END = 65535;

    /**
     * Can be used by clients to grab a random port in a range of ports
     *
     * @return a port to use for client/server comms
     */
    public static int randomPort() {
        // add 1 because upper bound is exclusive
        return ThreadLocalRandom.current().nextInt(PORT_START, PORT_END + 1);
    }

    /**
     * Build a channel with the given host and port and optional ssl properties.
     *
     * @param host the host to establish a connection with
     * @param port the port on which to communicate with the host
     * @return a constructed channel
     */
    public static ManagedChannel buildChannel(final String host, final int port)
            throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
        return buildChannel(host, port, null);
    }

    /**
     * Build a channel with the given host and port and optional ssl properties.
     *
     * @param host          the host to establish a connection with
     * @param port          the port on which to communicate with the host
     * @param sslProperties the properties by which to establish an ssl connection
     * @return a constructed channel
     */
    public static ManagedChannel buildChannel(final String host, final int port, final Map<String, String> sslProperties)
            throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(host, port)
                .directExecutor()
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .userAgent("testAgent");

        if (sslProperties != null) {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

            if(sslProperties.get(StandardSSLContextService.KEYSTORE.getName()) != null) {
                final KeyManagerFactory keyManager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                final KeyStore keyStore = KeyStore.getInstance(sslProperties.get(StandardSSLContextService.KEYSTORE_TYPE.getName()));
                final String keyStoreFile = sslProperties.get(StandardSSLContextService.KEYSTORE.getName());
                final String keyStorePassword = sslProperties.get(StandardSSLContextService.KEYSTORE_PASSWORD.getName());
                try (final InputStream is = new FileInputStream(keyStoreFile)) {
                    keyStore.load(is, keyStorePassword.toCharArray());
                }
                keyManager.init(keyStore, keyStorePassword.toCharArray());
                sslContextBuilder = sslContextBuilder.keyManager(keyManager);
            }

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
            channelBuilder = channelBuilder.sslContext(sslContextBuilder.build());
        } else {
            channelBuilder.usePlaintext(true);
        }
        return channelBuilder.build();
    }
}
