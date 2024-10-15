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
package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class ConnectionLoadBalanceServerTest {

    static final String LOCALHOST = "127.0.0.1";

    static final int SERVER_THREADS = 1;

    static final int SOCKET_TIMEOUT_MILLIS = 30000;

    static final Duration SOCKET_TIMEOUT = Duration.ofMillis(SOCKET_TIMEOUT_MILLIS);

    static SSLContext sslContext;

    @Mock
    EventReporter eventReporter;

    @BeforeAll
    static void setSslContext() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();
    }

    @Test
    void testSslContextSocketHandshakeCompleted() throws IOException {
        final ConnectionLoadBalanceServer server = getServer(new SslHandshakeCompletedLoadBalanceProtocol());

        try {
            server.start();

            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            try (final Socket socket = socketFactory.createSocket()) {
                final SSLSocket sslSocket = assertSocketConnected(socket, server.getPort());
                assertTimeoutPreemptively(SOCKET_TIMEOUT, sslSocket::startHandshake, "TLS handshake failed");
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void testHandshakeCompletedProtocolException() throws IOException {
        final ConnectionLoadBalanceServer server = getServer(new ReceiveFlowFilesSslExceptionLoadBalanceProtocol());

        try {
            server.start();

            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            try (final Socket socket = socketFactory.createSocket()) {
                final SSLSocket sslSocket = assertSocketConnected(socket, server.getPort());
                assertTimeoutPreemptively(SOCKET_TIMEOUT, sslSocket::startHandshake, "TLS handshake failed");
            }
        } finally {
            server.stop();
        }
    }

    private SSLSocket assertSocketConnected(final Socket socket, final int port) throws IOException {
        assertInstanceOf(SSLSocket.class, socket);

        final SSLSocket sslSocket = (SSLSocket) socket;
        final InetSocketAddress socketAddress = new InetSocketAddress(LOCALHOST, port);
        sslSocket.connect(socketAddress, SOCKET_TIMEOUT_MILLIS);

        assertTrue(sslSocket.isConnected());
        return sslSocket;
    }

    private ConnectionLoadBalanceServer getServer(final LoadBalanceProtocol loadBalanceProtocol) {
        return new ConnectionLoadBalanceServer(
                LOCALHOST,
                0,
                sslContext,
                SERVER_THREADS,
                loadBalanceProtocol,
                eventReporter,
                SOCKET_TIMEOUT_MILLIS
        );
    }

    static class SslHandshakeCompletedLoadBalanceProtocol implements LoadBalanceProtocol {

        @Override
        public void receiveFlowFiles(final Socket socket, final InputStream in, final OutputStream out) throws IOException {
            final SSLSocket sslSocket = (SSLSocket) socket;
            sslSocket.startHandshake();
        }
    }

    static class ReceiveFlowFilesSslExceptionLoadBalanceProtocol implements LoadBalanceProtocol {
        final AtomicBoolean receiveFlowFilesInvoked = new AtomicBoolean();

        @Override
        public void receiveFlowFiles(final Socket socket, final InputStream in, final OutputStream out) throws IOException {
            if (receiveFlowFilesInvoked.get()) {
                throw new SSLException(SSLException.class.getSimpleName());
            } else {
                final SSLSocket sslSocket = (SSLSocket) socket;
                sslSocket.startHandshake();
            }
            receiveFlowFilesInvoked.getAndSet(true);
        }
    }
}
