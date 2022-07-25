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
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ConnectionLoadBalanceServerTest {

    static final String LOCALHOST = "127.0.0.1";

    static final int SERVER_THREADS = 1;

    static final int SOCKET_TIMEOUT_MILLIS = 5000;

    static final Duration SOCKET_TIMEOUT = Duration.ofMillis(SOCKET_TIMEOUT_MILLIS);

    static final String ERROR_CATEGORY = "Load Balanced Connection";

    static SSLContext sslContext;

    @Mock
    EventReporter eventReporter;

    @BeforeAll
    static void setSslContext() throws TlsException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
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
    void testSslContextSocketHandshakeException() throws IOException {
        final ConnectionLoadBalanceServer server = getServer(new SslHandshakeExceptionLoadBalanceProtocol());

        try {
            server.start();

            final SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            try (final Socket socket = socketFactory.createSocket()) {
                final SSLSocket sslSocket = assertSocketConnected(socket, server.getPort());
                assertThrows(IOException.class, sslSocket::startHandshake);
            }

            verify(eventReporter).reportEvent(eq(Severity.ERROR), eq(ERROR_CATEGORY), anyString());
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
        final int port = NetworkUtils.getAvailableTcpPort();
        return new ConnectionLoadBalanceServer(
                LOCALHOST,
                port,
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

    static class SslHandshakeExceptionLoadBalanceProtocol implements LoadBalanceProtocol {

        @Override
        public void receiveFlowFiles(final Socket socket, final InputStream in, final OutputStream out) throws IOException {
            throw new SSLException("Handshake Failed");
        }
    }
}
