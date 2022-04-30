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

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Timeout(10)
@ExtendWith(MockitoExtension.class)
class TestListenGRPC {
    static final String LOCALHOST = "localhost";

    static final String SSL_SERVICE_ID = RestrictedSSLContextService.class.getSimpleName();

    static TlsConfiguration tlsConfiguration;

    static SSLContext sslContext;

    @Mock
    RestrictedSSLContextService sslContextService;

    TestRunner runner;

    @BeforeAll
    static void setTlsConfiguration() throws TlsException {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
    }

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(ListenGRPC.class);
    }

    @Test
    void testRunSocketListening() throws IOException {
        final int port = NetworkUtils.getAvailableTcpPort();

        runner.setProperty(ListenGRPC.PROP_SERVICE_PORT, Integer.toString(port));
        runner.assertValid();

        runner.run(1, false);

        assertSocketConnected(port, SocketFactory.getDefault());
    }

    @Test
    void testRunSocketListeningSslContextService() throws IOException, InitializationException {
        final int port = NetworkUtils.getAvailableTcpPort();

        runner.setProperty(ListenGRPC.PROP_SERVICE_PORT, Integer.toString(port));

        when(sslContextService.getIdentifier()).thenReturn(SSL_SERVICE_ID);
        when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);
        when(sslContextService.createContext()).thenReturn(sslContext);

        runner.addControllerService(SSL_SERVICE_ID, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenGRPC.PROP_SSL_CONTEXT_SERVICE, SSL_SERVICE_ID);
        runner.setProperty(ListenGRPC.PROP_USE_SECURE, Boolean.TRUE.toString());

        runner.assertValid();

        runner.run(1, false);

        assertSocketConnectedProtocolNegotiated(port, sslContext.getSocketFactory());
    }

    private void assertSocketConnected(final int port, final SocketFactory socketFactory) throws IOException {
        try (final Socket socket = socketFactory.createSocket()) {
            assertSocketConnected(port, socket);
        }
    }

    private void assertSocketConnectedProtocolNegotiated(final int port, final SSLSocketFactory socketFactory) throws IOException {
        try (final SSLSocket socket = (SSLSocket) socketFactory.createSocket()) {
            assertSocketConnected(port, socket);

            socket.startHandshake();

            final SSLSession session = socket.getSession();
            assertNotNull(session);
            assertNotNull(session.getCipherSuite());
        }
    }

    private void assertSocketConnected(final int port, final Socket socket) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(LOCALHOST, port);
        socket.connect(socketAddress);
        assertTrue(socket.isConnected());
    }
}
