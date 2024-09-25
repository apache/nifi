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
package org.apache.nifi.jetty.configuration.connector;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardServerConnectorFactoryTest {
    private static final int HTTP_PORT = 8080;

    private static final int HTTPS_PORT = 8443;

    private static final String[] INCLUDE_PROTOCOLS = new String[]{"TLSv1.2"};

    @Test
    void testGetServerConnector() {
        final Server server = new Server();
        final StandardServerConnectorFactory factory = new StandardServerConnectorFactory(server, HTTP_PORT);

        final ServerConnector serverConnector = factory.getServerConnector();

        final HttpConnectionFactory httpConnectionFactory = assertHttpConnectionFactoryFound(serverConnector);
        assertEquals(8192, httpConnectionFactory.getHttpConfiguration().getRequestHeaderSize());
    }

    @Test
    void testGetServerConnectorWithRequestHeaderSize() {
        final Server server = new Server();
        final StandardServerConnectorFactory factory = new StandardServerConnectorFactory(server, HTTP_PORT);
        factory.setRequestHeaderSize(16000);

        final ServerConnector serverConnector = factory.getServerConnector();

        final HttpConnectionFactory httpConnectionFactory = assertHttpConnectionFactoryFound(serverConnector);
        assertEquals(16000, httpConnectionFactory.getHttpConfiguration().getRequestHeaderSize());
    }

    @Test
    void testGetServerConnectorSecured() throws NoSuchAlgorithmException {
        final StandardServerConnectorFactory factory = createSecuredStandardServerConnectorFactory();

        final ServerConnector serverConnector = factory.getServerConnector();

        assertHttpConnectionFactoryFound(serverConnector);
        final SslConnectionFactory sslConnectionFactory = assertSslConnectionFactoryFound(serverConnector);

        final HttpConnectionFactory httpConnectionFactory = assertHttpConnectionFactoryFound(serverConnector);
        assertHttpConnectionFactorySecured(httpConnectionFactory);

        final SslContextFactory.Server sslContextFactory = sslConnectionFactory.getSslContextFactory();
        assertFalse(sslContextFactory.getNeedClientAuth());
        assertFalse(sslContextFactory.getWantClientAuth());
        assertNotNull(sslContextFactory.getIncludeProtocols());

        final HTTP2ServerConnectionFactory http2ConnectionFactory = serverConnector.getConnectionFactory(HTTP2ServerConnectionFactory.class);
        assertNull(http2ConnectionFactory);
    }

    @Test
    void testGetServerConnectorSecuredNeedClientAuthentication() throws NoSuchAlgorithmException {
        final StandardServerConnectorFactory factory = createSecuredStandardServerConnectorFactory();
        factory.setNeedClientAuth(true);
        factory.setIncludeSecurityProtocols(INCLUDE_PROTOCOLS);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertHttpConnectionFactoryFound(serverConnector);
        final SslConnectionFactory sslConnectionFactory = assertSslConnectionFactoryFound(serverConnector);

        final HttpConnectionFactory httpConnectionFactory = assertHttpConnectionFactoryFound(serverConnector);
        assertHttpConnectionFactorySecured(httpConnectionFactory);

        final SslContextFactory.Server sslContextFactory = sslConnectionFactory.getSslContextFactory();
        assertTrue(sslContextFactory.getNeedClientAuth());
        assertArrayEquals(INCLUDE_PROTOCOLS, sslContextFactory.getIncludeProtocols());
    }

    @Test
    void testGetServerConnectorSecuredHttp2AndHttp1() throws NoSuchAlgorithmException {
        final StandardServerConnectorFactory factory = createSecuredStandardServerConnectorFactory();
        factory.setApplicationLayerProtocols(new LinkedHashSet<>(Arrays.asList(ApplicationLayerProtocol.H2, ApplicationLayerProtocol.HTTP_1_1)));

        final ServerConnector serverConnector = factory.getServerConnector();

        final HttpConnectionFactory httpConnectionFactory = assertHttpConnectionFactoryFound(serverConnector);
        assertHttpConnectionFactorySecured(httpConnectionFactory);

        final SslConnectionFactory sslConnectionFactory = assertSslConnectionFactoryFound(serverConnector);
        final SslContextFactory.Server sslContextFactory = sslConnectionFactory.getSslContextFactory();
        assertFalse(sslContextFactory.getNeedClientAuth());

        assertHttp2ConnectionFactoriesFound(serverConnector);
    }

    @Test
    void testGetServerConnectorSecuredHttp2() throws NoSuchAlgorithmException {
        final StandardServerConnectorFactory factory = createSecuredStandardServerConnectorFactory();
        factory.setApplicationLayerProtocols(Collections.singleton(ApplicationLayerProtocol.H2));

        final ServerConnector serverConnector = factory.getServerConnector();

        final HttpConnectionFactory connectionFactory = serverConnector.getConnectionFactory(HttpConnectionFactory.class);
        assertNull(connectionFactory);

        final SslConnectionFactory sslConnectionFactory = assertSslConnectionFactoryFound(serverConnector);
        final SslContextFactory.Server sslContextFactory = sslConnectionFactory.getSslContextFactory();
        assertFalse(sslContextFactory.getNeedClientAuth());

        assertHttp2ConnectionFactoriesFound(serverConnector);
    }

    private StandardServerConnectorFactory createSecuredStandardServerConnectorFactory() throws NoSuchAlgorithmException {
        final Server server = new Server();
        final StandardServerConnectorFactory factory = new StandardServerConnectorFactory(server, HTTPS_PORT);
        final SSLContext sslContext = SSLContext.getDefault();
        factory.setSslContext(sslContext);
        return factory;
    }

    private HttpConnectionFactory assertHttpConnectionFactoryFound(final ServerConnector serverConnector) {
        assertNotNull(serverConnector);
        final HttpConnectionFactory connectionFactory = serverConnector.getConnectionFactory(HttpConnectionFactory.class);
        assertNotNull(connectionFactory);
        return connectionFactory;
    }

    private void assertHttp2ConnectionFactoriesFound(final ServerConnector serverConnector) {
        final HTTP2ServerConnectionFactory http2ConnectionFactory = serverConnector.getConnectionFactory(HTTP2ServerConnectionFactory.class);
        assertNotNull(http2ConnectionFactory);

        final ALPNServerConnectionFactory alpnServerConnectionFactory = serverConnector.getConnectionFactory(ALPNServerConnectionFactory.class);
        assertNotNull(alpnServerConnectionFactory);
    }

    private SslConnectionFactory assertSslConnectionFactoryFound(final ServerConnector serverConnector) {
        final SslConnectionFactory sslConnectionFactory = serverConnector.getConnectionFactory(SslConnectionFactory.class);
        assertNotNull(sslConnectionFactory);
        return sslConnectionFactory;
    }

    private void assertHttpConnectionFactorySecured(final HttpConnectionFactory httpConnectionFactory) {
        final HttpConfiguration configuration = httpConnectionFactory.getHttpConfiguration();
        assertEquals(HTTPS_PORT, configuration.getSecurePort());
        assertEquals(HttpScheme.HTTPS.asString(), configuration.getSecureScheme());
        final SecureRequestCustomizer secureRequestCustomizer = configuration.getCustomizer(SecureRequestCustomizer.class);
        assertNotNull(secureRequestCustomizer);
    }
}
