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
package org.apache.nifi.registry.jetty.connector;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationServerConnectorFactoryTest {

    private static final String SSL_PROTOCOL = "ssl";

    private static final String H2_PROTOCOL = "h2";

    private static final String INCLUDE_CIPHERS = ".*GCM.*";

    private static final String EXCLUDE_CIPHERS = "*.CBC.*";

    private static final String LOCALHOST = "127.0.0.1";

    private static final String PROPRIETARY_TRUST_STORE_TYPE = "JKS";

    static TlsConfiguration tlsConfiguration;

    Server server;

    @BeforeAll
    static void setTlsConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().trustStoreType(PROPRIETARY_TRUST_STORE_TYPE).build();
    }

    @BeforeEach
    void setServer() {
        server = new Server();
    }

    @Test
    void testGetServerConnectorRequiredProperties() {
        final int port = NetworkUtils.getAvailableTcpPort();
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, Integer.toString(port));

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorHostProperty() {
        final int port = NetworkUtils.getAvailableTcpPort();
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, Integer.toString(port));
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, LOCALHOST);

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertEquals(LOCALHOST, serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorHostPropertyEmpty() {
        final int port = NetworkUtils.getAvailableTcpPort();
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, Integer.toString(port));
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, StringUtils.EMPTY);

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorSslProperties() {
        final int port = NetworkUtils.getAvailableTcpPort();
        final Properties configuredProperties = getSecurityProperties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_PORT, Integer.toString(port));

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
        assertTrue(serverConnector.getProtocols().contains(SSL_PROTOCOL));
    }

    @Test
    void testGetServerConnectorHttp2Properties() {
        final int port = NetworkUtils.getAvailableTcpPort();
        final Properties configuredProperties = getSecurityProperties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_PORT, Integer.toString(port));
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_APPLICATION_PROTOCOLS, H2_PROTOCOL);
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_CIPHERSUITES_INCLUDE, INCLUDE_CIPHERS);
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_CIPHERSUITES_EXCLUDE, EXCLUDE_CIPHERS);

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());

        final List<String> protocols = serverConnector.getProtocols();
        assertTrue(protocols.contains(SSL_PROTOCOL));
        assertTrue(protocols.contains(H2_PROTOCOL));
    }

    private Properties getSecurityProperties() {
        final Properties securityProperties = new Properties();
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        return securityProperties;
    }

    private NiFiRegistryProperties getProperties(final Properties configuredProperties) {
        return new NiFiRegistryProperties(configuredProperties);
    }
}
