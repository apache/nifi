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
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.util.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.x500.X500Principal;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
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

    private static final String ALIAS = "entry-0";

    private static final String KEY_STORE_EXTENSION = ".p12";

    private static final String KEY_STORE_PASS = ApplicationServerConnectorFactoryTest.class.getName();

    @TempDir
    private static Path keyStoreDirectory;

    private static String keyStoreType;

    private static Path keyStorePath;

    Server server;

    @BeforeAll
    static void setConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder().build();
        keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), KEY_STORE_PASS.toCharArray(), new Certificate[]{certificate});

        keyStorePath = Files.createTempFile(keyStoreDirectory, ApplicationServerConnectorFactoryTest.class.getSimpleName(), KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEY_STORE_PASS.toCharArray());
        }

        keyStoreType = keyStore.getType().toUpperCase();
    }

    @BeforeEach
    void setServer() {
        server = new Server();
    }

    @Test
    void testGetServerConnectorRequiredProperties() {
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, "0");

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorHostProperty() {
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, "0");
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, LOCALHOST);

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertEquals(LOCALHOST, serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorHostPropertyEmpty() {
        final Properties configuredProperties = new Properties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, "0");
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, StringUtils.EMPTY);

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
    }

    @Test
    void testGetServerConnectorSslProperties() {
        final Properties configuredProperties = getSecurityProperties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_PORT, "0");

        final NiFiRegistryProperties properties = getProperties(configuredProperties);
        final ApplicationServerConnectorFactory factory = new ApplicationServerConnectorFactory(server, properties);

        final ServerConnector serverConnector = factory.getServerConnector();

        assertNotNull(serverConnector);
        assertNull(serverConnector.getHost());
        assertTrue(serverConnector.getProtocols().contains(SSL_PROTOCOL));
    }

    @Test
    void testGetServerConnectorHttp2Properties() {
        final Properties configuredProperties = getSecurityProperties();
        configuredProperties.put(NiFiRegistryProperties.WEB_HTTPS_PORT, "0");
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
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE, keyStorePath.toString());
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, keyStoreType);
        securityProperties.put(NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD, KEY_STORE_PASS);
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE, keyStorePath.toString());
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, keyStoreType);
        securityProperties.put(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, KEY_STORE_PASS);
        return securityProperties;
    }

    private NiFiRegistryProperties getProperties(final Properties configuredProperties) {
        return new NiFiRegistryProperties(configuredProperties);
    }
}
