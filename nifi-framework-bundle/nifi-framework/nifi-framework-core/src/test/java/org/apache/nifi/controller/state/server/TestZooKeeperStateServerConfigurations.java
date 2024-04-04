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
package org.apache.nifi.controller.state.server;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

// This class tests the behaviors involved with the ZooKeeperStateServer::create method.  The servers are not started,
// and TLS connections are not used.
public class TestZooKeeperStateServerConfigurations {
    private static final String INSECURE_ZOOKEEPER_PROPS = getPath("insecure.zookeeper.properties");
    private static final String SECURE_ZOOKEEPER_PROPS = getPath("secure.zookeeper.properties");
    private static final String ZOOKEEPER_PROPERTIES_FILE_KEY = "nifi.state.management.embedded.zookeeper.properties";
    private static final String ZOOKEEPER_CNXN_FACTORY = "org.apache.zookeeper.server.NettyServerCnxnFactory";

    private static final Map<String, String> INSECURE_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
    }};

    private static final Map<String, String> SECURE_NIFI_PROPS = new HashMap<>();

    private static final Map<String, String> INSECURE_NIFI_PROPS = new HashMap<String, String>() {{
        putAll(INSECURE_PROPS);
        put(NiFiProperties.WEB_HTTP_PORT, "8080");
        put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "false");
    }};

    private NiFiProperties secureNiFiProps;
    private NiFiProperties insecureNiFiProps;
    private QuorumPeerConfig secureQuorumPeerConfig;
    private QuorumPeerConfig insecureQuorumPeerConfig;
    private Properties secureZooKeeperProps;
    private Properties insecureZooKeeperProps;

    private static TlsConfiguration tlsConfiguration;

    @BeforeAll
    public static void setTlsConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();

        SECURE_NIFI_PROPS.put(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, SECURE_ZOOKEEPER_PROPS);
        SECURE_NIFI_PROPS.put(NiFiProperties.WEB_HTTPS_PORT, "8443");
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        SECURE_NIFI_PROPS.put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        SECURE_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
    }

    @BeforeEach
    public void setupWithValidProperties() throws IOException, QuorumPeerConfig.ConfigException {

        // Secure properties setup
        secureNiFiProps = NiFiProperties.createBasicNiFiProperties(null, SECURE_NIFI_PROPS);
        assertNotNull(secureNiFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer secureZooKeeperStateServer = ZooKeeperStateServer.create(secureNiFiProps);
        assertNotNull(secureZooKeeperStateServer);

        secureQuorumPeerConfig = secureZooKeeperStateServer.getQuorumPeerConfig();
        assertNotNull(secureQuorumPeerConfig);

        secureZooKeeperProps = new Properties();
        secureZooKeeperProps.load( FileUtils.openInputStream(new File(SECURE_ZOOKEEPER_PROPS)));
        assertNotNull(secureZooKeeperProps);

        // Insecure  properties setup
        insecureNiFiProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_NIFI_PROPS);
        assertNotNull(insecureNiFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer insecureZooKeeperStateServer = ZooKeeperStateServer.create(insecureNiFiProps);
        assertNotNull(insecureZooKeeperStateServer);

        insecureQuorumPeerConfig = insecureZooKeeperStateServer.getQuorumPeerConfig();
        assertNotNull(insecureQuorumPeerConfig);

        insecureZooKeeperProps = new Properties();
        insecureZooKeeperProps.load(FileUtils.openInputStream(new File(INSECURE_ZOOKEEPER_PROPS)));
        assertNotNull(insecureZooKeeperProps);
    }

    @AfterEach
    public void clearConnectionProperties() {
        Collections.unmodifiableSet(System.getProperties().stringPropertyNames()).stream()
                .filter(name -> name.startsWith("zookeeper."))
                .forEach(System::clearProperty);
    }

    // This test shows that a ZooKeeperStateServer cannot be created from empty NiFi properties.
    @Test
    public void testCreateFromEmptyNiFiProperties() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties emptyProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<>());

        assertNotNull(emptyProps);
        assertNull(ZooKeeperStateServer.create(emptyProps));
    }

    // This test shows that a ZooKeeperStateServer can be created from insecure NiFi properties.
    @Test
    public void testCreateFromValidInsecureNiFiProperties() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties insecureProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_PROPS);
        final ZooKeeperStateServer server = ZooKeeperStateServer.create(insecureProps);

        assertNotNull(server);
        assertNotNull(server.getQuorumPeerConfig().getClientPortAddress());
    }

    // This test shows that the client can specify a secure port and that port is used:
    @Test
    public void testCreateWithSpecifiedSecureClientPort() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        }});

        final ZooKeeperStateServer server = ZooKeeperStateServer.create(secureProps);
        assertNotNull(server);

        final QuorumPeerConfig config = server.getQuorumPeerConfig();
        assertEquals(secureZooKeeperProps.getProperty("secureClientPort"), String.valueOf(config.getSecureClientPortAddress().getPort()));
    }

    // This shows that a secure NiFi with an secure ZooKeeper will not have an insecure client address or port:
    @Test
    public void testCreateRemovesInsecureClientPort() {
        assertNotNull(secureZooKeeperProps.getProperty("secureClientPort"));
        assertNotEquals("", secureZooKeeperProps.getProperty("clientPort"));
        assertNull(secureQuorumPeerConfig.getClientPortAddress());
    }

    // This test shows that a connection class is set when none is specified (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithUnspecifiedConnectionClass() {
        assertEquals(org.apache.zookeeper.server.NettyServerCnxnFactory.class.getName(), System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }

    // This test shows that a specified connection class is honored (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithSpecifiedConnectionClass() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        }});

        assertNotNull(ZooKeeperStateServer.create(secureProps));
        assertEquals(ZOOKEEPER_CNXN_FACTORY, System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }

    private static String getPath(String path) {
        return new File("src/test/resources/TestZooKeeperStateServerConfigurations/" + path).getAbsolutePath();
    }
}
