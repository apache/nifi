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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.nifi.framework.cluster.zookeeper.SecureClientZooKeeperFactory;
import org.apache.nifi.framework.cluster.zookeeper.ZooKeeperClientConfig;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.nifi.leader.election.ITSecureClientZooKeeperFactory.createSecureClientProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Testing setting up a ZooKeeperStateServer with TLS
public class ITZooKeeperStateServerTLS {
    private static final String INSECURE_ZOOKEEPER_PROPS = getPath("insecure.zookeeper.properties");
    private static final String PARTIAL_ZOOKEEPER_PROPS = getPath("partial.zookeeper.properties");
    private static final String SECURE_ZOOKEEPER_PROPS = getPath("secure.zookeeper.properties");
    private static final String ZOOKEEPER_PROPERTIES_FILE_KEY = "nifi.state.management.embedded.zookeeper.properties";
    private static final String ZOOKEEPER_CNXN_FACTORY = "org.apache.zookeeper.server.NettyServerCnxnFactory";
    private static final String QUORUM_CONNECT_STRING = "node0.apache.org:2281,node1.apache.org:2281";

    private static final Map<String, String> SECURE_NIFI_PROPS = new HashMap<>();
    private static final Map<String, String> SECURE_ZOOKEEPER_NIFI_PROPS = new HashMap<>();

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

        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, SECURE_ZOOKEEPER_PROPS);
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.WEB_HTTPS_PORT, "8443");
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
        SECURE_ZOOKEEPER_NIFI_PROPS.put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
    }

    private static final Map<String, String> INSECURE_NIFI_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
        put(NiFiProperties.WEB_HTTP_HOST, "localhost");
        put(NiFiProperties.WEB_HTTP_PORT, "8080");
        put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "false");
    }};

    private NiFiProperties niFiProps;
    private static NiFiProperties clientProperties;
    private QuorumPeerConfig quorumPeerConfig;
    private Properties secureZooKeeperProps;
    private Properties insecureZooKeeperProps;
    private Properties partialZooKeeperProps;
    private ZooKeeperStateServer server;

    @BeforeEach
    public void setupWithValidProperties() throws IOException, QuorumPeerConfig.ConfigException {
        niFiProps = NiFiProperties.createBasicNiFiProperties(null, SECURE_NIFI_PROPS);
        assertNotNull(niFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer zooKeeperStateServer = ZooKeeperStateServer.create(niFiProps);
        assertNotNull(zooKeeperStateServer);

        quorumPeerConfig = zooKeeperStateServer.getQuorumPeerConfig();
        assertNotNull(quorumPeerConfig);

        secureZooKeeperProps = new Properties();
        secureZooKeeperProps.load(FileUtils.openInputStream(new File(SECURE_ZOOKEEPER_PROPS)));
        assertNotNull(secureZooKeeperProps);

        insecureZooKeeperProps = new Properties();
        insecureZooKeeperProps.load(FileUtils.openInputStream(new File(INSECURE_ZOOKEEPER_PROPS)));
        assertNotNull(insecureZooKeeperProps);

        partialZooKeeperProps = new Properties();
        partialZooKeeperProps.load(FileUtils.openInputStream(new File(PARTIAL_ZOOKEEPER_PROPS)));
        assertNotNull(partialZooKeeperProps);
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
        final NiFiProperties insecureProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_NIFI_PROPS);
        final ZooKeeperStateServer server = ZooKeeperStateServer.create(insecureProps);

        assertNotNull(server);
        assertNotNull(server.getQuorumPeerConfig().getClientPortAddress());
    }

    // This test shows that a ZK TLS config with some but not all values throws an exception:
    @Test
    public void testCreateFromPartialZooKeeperTlsProperties() {
        final NiFiProperties partialProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, PARTIAL_ZOOKEEPER_PROPS);
        }});

        assertThrows(QuorumPeerConfig.ConfigException.class, () ->
                ZooKeeperStateServer.create(partialProps));
    }

    // This test shows that a ZK TLS config with all values set is valid and a server can be created using it:
    @Test
    public void testCreateFromCompleteZooKeeperTlsProperties() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties completeProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        }});

        assertNotNull(ZooKeeperStateServer.create(completeProps));
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

    // This shows that a secure NiFi with an insecure ZooKeeper will not have an insecure client address or port:
    @Test
    public void testCreateRemovesInsecureClientPort() {
        assertNotNull(insecureZooKeeperProps.getProperty("clientPort"));
        assertNotEquals("", insecureZooKeeperProps.getProperty("clientPort"));
        assertNull(quorumPeerConfig.getClientPortAddress());
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

    @AfterEach
    public void tearDown() throws Exception {
        Collections.unmodifiableSet(System.getProperties().stringPropertyNames()).stream()
                .filter(name -> name.startsWith("zookeeper."))
                .forEach(System::clearProperty);

        if(server != null) {
            File stateDir = server.getQuorumPeerConfig().getDataDir().getParentFile();
            deleteRecursively(stateDir);
            server.shutdown();
            server = null;
        }
    }

    private void deleteRecursively(final File file) {
        final File[] children = file.listFiles();
        if (children != null) {
            for (final File child : children) {
                deleteRecursively(child);
            }
        }

        file.delete();
    }

    // Connect to a secure ZooKeeperStateServer
    @Test
    public void testSecureClientQuorumConnectString() throws Exception {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort", "0"));
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, QUORUM_CONNECT_STRING);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getSecureClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getSecureZooKeeperClient(serverPort);
        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);
        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    // Connect to an insecure ZooKeeperStateServer with an insecure client (ensure insecure setup still works)
    @Test
    public void testInsecureZooKeeperWithInsecureClient() throws Exception {
        final String connect = "localhost:" + 2381;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(INSECURE_NIFI_PROPS);
            put(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, INSECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getInsecureZooKeeperClient(INSECURE_NIFI_PROPS, connect);
        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);
        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    // Fail to connect to a secure ZooKeeperStateServer with insecure client configuration
    @Test
    public void testSecureZooKeeperStateServerWithInsecureClient() throws Exception {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort", "0"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getSecureClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getInsecureZooKeeperClient(INSECURE_NIFI_PROPS, connect);
        client.start();
        final String testPath = "/test";

        // Expect this to fail with ConnectionLossException
        assertThrows(KeeperException.ConnectionLossException.class,
                () -> client.create().forPath(testPath, new byte[0]));
    }

    // Fail to connect to a secure ZooKeeperStateServer with missing client configuration
    @Test
    public void testSecureZooKeeperStateServerWithMissingClientConfiguration() throws Exception {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort", "0"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getSecureClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getInsecureZooKeeperClient(INSECURE_NIFI_PROPS, connect);
        client.start();
        final String testPath = "/test";

        // Expect this to fail with ConnectionLossException
        assertThrows(KeeperException.ConnectionLossException.class,
                () -> client.create().forPath(testPath, new byte[0]));
    }

    // Connect to a secure ZooKeeperStateServer
    @Test
    public void testSecureClientConnection() throws Exception {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort", "0"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getSecureClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getSecureZooKeeperClient(serverPort);
        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);
        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    // Connect to an insecure ZooKeeperStateServer
    @Test
    public void testClientSecureFalseClientPortNoTls() throws Exception {
        final int actualPort = Integer.parseInt(insecureZooKeeperProps.getProperty("clientPort", "3000"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(INSECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getInsecureZooKeeperClient(INSECURE_NIFI_PROPS, connect);
        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);
        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    @Test
    public void testClientSecureFalseWithClientSecurePortAndNoTls() {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(INSECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        QuorumPeerConfig.ConfigException configException =
                assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZooKeeperStateServer.create(validZkClientProps));

        assertEquals("ZooKeeper properties file src/test/resources/TestZooKeeperStateServerConfigurations/secure.zookeeper.properties was " +
                "configured to be secure but there was no valid TLS config present in nifi.properties or nifi.zookeeper.client.secure was set to false. Check the administration guide",
                configException.getMessage());
    }

    @Test
    public void testClientSecureTrueWithInsecureZooKeeperAndTlsSet() {
        final int actualPort = Integer.parseInt(insecureZooKeeperProps.getProperty("clientPort"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        QuorumPeerConfig.ConfigException configException =
                assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZooKeeperStateServer.create(validZkClientProps));
        assertEquals("NiFi was configured to be secure but secureClientPort could not be retrieved from zookeeper.properties file",
                configException.getMessage());
    }

    @Test
    public void testClientSecureTrueWithNoTls() {
        final int actualPort = Integer.parseInt(insecureZooKeeperProps.getProperty("clientPort"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(INSECURE_NIFI_PROPS);
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        QuorumPeerConfig.ConfigException configException =
                assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZooKeeperStateServer.create(validZkClientProps));
        assertEquals(NiFiProperties.ZOOKEEPER_CLIENT_SECURE + " is true but no TLS configuration is present in nifi.properties",
                configException.getMessage());
    }

    @Test
    public void testZooKeeperMissingPortSettings() {
        final String connect = "localhost:" + 2181;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, PARTIAL_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        QuorumPeerConfig.ConfigException configException =
                assertThrows(QuorumPeerConfig.ConfigException.class, () -> ZooKeeperStateServer.create(validZkClientProps));
        assertEquals("No clientAddress or secureClientAddress is set in zookeeper.properties",
                configException.getMessage());
    }

    @Test
    public void testClientSecureFalseAndOnlyZooKeeperClientPortSetWithTlsProperties() throws Exception {
        final String connect = "localhost:" + 2181;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "false");
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
    }

    // Connect to a secure ZooKeeperStateServer with ZooKeeper.
    @Test
    public void testSecureClientConnectionWithZooKeeperSecurityProperties() throws Exception {
        final int actualPort = Integer.parseInt(secureZooKeeperProps.getProperty("secureClientPort", "0"));
        final String connect = "localhost:" + actualPort;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_ZOOKEEPER_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getSecureClientPortAddress().getPort();
        assertEquals(actualPort, serverPort);
        server.start();

        // Set up a ZK client
        CuratorFramework client = getSecureZooKeeperClient(serverPort);
        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);
        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    private static String getPath(String path) {
        return new File("src/test/resources/TestZooKeeperStateServerConfigurations/" + path).getPath();
    }

    private CuratorFramework getSecureZooKeeperClient(final int port) {
        // TODO: port being set needs to be based on port set in nifi.properties, should create client in the same
        clientProperties = createSecureClientProperties(
                port,
                Paths.get(tlsConfiguration.getKeystorePath()),
                tlsConfiguration.getKeystoreType().getType(),
                tlsConfiguration.getKeystorePassword(),
                Paths.get(tlsConfiguration.getTruststorePath()),
                tlsConfiguration.getTruststoreType().getType(),
                tlsConfiguration.getTruststorePassword()
        );

        final ZooKeeperClientConfig zkClientConfig =
                ZooKeeperClientConfig.createConfig(clientProperties);

        return getCuratorFramework(zkClientConfig, new SecureClientZooKeeperFactory(zkClientConfig));
    }

    private CuratorFramework getInsecureZooKeeperClient(final Map<String, String> nifiClientProps, final String connectString) {
        // set up zkClientConfig
        Map<String, String> supplementedProps = nifiClientProps;
        supplementedProps.put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connectString);
        NiFiProperties insecureClientProperties = NiFiProperties.createBasicNiFiProperties(null, supplementedProps);
        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(insecureClientProperties);

        ZookeeperFactory factory = null;
        try {
            factory = new DefaultZookeeperFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return getCuratorFramework(zkClientConfig, factory);
    }

    private CuratorFramework getCuratorFramework(ZooKeeperClientConfig zkClientConfig, ZookeeperFactory factory) {
        final CuratorFrameworkFactory.Builder clientBuilder = CuratorFrameworkFactory.builder()
                .connectString(zkClientConfig.getConnectString())
                .sessionTimeoutMs(zkClientConfig.getSessionTimeoutMillis())
                .connectionTimeoutMs(zkClientConfig.getConnectionTimeoutMillis())
                .retryPolicy(new RetryOneTime(200))
                .defaultData(new byte[0])
                .zookeeperFactory(factory);

        return clientBuilder.build();
    }

}
