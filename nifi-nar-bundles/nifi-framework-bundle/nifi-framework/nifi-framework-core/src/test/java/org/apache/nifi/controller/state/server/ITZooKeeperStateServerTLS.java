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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nifi.controller.cluster.SecureClientZooKeeperFactory;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.nifi.leader.election.ITSecureClientZooKeeperFactory.createSecureClientProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// Testing setting up a ZooKeeperStateServer with TLS
public class ITZooKeeperStateServerTLS {
    private static final String KEY_STORE = getPath("keystore.jks");
    private static final String TRUST_STORE = getPath("truststore.jks");
    private static final String STORE_TYPE = "JKS";
    private static final String INSECURE_ZOOKEEPER_PROPS = getPath("insecure.zookeeper.properties");
    private static final String PARTIAL_ZOOKEEPER_PROPS = getPath("partial.zookeeper.properties");
    private static final String COMPLETE_ZOOKEEPER_PROPS = getPath("complete.zookeeper.properties");
    private static final String SECURE_ZOOKEEPER_PROPS = getPath("secure.zookeeper.properties");
    private static final String ZOOKEEPER_PROPERTIES_FILE_KEY = "nifi.state.management.embedded.zookeeper.properties";
    private static final String ZOOKEEPER_CNXN_FACTORY = "org.apache.zookeeper.server.NettyServerCnxnFactory";
    private static final String CONNECT_STRING = "localhost:2281";
    private static final String QUORUM_CONNECT_STRING = "node0.apache.org:2281,node1.apache.org:2281";

    private static final Map<String, String> INSECURE_NIFI_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
        put(NiFiProperties.WEB_HTTP_HOST, "localhost");
        put(NiFiProperties.WEB_HTTP_PORT, "8080");
        put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "false");
    }};

    private static final String TEST_PASSWORD = "passwordpassword";

    private static final Map<String, String> SECURE_NIFI_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        put("nifi.web.https.port", "8443");
        put("nifi.security.keystore", KEY_STORE);
        put("nifi.security.keystoreType", STORE_TYPE);
        put("nifi.security.keystorePasswd", TEST_PASSWORD);
        put("nifi.security.truststore", TRUST_STORE);
        put("nifi.security.truststoreType", STORE_TYPE);
        put("nifi.security.truststorePasswd", TEST_PASSWORD);
    }};

    private NiFiProperties niFiProps;
    private static NiFiProperties clientProperties;
    private QuorumPeerConfig quorumPeerConfig;
    private Properties secureZooKeeperProps;
    private Properties insecureZooKeeperProps;
    private ZooKeeperStateServer server;

    @Before
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
    }

    @After
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
        Assert.assertNull(ZooKeeperStateServer.create(emptyProps));
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

        Assert.assertThrows(QuorumPeerConfig.ConfigException.class, () ->
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
        Assert.assertNotEquals(insecureZooKeeperProps.getProperty("clientPort"), "");
        Assert.assertNull(quorumPeerConfig.getClientPortAddress());
    }

    // This test shows that a connection class is set when none is specified (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithUnspecifiedConnectionClass() {
        assertEquals(ZooKeeperStateServer.SERVER_CNXN_FACTORY, System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
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

    private CloseableHttpClient httpclient;

    @Before
    public void setUp() throws Exception {
        httpclient = HttpClients.createDefault();
    }

    @After
    public void tearDown() throws Exception {
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
        final String connect = "localhost:" + actualPort;
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
        final int actualPort = Integer.parseInt(insecureZooKeeperProps.getProperty("clientPort", "0"));
        final String connect = "localhost:" + 2381;
        final NiFiProperties validZkClientProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(INSECURE_NIFI_PROPS);
            put(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, INSECURE_ZOOKEEPER_PROPS);
            put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, connect);
        }});

        server = ZooKeeperStateServer.create(validZkClientProps);
        assertNotNull(server);
        final int serverPort = server.getQuorumPeerConfig().getClientPortAddress().getPort();
        //assertEquals(actualPort, 2381);
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

    // Connect to a secure ZooKeeperStateServer with insecure client configuration
    @Test(expected = KeeperException.ConnectionLossException.class)
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
        final String createResult = client.create().forPath(testPath, new byte[0]);
    }

    // Connect to a secure ZooKeeperStateServer with missing client configuration
    @Test(expected = KeeperException.ConnectionLossException.class)
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
        final String createResult = client.create().forPath(testPath, new byte[0]);
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

    private static String getPath(String path) {
        return new File("src/test/resources/TestZooKeeperStateServerConfigurations/" + path).getAbsolutePath();
    }

    private CuratorFramework getSecureZooKeeperClient(final int port) {
        // TODO: port being set needs to be based on port set in nifi.properties, should create client in the same
        clientProperties = createSecureClientProperties(
                port,
                Paths.get(KEY_STORE),
                STORE_TYPE,
                TEST_PASSWORD,
                Paths.get(TRUST_STORE),
                STORE_TYPE,
                TEST_PASSWORD
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
