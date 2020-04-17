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
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// This class tests the behaviors involved with the ZooKeeperStateServer::create method.  The servers are not started,
// and TLS connections are not used.
public class TestZooKeeperStateServerConfigurations {
    private static final String KEY_STORE = getPath("keystore.jks");
    private static final String TRUST_STORE = getPath("truststore.jks");
    private static final String INSECURE_ZOOKEEPER_PROPS = getPath("insecure.zookeeper.properties");
    private static final String SECURE_ZOOKEEPER_PROPS = getPath("secure.zookeeper.properties");
    private static final String ZOOKEEPER_PROPERTIES_FILE_KEY = "nifi.state.management.embedded.zookeeper.properties";
    private static final String ZOOKEEPER_CNXN_FACTORY = "org.apache.zookeeper.server.NettyServerCnxnFactory";
    private static final String KEYSTORE_PASSWORD = "passwordpassword";
    private static final String TRUSTSTORE_PASSWORD = "passwordpassword";
    private static final String STORE_TYPE = "JKS";

    private static final Map<String, String> INSECURE_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS);
    }};

    private static final Map<String, String> INSECURE_NIFI_PROPS = new HashMap<String, String>() {{
        putAll(INSECURE_PROPS);
        put(NiFiProperties.WEB_HTTP_PORT, "8080");
        put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "false");
    }};

    private static final Map<String, String> SECURE_NIFI_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        put(NiFiProperties.WEB_HTTPS_PORT, "8443");
        put(NiFiProperties.SECURITY_KEYSTORE, KEY_STORE);
        put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, KEYSTORE_PASSWORD);
        put(NiFiProperties.SECURITY_KEYSTORE_TYPE, STORE_TYPE);
        put(NiFiProperties.SECURITY_TRUSTSTORE, TRUST_STORE);
        put(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, TRUSTSTORE_PASSWORD);
        put(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, STORE_TYPE);
        put(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
    }};

    private NiFiProperties secureNiFiProps;
    private NiFiProperties insecureNiFiProps;
    private QuorumPeerConfig secureQuorumPeerConfig;
    private QuorumPeerConfig insecureQuorumPeerConfig;
    private Properties secureZooKeeperProps;
    private Properties insecureZooKeeperProps;

    @Before
    public void setupWithValidProperties() throws IOException, QuorumPeerConfig.ConfigException {

        // Secure properties setup
        secureNiFiProps = NiFiProperties.createBasicNiFiProperties(null, SECURE_NIFI_PROPS);
        Assert.assertNotNull(secureNiFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer secureZooKeeperStateServer = ZooKeeperStateServer.create(secureNiFiProps);
        Assert.assertNotNull(secureZooKeeperStateServer);

        secureQuorumPeerConfig = secureZooKeeperStateServer.getQuorumPeerConfig();
        Assert.assertNotNull(secureQuorumPeerConfig);

        secureZooKeeperProps = new Properties();
        secureZooKeeperProps.load( FileUtils.openInputStream(new File(SECURE_ZOOKEEPER_PROPS)));
        Assert.assertNotNull(secureZooKeeperProps);

        // Insecure  properties setup
        insecureNiFiProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_NIFI_PROPS);
        Assert.assertNotNull(insecureNiFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer insecureZooKeeperStateServer = ZooKeeperStateServer.create(insecureNiFiProps);
        Assert.assertNotNull(insecureZooKeeperStateServer);

        insecureQuorumPeerConfig = insecureZooKeeperStateServer.getQuorumPeerConfig();
        Assert.assertNotNull(insecureQuorumPeerConfig);

        insecureZooKeeperProps = new Properties();
        insecureZooKeeperProps.load(FileUtils.openInputStream(new File(INSECURE_ZOOKEEPER_PROPS)));
        Assert.assertNotNull(insecureZooKeeperProps);
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

        Assert.assertNotNull(emptyProps);
        Assert.assertNull(ZooKeeperStateServer.create(emptyProps));
    }

    // This test shows that a ZooKeeperStateServer can be created from insecure NiFi properties.
    @Test
    public void testCreateFromValidInsecureNiFiProperties() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties insecureProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_PROPS);
        final ZooKeeperStateServer server = ZooKeeperStateServer.create(insecureProps);

        Assert.assertNotNull(server);
        Assert.assertNotNull(server.getQuorumPeerConfig().getClientPortAddress());
    }

    // This test shows that the client can specify a secure port and that port is used:
    @Test
    public void testCreateWithSpecifiedSecureClientPort() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        }});

        final ZooKeeperStateServer server = ZooKeeperStateServer.create(secureProps);
        Assert.assertNotNull(server);

        final QuorumPeerConfig config = server.getQuorumPeerConfig();
        Assert.assertEquals(secureZooKeeperProps.getProperty("secureClientPort"), String.valueOf(config.getSecureClientPortAddress().getPort()));
    }

    // This shows that a secure NiFi with an secure ZooKeeper will not have an insecure client address or port:
    @Test
    public void testCreateRemovesInsecureClientPort() {
        Assert.assertNotNull(secureZooKeeperProps.getProperty("secureClientPort"));
        Assert.assertNotEquals(secureZooKeeperProps.getProperty("clientPort"), "");
        Assert.assertNull(secureQuorumPeerConfig.getClientPortAddress());
    }

    // This test shows that a connection class is set when none is specified (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithUnspecifiedConnectionClass() {
        Assert.assertEquals(org.apache.zookeeper.server.NettyServerCnxnFactory.class.getName(), System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }

    // This test shows that a specified connection class is honored (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithSpecifiedConnectionClass() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS);
        }});

        Assert.assertNotNull(ZooKeeperStateServer.create(secureProps));
        Assert.assertEquals(ZOOKEEPER_CNXN_FACTORY, System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }

    private static String getPath(String path) {
        return new File("src/test/resources/TestZooKeeperStateServerConfigurations/" + path).getAbsolutePath();
    }
}
