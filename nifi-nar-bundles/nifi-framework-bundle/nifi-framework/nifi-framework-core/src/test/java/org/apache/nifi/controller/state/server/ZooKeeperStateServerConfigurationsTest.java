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

import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


// This class tests the behaviors involved with the ZooKeeperStateServer::create method.  The servers are not started,
// and TLS connections are not used.
public class ZooKeeperStateServerConfigurationsTest {
    private static final URL KEY_STORE = loadResource( "keystore.jks");
    private static final URL TRUST_STORE = loadResource( "truststore.jks");
    private static final URL INSECURE_ZOOKEEPER_PROPS_URL = loadResource( "insecure.zookeeper.properties");
    private static final URL PARTIAL_ZOOKEEPER_PROPS_URL = loadResource( "partial.zookeeper.properties");
    private static final URL COMPLETE_ZOOKEEPER_PROPS_URL = loadResource( "complete.zookeeper.properties");
    private static final URL SECURE_ZOOKEEPER_PROPS_URL = loadResource( "secure.zookeeper.properties");
    private static final String ZOOKEEPER_PROPERTIES_FILE_KEY = "nifi.state.management.embedded.zookeeper.properties";

    private static final Map<String, String> INSECURE_NIFI_PROPS = new HashMap<String, String>() {{
        put(ZOOKEEPER_PROPERTIES_FILE_KEY, INSECURE_ZOOKEEPER_PROPS_URL.getPath());
    }};

    private static final Map<String, String> SECURE_NIFI_PROPS = new HashMap<String, String>() {{
        putAll(INSECURE_NIFI_PROPS);
        put("nifi.web.https.port", "8443");
        put("nifi.security.keystore", KEY_STORE.getPath());
        put("nifi.security.keystorePasswd", "passwordpassword");
        put("nifi.security.truststore", TRUST_STORE.getPath());
        put("nifi.security.truststorePasswd", "passwordpassword");
    }};

    private NiFiProperties niFiProps;
    private QuorumPeerConfig quorumPeerConfig;
    private Properties secureZooKeeperProps;
    private Properties insecureZooKeeperProps;


    @Before
    public void setupWithValidProperties() throws IOException, QuorumPeerConfig.ConfigException {
        niFiProps = NiFiProperties.createBasicNiFiProperties(null, SECURE_NIFI_PROPS);
        Assert.assertNotNull(niFiProps);

        // This shows that a ZooKeeper server is created from valid NiFi properties:
        final ZooKeeperStateServer zooKeeperStateServer = ZooKeeperStateServer.create(niFiProps);
        Assert.assertNotNull(zooKeeperStateServer);

        quorumPeerConfig = zooKeeperStateServer.getQuorumPeerConfig();
        Assert.assertNotNull(quorumPeerConfig);

        secureZooKeeperProps = new Properties();
        secureZooKeeperProps.load(SECURE_ZOOKEEPER_PROPS_URL.openStream());
        Assert.assertNotNull(secureZooKeeperProps);

        insecureZooKeeperProps = new Properties();
        insecureZooKeeperProps.load(INSECURE_ZOOKEEPER_PROPS_URL.openStream());
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
        final NiFiProperties insecureProps = NiFiProperties.createBasicNiFiProperties(null, INSECURE_NIFI_PROPS);
        final ZooKeeperStateServer server = ZooKeeperStateServer.create(insecureProps);

        Assert.assertNotNull(server);
        Assert.assertNotNull(server.getQuorumPeerConfig().getClientPortAddress());
    }


    // This test shows that a ZK TLS config with some but not all values throws an exception:
    @Test
    public void testCreateFromPartialZooKeeperTlsProperties() {
        final NiFiProperties partialProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, PARTIAL_ZOOKEEPER_PROPS_URL.getPath());
        }});

        Assert.assertThrows(QuorumPeerConfig.ConfigException.class, ()-> {
            ZooKeeperStateServer.create(partialProps);
        });
    }


    // This test shows that a ZK TLS config with all values set is valid and a server can be created using it:
    @Test
    public void testCreateFromCompleteZooKeeperTlsProperties() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties completeProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, COMPLETE_ZOOKEEPER_PROPS_URL.getPath());
        }});

        Assert.assertNotNull(ZooKeeperStateServer.create(completeProps));
    }


    // This test shows that the client can specify a secure port and that port is used:
    @Test
    public void testCreateWithSpecifiedSecureClientPort() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS_URL.getPath());
        }});

        final ZooKeeperStateServer server = ZooKeeperStateServer.create(secureProps);
        Assert.assertNotNull(server);

        final QuorumPeerConfig config = server.getQuorumPeerConfig();
        Assert.assertEquals(secureZooKeeperProps.getProperty("secureClientPort"), String.valueOf(config.getSecureClientPortAddress().getPort()));
    }


    // This shows that a secure NiFi with an insecure ZooKeeper will not have an insecure client address or port:
    @Test
    public void testCreateRemovesInsecureClientPort() {
        Assert.assertNotNull(insecureZooKeeperProps.getProperty("clientPort"));
        Assert.assertNotEquals(insecureZooKeeperProps.getProperty("clientPort"), "");
        Assert.assertNull(quorumPeerConfig.getClientPortAddress());
    }


    // This test to shows that an available port is selected when none is specified:
    @Test
    public void testCreateWithUnspecifiedSecureClientPort() {
        final int port = quorumPeerConfig.getSecureClientPortAddress().getPort();

        Assert.assertNotEquals(0, port);
        Assert.assertNotEquals(1181, port);
        Assert.assertNotEquals(2181, port);
        Assert.assertNotEquals(2182, port);
        Assert.assertTrue(port > ZooKeeperStateServer.MIN_AVAILABLE_PORT);
    }

    // This test shows that a connection class is set when none is specified (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithUnspecifiedConnectionClass() {
        Assert.assertEquals(ZooKeeperStateServer.SERVER_CNXN_FACTORY, System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }


    // This test shows that a specified connection class is honored (QuorumPeerConfig::parseProperties sets the System property):
    @Test
    public void testCreateWithSpecifiedConnectionClass() throws IOException, QuorumPeerConfig.ConfigException {
        final NiFiProperties secureProps = NiFiProperties.createBasicNiFiProperties(null, new HashMap<String, String>() {{
            putAll(SECURE_NIFI_PROPS);
            put(ZOOKEEPER_PROPERTIES_FILE_KEY, SECURE_ZOOKEEPER_PROPS_URL.getPath());
        }});

        Assert.assertNotNull(ZooKeeperStateServer.create(secureProps));
        Assert.assertEquals(secureZooKeeperProps.getProperty("serverCnxnFactory"), System.getProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY));
    }


    // This test shows that NiFi TLS properties are propagated System properties for ZooKeeper:
    @Test
    public void testCreateFromPropagatedNiFiProperties() {
        ZooKeeperStateServer.ZOOKEEPER_TO_NIFI_PROPERTIES.keySet().forEach((key) -> {
            Assert.assertEquals( niFiProps.getProperty(ZooKeeperStateServer.ZOOKEEPER_TO_NIFI_PROPERTIES.get(key)),  System.getProperty("zookeeper." + key));
        });
    }


    private static URL loadResource(String path) {
        return ZooKeeperStateServerConfigurationsTest.class.getResource("/" + ZooKeeperStateServerConfigurationsTest.class.getSimpleName() + "/" + path);
    }
}
