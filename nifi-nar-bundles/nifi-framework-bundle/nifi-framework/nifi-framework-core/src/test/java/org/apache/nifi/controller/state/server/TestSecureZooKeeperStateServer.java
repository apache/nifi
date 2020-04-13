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
package org.apache.nifi.controller.state.server.zookeeper;

import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.controller.leader.election.CuratorLeaderElectionManager.SecureClientZookeeperFactory;
import org.apache.nifi.controller.state.server.ZooKeeperStateServer;
import org.apache.nifi.security.util.CertificateUtils;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestSecureZooKeeperStateServer {

    private static final String NETTY_SERVER_CNXN_FACTORY =
        "org.apache.zookeeper.server.NettyServerCnxnFactory";

    private static final String NETTY_CLIENT_CNXN_SOCKET =
        "org.apache.zookeeper.ClientCnxnSocketNetty";

    private static final String KEYSTORE_TYPE = "PKCS12";

    private static final String KEYSTORE_TYPE_EXT = ".p12";

    private static final String TEST_PASSWORD = "testpass";

    private static ZooKeeperStateServer zkServer;
    private static NiFiProperties clientProperties;
    private static Path tempDir;
    private static Path dataDir;
    private static Path zkServerConfig;
    private static Path serverKeyStore;
    private static Path serverTrustStore;
    private static Path clientKeyStore;
    private static Path clientTrustStore;
    private static int clientPort;

    @BeforeClass
    public static void setup() throws IOException, ConfigException, GeneralSecurityException {
        tempDir = Paths.get("target/TestSecureZooKeeperStateServer");
        dataDir = tempDir.resolve("state");
        zkServerConfig = tempDir.resolve("zookeeper.properties");
        serverKeyStore = tempDir.resolve("server.keystore" + KEYSTORE_TYPE_EXT);
        serverTrustStore = tempDir.resolve("server.truststore" + KEYSTORE_TYPE_EXT);
        clientKeyStore = tempDir.resolve("client.keystore" + KEYSTORE_TYPE_EXT);
        clientTrustStore = tempDir.resolve("client.truststore" + KEYSTORE_TYPE_EXT);
        clientPort = InstanceSpec.getRandomPort();

        Files.createDirectory(tempDir);

        final X509Certificate clientCert = createKeyStore("client", TEST_PASSWORD, clientKeyStore, KEYSTORE_TYPE);
        final X509Certificate serverCert = createKeyStore("zookeeper", TEST_PASSWORD, serverKeyStore, KEYSTORE_TYPE);

        createTrustStore(serverCert, "zookeeper", TEST_PASSWORD, clientTrustStore, KEYSTORE_TYPE);
        createTrustStore(clientCert, "client", TEST_PASSWORD, serverTrustStore, KEYSTORE_TYPE);

        createZooKeeperServerConfig(
            dataDir,
            clientPort,
            serverKeyStore,
            TEST_PASSWORD,
            serverTrustStore,
            TEST_PASSWORD,
            NETTY_SERVER_CNXN_FACTORY,
            zkServerConfig
        );

        clientProperties = createClientProperties(
            clientPort,
            clientKeyStore,
            TEST_PASSWORD,
            clientTrustStore,
            TEST_PASSWORD
        );

        final NiFiProperties serverProperties = createServerProperties(zkServerConfig);

        zkServer = ZooKeeperStateServer.create(serverProperties);

        if (zkServer != null) zkServer.start();
    }

    @AfterClass
    public static void cleanup() {
        if (zkServer != null) zkServer.shutdown();

        if (tempDir != null) {
            final List<Path> files = Arrays.asList(
                dataDir.resolve("version-2/snapshot.0"),
                dataDir.resolve("version-2/log.1"),
                dataDir.resolve("version-2"),
                dataDir.resolve("myid"),
                dataDir,
                serverKeyStore,
                serverTrustStore,
                clientKeyStore,
                clientTrustStore,
                zkServerConfig,
                tempDir
            );

            files.forEach(p -> {
                try {
                    if (p != null) Files.deleteIfExists(p);
                } catch (final IOException ioe) {}
            });
        }
    }

    @Test
    public void testZooKeeperServerCreated() {
        Assert.assertNotNull(zkServer);
    }

    @Test
    public void testZooKeeperServerCreatePath() throws Exception {
        final ZooKeeperClientConfig zkClientConfig =
            ZooKeeperClientConfig.createConfig(clientProperties);

        final CuratorFrameworkFactory.Builder clientBuilder = CuratorFrameworkFactory.builder()
            .connectString(zkClientConfig.getConnectString())
            .sessionTimeoutMs(zkClientConfig.getSessionTimeoutMillis())
            .connectionTimeoutMs(zkClientConfig.getConnectionTimeoutMillis())
            .retryPolicy(new RetryOneTime(1000))
            .defaultData(new byte[0])
            .zookeeperFactory(new SecureClientZookeeperFactory(zkClientConfig));

        final CuratorFramework client = clientBuilder.build();

        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);

        Assert.assertEquals(createResult, testPath);
        Assert.assertNotNull(checkExistsResult);
    }

    private static void createZooKeeperServerConfig(final Path dataDir, final int clientPort,
        final Path keyStore, final String keyStorePassword, final Path trustStore,
        final String trustStorePassword, final String serverConnectionFactory,
        final Path zkServerConfig) throws IOException {

        try (final PrintWriter writer = new PrintWriter(zkServerConfig.toFile())) {
            writer.println("tickTime=2000");
            writer.println(String.format("dataDir=%s", dataDir));
            writer.println("initLimit=10");
            writer.println("syncLimit=5");
            writer.println("4lw.commands.whitelist=ruok");
            writer.println(String.format("serverCnxnFactory=%s", serverConnectionFactory));
            writer.println(String.format("secureClientPort=%d", clientPort));
            writer.println(String.format("ssl.keyStore.location=%s", keyStore));
            writer.println(String.format("ssl.keyStore.password=%s", keyStorePassword));
            writer.println(String.format("ssl.trustStore.location=%s", trustStore));
            writer.println(String.format("ssl.trustStore.password=%s", trustStorePassword));
        }
    }

    private static NiFiProperties createClientProperties(final int clientPort,
        final Path keyStore, final String keyStorePassword, final Path trustStore,
        final String trustStorePassword) {

        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, String.format("localhost:%d", clientPort));
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_CONNECTION_SOCKET, NETTY_CLIENT_CNXN_SOCKET);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SSL_KEYSTORE_LOCATION, keyStore.toString());
        properties.setProperty(NiFiProperties.ZOOKEEPER_SSL_KEYSTORE_PASSWORD, keyStorePassword);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SSL_TRUSTSTORE_LOCATION, trustStore.toString());
        properties.setProperty(NiFiProperties.ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD, trustStorePassword);

        return new StandardNiFiProperties(properties);
    }

    private static NiFiProperties createServerProperties(final Path zkServerConfig) {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, zkServerConfig.toString());
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_START_EMBEDDED_ZOOKEEPER, "true");

        return new StandardNiFiProperties(properties);
    }

    private static X509Certificate createKeyStore(final String alias,
        final String password, final Path path, final String keyStoreType)
        throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        try (final FileOutputStream outputStream = new FileOutputStream(path.toFile())) {
            final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();

            final X509Certificate selfSignedCert = CertificateUtils.generateSelfSignedX509Certificate(
                keyPair, "CN=localhost", "SHA256withRSA", 365
            );

            final char[] passwordChars = password.toCharArray();
            final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, keyPair.getPrivate(), passwordChars,
                new Certificate[]{selfSignedCert});
            keyStore.store(outputStream, passwordChars);

            return selfSignedCert;
        }
    }

    private static void createTrustStore(final X509Certificate cert,
        final String alias, final String password, final Path path, final String keyStoreType)
        throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        try (final FileOutputStream outputStream = new FileOutputStream(path.toFile())) {
            final KeyStore trustStore = KeyStore.getInstance(keyStoreType);
            trustStore.load(null, null);
            trustStore.setCertificateEntry(alias, cert);
            trustStore.store(outputStream, password.toCharArray());
        }
    }

}
