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
package org.apache.nifi.leader.election;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.nifi.controller.cluster.SecureClientZooKeeperFactory;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ITSecureClientZooKeeperFactory {

    private static final String NETTY_SERVER_CNXN_FACTORY =
        "org.apache.zookeeper.server.NettyServerCnxnFactory";

    private static final String KEYSTORE_TYPE = "PKCS12";
    private static final String KEYSTORE_TYPE_EXT = ".p12";
    private static final String TEST_PASSWORD = "testpass";

    private static ZooKeeperServer zkServer;
    private static ServerCnxnFactory serverConnectionFactory;
    private static NiFiProperties clientProperties;
    private static Path tempDir;
    private static Path dataDir;
    private static Path serverKeyStore;
    private static Path serverTrustStore;
    private static Path clientKeyStore;
    private static Path clientTrustStore;
    private static int clientPort;

    @BeforeClass
    public static void setup() throws IOException, GeneralSecurityException, InterruptedException {
        tempDir = Paths.get("target/TestSecureClientZooKeeperFactory");
        dataDir = tempDir.resolve("state");
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

        clientProperties = createSecureClientProperties(
            clientPort,
            clientKeyStore,
            KEYSTORE_TYPE,
            TEST_PASSWORD,
            clientTrustStore,
            KEYSTORE_TYPE,
            TEST_PASSWORD
        );

        serverConnectionFactory = createAndStartServer(
            dataDir,
            tempDir,
            clientPort,
            serverKeyStore,
            TEST_PASSWORD,
            serverTrustStore,
            TEST_PASSWORD
        );

        zkServer = serverConnectionFactory.getZooKeeperServer();
    }

    @AfterClass
    public static void cleanup() {
        if (serverConnectionFactory != null) {
            try {
                serverConnectionFactory.shutdown();
            } catch (final Exception ignore) {}

            try {
                zkServer.shutdown();
            } catch (final Exception ignore) {}
        }

        if (tempDir != null) {
            final List<Path> files = Arrays.asList(
                dataDir.resolve("version-2/snapshot.0"),
                dataDir.resolve("version-2/log.1"),
                dataDir.resolve("version-2"),
                dataDir.resolve("myid"),
                serverKeyStore,
                serverTrustStore,
                clientKeyStore,
                clientTrustStore,
                dataDir,
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
    public void testValidCnxnSocketName() {
        assertEquals("org.apache.zookeeper.ClientCnxnSocketNetty", SecureClientZooKeeperFactory.NETTY_CLIENT_CNXN_SOCKET);
    }

    @Test(timeout = 30_000)
    public void testServerCreatePath() throws Exception {
        final ZooKeeperClientConfig zkClientConfig =
            ZooKeeperClientConfig.createConfig(clientProperties);

        final CuratorFrameworkFactory.Builder clientBuilder = CuratorFrameworkFactory.builder()
            .connectString(zkClientConfig.getConnectString())
            .sessionTimeoutMs(zkClientConfig.getSessionTimeoutMillis())
            .connectionTimeoutMs(zkClientConfig.getConnectionTimeoutMillis())
            .retryPolicy(new RetryOneTime(1000))
            .defaultData(new byte[0])
            .zookeeperFactory(new SecureClientZooKeeperFactory(zkClientConfig));

        final CuratorFramework client = clientBuilder.build();

        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);

        assertEquals(createResult, testPath);
        assertNotNull(checkExistsResult);
    }

    public static ServerCnxnFactory createAndStartServer(final Path dataDir,
                                                         final Path tempDir, final int clientPort, final Path keyStore,
                                                         final String keyStorePassword, final Path trustStore,
                                                         final String trustStorePassword) throws IOException, InterruptedException {

        final ClientX509Util x509Util = new ClientX509Util();
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, NETTY_SERVER_CNXN_FACTORY);
        System.setProperty(x509Util.getSslAuthProviderProperty(), "x509");
        System.setProperty(x509Util.getSslKeystoreLocationProperty(), keyStore.toString());
        System.setProperty(x509Util.getSslKeystorePasswdProperty(), keyStorePassword);
        System.setProperty(x509Util.getSslTruststoreLocationProperty(), trustStore.toString());
        System.setProperty(x509Util.getSslTruststorePasswdProperty(), trustStorePassword);
        System.setProperty("zookeeper.authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");

        ZooKeeperServer zkServer = new ZooKeeperServer(dataDir.toFile(), dataDir.toFile(), 2000);
        ServerCnxnFactory secureConnectionFactory = ServerCnxnFactory.createFactory(clientPort, -1);
        secureConnectionFactory.configure(new InetSocketAddress(clientPort), -1, true);
        secureConnectionFactory.startup(zkServer);

        return secureConnectionFactory;
    }

    public static NiFiProperties createSecureClientProperties(final int clientPort,
                                                              final Path keyStore, final String keyStoreType, final String keyStorePassword,
                                                              final Path trustStore, final String trustStoreType, final String trustStorePassword) {

        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, String.format("localhost:%d", clientPort));
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, Boolean.TRUE.toString());
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, keyStore.toString());
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, keyStoreType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, keyStorePassword);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, trustStore.toString());
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, trustStorePassword);

        return new NiFiProperties(properties);
    }

    public static X509Certificate createKeyStore(final String alias,
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

    public static void createTrustStore(final X509Certificate cert,
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