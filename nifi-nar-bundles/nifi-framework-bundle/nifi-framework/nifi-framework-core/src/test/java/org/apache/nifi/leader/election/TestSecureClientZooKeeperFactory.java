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

import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.controller.leader.election.CuratorLeaderElectionManager.SecureClientZooKeeperFactory;

import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestSecureClientZooKeeperFactory {

    private static final String NETTY_SERVER_CNXN_FACTORY =
        "org.apache.zookeeper.server.NettyServerCnxnFactory";

    private static final String CLIENT_KEYSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/client.keystore.p12";
    private static final String CLIENT_TRUSTSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/client.truststore.p12";
    private static final String SERVER_KEYSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/server.keystore.p12";
    private static final String SERVER_TRUSTSTORE = "src/test/resources/TestSecureClientZooKeeperFactory/server.truststore.p12";
    private static final String KEYSTORE_TYPE = "PKCS12";
    private static final String TEST_PASSWORD = "testpass";

    private static ZooKeeperServer zkServer;
    private static ServerCnxnFactory serverConnectionFactory;
    private static NiFiProperties clientProperties;
    private static Path tempDir;
    private static Path dataDir;
    private static int clientPort;

    @BeforeClass
    public static void setup() throws IOException, GeneralSecurityException, InterruptedException {
        tempDir = Paths.get("target/TestSecureClientZooKeeperFactory");
        dataDir = tempDir.resolve("state");
        clientPort = InstanceSpec.getRandomPort();

        Files.createDirectory(tempDir);

        clientProperties = createClientProperties(
            clientPort,
            CLIENT_KEYSTORE,
            KEYSTORE_TYPE,
            TEST_PASSWORD,
            CLIENT_TRUSTSTORE,
            KEYSTORE_TYPE,
            TEST_PASSWORD
        );

        serverConnectionFactory = createAndStartServer(
            dataDir,
            tempDir,
            clientPort,
            SERVER_KEYSTORE,
            TEST_PASSWORD,
            SERVER_TRUSTSTORE,
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

        Assert.assertEquals(createResult, testPath);
        Assert.assertNotNull(checkExistsResult);
    }

    private static ServerCnxnFactory createAndStartServer(final Path dataDir,
        final Path tempDir, final int clientPort, final String keyStore,
        final String keyStorePassword, final String trustStore,
        final String trustStorePassword) throws IOException, InterruptedException {

        final ClientX509Util x509Util = new ClientX509Util();
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, NETTY_SERVER_CNXN_FACTORY);
        System.setProperty(x509Util.getSslAuthProviderProperty(), "x509");
        System.setProperty(x509Util.getSslKeystoreLocationProperty(), keyStore);
        System.setProperty(x509Util.getSslKeystorePasswdProperty(), keyStorePassword);
        System.setProperty(x509Util.getSslTruststoreLocationProperty(), trustStore);
        System.setProperty(x509Util.getSslTruststorePasswdProperty(), trustStorePassword);
        System.setProperty("zookeeper.authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");

        ZooKeeperServer zkServer = new ZooKeeperServer(dataDir.toFile(), dataDir.toFile(), 2000);
        ServerCnxnFactory secureConnectionFactory = ServerCnxnFactory.createFactory(clientPort, -1);
        secureConnectionFactory.configure(new InetSocketAddress(clientPort), -1, true);
        secureConnectionFactory.startup(zkServer);

        return secureConnectionFactory;
    }

    private static NiFiProperties createClientProperties(final int clientPort,
        final String keyStore, final String keyStoreType, final String keyStorePassword,
        final String trustStore, final String trustStoreType, final String trustStorePassword) {

        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, String.format("localhost:%d", clientPort));
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "true");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, keyStore);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, keyStoreType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, keyStorePassword);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, trustStore);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, trustStoreType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, trustStorePassword);

        return new StandardNiFiProperties(properties);
    }

}
