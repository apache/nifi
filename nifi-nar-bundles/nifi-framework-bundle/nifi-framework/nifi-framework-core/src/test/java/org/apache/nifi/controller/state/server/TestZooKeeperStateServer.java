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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.common.X509Exception.SSLContextException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestZooKeeperStateServer {

    private static ZooKeeperStateServer zkServer;
    private static Path tempDir;
    private static Path dataDir;
    private static Path zkServerConfig;
    private static int clientPort;

    @BeforeClass
    public static void setup() throws IOException, ConfigException {
        tempDir = Paths.get("target/TestZooKeeperStateServer");
        dataDir = tempDir.resolve("state");
        zkServerConfig = tempDir.resolve("zookeeper.properties");
        clientPort = InstanceSpec.getRandomPort();

        Files.createDirectory(tempDir);

        try (final PrintWriter writer = new PrintWriter(zkServerConfig.toFile())) {
            writer.println("tickTime=2000");
            writer.println(String.format("dataDir=%s", dataDir));
            writer.println(String.format("clientPort=%d", clientPort));
            writer.println("initLimit=10");
            writer.println("syncLimit=5");
            writer.println("4lw.commands.whitelist=ruok");
        }

        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES, zkServerConfig.toString());
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_START_EMBEDDED_ZOOKEEPER, Boolean.TRUE.toString());

        zkServer = ZooKeeperStateServer.create(new NiFiProperties(properties));

        if (zkServer != null) zkServer.start();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        if (zkServer != null) {
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
                zkServerConfig,
                tempDir
            );

            files.forEach(p -> {
                try {
                    if (p != null) Files.deleteIfExists(p);
                } catch (final IOException ignore) {}
            });
        }
    }

    @Test
    public void testServerCreated() {
        Assert.assertNotNull(zkServer);
    }

    @Test
    public void testServerOk() throws IOException, SSLContextException {
        final String imok = FourLetterWordMain.send4LetterWord("localhost",
            clientPort, "ruok", false, 1000);
        Assert.assertEquals(imok, "imok\n");
    }

    @Test
    public void testServerCreatePath() throws Exception {
        final CuratorFramework client =
            CuratorFrameworkFactory.newClient(
                String.format("localhost:%d", clientPort),
                new RetryOneTime(1000));

        client.start();
        final String testPath = "/test";
        final String createResult = client.create().forPath(testPath, new byte[0]);
        final Stat checkExistsResult = client.checkExists().forPath(testPath);

        Assert.assertEquals(createResult, testPath);
        Assert.assertNotNull(checkExistsResult);
    }

}
