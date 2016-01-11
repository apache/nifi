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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperStateServer extends ZooKeeperServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperStateServer.class);

    private QuorumPeerConfig quorumPeerConfig;

    private ZooKeeperStateServer(final Properties zkProperties) throws IOException, ConfigException {
        quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(zkProperties);
    }

    public synchronized void start() throws IOException {
        logger.info("Starting Embedded ZooKeeper Server");

        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumPeerConfig);
        try {
            runFromConfig(serverConfig);
        } catch (final IOException ioe) {
            throw new IOException("Failed to start embedded ZooKeeper Server", ioe);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start embedded ZooKeeper Server", e);
        }
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
    }

    public static ZooKeeperStateServer create(final NiFiProperties properties) throws IOException, ConfigException {
        final File propsFile = properties.getEmbeddedZooKeeperPropertiesFile();
        if (propsFile == null) {
            return null;
        }

        if (!propsFile.exists() || !propsFile.canRead()) {
            throw new IOException("Cannot create Embedded ZooKeeper Server because the Properties File " + propsFile.getAbsolutePath()
                + " referenced in nifi.properties does not exist or cannot be read");
        }

        final Properties zkProperties = new Properties();
        try (final InputStream fis = new FileInputStream(propsFile);
            final InputStream bis = new BufferedInputStream(fis)) {
            zkProperties.load(bis);
        }

        return new ZooKeeperStateServer(zkProperties);
    }
}
