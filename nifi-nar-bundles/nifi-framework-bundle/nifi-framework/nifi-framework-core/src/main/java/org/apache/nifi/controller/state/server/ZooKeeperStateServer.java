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
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperStateServer extends ZooKeeperServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperStateServer.class);

    private final QuorumPeerConfig quorumPeerConfig;
    private volatile boolean started = false;

    private ServerCnxnFactory connectionFactory;
    private FileTxnSnapLog transactionLog;
    private ZooKeeperServer embeddedZkServer;
    private QuorumPeer quorumPeer;
    private DatadirCleanupManager datadirCleanupManager;

    private ZooKeeperStateServer(final Properties zkProperties) throws IOException, ConfigException {
        quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(zkProperties);
    }

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        if (quorumPeerConfig.getPurgeInterval() > 0) {
            datadirCleanupManager = new DatadirCleanupManager(quorumPeerConfig
                    .getDataDir(), quorumPeerConfig.getDataLogDir(), quorumPeerConfig
                    .getSnapRetainCount(), quorumPeerConfig.getPurgeInterval());
            datadirCleanupManager.start();
        }

        if (quorumPeerConfig.isDistributed()) {
            startDistributed();
        } else {
            startStandalone();
        }

        started = true;
    }

    private void startStandalone() throws IOException {
        logger.info("Starting Embedded ZooKeeper Server");

        final ServerConfig config = new ServerConfig();
        config.readFrom(quorumPeerConfig);
        try {
            transactionLog = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));

            embeddedZkServer = new ZooKeeperServer();
            embeddedZkServer.setTxnLogFactory(transactionLog);
            embeddedZkServer.setTickTime(config.getTickTime());
            embeddedZkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            embeddedZkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());

            connectionFactory = ServerCnxnFactory.createFactory();
            connectionFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
            connectionFactory.startup(embeddedZkServer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Embedded ZooKeeper Server interrupted", e);
        } catch (final IOException ioe) {
            throw new IOException("Failed to start embedded ZooKeeper Server", ioe);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start embedded ZooKeeper Server", e);
        }
    }

    private void startDistributed() throws IOException {
        logger.info("Starting Embedded ZooKeeper Peer");

        try {
            transactionLog = new FileTxnSnapLog(new File(quorumPeerConfig.getDataLogDir()), new File(quorumPeerConfig.getDataDir()));

            connectionFactory = ServerCnxnFactory.createFactory();
            connectionFactory.configure(quorumPeerConfig.getClientPortAddress(), quorumPeerConfig.getMaxClientCnxns());

            quorumPeer = new QuorumPeer();
            quorumPeer.setClientPortAddress(quorumPeerConfig.getClientPortAddress());
            quorumPeer.setTxnFactory(new FileTxnSnapLog(new File(quorumPeerConfig.getDataLogDir()), new File(quorumPeerConfig.getDataDir())));
            quorumPeer.setQuorumPeers(quorumPeerConfig.getServers());
            quorumPeer.setElectionType(quorumPeerConfig.getElectionAlg());
            quorumPeer.setMyid(quorumPeerConfig.getServerId());
            quorumPeer.setTickTime(quorumPeerConfig.getTickTime());
            quorumPeer.setMinSessionTimeout(quorumPeerConfig.getMinSessionTimeout());
            quorumPeer.setMaxSessionTimeout(quorumPeerConfig.getMaxSessionTimeout());
            quorumPeer.setInitLimit(quorumPeerConfig.getInitLimit());
            quorumPeer.setSyncLimit(quorumPeerConfig.getSyncLimit());
            quorumPeer.setQuorumVerifier(quorumPeerConfig.getQuorumVerifier());
            quorumPeer.setCnxnFactory(connectionFactory);
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
            quorumPeer.setLearnerType(quorumPeerConfig.getPeerType());
            quorumPeer.setSyncEnabled(quorumPeerConfig.getSyncEnabled());
            quorumPeer.setQuorumListenOnAllIPs(quorumPeerConfig.getQuorumListenOnAllIPs());

            quorumPeer.start();
        } catch (final IOException ioe) {
            throw new IOException("Failed to start embedded ZooKeeper Peer", ioe);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start embedded ZooKeeper Peer", e);
        }
    }

    @Override
    public synchronized void shutdown() {
        if (started) {
            started = false;

            if (transactionLog != null) {
                try {
                    transactionLog.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close Transaction Log", ioe);
                }
            }

            if (connectionFactory != null) {
                connectionFactory.shutdown();
            }

            if (quorumPeer != null && quorumPeer.isRunning()) {
                quorumPeer.shutdown();
            }

            if (embeddedZkServer != null && embeddedZkServer.isRunning()) {
                embeddedZkServer.shutdown();
            }

            if (datadirCleanupManager != null) {
                datadirCleanupManager.shutdown();
            }
        }
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
