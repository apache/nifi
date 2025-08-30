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

package org.apache.nifi.controller.state.providers.zookeeper.server;

import org.apache.nifi.framework.cluster.zookeeper.ZooKeeperClientConfig;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

public class ZooKeeperStateServer extends ZooKeeperServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperStateServer.class);

    private static final int MIN_PORT = 1024;
    private static final int MAX_PORT = 65535;
    private static final String ZOOKEEPER_SSL_QUORUM = "sslQuorum";
    private static final String ZOOKEEPER_PORT_UNIFICATION = "portUnification";
    private static final String ZOOKEEPER_SERVER_CNXN_FACTORY = "serverCnxnFactory";
    private final QuorumPeerConfig quorumPeerConfig;
    private volatile boolean started = false;

    private ServerCnxnFactory connectionFactory;
    private FileTxnSnapLog transactionLog;
    private ZooKeeperServer embeddedZkServer;
    private QuorumPeer quorumPeer;
    private DatadirCleanupManager datadirCleanupManager;

    private ZooKeeperStateServer(final QuorumPeerConfig config) {
        quorumPeerConfig = config;
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
            for (int i = 0; i < 10; i++) {
                try {
                    transactionLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
                    break;
                } catch (final FileTxnSnapLog.DatadirException dde) {
                    // The constructor for FileTxnSnapLog sometimes throws a DatadirException indicating that it is unable to create data directory,
                    // but the data directory already exists. It appears to be a race condition with another ZooKeeper thread. Even if we create the
                    // directory before entering the constructor, we sometimes see the issue occur. So we just give it up to 10 tries
                    try {
                        Thread.sleep(50L);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            embeddedZkServer = new ZooKeeperServer();
            embeddedZkServer.setTxnLogFactory(transactionLog);
            embeddedZkServer.setTickTime(config.getTickTime());
            embeddedZkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            embeddedZkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());

            connectionFactory = ServerCnxnFactory.createFactory();
            final int listenBacklog = quorumPeerConfig.getClientPortListenBacklog();
            connectionFactory.configure(getAvailableSocketAddress(config), config.getMaxClientCnxns(), listenBacklog, quorumPeerConfig.isSslQuorum());
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
            transactionLog = new FileTxnSnapLog(quorumPeerConfig.getDataLogDir(), quorumPeerConfig.getDataDir());
            connectionFactory = ServerCnxnFactory.createFactory();
            final int listenBacklog = quorumPeerConfig.getClientPortListenBacklog();
            connectionFactory.configure(getAvailableSocketAddress(quorumPeerConfig), quorumPeerConfig.getMaxClientCnxns(), listenBacklog, quorumPeerConfig.isSslQuorum());

            quorumPeer = new QuorumPeer();

            // Set the secure connection factory if the quorum is supposed to be secure.
            if (quorumPeerConfig.isSslQuorum()) {
                quorumPeer.setSecureCnxnFactory(connectionFactory);
            } else {
                quorumPeer.setCnxnFactory(connectionFactory);
            }

            quorumPeer.setTxnFactory(new FileTxnSnapLog(quorumPeerConfig.getDataLogDir(), quorumPeerConfig.getDataDir()));
            quorumPeer.setElectionType(quorumPeerConfig.getElectionAlg());
            quorumPeer.setMyid(quorumPeerConfig.getServerId());
            quorumPeer.setTickTime(quorumPeerConfig.getTickTime());
            quorumPeer.setMinSessionTimeout(quorumPeerConfig.getMinSessionTimeout());
            quorumPeer.setMaxSessionTimeout(quorumPeerConfig.getMaxSessionTimeout());
            quorumPeer.setInitLimit(quorumPeerConfig.getInitLimit());
            quorumPeer.setSyncLimit(quorumPeerConfig.getSyncLimit());
            quorumPeer.setQuorumVerifier(quorumPeerConfig.getQuorumVerifier(), false);
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
            quorumPeer.setLearnerType(quorumPeerConfig.getPeerType());
            quorumPeer.setSyncEnabled(quorumPeerConfig.getSyncEnabled());
            quorumPeer.setQuorumListenOnAllIPs(quorumPeerConfig.getQuorumListenOnAllIPs());
            quorumPeer.setSslQuorum(quorumPeerConfig.isSslQuorum());

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

            if (quorumPeer != null && quorumPeer.isRunning()) {
                quorumPeer.shutdown();
            }

            if (connectionFactory != null) {
                try {
                    connectionFactory.shutdown();
                } catch (Exception e) {
                    logger.warn("Failed to shutdown Connection Factory", e);
                }
            }

            if (embeddedZkServer != null && embeddedZkServer.isRunning()) {
                try {
                    embeddedZkServer.shutdown();
                } catch (Exception e) {
                    logger.warn("Failed to shutdown Embedded Zookeeper", e);
                }
            }

            if (transactionLog != null) {
                try {
                    transactionLog.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close Transaction Log", ioe);
                }
            }

            if (datadirCleanupManager != null) {
                datadirCleanupManager.shutdown();
            }
        }
    }

    public static ZooKeeperStateServer create(final NiFiProperties properties) throws IOException, ConfigException {
        final File propsFile = properties.getEmbeddedZooKeeperPropertiesFile();
        if (propsFile == null) {
            throw new IllegalStateException("Embedded ZooKeeper Properties not configured");
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

        return new ZooKeeperStateServer(reconcileProperties(properties, zkProperties));
    }

    /**
     * Reconcile properties between the nifi.properties and zookeeper.properties (zoo.cfg) files. Most of the ZooKeeper server properties are derived from
     * the zookeeper.properties file, while the TLS key/truststore properties are taken from nifi.properties.
     * @param niFiProperties NiFiProperties file containing ZooKeeper client and TLS configuration
     * @param zkProperties The zookeeper.properties file containing ZooKeeper server configuration
     * @return A reconciled QuorumPeerConfig which will include TLS properties set if they are available.
     * @throws IOException If configuration files fail to parse.
     * @throws ConfigException If secure configuration is not as expected. Check administration documentation.
     */
    private static QuorumPeerConfig reconcileProperties(NiFiProperties niFiProperties, Properties zkProperties) throws IOException, ConfigException {
        QuorumPeerConfig peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(zkProperties);

        final boolean niFiConfigIsSecure = isNiFiConfigSecureForZooKeeper(niFiProperties);
        final boolean zooKeeperConfigIsSecure = isZooKeeperConfigSecure(peerConfig);

        if (!zooKeeperConfigIsSecure && !niFiConfigIsSecure) {
            logger.debug("{} property is set to false or is not present, and zookeeper.properties file does not contain secureClientPort property, so embedded ZooKeeper will be started without TLS",
                    NiFiProperties.ZOOKEEPER_CLIENT_SECURE);
            return peerConfig;
        }

        // If secureClientPort is set but no TLS config is set, fail to start.
        if (zooKeeperConfigIsSecure && !niFiConfigIsSecure) {
            throw new ConfigException(
                    String.format("ZooKeeper properties file %s was configured to be secure but there was no valid TLS config present in nifi.properties or " +
                                  "nifi.zookeeper.client.secure was set to false. Check the administration guide",
                                   niFiProperties.getProperty(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES)));
        }

        // Remove any insecure ports if they were set in zookeeper.properties
        ensureOnlySecurePortsAreEnabled(peerConfig, zkProperties);

        // Set base ZooKeeper TLS server properties
        setTlsProperties(zkProperties, new ZooKeeperServerX509Util(), niFiProperties);
        // Set quorum ZooKeeper TLS server properties
        setTlsProperties(zkProperties, new ZooKeeperQuorumX509Util(), niFiProperties);
        // Set TLS client port:
        zkProperties.setProperty("secureClientPort", getSecurePort(peerConfig));

        // Set the required connection factory for TLS
        zkProperties.setProperty(ZOOKEEPER_SERVER_CNXN_FACTORY, NettyServerCnxnFactory.class.getName());
        zkProperties.setProperty(ZOOKEEPER_SSL_QUORUM, Boolean.TRUE.toString());

        // Port unification allows both secure and insecure connections - setting to false means only secure connections will be allowed.
        zkProperties.setProperty(ZOOKEEPER_PORT_UNIFICATION, Boolean.FALSE.toString());

        // Recreate and reload the adjusted properties to ensure they're still valid for ZK:
        peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(zkProperties);
        return peerConfig;
    }

    private static boolean isZooKeeperConfigSecure(QuorumPeerConfig peerConfig) throws ConfigException {
        InetSocketAddress secureAddress = peerConfig.getSecureClientPortAddress();
        InetSocketAddress insecureAddress = peerConfig.getClientPortAddress();

        if (secureAddress == null && insecureAddress == null) {
            throw new ConfigException("No clientAddress or secureClientAddress is set in zookeeper.properties");
        }

        return secureAddress != null;
    }

    /**
     * Verify whether the NiFi properties portion of ZooKeeper are correctly configured for TLS or not
     * @param niFiProperties The loaded nifi.properties
     * @return True if NiFi has TLS configuration and the property nifi.zookeeper.client.secure=true, otherwise false or configuration exception
     * @throws ConfigException If nifi.zookeeper.client.secure=true but no TLS configuration is present
     */
    private static boolean isNiFiConfigSecureForZooKeeper(NiFiProperties niFiProperties) throws ConfigException {
        final boolean isTlsConfigPresent = niFiProperties.isZooKeeperTlsConfigurationPresent() || niFiProperties.isTlsConfigurationPresent();
        final boolean isZooKeeperClientSecure = niFiProperties.isZooKeeperClientSecure();

        if (isZooKeeperClientSecure && !isTlsConfigPresent) {
            throw new ConfigException(String.format("%s is true but no TLS configuration is present in nifi.properties", NiFiProperties.ZOOKEEPER_CLIENT_SECURE));
        }

        return (isZooKeeperClientSecure && isTlsConfigPresent);
    }

    private static void ensureOnlySecurePortsAreEnabled(QuorumPeerConfig config, Properties zkProperties) {

        // Remove plaintext client ports and addresses and warn if set, see NIFI-7203:
        InetSocketAddress clientPort = config.getClientPortAddress();
        InetSocketAddress secureClientPort = config.getSecureClientPortAddress();

        if (clientPort != null && secureClientPort != null) {
            zkProperties.remove("clientPort");
            zkProperties.remove("clientPortAddress");
            logger.warn("Invalid configuration was detected: A secure NiFi with an embedded ZooKeeper was configured for insecure connections. " +
                    "Insecure ports have been removed from embedded ZooKeeper configuration to deactivate insecure connections");
        }
    }

    private static void setTlsProperties(Properties zooKeeperProperties, X509Util zooKeeperUtil, NiFiProperties niFiProperties) {
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslKeystoreLocationProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, NiFiProperties.SECURITY_KEYSTORE));
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslKeystorePasswdProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, NiFiProperties.SECURITY_KEYSTORE_PASSWD));
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslKeystoreTypeProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, NiFiProperties.SECURITY_KEYSTORE_TYPE));
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslTruststoreLocationProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, NiFiProperties.SECURITY_TRUSTSTORE));
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslTruststorePasswdProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
        zooKeeperProperties.setProperty(zooKeeperUtil.getSslTruststoreTypeProperty(),
                ZooKeeperClientConfig.getPreferredProperty(niFiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
    }

    private static String getSecurePort(QuorumPeerConfig peerConfig) throws ConfigException {
        final InetSocketAddress secureClientAddress = peerConfig.getSecureClientPortAddress();
        String secureClientPort = null;

        if (secureClientAddress != null && secureClientAddress.getPort() >= MIN_PORT && secureClientAddress.getPort() <= MAX_PORT) {
            secureClientPort = String.valueOf(secureClientAddress.getPort());
            if (logger.isDebugEnabled()) {
                logger.debug("Secure client port retrieved from ZooKeeper configuration: {}", secureClientPort);
            }
            return secureClientPort;
        } else {
            throw new ConfigException(String.format("NiFi was configured to be secure but secureClientPort could not be retrieved from zookeeper.properties file or it was not " +
                    "in valid port range %d - %d", MIN_PORT, MAX_PORT));
        }
    }

    private static InetSocketAddress getAvailableSocketAddress(ServerConfig config) {
        return config.getSecureClientPortAddress() != null ? config.getSecureClientPortAddress() : config.getClientPortAddress();
    }

    private static InetSocketAddress getAvailableSocketAddress(QuorumPeerConfig quorumConfig) {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumConfig);
        return getAvailableSocketAddress(serverConfig);
    }
}
