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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ZooKeeperStateServer extends ZooKeeperServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperStateServer.class);

    static final int MIN_AVAILABLE_PORT = 2288;
    static final String SERVER_CNXN_FACTORY = "org.apache.zookeeper.server.NettyServerCnxnFactory";
    static final String ZOOKEEPER_SSL_QUORUM = "sslQuorum";
    static final String ZOOKEEPER_PORT_UNIFICATION = "portUnification";

    static final Map<String, String> ZOOKEEPER_TLS_TO_NIFI_PROPERTIES = new HashMap<String, String>() {{
        put("keyStore.location", "security.keystore");
        put("keyStore.password", "security.keystorePasswd");
        put("keyStore.type", "security.keystoreType");
        put("trustStore.location", "security.truststore");
        put("trustStore.password", "security.truststorePasswd");
        put("trustStore.type", "security.truststoreType");
    }};

    static final String ZOOKEEPER_QUORUM_TLS_PREFIX = "ssl.quorum";
    static final String ZOOKEEPER_TLS_PREFIX = "ssl";
    static final String ZOOKEEPER_SECURITY_PROPERTY_PREFIX = "nifi.zookeeper";
    static final String NIFI_SECURITY_PROPERTY_PREFIX = "nifi";

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

    // Expose the configuration for verification + testing:
    final QuorumPeerConfig getQuorumPeerConfig() {
        return quorumPeerConfig;
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
            for (int i=0; i < 10; i++) {
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
            connectionFactory.configure(getAvailableSocketAddress(config), config.getMaxClientCnxns(), quorumPeerConfig.isSslQuorum());
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
            connectionFactory.configure(getAvailableSocketAddress(quorumPeerConfig), quorumPeerConfig.getMaxClientCnxns(), quorumPeerConfig.isSslQuorum());

            quorumPeer = new QuorumPeer();

            // Set the secure connection factory if the quorum is supposed to be secure.
            if(quorumPeerConfig.isSslQuorum()) {
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

        return new ZooKeeperStateServer(reconcileProperties(properties, zkProperties));
    }

    /**
     * Reconcile properties between the nifi.properties and zookeeper.properties (zoo.cfg) files. Most of the ZooKeeper server properties are derived from
     * the zookeeper.properties file, while the TLS key/truststore properties are taken from nifi.properties.
     * @param niFiProperties NiFiProperties file containing ZooKeeper client and TLS configuration
     * @param zkProperties The zookeeper.properties file containing Zookeeper server configuration
     * @return A reconciled QuorumPeerConfig which will include TLS properties set if they are available.
     * @throws IOException If configuration files fail to parse.
     * @throws ConfigException If secure configuration is not as expected. Check administration documentation.
     */
    private static QuorumPeerConfig reconcileProperties(NiFiProperties niFiProperties, Properties zkProperties) throws IOException, ConfigException {
        QuorumPeerConfig peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(zkProperties);

        // If secureClientPortAddress is set but no TLS config is set, fail to start.
        final boolean isTLSConfigPresent = niFiProperties.isTlsConfigurationPresent() || niFiProperties.isZooKeeperTlsConfigurationPresent();
        if (peerConfig.getSecureClientPortAddress() != null && !isTLSConfigPresent) {
            throw new ConfigException(
                    String.format("Property secureClientPort was set in %s but there was no TLS config present in nifi.properties",
                    niFiProperties.getProperty(NiFiProperties.STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES)));
        }

        // If this is an insecure NiFi no changes are needed:
        if (!isTLSConfigPresent) {
            logger.info("ZooKeeper is not secure because appropriate TLS configuration was not provided. Please refer to administration guide.");
            return peerConfig;
        }

        // Otherwise the following sets secure TLS settings for embedded Zookeeper

        // Remove plaintext client ports and addresses and warn if set, see NIFI-7203:
        InetSocketAddress clientPort = peerConfig.getClientPortAddress();
        if (clientPort != null) {
            zkProperties.remove("clientPort");
            zkProperties.remove("clientPortAddress");
            logger.warn("Invalid configuration detected: secure NiFi with embedded ZooKeeper configured for insecure connections. " +
                    "Removed insecure port from embedded ZooKeeper configuration to deactivate insecure connections.");
        }

        // Set the TLS properties, preferring ZK TLS from nifi.properties over the default TLS properties.
        for (Map.Entry<String, String> propertyMapping : ZOOKEEPER_TLS_TO_NIFI_PROPERTIES.entrySet()) {
            String preferredValue = getPreferredTLSProperty(niFiProperties, propertyMapping.getValue());
            zkProperties.setProperty(String.join(".", ZOOKEEPER_TLS_PREFIX, propertyMapping.getKey()), preferredValue);
            zkProperties.setProperty(String.join(".", ZOOKEEPER_QUORUM_TLS_PREFIX, propertyMapping.getKey()), preferredValue);
        }

        // Set TLS client port:
        zkProperties.setProperty("secureClientPort", getSecurePort(peerConfig));

        // Set the required connection factory for TLS
        final String cnxnPropKey = "serverCnxnFactory";
        zkProperties.setProperty(cnxnPropKey, SERVER_CNXN_FACTORY);
        zkProperties.setProperty(ZOOKEEPER_SSL_QUORUM, "true");

        // Port unification allows both secure and insecure connections - setting to false means only secure connections will be allowed.
        zkProperties.setProperty(ZOOKEEPER_PORT_UNIFICATION, "false");

        // Recreate and reload the adjusted properties to ensure they're still valid for ZK:
        peerConfig = new QuorumPeerConfig();
        peerConfig.parseProperties(zkProperties);
        return peerConfig;
    }

    private static String getSecurePort(QuorumPeerConfig peerConfig) throws ConfigException {
        final InetSocketAddress secureClientAddress = peerConfig.getSecureClientPortAddress();
        String secureClientPort = null;

        if (secureClientAddress != null && String.valueOf(secureClientAddress.getPort()) != null) {
            secureClientPort = String.valueOf(secureClientAddress.getPort());
            logger.info("Secure client port retrieved from ZooKeeper configuration: {}", secureClientPort);
            return secureClientPort;
        } else {
            throw new ConfigException(String.format("NiFi was configured to be secure but ZooKeeper secureClientPort could not be retrieved from zookeeper.properties file."));
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

    /**
     * Retrieves individual TLS properties - choosing ZooKeeper TLS properties first or NiFi TLS properties as an alternative.
     * @param properties The NiFi properties configuration.
     * @param propertyName The preferred property to get from NiFi properties.
     * @return Returns the property in order of the above preference.
     */
    private static String getPreferredTLSProperty(final NiFiProperties properties, String propertyName) throws ConfigException {
        String zooKeeperTLSProperty = String.join(".", ZOOKEEPER_SECURITY_PROPERTY_PREFIX, propertyName);
        String nifiTLSProperty = String.join(".", NIFI_SECURITY_PROPERTY_PREFIX, propertyName);

        if(properties.isZooKeeperTlsConfigurationPresent()) {
            return properties.getProperty(zooKeeperTLSProperty);
        } else if(properties.isTlsConfigurationPresent()) {
            return properties.getProperty(nifiTLSProperty);
        } else {
            throw new ConfigException(String.format("Both properties %s and %s were missing. Either one must be set to start ZooKeeper with TLS.", zooKeeperTLSProperty, nifiTLSProperty));
        }
    }
}
