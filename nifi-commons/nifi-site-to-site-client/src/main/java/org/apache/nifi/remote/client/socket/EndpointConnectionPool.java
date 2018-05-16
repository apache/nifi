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
package org.apache.nifi.remote.client.socket;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.RemoteDestination;
import org.apache.nifi.remote.RemoteResourceInitiator;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.PeerSelector;
import org.apache.nifi.remote.client.PeerStatusProvider;
import org.apache.nifi.remote.client.SiteInfoProvider;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.exception.UnreachableClusterException;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.remote.util.EventReportUtil.error;
import static org.apache.nifi.remote.util.EventReportUtil.warn;

public class EndpointConnectionPool implements PeerStatusProvider {

    private static final Logger logger = LoggerFactory.getLogger(EndpointConnectionPool.class);

    private final ConcurrentMap<PeerDescription, BlockingQueue<EndpointConnection>> connectionQueueMap = new ConcurrentHashMap<>();

    private final Set<EndpointConnection> activeConnections = Collections.synchronizedSet(new HashSet<>());

    private final EventReporter eventReporter;
    private final SSLContext sslContext;
    private final ScheduledExecutorService taskExecutor;
    private final int idleExpirationMillis;
    private final RemoteDestination remoteDestination;

    private volatile int commsTimeout;
    private volatile boolean shutdown = false;

    private final SiteInfoProvider siteInfoProvider;
    private final PeerSelector peerSelector;
    private final InetAddress localAddress;

    public EndpointConnectionPool(final RemoteDestination remoteDestination, final int commsTimeoutMillis, final int idleExpirationMillis,
        final SSLContext sslContext, final EventReporter eventReporter, final File persistenceFile, final SiteInfoProvider siteInfoProvider,
        final InetAddress localAddress) {
        Objects.requireNonNull(remoteDestination, "Remote Destination/Port Identifier cannot be null");

        this.remoteDestination = remoteDestination;
        this.sslContext = sslContext;
        this.eventReporter = eventReporter;
        this.commsTimeout = commsTimeoutMillis;
        this.idleExpirationMillis = idleExpirationMillis;
        this.localAddress = localAddress;

        this.siteInfoProvider = siteInfoProvider;

        peerSelector = new PeerSelector(this, persistenceFile);
        peerSelector.setEventReporter(eventReporter);

        // Initialize a scheduled executor and run some maintenance tasks in the background to kill off old, unused
        // connections and keep our list of peers up-to-date.
        taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName("NiFi Site-to-Site Connection Pool Maintenance");
                thread.setDaemon(true);
                return thread;
            }
        });

        taskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                peerSelector.refreshPeers();
            }
        }, 0, 5, TimeUnit.SECONDS);

        taskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                cleanupExpiredSockets();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private String getPortIdentifier(final TransferDirection transferDirection) throws IOException {
        if (remoteDestination.getIdentifier() != null) {
            return remoteDestination.getIdentifier();
        }
        return siteInfoProvider.getPortIdentifier(remoteDestination.getName(), transferDirection);
    }

    public EndpointConnection getEndpointConnection(final TransferDirection direction) throws IOException {
        return getEndpointConnection(direction, null);
    }

    public EndpointConnection getEndpointConnection(final TransferDirection direction, final SiteToSiteClientConfig config) throws IOException {
        //
        // Attempt to get a connection state that already exists for this URL.
        //
        FlowFileCodec codec = null;
        CommunicationsSession commsSession = null;
        SocketClientProtocol protocol = null;
        EndpointConnection connection;
        Peer peer = null;

        final URI clusterUrl;
        try {
            clusterUrl = siteInfoProvider.getActiveClusterUrl();
        } catch (final IOException ioe) {
            throw new UnreachableClusterException("Unable to refresh details from any of the configured remote instances.", ioe);
        }

        do {
            final List<EndpointConnection> addBack = new ArrayList<>();
            logger.debug("{} getting next peer status", this);
            final PeerStatus peerStatus = peerSelector.getNextPeerStatus(direction);
            logger.debug("{} next peer status = {}", this, peerStatus);
            if (peerStatus == null) {
                return null;
            }

            final PeerDescription peerDescription = peerStatus.getPeerDescription();
            BlockingQueue<EndpointConnection> connectionQueue = connectionQueueMap.get(peerDescription);
            if (connectionQueue == null) {
                connectionQueue = new LinkedBlockingQueue<>();
                BlockingQueue<EndpointConnection> existing = connectionQueueMap.putIfAbsent(peerDescription, connectionQueue);
                if (existing != null) {
                    connectionQueue = existing;
                }
            }

            try {
                connection = connectionQueue.poll();
                logger.debug("{} Connection State for {} = {}", this, clusterUrl, connection);
                final String portId = getPortIdentifier(direction);

                if (connection == null && !addBack.isEmpty()) {
                    // all available connections have been penalized.
                    logger.debug("{} all Connections for {} are penalized; returning no Connection", this, portId);
                    return null;
                }

                if (connection != null && connection.getPeer().isPenalized(portId)) {
                    // we have a connection, but it's penalized. We want to add it back to the queue
                    // when we've found one to use.
                    addBack.add(connection);
                    continue;
                }

                // if we can't get an existing Connection, create one
                if (connection == null) {
                    logger.debug("{} No Connection available for Port {}; creating new Connection", this, portId);
                    protocol = new SocketClientProtocol();
                    protocol.setDestination(new IdEnrichedRemoteDestination(remoteDestination, portId));
                    protocol.setEventReporter(eventReporter);

                    final long penalizationMillis = remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS);
                    try {
                        logger.debug("{} Establishing site-to-site connection with {}", this, peerStatus);
                        commsSession = establishSiteToSiteConnection(peerStatus);
                    } catch (final IOException ioe) {
                        peerSelector.penalize(peerStatus.getPeerDescription(), penalizationMillis);
                        throw ioe;
                    }

                    final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
                    final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
                    try {
                        logger.debug("{} Negotiating protocol", this);
                        RemoteResourceInitiator.initiateResourceNegotiation(protocol, dis, dos);
                    } catch (final HandshakeException e) {
                        try {
                            commsSession.close();
                        } catch (final IOException ioe) {
                            throw e;
                        }
                    }

                    final String peerUrl = "nifi://" + peerDescription.getHostname() + ":" + peerDescription.getPort();
                    peer = new Peer(peerDescription, commsSession, peerUrl, clusterUrl.toString());

                    // set properties based on config
                    if (config != null) {
                        protocol.setTimeout((int) config.getTimeout(TimeUnit.MILLISECONDS));
                        protocol.setPreferredBatchCount(config.getPreferredBatchCount());
                        protocol.setPreferredBatchSize(config.getPreferredBatchSize());
                        protocol.setPreferredBatchDuration(config.getPreferredBatchDuration(TimeUnit.MILLISECONDS));
                    }

                    // perform handshake
                    try {
                        logger.debug("{} performing handshake", this);
                        protocol.handshake(peer);

                        // handle error cases
                        if (protocol.isDestinationFull()) {
                            logger.warn("{} {} indicates that port {}'s destination is full; penalizing peer",
                                    this, peer, config.getPortName() == null ? config.getPortIdentifier() : config.getPortName());

                            peerSelector.penalize(peer, penalizationMillis);
                            try {
                                peer.close();
                            } catch (final IOException ioe) {
                            }

                            continue;
                        } else if (protocol.isPortInvalid()) {
                            peerSelector.penalize(peer, penalizationMillis);
                            cleanup(protocol, peer);
                            throw new PortNotRunningException(peer.toString() + " indicates that port " + portId + " is not running");
                        } else if (protocol.isPortUnknown()) {
                            peerSelector.penalize(peer, penalizationMillis);
                            cleanup(protocol, peer);
                            throw new UnknownPortException(peer.toString() + " indicates that port " + portId + " is not known");
                        }

                        // negotiate the FlowFileCodec to use
                        logger.debug("{} negotiating codec", this);
                        codec = protocol.negotiateCodec(peer);
                        logger.debug("{} negotiated codec is {}", this, codec);
                    } catch (final PortNotRunningException | UnknownPortException e) {
                        throw e;
                    } catch (final Exception e) {
                        peerSelector.penalize(peer, penalizationMillis);
                        cleanup(protocol, peer);

                        final String message = String.format("%s failed to communicate with %s due to %s", this, peer == null ? clusterUrl : peer, e.toString());
                        error(logger, eventReporter, message);
                        if (logger.isDebugEnabled()) {
                            logger.error("", e);
                        }
                        throw e;
                    }

                    connection = new EndpointConnection(peer, protocol, codec);
                } else {
                    final long lastTimeUsed = connection.getLastTimeUsed();
                    final long millisSinceLastUse = System.currentTimeMillis() - lastTimeUsed;

                    if (commsTimeout > 0L && millisSinceLastUse >= commsTimeout) {
                        cleanup(connection.getSocketClientProtocol(), connection.getPeer());
                        connection = null;
                    } else {
                        codec = connection.getCodec();
                        peer = connection.getPeer();
                        commsSession = peer.getCommunicationsSession();
                        protocol = connection.getSocketClientProtocol();
                    }
                }
            } catch (final Throwable t) {
                if (commsSession != null) {
                    try {
                        commsSession.close();
                    } catch (final IOException ioe) {
                    }
                }

                throw t;
            } finally {
                if (!addBack.isEmpty()) {
                    connectionQueue.addAll(addBack);
                    addBack.clear();
                }
            }

        } while (connection == null || codec == null || commsSession == null || protocol == null);

        activeConnections.add(connection);
        return connection;
    }

    public boolean offer(final EndpointConnection endpointConnection) {
        final Peer peer = endpointConnection.getPeer();
        if (peer == null) {
            return false;
        }

        final BlockingQueue<EndpointConnection> connectionQueue = connectionQueueMap.get(peer.getDescription());
        if (connectionQueue == null) {
            return false;
        }

        activeConnections.remove(endpointConnection);
        if (shutdown) {
            terminate(endpointConnection);
            return false;
        } else {
            endpointConnection.setLastTimeUsed();
            return connectionQueue.offer(endpointConnection);
        }
    }

    private void cleanup(final SocketClientProtocol protocol, final Peer peer) {
        if (protocol != null && peer != null) {
            try {
                protocol.shutdown(peer);
            } catch (final TransmissionDisabledException e) {
                // User disabled transmission.... do nothing.
                logger.debug(this + " Transmission Disabled by User");
            } catch (IOException e1) {
            }
        }

        if (peer != null) {
            try {
                peer.close();
            } catch (final TransmissionDisabledException e) {
                // User disabled transmission.... do nothing.
                logger.debug(this + " Transmission Disabled by User");
            } catch (IOException e1) {
            }
        }
    }

    @Override
    public PeerDescription getBootstrapPeerDescription() throws IOException {
        final String hostname = siteInfoProvider.getActiveClusterUrl().getHost();
        final Integer port = siteInfoProvider.getSiteToSitePort();
        if (port == null) {
            throw new IOException("Remote instance of NiFi is not configured to allow RAW Socket site-to-site communications");
        }

        final boolean secure = siteInfoProvider.isSecure();
        return new PeerDescription(hostname, port, secure);
    }

    @Override
    public Set<PeerStatus> fetchRemotePeerStatuses(final PeerDescription peerDescription) throws IOException {
        final String hostname = peerDescription.getHostname();
        final int port = peerDescription.getPort();
        final URI clusterUrl = siteInfoProvider.getActiveClusterUrl();

        final PeerDescription clusterPeerDescription = new PeerDescription(hostname, port, clusterUrl.toString().startsWith("https://"));
        final CommunicationsSession commsSession = establishSiteToSiteConnection(hostname, port);

        final Peer peer = new Peer(clusterPeerDescription, commsSession, "nifi://" + hostname + ":" + port, clusterUrl.toString());
        final SocketClientProtocol clientProtocol = new SocketClientProtocol();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        RemoteResourceInitiator.initiateResourceNegotiation(clientProtocol, dis, dos);

        clientProtocol.setTimeout(commsTimeout);
        if (clientProtocol.getVersionNegotiator().getVersion() < 5) {
            String portId = getPortIdentifier(TransferDirection.RECEIVE);
            if (portId == null) {
                portId = getPortIdentifier(TransferDirection.SEND);
            }

            if (portId == null) {
                peer.close();
                throw new IOException("Failed to determine the identifier of port " + remoteDestination.getName());
            }
            clientProtocol.handshake(peer, portId);
        } else {
            clientProtocol.handshake(peer, null);
        }

        final Set<PeerStatus> peerStatuses = clientProtocol.getPeerStatuses(peer);

        try {
            clientProtocol.shutdown(peer);
        } catch (final IOException e) {
            final String message = String.format("%s Failed to shutdown protocol when updating list of peers due to %s", this, e.toString());
            warn(logger, eventReporter, message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        try {
            peer.close();
        } catch (final IOException e) {
            final String message = String.format("%s Failed to close resources when updating list of peers due to %s", this, e.toString());
            warn(logger, eventReporter, message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        return peerStatuses;
    }

    private CommunicationsSession establishSiteToSiteConnection(final PeerStatus peerStatus) throws IOException {
        final PeerDescription description = peerStatus.getPeerDescription();
        return establishSiteToSiteConnection(description.getHostname(), description.getPort());
    }

    private CommunicationsSession establishSiteToSiteConnection(final String hostname, final int port) throws IOException {
        final boolean siteToSiteSecure = siteInfoProvider.isSecure();

        CommunicationsSession commsSession = null;
        try {
            if (siteToSiteSecure) {
                if (sslContext == null) {
                    throw new IOException("Unable to communicate with " + hostname + ":" + port
                            + " because it requires Secure Site-to-Site communications, but this instance is not configured for secure communications");
                }

                final SSLSocketChannel socketChannel = new SSLSocketChannel(sslContext, hostname, port, localAddress, true);
                socketChannel.connect();

                commsSession = new SSLSocketChannelCommunicationsSession(socketChannel);

                try {
                    commsSession.setUserDn(socketChannel.getDn());
                } catch (final CertificateException ex) {
                    throw new IOException(ex);
                }
            } else {
                final SocketChannel socketChannel = SocketChannel.open();
                if (localAddress != null) {
                    final SocketAddress localSocketAddress = new InetSocketAddress(localAddress, 0);
                    socketChannel.socket().bind(localSocketAddress);
                }

                socketChannel.socket().connect(new InetSocketAddress(hostname, port), commsTimeout);
                socketChannel.socket().setSoTimeout(commsTimeout);

                commsSession = new SocketChannelCommunicationsSession(socketChannel);
            }

            commsSession.getOutput().getOutputStream().write(CommunicationsSession.MAGIC_BYTES);
        } catch (final IOException ioe) {
            if (commsSession != null) {
                commsSession.close();
            }

            throw ioe;
        }

        return commsSession;
    }

    private void cleanupExpiredSockets() {
        for (final BlockingQueue<EndpointConnection> connectionQueue : connectionQueueMap.values()) {
            final List<EndpointConnection> connections = new ArrayList<>();

            EndpointConnection connection;
            while ((connection = connectionQueue.poll()) != null) {
                // If the socket has not been used in 10 seconds, shut it down.
                final long lastUsed = connection.getLastTimeUsed();
                if (lastUsed < System.currentTimeMillis() - idleExpirationMillis) {
                    try {
                        connection.getSocketClientProtocol().shutdown(connection.getPeer());
                    } catch (final Exception e) {
                        logger.debug("Failed to shut down {} using {} due to {}",
                                connection.getSocketClientProtocol(), connection.getPeer(), e);
                    }

                    terminate(connection);
                } else {
                    connections.add(connection);
                }
            }

            connectionQueue.addAll(connections);
        }
    }

    public void shutdown() {
        shutdown = true;
        taskExecutor.shutdown();
        peerSelector.clear();

        for (final EndpointConnection conn : activeConnections) {
            conn.getPeer().getCommunicationsSession().interrupt();
        }

        for (final BlockingQueue<EndpointConnection> connectionQueue : connectionQueueMap.values()) {
            EndpointConnection state;
            while ((state = connectionQueue.poll()) != null) {
                terminate(state);
            }
        }
    }

    public void terminate(final EndpointConnection connection) {
        activeConnections.remove(connection);
        cleanup(connection.getSocketClientProtocol(), connection.getPeer());
    }

    @Override
    public String toString() {
        return "EndpointConnectionPool[Cluster URL=" + siteInfoProvider.getClusterUrls() + "]";
    }

    private class IdEnrichedRemoteDestination implements RemoteDestination {

        private final RemoteDestination original;
        private final String identifier;

        public IdEnrichedRemoteDestination(final RemoteDestination original, final String identifier) {
            this.original = original;
            this.identifier = identifier;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getName() {
            return original.getName();
        }

        @Override
        public long getYieldPeriod(final TimeUnit timeUnit) {
            return original.getYieldPeriod(timeUnit);
        }

        @Override
        public boolean isUseCompression() {
            return original.isUseCompression();
        }
    }


}
