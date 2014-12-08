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
package org.apache.nifi.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateExpiredException;
import javax.security.cert.CertificateNotYetValidException;

import org.apache.nifi.cluster.ClusterNodeInformation;
import org.apache.nifi.cluster.NodeInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.SchedulingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientHandlerException;

public class StandardRemoteGroupPort extends AbstractPort implements RemoteGroupPort {
    public static final String USER_AGENT = "NiFi-Site-to-Site";
    public static final String CONTENT_TYPE = "application/octet-stream";
    
    public static final int GZIP_COMPRESSION_LEVEL = 1;
    public static final long PEER_REFRESH_PERIOD = 60000L;
    
    private static final String CATEGORY = "Site to Site";
    
    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteGroupPort.class);
    private final RemoteProcessGroup remoteGroup;
    private final SSLContext sslContext;
    private final AtomicBoolean useCompression = new AtomicBoolean(false);
    private final AtomicBoolean targetExists = new AtomicBoolean(true);
    private final AtomicBoolean targetRunning = new AtomicBoolean(true);
    private final AtomicLong peerIndex = new AtomicLong(0L);
    
    private volatile List<PeerStatus> peerStatuses;
    private volatile long peerRefreshTime = 0L;
    private final ReentrantLock peerRefreshLock = new ReentrantLock();
    
    private final ConcurrentMap<String, BlockingQueue<EndpointConnectionState>> endpointConnectionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<PeerStatus, Long> peerTimeoutExpirations = new ConcurrentHashMap<>();
    
    private final Set<CommunicationsSession> activeCommsChannels = new HashSet<>();
    private final Lock interruptLock = new ReentrantLock();
    private boolean shutdown = false;   // guarded by codecLock
    
    public StandardRemoteGroupPort(final String id, final String name, final ProcessGroup processGroup, final RemoteProcessGroup remoteGroup, 
            final TransferDirection direction, final ConnectableType type, final SSLContext sslContext, final ProcessScheduler scheduler) {
        // remote group port id needs to be unique but cannot just be the id of the port
        // in the remote group instance. this supports referencing the same remote
        // instance more than once.
        super(id, name, processGroup, type, scheduler);
        
        this.remoteGroup = remoteGroup;
        this.sslContext = sslContext;
        setScheduldingPeriod(MINIMUM_SCHEDULING_NANOS + " nanos");
    }
    
    @Override
    public boolean isTargetRunning() {
        return targetRunning.get();
    }

    public void setTargetRunning(boolean targetRunning) {
        this.targetRunning.set(targetRunning);
    }
    
    @Override
    public boolean isTriggerWhenEmpty() {
        return getConnectableType() == ConnectableType.REMOTE_OUTPUT_PORT;
    }
    
    @Override
    public void shutdown() {
        super.shutdown();
        
        peerTimeoutExpirations.clear();
        interruptLock.lock();
        try {
            this.shutdown = true;
            
            for ( final CommunicationsSession commsSession : activeCommsChannels ) {
                commsSession.interrupt();
            }
            
            for ( final BlockingQueue<EndpointConnectionState> queue : endpointConnectionMap.values() ) {
                EndpointConnectionState state;
                while ( (state = queue.poll()) != null)  {
                    cleanup(state.getSocketClientProtocol(), state.getPeer());
                }
            }
            
            endpointConnectionMap.clear();
        } finally {
            interruptLock.unlock();
        }
    }
    
    @Override
    public void onSchedulingStart() {
        super.onSchedulingStart();
        
        interruptLock.lock();
        try {
            this.shutdown = false;
        } finally {
            interruptLock.unlock();
        }
    }
    
    
    void cleanupSockets() {
        final List<EndpointConnectionState> states = new ArrayList<>();
        
        for ( final BlockingQueue<EndpointConnectionState> queue : endpointConnectionMap.values() ) {
            states.clear();
            
            EndpointConnectionState state;
            while ((state = queue.poll()) != null) {
                // If the socket has not been used in 10 seconds, shut it down.
                final long lastUsed = state.getLastTimeUsed();
                if ( lastUsed < System.currentTimeMillis() - 10000L ) {
                    try {
                        state.getSocketClientProtocol().shutdown(state.getPeer());
                    } catch (final Exception e) {
                        logger.debug("Failed to shut down {} using {} due to {}", 
                            new Object[] {state.getSocketClientProtocol(), state.getPeer(), e} );
                    }
                    
                    cleanup(state.getSocketClientProtocol(), state.getPeer());
                } else {
                    states.add(state);
                }
            }
            
            queue.addAll(states);
        }
    }
    
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if ( !remoteGroup.isTransmitting() ) {
            logger.debug("{} {} is not transmitting; will not send/receive", this, remoteGroup);
            return;
        }

        if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT && session.getQueueSize().getObjectCount() == 0 ) {
            logger.debug("{} No data to send", this);
            return;
        }
        
        String url = getRemoteProcessGroup().getTargetUri().toString();
        Peer peer = null;
        final PeerStatus peerStatus = getNextPeerStatus();
        if ( peerStatus == null ) {
            logger.debug("{} Unable to determine the next peer to communicate with; all peers must be penalized, so yielding context", this);
            context.yield();
            return;
        }
        
        url = "nifi://" + peerStatus.getHostname() + ":" + peerStatus.getPort();
        
        //
        // Attempt to get a connection state that already exists for this URL.
        //
        BlockingQueue<EndpointConnectionState> connectionStateQueue = endpointConnectionMap.get(url);
        if ( connectionStateQueue == null ) {
            connectionStateQueue = new LinkedBlockingQueue<>();
            BlockingQueue<EndpointConnectionState> existingQueue = endpointConnectionMap.putIfAbsent(url, connectionStateQueue);
            if ( existingQueue != null ) {
                connectionStateQueue = existingQueue;
            }
        }
        
        FlowFileCodec codec = null;
        CommunicationsSession commsSession = null;
        SocketClientProtocol protocol = null;
        EndpointConnectionState connectionState;
        
        do {
            connectionState = connectionStateQueue.poll();
            logger.debug("{} Connection State for {} = {}", this, url, connectionState);
            
            // if we can't get an existing ConnectionState, create one
            if ( connectionState == null ) {
                protocol = new SocketClientProtocol();
                protocol.setPort(this);
    
                try {
                    commsSession = establishSiteToSiteConnection(peerStatus);
                    final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
                    final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
                    try {
                        RemoteResourceFactory.initiateResourceNegotiation(protocol, dis, dos);
                    } catch (final HandshakeException e) {
                        try {
                            commsSession.close();
                        } catch (final IOException ioe) {
                            final String message = String.format("%s unable to close communications session %s due to %s; resources may not be appropriately cleaned up",
                                this, commsSession, ioe.toString());
                            logger.error(message);
                            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
                        }
                    }
                } catch (final IOException e) {
                    final String message = String.format("%s failed to communicate with %s due to %s", this, peer == null ? url : peer, e.toString());
                    logger.error(message);
                    if ( logger.isDebugEnabled() ) {
                        logger.error("", e);
                    }
                    remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
                    session.rollback();
                    return;
                }
                
                
                peer = new Peer(commsSession, url);
                
                // perform handshake
                try {
                    protocol.handshake(peer);
                    
                    // handle error cases
                    if ( protocol.isDestinationFull() ) {
                        logger.warn("{} {} indicates that port's destination is full; penalizing peer", this, peer);
                        penalize(peer);
                        cleanup(protocol, peer);
                        return;
                    } else if ( protocol.isPortInvalid() ) {
                        penalize(peer);
                        context.yield();
                        cleanup(protocol, peer);
                        this.targetRunning.set(false);
                        final String message = String.format("%s failed to communicate with %s because the remote instance indicates that the port is not in a valid state", this, peer);
                        logger.error(message);
                        remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
                        return;
                    } else if ( protocol.isPortUnknown() ) {
                        penalize(peer);
                        context.yield();
                        cleanup(protocol, peer);
                        this.targetExists.set(false);
                        final String message = String.format("%s failed to communicate with %s because the remote instance indicates that the port no longer exists", this, peer);
                        logger.error(message);
                        remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
                        return;
                    }
                    
                    // negotiate the FlowFileCodec to use
                    codec = protocol.negotiateCodec(peer);
                } catch (final Exception e) {
                    penalize(peer);
                    cleanup(protocol, peer);
                    
                    final String message = String.format("%s failed to communicate with %s due to %s", this, peer == null ? url : peer, e.toString());
                    logger.error(message);
                    if ( logger.isDebugEnabled() ) {
                        logger.error("", e);
                    }
                    remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
                    session.rollback();
                    return;                    
                }
                
                connectionState = new EndpointConnectionState(peer, protocol, codec);
            } else {
                final long lastTimeUsed = connectionState.getLastTimeUsed();
                final long millisSinceLastUse = System.currentTimeMillis() - lastTimeUsed;
                final long timeoutMillis = remoteGroup.getCommunicationsTimeout(TimeUnit.MILLISECONDS);
                
                if ( timeoutMillis > 0L && millisSinceLastUse >= timeoutMillis ) {
                    cleanup(connectionState.getSocketClientProtocol(), connectionState.getPeer());
                    connectionState = null;
                } else {
                    codec = connectionState.getCodec();
                    peer = connectionState.getPeer();
                    commsSession = peer.getCommunicationsSession();
                    protocol = connectionState.getSocketClientProtocol();
                }
            }
        } while ( connectionState == null || codec == null || commsSession == null || protocol == null );
        
            
        try {
            interruptLock.lock();
            try {
                if ( shutdown ) {
                    peer.getCommunicationsSession().interrupt();
                }
                
                activeCommsChannels.add(peer.getCommunicationsSession());
            } finally {
                interruptLock.unlock();
            }
            
            if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT ) {
                transferFlowFiles(peer, protocol, context, session, codec);
            } else {
                receiveFlowFiles(peer, protocol, context, session, codec);
            }

            if ( peer.isPenalized() ) {
                logger.debug("{} {} was penalized", this, peer);
                penalize(peer);
            }
            
            interruptLock.lock();
            try {
                if ( shutdown ) {
                    peer.getCommunicationsSession().interrupt();
                }
                
                activeCommsChannels.remove(peer.getCommunicationsSession());
            } finally {
                interruptLock.unlock();
            }

            session.commit();
            
            connectionState.setLastTimeUsed();
            connectionStateQueue.add(connectionState);
        } catch (final TransmissionDisabledException e) {
            cleanup(protocol, peer);
            session.rollback();
        } catch (final Exception e) {
            penalize(peer);

            final String message = String.format("%s failed to communicate with %s (%s) due to %s", this, peer == null ? url : peer, protocol, e.toString());
            logger.error(message);
            if ( logger.isDebugEnabled() ) {
                logger.error("", e);
            }
            
            cleanup(protocol, peer);
            
            remoteGroup.getEventReporter().reportEvent(Severity.ERROR, CATEGORY, message);
            session.rollback();
        }
    }

    
    /**
     * Updates internal state map to penalize a PeerStatus that points to the specified peer
     * @param peer
     */
    private void penalize(final Peer peer) {
        String host;
        int port;
        try {
            final URI uri = new URI(peer.getUrl());
            host = uri.getHost();
            port = uri.getPort();
        } catch (final URISyntaxException e) {
            host = peer.getHost();
            port = -1;
        }
        
        final PeerStatus status = new PeerStatus(host, port, true, 1);
        Long expiration = peerTimeoutExpirations.get(status);
        if ( expiration == null ) {
            expiration = Long.valueOf(0L);
        }
        
        final long penalizationMillis = getYieldPeriod(TimeUnit.MILLISECONDS);
        final long newExpiration = Math.max(expiration, System.currentTimeMillis() + penalizationMillis);
        peerTimeoutExpirations.put(status, Long.valueOf(newExpiration));
    }
    
    
    private void cleanup(final SocketClientProtocol protocol, final Peer peer) {
        if ( protocol != null && peer != null ) {
            try {
                protocol.shutdown(peer);
            } catch (final TransmissionDisabledException e) {
                // User disabled transmission.... do nothing.
                logger.debug(this + " Transmission Disabled by User");
            } catch (IOException e1) {
            }
        }
        
        if ( peer != null ) {
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
    public String getYieldPeriod() {
        // delegate yield duration to remote process group
        return remoteGroup.getYieldDuration();
    }
    
    public CommunicationsSession establishSiteToSiteConnection(final PeerStatus peerStatus) throws IOException {
        final String destinationUri = "nifi://" + peerStatus.getHostname() + ":" + peerStatus.getPort();

        CommunicationsSession commsSession = null;
        try {
        if ( peerStatus.isSecure() ) {
            if ( sslContext == null ) {
                throw new IOException("Unable to communicate with " + peerStatus.getHostname() + ":" + peerStatus.getPort() + " because it requires Secure Site-to-Site communications, but this instance is not configured for secure communications");
            }
            
            final SSLSocketChannel socketChannel = new SSLSocketChannel(sslContext, peerStatus.getHostname(), peerStatus.getPort(), true);
                socketChannel.connect();
    
            commsSession = new SSLSocketChannelCommunicationsSession(socketChannel, destinationUri);
                
                try {
                    commsSession.setUserDn(socketChannel.getDn());
                } catch (final CertificateNotYetValidException | CertificateExpiredException ex) {
                    throw new IOException(ex);
                }
        } else {
            final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(peerStatus.getHostname(), peerStatus.getPort()));
            commsSession = new SocketChannelCommunicationsSession(socketChannel, destinationUri);
        }

        commsSession.getOutput().getOutputStream().write(CommunicationsSession.MAGIC_BYTES);

        commsSession.setUri(destinationUri);
        } catch (final IOException ioe) {
            if ( commsSession != null ) {
                commsSession.close();
            }
            
            throw ioe;
        }
        
        return commsSession;
    }
    
    private PeerStatus getNextPeerStatus() {
        List<PeerStatus> peerList = peerStatuses;
        if ( (peerList == null || peerList.isEmpty() || System.currentTimeMillis() > peerRefreshTime + PEER_REFRESH_PERIOD) && peerRefreshLock.tryLock() ) {
            try {
                try {
                    peerList = createPeerStatusList();
                } catch (final IOException | BadRequestException | HandshakeException | UnknownPortException | PortNotRunningException | ClientHandlerException e) {
                    final String message = String.format("%s Failed to update list of peers due to %s", this, e.toString());
                    logger.warn(message);
                    if ( logger.isDebugEnabled() ) {
                        logger.warn("", e);
                    }
                    remoteGroup.getEventReporter().reportEvent(Severity.WARNING, CATEGORY, message);
                }
                
                this.peerStatuses = peerList;
                peerRefreshTime = System.currentTimeMillis();
            } finally {
                peerRefreshLock.unlock();
            }
        }

        if ( peerList == null || peerList.isEmpty() ) {
            return null;
        }

        PeerStatus peerStatus;
        for (int i=0; i < peerList.size(); i++) {
            final long idx = peerIndex.getAndIncrement();
            final int listIndex = (int) (idx % peerList.size());
            peerStatus = peerList.get(listIndex);
            
            if ( isPenalized(peerStatus) ) {
                logger.debug("{} {} is penalized; will not communicate with this peer", this, peerStatus);
            } else {
                return peerStatus;
            }
        }
        
        logger.debug("{} All peers appear to be penalized; returning null", this);
        return null;
    }
    
    private boolean isPenalized(final PeerStatus peerStatus) {
        final Long expirationEnd = peerTimeoutExpirations.get(peerStatus);
        return (expirationEnd == null ? false : expirationEnd > System.currentTimeMillis() );
    }
    
    private List<PeerStatus> createPeerStatusList() throws IOException, BadRequestException, HandshakeException, UnknownPortException, PortNotRunningException {
        final Set<PeerStatus> statuses = remoteGroup.getPeerStatuses();
        if ( statuses == null ) {
            return new ArrayList<>();
        }
        
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> nodeInfos = new ArrayList<>();
        for ( final PeerStatus peerStatus : statuses ) {
            final NodeInformation nodeInfo = new NodeInformation(peerStatus.getHostname(), peerStatus.getPort(), 0, peerStatus.isSecure(), peerStatus.getFlowFileCount());
            nodeInfos.add(nodeInfo);
        }
        clusterNodeInfo.setNodeInformation(nodeInfos);
        return formulateDestinationList(clusterNodeInfo);
    }
    
    private void transferFlowFiles(final Peer peer, final ClientProtocol protocol, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        protocol.transferFlowFiles(peer, context, session, codec);
    }
    
    private void receiveFlowFiles(final Peer peer, final ClientProtocol protocol, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        protocol.receiveFlowFiles(peer, context, session, codec);
    }

    private List<PeerStatus> formulateDestinationList(final ClusterNodeInformation clusterNodeInfo) throws IOException {
        return formulateDestinationList(clusterNodeInfo, getConnectableType());
    }
    
    static List<PeerStatus> formulateDestinationList(final ClusterNodeInformation clusterNodeInfo, final ConnectableType connectableType) {
        final Collection<NodeInformation> nodeInfoSet = clusterNodeInfo.getNodeInformation();
        final int numDestinations = Math.max(128, nodeInfoSet.size());
        final Map<NodeInformation, Integer> entryCountMap = new HashMap<>();

        long totalFlowFileCount = 0L;
        for (final NodeInformation nodeInfo : nodeInfoSet) {
            totalFlowFileCount += nodeInfo.getTotalFlowFiles();
        }

        int totalEntries = 0;
        for (final NodeInformation nodeInfo : nodeInfoSet) {
            final int flowFileCount = nodeInfo.getTotalFlowFiles();
            // don't allow any node to get more than 80% of the data
            final double percentageOfFlowFiles = Math.min(0.8D, ((double) flowFileCount / (double) totalFlowFileCount));
            final double relativeWeighting = (connectableType == ConnectableType.REMOTE_INPUT_PORT) ? (1 - percentageOfFlowFiles) : percentageOfFlowFiles;
            final int entries = Math.max(1, (int) (numDestinations * relativeWeighting));
            
            entryCountMap.put(nodeInfo, Math.max(1, entries));
            totalEntries += entries;
        }
        
        final List<PeerStatus> destinations = new ArrayList<>(totalEntries);
        for (int i=0; i < totalEntries; i++) {
            destinations.add(null);
        }
        for ( final Map.Entry<NodeInformation, Integer> entry : entryCountMap.entrySet() ) {
            final NodeInformation nodeInfo = entry.getKey();
            final int numEntries = entry.getValue();
            
            int skipIndex = numEntries;
            for (int i=0; i < numEntries; i++) {
                int n = (skipIndex * i);
                while (true) {
                    final int index = n % destinations.size();
                    PeerStatus status = destinations.get(index);
                    if ( status == null ) {
                        status = new PeerStatus(nodeInfo.getHostname(), nodeInfo.getSiteToSitePort(), nodeInfo.isSiteToSiteSecure(), nodeInfo.getTotalFlowFiles());
                        destinations.set(index, status);
                        break;
                    } else {
                        n++;
                    }
                }
            }
        }

        final StringBuilder distributionDescription = new StringBuilder();
        distributionDescription.append("New Weighted Distribution of Nodes:");
        for ( final Map.Entry<NodeInformation, Integer> entry : entryCountMap.entrySet() ) {
            final double percentage = entry.getValue() * 100D / (double) destinations.size();
            distributionDescription.append("\n").append(entry.getKey()).append(" will receive ").append(percentage).append("% of FlowFiles");
        }
        logger.info(distributionDescription.toString());

        // Jumble the list of destinations.
        return destinations;
    }
    
    
    @Override
    public boolean getTargetExists() {
        return targetExists.get();
    }

    @Override
    public boolean isValid() {
        return getValidationErrors().isEmpty();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final Collection<ValidationResult> validationErrors = new ArrayList<>();
        ValidationResult error = null;
        if (!targetExists.get()) {
            error = new ValidationResult.Builder()
                .explanation(String.format("Remote instance indicates that port '%s' no longer exists.", getName()))
                .subject(String.format("Remote port '%s'", getName()))
                .valid(false)
                .build();
        } else if ( getConnectableType() == ConnectableType.REMOTE_OUTPUT_PORT && getConnections(Relationship.ANONYMOUS).isEmpty() ) {
            error = new ValidationResult.Builder()
                .explanation(String.format("Port '%s' has no outbound connections", getName()))
                .subject(String.format("Remote port '%s'", getName()))
                .valid(false)
                .build();
        }
        
        if ( error != null ) {
            validationErrors.add(error);
        }
        
        return validationErrors;
    }
    
    @Override
    public void verifyCanStart() {
        super.verifyCanStart();
        
        if ( getConnectableType() == ConnectableType.REMOTE_INPUT_PORT && getIncomingConnections().isEmpty() ) {
            throw new IllegalStateException("Port " + getName() + " has no incoming connections");
        }
    }
    
    @Override
    public void setUseCompression(final boolean useCompression) {
        this.useCompression.set(useCompression);
    }
    
    @Override
    public boolean isUseCompression() {
        return useCompression.get();
    }
    
    @Override
    public String toString() {
        return "RemoteGroupPort[name=" + getName() + ",target=" + remoteGroup.getTargetUri().toString() + "]";
    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup() {
        return remoteGroup;
    }
    
    @Override
    public TransferDirection getTransferDirection() {
        return (getConnectableType() == ConnectableType.REMOTE_INPUT_PORT) ? TransferDirection.SEND : TransferDirection.RECEIVE;
    }
    
    public void setTargetExists(final boolean exists) {
        this.targetExists.set(exists);
    }
    
    @Override
    public void removeConnection(final Connection connection) throws IllegalArgumentException, IllegalStateException {
        super.removeConnection(connection);
        
        // If the Port no longer exists on the remote instance and this is the last Connection, tell 
        // RemoteProcessGroup to remove me
        if ( !getTargetExists() && !hasIncomingConnection() && getConnections().isEmpty() ) {
            remoteGroup.removeNonExistentPort(this);
        }
    }
    
    
    private static class EndpointConnectionState {
        private final Peer peer;
        private final SocketClientProtocol socketClientProtocol;
        private final FlowFileCodec codec;
        private volatile long lastUsed;
        
        private EndpointConnectionState(final Peer peer, final SocketClientProtocol socketClientProtocol, final FlowFileCodec codec) {
            this.peer = peer;
            this.socketClientProtocol = socketClientProtocol;
            this.codec = codec;
        }
        
        public FlowFileCodec getCodec() {
            return codec;
        }
        
        public SocketClientProtocol getSocketClientProtocol() {
            return socketClientProtocol;
        }
        
        public Peer getPeer() {
            return peer;
        }
        
        public void setLastTimeUsed() {
            lastUsed = System.currentTimeMillis();
        }
        
        public long getLastTimeUsed() {
            return lastUsed;
        }
    }

    
    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }
    
    
    @Override
    public boolean isSideEffectFree() {
        return false;
    }
}
