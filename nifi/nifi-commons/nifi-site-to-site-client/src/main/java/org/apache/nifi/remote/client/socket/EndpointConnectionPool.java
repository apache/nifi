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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateExpiredException;
import javax.security.cert.CertificateNotYetValidException;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.RemoteDestination;
import org.apache.nifi.remote.RemoteResourceInitiator;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.apache.nifi.remote.util.PeerStatusCache;
import org.apache.nifi.remote.util.RemoteNiFiUtils;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointConnectionPool {
    public static final long PEER_REFRESH_PERIOD = 60000L;
    public static final String CATEGORY = "Site-to-Site";
    public static final long REMOTE_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    private static final long PEER_CACHE_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

	private static final Logger logger = LoggerFactory.getLogger(EndpointConnectionPool.class);
	
	private final BlockingQueue<EndpointConnection> connectionQueue = new LinkedBlockingQueue<>();
    private final ConcurrentMap<PeerStatus, Long> peerTimeoutExpirations = new ConcurrentHashMap<>();
    private final URI clusterUrl;
    private final String apiUri;
    
    private final AtomicLong peerIndex = new AtomicLong(0L);
    
    private final ReentrantLock peerRefreshLock = new ReentrantLock();
    private volatile List<PeerStatus> peerStatuses;
    private volatile long peerRefreshTime = 0L;
    private volatile PeerStatusCache peerStatusCache;
    private final Set<EndpointConnection> activeConnections = Collections.synchronizedSet(new HashSet<EndpointConnection>());

    private final File peersFile;
    private final EventReporter eventReporter;
    private final SSLContext sslContext;
    private final ScheduledExecutorService taskExecutor;
    private final int idleExpirationMillis;
    
    private final ReadWriteLock listeningPortRWLock = new ReentrantReadWriteLock();
    private final Lock remoteInfoReadLock = listeningPortRWLock.readLock();
    private final Lock remoteInfoWriteLock = listeningPortRWLock.writeLock();
    private Integer siteToSitePort;
    private Boolean siteToSiteSecure;
    private long remoteRefreshTime;
    private final Map<String, String> inputPortMap = new HashMap<>();	// map input port name to identifier
    private final Map<String, String> outputPortMap = new HashMap<>();	// map output port name to identifier
    
    private volatile int commsTimeout;
    private volatile boolean shutdown = false;
    
    
    public EndpointConnectionPool(final String clusterUrl, final int commsTimeoutMillis, final int idleExpirationMillis, 
            final EventReporter eventReporter, final File persistenceFile) 
    {
    	this(clusterUrl, commsTimeoutMillis, idleExpirationMillis, null, eventReporter, persistenceFile);
    }
    
    public EndpointConnectionPool(final String clusterUrl, final int commsTimeoutMillis, final int idleExpirationMillis,
            final SSLContext sslContext, final EventReporter eventReporter, final File persistenceFile) 
    {
    	try {
    		this.clusterUrl = new URI(clusterUrl);
    	} catch (final URISyntaxException e) {
    		throw new IllegalArgumentException("Invalid Cluster URL: " + clusterUrl);
    	}
    	
    	// Trim the trailing /
        String uriPath = this.clusterUrl.getPath();
        if (uriPath.endsWith("/")) {
            uriPath = uriPath.substring(0, uriPath.length() - 1);
        }
        apiUri = this.clusterUrl.getScheme() + "://" + this.clusterUrl.getHost() + ":" + this.clusterUrl.getPort() + uriPath + "-api";
        
    	this.sslContext = sslContext;
    	this.peersFile = persistenceFile;
    	this.eventReporter = eventReporter;
    	this.commsTimeout = commsTimeoutMillis;
    	this.idleExpirationMillis = idleExpirationMillis;
    	
    	Set<PeerStatus> recoveredStatuses;
    	if ( persistenceFile != null && persistenceFile.exists() ) {
    		try {
    			recoveredStatuses = recoverPersistedPeerStatuses(peersFile);	
    			this.peerStatusCache = new PeerStatusCache(recoveredStatuses, peersFile.lastModified());
    		} catch (final IOException ioe) {
    			logger.warn("Failed to recover peer statuses from {} due to {}; will continue without loading information from file", persistenceFile, ioe);
    		}
    	} else {
    		peerStatusCache = null;
    	}

    	// Initialize a scheduled executor and run some maintenance tasks in the background to kill off old, unused
    	// connections and keep our list of peers up-to-date.
    	taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
    		private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    		
			@Override
			public Thread newThread(final Runnable r) {
				final Thread thread = defaultFactory.newThread(r);
				thread.setName("NiFi Site-to-Site Connection Pool Maintenance");
				return thread;
			}
    	});

    	taskExecutor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				refreshPeers();
			}
    	}, 0, 5, TimeUnit.SECONDS);

    	taskExecutor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				cleanupExpiredSockets();
			}
    	}, 5, 5, TimeUnit.SECONDS);
    }
    
    
    public EndpointConnection getEndpointConnection(final RemoteDestination remoteDestination, final TransferDirection direction) throws IOException, HandshakeException, PortNotRunningException, UnknownPortException, ProtocolException {
        return getEndpointConnection(remoteDestination, direction, null);
    }
    
    
    public EndpointConnection getEndpointConnection(final RemoteDestination remoteDestination, final TransferDirection direction, final SiteToSiteClientConfig config) throws IOException, HandshakeException, PortNotRunningException, UnknownPortException, ProtocolException {
    	//
        // Attempt to get a connection state that already exists for this URL.
        //
        FlowFileCodec codec = null;
        CommunicationsSession commsSession = null;
        SocketClientProtocol protocol = null;
        EndpointConnection connection;
        Peer peer = null;
        
        final List<EndpointConnection> addBack = new ArrayList<>();
        try {
            do {
                connection = connectionQueue.poll();
                logger.debug("{} Connection State for {} = {}", this, clusterUrl, connection);
                
                if ( connection == null && !addBack.isEmpty() ) {
                    // all available connections have been penalized.
                    logger.debug("{} all Connections for {} are penalized; returning no Connection", this, remoteDestination.getIdentifier());
                    return null;
                }
                
                if ( connection != null && connection.getPeer().isPenalized(remoteDestination.getIdentifier()) ) {
                    // we have a connection, but it's penalized. We want to add it back to the queue
                    // when we've found one to use.
                    addBack.add(connection);
                    continue;
                }
                
                // if we can't get an existing Connection, create one
                if ( connection == null ) {
                    logger.debug("{} No Connection available for Port {}; creating new Connection", this, remoteDestination.getIdentifier());
                    protocol = new SocketClientProtocol();
                    protocol.setDestination(remoteDestination);

                    logger.debug("{} getting next peer status", this);
                    final PeerStatus peerStatus = getNextPeerStatus(direction);
                    logger.debug("{} next peer status = {}", this, peerStatus);
                    if ( peerStatus == null ) {
                        return null;
                    }

                    try {
                        logger.debug("{} Establishing site-to-site connection with {}", this, peerStatus);
                        commsSession = establishSiteToSiteConnection(peerStatus);
                    } catch (final IOException ioe) {
                        penalize(peerStatus, remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS));
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
                
                    final String peerUrl = "nifi://" + peerStatus.getHostname() + ":" + peerStatus.getPort();
                    peer = new Peer(commsSession, peerUrl, clusterUrl.toString());
    
                    // set properties based on config
                    if ( config != null ) {
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
                        if ( protocol.isDestinationFull() ) {
                            logger.warn("{} {} indicates that port's destination is full; penalizing peer", this, peer);
                            penalize(peer, remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS));
                            connectionQueue.offer(connection);
                            continue;
                        } else if ( protocol.isPortInvalid() ) {
                        	penalize(peer, remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS));
                        	cleanup(protocol, peer);
                        	throw new PortNotRunningException(peer.toString() + " indicates that port " + remoteDestination.getIdentifier() + " is not running");
                        } else if ( protocol.isPortUnknown() ) {
                        	penalize(peer, remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS));
                        	cleanup(protocol, peer);
                        	throw new UnknownPortException(peer.toString() + " indicates that port " + remoteDestination.getIdentifier() + " is not known");
                        }
                        
                        // negotiate the FlowFileCodec to use
                        logger.debug("{} negotiating codec", this);
                        codec = protocol.negotiateCodec(peer);
                        logger.debug("{} negotiated codec is {}", this, codec);
                    } catch (final PortNotRunningException | UnknownPortException e) {
                    	throw e;
                    } catch (final Exception e) {
                        penalize(peer, remoteDestination.getYieldPeriod(TimeUnit.MILLISECONDS));
                        cleanup(protocol, peer);
                        
                        final String message = String.format("%s failed to communicate with %s due to %s", this, peer == null ? clusterUrl : peer, e.toString());
                        logger.error(message);
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", e);
                        }
                        throw e;
                    }
                    
                    connection = new EndpointConnection(peer, protocol, codec);
                } else {
                    final long lastTimeUsed = connection.getLastTimeUsed();
                    final long millisSinceLastUse = System.currentTimeMillis() - lastTimeUsed;
                    
                    if ( commsTimeout > 0L && millisSinceLastUse >= commsTimeout ) {
                        cleanup(connection.getSocketClientProtocol(), connection.getPeer());
                        connection = null;
                    } else {
                        codec = connection.getCodec();
                        peer = connection.getPeer();
                        commsSession = peer.getCommunicationsSession();
                        protocol = connection.getSocketClientProtocol();
                    }
                }
            } while ( connection == null || codec == null || commsSession == null || protocol == null );
        } finally {
            if ( !addBack.isEmpty() ) {
                connectionQueue.addAll(addBack);
            }
        }
        
        activeConnections.add(connection);
        return connection;
    }
    
    
    public boolean offer(final EndpointConnection endpointConnection) {
    	final Peer peer = endpointConnection.getPeer();
    	if ( peer == null ) {
    		return false;
    	}
    	
    	final String url = peer.getUrl();
    	if ( url == null ) {
    		return false;
    	}
    	
    	activeConnections.remove(endpointConnection);
    	if ( shutdown ) {
    	    terminate(endpointConnection);
    	    return false;
    	} else {
    	    endpointConnection.setLastTimeUsed();
    	    return connectionQueue.offer(endpointConnection);
    	}
    }
    
    private void penalize(final PeerStatus status, final long penalizationMillis) {
        Long expiration = peerTimeoutExpirations.get(status);
        if ( expiration == null ) {
            expiration = Long.valueOf(0L);
        }
        
        final long newExpiration = Math.max(expiration, System.currentTimeMillis() + penalizationMillis);
        peerTimeoutExpirations.put(status, Long.valueOf(newExpiration));
    }
    
    /**
     * Updates internal state map to penalize a PeerStatus that points to the specified peer
     * @param peer
     */
    public void penalize(final Peer peer, final long penalizationMillis) {
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
        penalize(status, penalizationMillis);
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
    
    private boolean isPeerRefreshNeeded(final List<PeerStatus> peerList) {
        return (peerList == null || peerList.isEmpty() || System.currentTimeMillis() > peerRefreshTime + PEER_REFRESH_PERIOD);
    }
    
    private PeerStatus getNextPeerStatus(final TransferDirection direction) {
        List<PeerStatus> peerList = peerStatuses;
        if ( isPeerRefreshNeeded(peerList) ) {
            peerRefreshLock.lock();
            try {
                // now that we have the lock, check again that we need to refresh (because another thread
                // could have been refreshing while we were waiting for the lock).
                peerList = peerStatuses;
                if (isPeerRefreshNeeded(peerList)) {
                    try {
                        peerList = createPeerStatusList(direction);
                    } catch (final Exception e) {
                        final String message = String.format("%s Failed to update list of peers due to %s", this, e.toString());
                        logger.warn(message);
                        if ( logger.isDebugEnabled() ) {
                            logger.warn("", e);
                        }
                        
                        if ( eventReporter != null ) {
                        	eventReporter.reportEvent(Severity.WARNING, CATEGORY, message);
                        }
                    }
                    
                    this.peerStatuses = peerList;
                    peerRefreshTime = System.currentTimeMillis();
                }
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
    
    private List<PeerStatus> createPeerStatusList(final TransferDirection direction) throws IOException, HandshakeException, UnknownPortException, PortNotRunningException {
        Set<PeerStatus> statuses = getPeerStatuses();
        if ( statuses == null ) {
            refreshPeers();
            statuses = getPeerStatuses();
            if ( statuses == null ) {
                logger.debug("{} found no peers to connect to", this);
                return Collections.emptyList();
            }
        }
        
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> nodeInfos = new ArrayList<>();
        for ( final PeerStatus peerStatus : statuses ) {
            final NodeInformation nodeInfo = new NodeInformation(peerStatus.getHostname(), peerStatus.getPort(), 0, peerStatus.isSecure(), peerStatus.getFlowFileCount());
            nodeInfos.add(nodeInfo);
        }
        clusterNodeInfo.setNodeInformation(nodeInfos);
        return formulateDestinationList(clusterNodeInfo, direction);
    }
    
    
    private Set<PeerStatus> getPeerStatuses() {
        final PeerStatusCache cache = this.peerStatusCache;
        if (cache == null || cache.getStatuses() == null || cache.getStatuses().isEmpty()) {
            return null;
        }

        if (cache.getTimestamp() + PEER_CACHE_MILLIS < System.currentTimeMillis()) {
            final Set<PeerStatus> equalizedSet = new HashSet<>(cache.getStatuses().size());
            for (final PeerStatus status : cache.getStatuses()) {
                final PeerStatus equalizedStatus = new PeerStatus(status.getHostname(), status.getPort(), status.isSecure(), 1);
                equalizedSet.add(equalizedStatus);
            }

            return equalizedSet;
        }

        return cache.getStatuses();
    }

    private Set<PeerStatus> fetchRemotePeerStatuses() throws IOException, HandshakeException, UnknownPortException, PortNotRunningException {
    	final String hostname = clusterUrl.getHost();
        final Integer port = getSiteToSitePort();
        if ( port == null ) {
            throw new IOException("Remote instance of NiFi is not configured to allow site-to-site communications");
        }
    	
    	final CommunicationsSession commsSession = establishSiteToSiteConnection(hostname, port);
        final Peer peer = new Peer(commsSession, "nifi://" + hostname + ":" + port, clusterUrl.toString());
        final SocketClientProtocol clientProtocol = new SocketClientProtocol();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        RemoteResourceInitiator.initiateResourceNegotiation(clientProtocol, dis, dos);

        clientProtocol.setTimeout(commsTimeout);
        clientProtocol.handshake(peer, null);
        final Set<PeerStatus> peerStatuses = clientProtocol.getPeerStatuses(peer);
        persistPeerStatuses(peerStatuses);

        try {
            clientProtocol.shutdown(peer);
        } catch (final IOException e) {
            final String message = String.format("%s Failed to shutdown protocol when updating list of peers due to %s", this, e.toString());
            logger.warn(message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        try {
            peer.close();
        } catch (final IOException e) {
            final String message = String.format("%s Failed to close resources when updating list of peers due to %s", this, e.toString());
            logger.warn(message);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }

        return peerStatuses;
    }


    private void persistPeerStatuses(final Set<PeerStatus> statuses) {
    	if ( peersFile == null ) {
    		return;
    	}
    	
        try (final OutputStream fos = new FileOutputStream(peersFile);
             final OutputStream out = new BufferedOutputStream(fos)) {

            for (final PeerStatus status : statuses) {
                final String line = status.getHostname() + ":" + status.getPort() + ":" + status.isSecure() + "\n";
                out.write(line.getBytes(StandardCharsets.UTF_8));
            }

        } catch (final IOException e) {
            logger.error("Failed to persist list of Peers due to {}; if restarted and peer's NCM is down, may be unable to transfer data until communications with NCM are restored", e.toString(), e);
        }
    }

    private Set<PeerStatus> recoverPersistedPeerStatuses(final File file) throws IOException {
        if (!file.exists()) {
            return null;
        }

        final Set<PeerStatus> statuses = new HashSet<>();
        try (final InputStream fis = new FileInputStream(file);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {

            String line;
            while ((line = reader.readLine()) != null) {
                final String[] splits = line.split(Pattern.quote(":"));
                if (splits.length != 3) {
                    continue;
                }

                final String hostname = splits[0];
                final int port = Integer.parseInt(splits[1]);
                final boolean secure = Boolean.parseBoolean(splits[2]);

                statuses.add(new PeerStatus(hostname, port, secure, 1));
            }
        }

        return statuses;
    }
    
    
    private CommunicationsSession establishSiteToSiteConnection(final PeerStatus peerStatus) throws IOException {
    	return establishSiteToSiteConnection(peerStatus.getHostname(), peerStatus.getPort());
    }
    
    private CommunicationsSession establishSiteToSiteConnection(final String hostname, final int port) throws IOException {
    	final boolean siteToSiteSecure = isSecure();
        final String destinationUri = "nifi://" + hostname + ":" + port;

        CommunicationsSession commsSession = null;
        try {
	        if ( siteToSiteSecure ) {
	            if ( sslContext == null ) {
	                throw new IOException("Unable to communicate with " + hostname + ":" + port + " because it requires Secure Site-to-Site communications, but this instance is not configured for secure communications");
	            }
	            
	            final SSLSocketChannel socketChannel = new SSLSocketChannel(sslContext, hostname, port, true);
	            socketChannel.connect();
	    
	            commsSession = new SSLSocketChannelCommunicationsSession(socketChannel, destinationUri);
	                
	                try {
	                    commsSession.setUserDn(socketChannel.getDn());
	                } catch (final CertificateNotYetValidException | CertificateExpiredException ex) {
	                    throw new IOException(ex);
	                }
	        } else {
	            final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(hostname, port));
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
    
    
    static List<PeerStatus> formulateDestinationList(final ClusterNodeInformation clusterNodeInfo, final TransferDirection direction) {
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
            final double relativeWeighting = (direction == TransferDirection.RECEIVE) ? (1 - percentageOfFlowFiles) : percentageOfFlowFiles;
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
            distributionDescription.append("\n").append(entry.getKey()).append(" will receive ").append(percentage).append("% of data");
        }
        logger.info(distributionDescription.toString());

        // Jumble the list of destinations.
        return destinations;
    }
    
    
    private void cleanupExpiredSockets() {
        final List<EndpointConnection> connections = new ArrayList<>();
        
        EndpointConnection connection;
        while ((connection = connectionQueue.poll()) != null) {
            // If the socket has not been used in 10 seconds, shut it down.
            final long lastUsed = connection.getLastTimeUsed();
            if ( lastUsed < System.currentTimeMillis() - idleExpirationMillis ) {
                try {
                    connection.getSocketClientProtocol().shutdown(connection.getPeer());
                } catch (final Exception e) {
                    logger.debug("Failed to shut down {} using {} due to {}", 
                        new Object[] {connection.getSocketClientProtocol(), connection.getPeer(), e} );
                }
                
                terminate(connection);
            } else {
                connections.add(connection);
            }
        }
        
        connectionQueue.addAll(connections);
    }
    
    public void shutdown() {
        shutdown = true;
    	taskExecutor.shutdown();
    	peerTimeoutExpirations.clear();
        
       for ( final EndpointConnection conn : activeConnections ) {
           conn.getPeer().getCommunicationsSession().interrupt();
        }
        
        EndpointConnection state;
        while ( (state = connectionQueue.poll()) != null)  {
            cleanup(state.getSocketClientProtocol(), state.getPeer());
        }
    }
    
    public void terminate(final EndpointConnection connection) {
        cleanup(connection.getSocketClientProtocol(), connection.getPeer());
    }
    
    private void refreshPeers() {
        final PeerStatusCache existingCache = peerStatusCache;
        if (existingCache != null && (existingCache.getTimestamp() + PEER_CACHE_MILLIS > System.currentTimeMillis())) {
            return;
        }

        try {
            final Set<PeerStatus> statuses = fetchRemotePeerStatuses();
            peerStatusCache = new PeerStatusCache(statuses);
            logger.info("{} Successfully refreshed Peer Status; remote instance consists of {} peers", this, statuses.size());
        } catch (Exception e) {
            logger.warn("{} Unable to refresh Remote Group's peers due to {}", this, e);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }
        }
    }
    
    
    public String getInputPortIdentifier(final String portName) throws IOException {
        return getPortIdentifier(portName, inputPortMap);
    }
    
    public String getOutputPortIdentifier(final String portName) throws IOException {
    	return getPortIdentifier(portName, outputPortMap);
    }
    
    
    private String getPortIdentifier(final String portName, final Map<String, String> portMap) throws IOException {
    	String identifier;
    	remoteInfoReadLock.lock();
        try {
        	identifier = portMap.get(portName);
        } finally {
        	remoteInfoReadLock.unlock();
        }
        
        if ( identifier != null ) {
        	return identifier;
        }
        
        refreshRemoteInfo();

    	remoteInfoReadLock.lock();
        try {
        	return portMap.get(portName);
        } finally {
        	remoteInfoReadLock.unlock();
        }
    }
    
    
    private ControllerDTO refreshRemoteInfo() throws IOException {
    	final boolean webInterfaceSecure = clusterUrl.toString().startsWith("https");
        final RemoteNiFiUtils utils = new RemoteNiFiUtils(webInterfaceSecure ? sslContext : null);
		final ControllerDTO controller = utils.getController(URI.create(apiUri + "/controller"), commsTimeout);
        
        remoteInfoWriteLock.lock();
        try {
            this.siteToSitePort = controller.getRemoteSiteListeningPort();
            this.siteToSiteSecure = controller.isSiteToSiteSecure();
            
            inputPortMap.clear();
            for (final PortDTO inputPort : controller.getInputPorts()) {
            	inputPortMap.put(inputPort.getName(), inputPort.getId());
            }
            
            outputPortMap.clear();
            for ( final PortDTO outputPort : controller.getOutputPorts()) {
            	outputPortMap.put(outputPort.getName(), outputPort.getId());
            }
            
            this.remoteRefreshTime = System.currentTimeMillis();
        } finally {
        	remoteInfoWriteLock.unlock();
        }
        
        return controller;
    }
    
    /**
     * @return the port that the remote instance is listening on for
     * site-to-site communication, or <code>null</code> if the remote instance
     * is not configured to allow site-to-site communications.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    private Integer getSiteToSitePort() throws IOException {
        Integer listeningPort;
        remoteInfoReadLock.lock();
        try {
            listeningPort = this.siteToSitePort;
            if (listeningPort != null && this.remoteRefreshTime > System.currentTimeMillis() - REMOTE_REFRESH_MILLIS) {
                return listeningPort;
            }
        } finally {
        	remoteInfoReadLock.unlock();
        }

        final ControllerDTO controller = refreshRemoteInfo();
        listeningPort = controller.getRemoteSiteListeningPort();

        return listeningPort;
    }
 
    
    
    
    /**
     * Returns {@code true} if the remote instance is configured for secure site-to-site communications,
     * {@code false} otherwise.
     * 
     * @return
     * @throws IOException
     */
    public boolean isSecure() throws IOException {
        remoteInfoReadLock.lock();
        try {
            final Boolean secure = this.siteToSiteSecure;
            if (secure != null && this.remoteRefreshTime > System.currentTimeMillis() - REMOTE_REFRESH_MILLIS) {
                return secure;
            }
        } finally {
        	remoteInfoReadLock.unlock();
        }

        final ControllerDTO controller = refreshRemoteInfo();
        final Boolean isSecure = controller.isSiteToSiteSecure();
        if ( isSecure == null ) {
            throw new IOException("Remote NiFi instance " + clusterUrl + " is not currently configured to accept site-to-site connections");
        }
        
        return isSecure;
    }
}
