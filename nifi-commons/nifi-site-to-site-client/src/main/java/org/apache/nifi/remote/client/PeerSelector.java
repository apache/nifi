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
package org.apache.nifi.remote.client;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.PeerStatusCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.nifi.remote.util.EventReportUtil.error;
import static org.apache.nifi.remote.util.EventReportUtil.warn;

public class PeerSelector {

    private static final Logger logger = LoggerFactory.getLogger(PeerSelector.class);
    private static final long PEER_CACHE_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    private static final long PEER_REFRESH_PERIOD = 60000L;

    private final ReentrantLock peerRefreshLock = new ReentrantLock();
    private volatile List<PeerStatus> peerStatuses;
    private volatile Set<PeerStatus> lastFetchedQueryablePeers;
    private volatile long peerRefreshTime = 0L;
    private final AtomicLong peerIndex = new AtomicLong(0L);
    private volatile PeerStatusCache peerStatusCache;
    private final PeerPersistence peerPersistence;

    private EventReporter eventReporter;

    private final PeerStatusProvider peerStatusProvider;
    private final ConcurrentMap<PeerDescription, Long> peerTimeoutExpirations = new ConcurrentHashMap<>();

    static class SystemTime {
        long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }
    private SystemTime systemTime = new SystemTime();

    /**
     * Replace the SystemTime instance.
     * This method is purely used by unit testing, to emulate peer refresh period.
     */
    void setSystemTime(final SystemTime systemTime) {
        logger.info("Replacing systemTime instance to {}.", systemTime);
        this.systemTime = systemTime;
    }

    public PeerSelector(final PeerStatusProvider peerStatusProvider, final PeerPersistence peerPersistence) {
        this.peerStatusProvider = peerStatusProvider;
        this.peerPersistence = peerPersistence;

        try {
            PeerStatusCache restoredPeerStatusCache = null;
            if (peerPersistence != null) {
                restoredPeerStatusCache = peerPersistence.restore();
                if (restoredPeerStatusCache != null) {
                    final SiteToSiteTransportProtocol currentProtocol = peerStatusProvider.getTransportProtocol();
                    final SiteToSiteTransportProtocol cachedProtocol = restoredPeerStatusCache.getTransportProtocol();
                    if (!currentProtocol.equals(cachedProtocol)) {
                        logger.info("Discard stored peer statuses in {} because transport protocol has changed from {} to {}",
                            peerPersistence.getClass().getSimpleName(), cachedProtocol, currentProtocol);
                        restoredPeerStatusCache = null;
                    }
                }
            }
            this.peerStatusCache = restoredPeerStatusCache;

        } catch (final IOException ioe) {
            logger.warn("Failed to recover peer statuses from {} due to {}; will continue without loading information from file",
                peerPersistence.getClass().getSimpleName(), ioe);
        }
    }

    private void persistPeerStatuses() {
        try {
            peerPersistence.save(peerStatusCache);
        } catch (final IOException e) {
            error(logger, eventReporter, "Failed to persist list of Peers due to {}; if restarted" +
                " and the nodes specified at the RPG are down," +
                " may be unable to transfer data until communications with those nodes are restored", e.toString());
            logger.error("", e);
        }
    }

    List<PeerStatus> formulateDestinationList(final Set<PeerStatus> statuses, final TransferDirection direction) {

        final int numDestinations = Math.max(128, statuses.size());
        final Map<PeerStatus, Integer> entryCountMap = new HashMap<>();

        long totalFlowFileCount = 0L;
        for (final PeerStatus nodeInfo : statuses) {
            totalFlowFileCount += nodeInfo.getFlowFileCount();
        }

        int totalEntries = 0;
        for (final PeerStatus nodeInfo : statuses) {
            final int flowFileCount = nodeInfo.getFlowFileCount();
            // don't allow any node to get more than 80% of the data
            final double percentageOfFlowFiles = Math.min(0.8D, ((double) flowFileCount / (double) totalFlowFileCount));
            final double relativeWeighting = (direction == TransferDirection.SEND) ? (1 - percentageOfFlowFiles) : percentageOfFlowFiles;
            final int entries = Math.max(1, (int) (numDestinations * relativeWeighting));

            entryCountMap.put(nodeInfo, Math.max(1, entries));
            totalEntries += entries;
        }

        final List<PeerStatus> destinations = new ArrayList<>(totalEntries);
        for (int i = 0; i < totalEntries; i++) {
            destinations.add(null);
        }
        for (final Map.Entry<PeerStatus, Integer> entry : entryCountMap.entrySet()) {
            final PeerStatus nodeInfo = entry.getKey();
            final int numEntries = entry.getValue();

            int skipIndex = numEntries;
            for (int i = 0; i < numEntries; i++) {
                int n = (skipIndex * i);
                while (true) {
                    final int index = n % destinations.size();
                    PeerStatus status = destinations.get(index);
                    if (status == null) {
                        status = new PeerStatus(nodeInfo.getPeerDescription(), nodeInfo.getFlowFileCount(), nodeInfo.isQueryForPeers());
                        destinations.set(index, status);
                        break;
                    } else {
                        n++;
                    }
                }
            }
        }

        // Shuffle destinations to provide better distribution.
        // Without this, same host will be used continuously, especially when remote peers have the same number of queued files.
        // Use Random(0) to provide consistent result for unit testing. Randomness is not important to shuffle destinations.
        Collections.shuffle(destinations, new Random(0));

        final StringBuilder distributionDescription = new StringBuilder();
        distributionDescription.append("New Weighted Distribution of Nodes:");
        for (final Map.Entry<PeerStatus, Integer> entry : entryCountMap.entrySet()) {
            final double percentage = entry.getValue() * 100D / destinations.size();
            distributionDescription.append("\n").append(entry.getKey()).append(" will receive ").append(percentage).append("% of data");
        }
        logger.info(distributionDescription.toString());

        // Jumble the list of destinations.
        return destinations;
    }

    /**
     * Updates internal state map to penalize a PeerStatus that points to the
     * specified peer
     *
     * @param peer the peer
     * @param penalizationMillis period of time to penalize a given peer
     */
    public void penalize(final Peer peer, final long penalizationMillis) {
        penalize(peer.getDescription(), penalizationMillis);
    }

    public void penalize(final PeerDescription peerDescription, final long penalizationMillis) {
        Long expiration = peerTimeoutExpirations.get(peerDescription);
        if (expiration == null) {
            expiration = Long.valueOf(0L);
        }

        final long newExpiration = Math.max(expiration, systemTime.currentTimeMillis() + penalizationMillis);
        peerTimeoutExpirations.put(peerDescription, Long.valueOf(newExpiration));
    }

    public boolean isPenalized(final PeerStatus peerStatus) {
        final Long expirationEnd = peerTimeoutExpirations.get(peerStatus.getPeerDescription());
        return (expirationEnd != null && expirationEnd > systemTime.currentTimeMillis());
    }

    public void clear() {
        peerTimeoutExpirations.clear();
    }

    private boolean isPeerRefreshNeeded(final List<PeerStatus> peerList) {
        return (peerList == null || peerList.isEmpty() || systemTime.currentTimeMillis() > peerRefreshTime + PEER_REFRESH_PERIOD);
    }

    /**
     * Return status of a peer that will be used for the next communication.
     * The peer with less workload will be selected with higher probability.
     * @param direction the amount of workload is calculated based on transaction direction,
     *                  for SEND, a peer with less flow files is preferred,
     *                  for RECEIVE, a peer with more flow files is preferred
     * @return a selected peer, if there is no available peer or all peers are penalized, then return null
     */
    public ArrayList<PeerStatus> getPeerStatuses(final TransferDirection direction) {
        List<PeerStatus> peerList = peerStatuses;
        if (isPeerRefreshNeeded(peerList)) {
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
                        warn(logger, eventReporter, message);
                        if (logger.isDebugEnabled()) {
                            logger.warn("", e);
                        }
                    }

                    this.peerStatuses = peerList;
                    peerRefreshTime = systemTime.currentTimeMillis();
                }
            } finally {
                peerRefreshLock.unlock();
            }
        }


        if (peerList == null || peerList.isEmpty()) {
            return new ArrayList<PeerStatus>();
        }

        ArrayList<PeerStatus> retVal = new ArrayList<>(peerList);
        retVal.removeIf(p -> isPenalized(p));

        return retVal;
    }

    private List<PeerStatus> createPeerStatusList(final TransferDirection direction) throws IOException {
        Set<PeerStatus> statuses = getPeerStatuses();
        if (statuses == null) {
            refreshPeers();
            statuses = getPeerStatuses();
            if (statuses == null) {
                logger.debug("{} found no peers to connect to", this);
                return Collections.emptyList();
            }
        }
        return formulateDestinationList(statuses, direction);
    }

    private Set<PeerStatus> getPeerStatuses() {
        final PeerStatusCache cache = this.peerStatusCache;
        if (cache == null || cache.getStatuses() == null || cache.getStatuses().isEmpty()) {
            return null;
        }

        if (cache.getTimestamp() + PEER_CACHE_MILLIS < systemTime.currentTimeMillis()) {
            final Set<PeerStatus> equalizedSet = new HashSet<>(cache.getStatuses().size());
            for (final PeerStatus status : cache.getStatuses()) {
                final PeerStatus equalizedStatus = new PeerStatus(status.getPeerDescription(), 1, status.isQueryForPeers());
                equalizedSet.add(equalizedStatus);
            }

            return equalizedSet;
        }

        return cache.getStatuses();
    }

    public void refreshPeers() {
        final PeerStatusCache existingCache = peerStatusCache;
        if (existingCache != null && (existingCache.getTimestamp() + PEER_CACHE_MILLIS > systemTime.currentTimeMillis())) {
            return;
        }

        try {
            final Set<PeerStatus> statuses = fetchRemotePeerStatuses();
            peerStatusCache = new PeerStatusCache(statuses, System.currentTimeMillis(), peerStatusProvider.getTransportProtocol());
            persistPeerStatuses();
            logger.info("{} Successfully refreshed Peer Status; remote instance consists of {} peers", this, statuses.size());
        } catch (Exception e) {
            warn(logger, eventReporter, "{} Unable to refresh Remote Group's peers due to {}", this, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("", e);
            }
        }
    }

    public void setEventReporter(EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    private Set<PeerStatus> fetchRemotePeerStatuses() throws IOException {
        final Set<PeerDescription> peersToRequestClusterInfoFrom = new HashSet<>();

        // Look at all of the peers that we fetched last time.
        final Set<PeerStatus> lastFetched = lastFetchedQueryablePeers;
        if (lastFetched != null && !lastFetched.isEmpty()) {
            lastFetched.stream().map(peer -> peer.getPeerDescription())
                    .forEach(desc -> peersToRequestClusterInfoFrom.add(desc));
        }

        // Always add the configured node info to the list of peers to communicate with
        peersToRequestClusterInfoFrom.add(peerStatusProvider.getBootstrapPeerDescription());

        logger.debug("Fetching remote peer statuses from: {}", peersToRequestClusterInfoFrom);
        Exception lastFailure = null;
        for (final PeerDescription peerDescription : peersToRequestClusterInfoFrom) {
            try {
                final Set<PeerStatus> statuses = peerStatusProvider.fetchRemotePeerStatuses(peerDescription);
                lastFetchedQueryablePeers = statuses.stream()
                        .filter(p -> p.isQueryForPeers())
                        .collect(Collectors.toSet());

                return statuses;
            } catch (final Exception e) {
                logger.warn("Could not communicate with {}:{} to determine which nodes exist in the remote NiFi cluster, due to {}",
                        peerDescription.getHostname(), peerDescription.getPort(), e.toString());
                lastFailure = e;
            }
        }

        final IOException ioe = new IOException("Unable to communicate with remote NiFi cluster in order to determine which nodes exist in the remote cluster");
        if (lastFailure != null) {
            ioe.addSuppressed(lastFailure);
        }

        throw ioe;
    }

}
