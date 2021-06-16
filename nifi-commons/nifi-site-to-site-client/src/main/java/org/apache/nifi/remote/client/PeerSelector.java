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

import static org.apache.nifi.remote.util.EventReportUtil.error;
import static org.apache.nifi.remote.util.EventReportUtil.warn;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.PeerStatusCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service which maintains state around peer (NiFi node(s) in a remote instance (cluster or
 * standalone)). There is an internal cache which stores identifying information about each
 * node and the current workload of each in number of flowfiles being processed. Individual
 * nodes can be penalized for an amount of time (see {@link #penalize(Peer, long)}) to avoid
 * sending/receiving data from them. Attempts are made to balance communications ("busier"
 * nodes will {@code TransferDirection.SEND} more and {@code TransferDirection.RECEIVE} fewer
 * flowfiles from this instance).
 */
public class PeerSelector {
    private static final Logger logger = LoggerFactory.getLogger(PeerSelector.class);

    // The timeout for the peer status cache
    private static final long PEER_CACHE_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    // The service which saves the peer state to persistent storage
    private final PeerPersistence peerPersistence;

    // The service which retrieves peer state
    private final PeerStatusProvider peerStatusProvider;

    // Maps the peer description to a millisecond penalty expiration
    private final ConcurrentMap<PeerDescription, Long> peerPenaltyExpirations = new ConcurrentHashMap<>();

    // The most recently fetched peer statuses
    private volatile PeerStatusCache peerStatusCache;

    private EventReporter eventReporter;

    /**
     * Returns a peer selector with the provided collaborators.
     *
     * @param peerStatusProvider the service which retrieves peer state
     * @param peerPersistence    the service which persists peer state
     */
    public PeerSelector(final PeerStatusProvider peerStatusProvider, final PeerPersistence peerPersistence) {
        this.peerStatusProvider = peerStatusProvider;
        this.peerPersistence = peerPersistence;

        // On instantiation, retrieve the peer status cache
        restoreInitialPeerStatusCache();
    }

    /**
     * Populates the peer status cache from the peer persistence provider (e.g. the file system or
     * persisted cluster state). If this fails, it will log a warning and continue, as it is not
     * required for startup. If the cached protocol differs from the currently configured protocol,
     * the cache will be cleared.
     */
    private void restoreInitialPeerStatusCache() {
        try {
            PeerStatusCache restoredPeerStatusCache = null;
            if (peerPersistence != null) {
                restoredPeerStatusCache = peerPersistence.restore();

                // If there is an existing cache, ensure that the protocol matches the current protocol
                if (restoredPeerStatusCache != null) {
                    final SiteToSiteTransportProtocol currentProtocol = peerStatusProvider.getTransportProtocol();
                    final SiteToSiteTransportProtocol cachedProtocol = restoredPeerStatusCache.getTransportProtocol();

                    final String currentRemoteInstanceUris = peerStatusProvider.getRemoteInstanceUris();
                    final String cachedRemoteInstanceUris = restoredPeerStatusCache.getRemoteInstanceUris();

                    // If the remote instance URIs have changed, clear the cache
                    if (!currentRemoteInstanceUris.equals(cachedRemoteInstanceUris)) {
                        logger.info("Discard stored peer statuses in {} because remote instance URIs has changed from {} to {}",
                                peerPersistence.getClass().getSimpleName(), cachedRemoteInstanceUris, currentRemoteInstanceUris);
                        restoredPeerStatusCache = null;
                    }

                    // If the protocols have changed, clear the cache
                    if (!currentProtocol.equals(cachedProtocol)) {
                        logger.warn("Discard stored peer statuses in {} because transport protocol has changed from {} to {}",
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

    /**
     * Returns the normalized weight for this ratio of peer flowfiles to total flowfiles and the given direction. The number will be
     * a Double between 0 and 100 indicating the percent of all flowfiles the peer
     * should send/receive. The transfer direction is <em>from the perspective of this node to the peer</em>
     * (i.e. how many flowfiles should <em>this node send</em> to the peer, or how many flowfiles
     * should <em>this node receive</em> from the peer).
     *
     * @param direction          the transfer direction ({@code SEND} weights the destinations higher if they have fewer flowfiles, {@code RECEIVE} weights them higher if they have more)
     * @param totalFlowFileCount the total flowfile count in the remote instance (standalone or cluster)
     * @param flowFileCount      the flowfile count for the given peer
     * @param peerCount          the number of peers in the remote instance
     * @return the normalized weight of this peer
     */
    private static double calculateNormalizedWeight(TransferDirection direction, long totalFlowFileCount, int flowFileCount, int peerCount) {
        // If there is only a single remote, send/receive all data to/from it
        if (peerCount == 1) {
            return 100;
        }

        double cappedPercent;
        // If no flowfiles exist in the remote instance, evenly weight each node with 1/N
        if (totalFlowFileCount == 0) {
            cappedPercent = 1.0 / peerCount;
        } else {
            final double percentageOfFlowFiles = ((double) flowFileCount / totalFlowFileCount);
            cappedPercent = percentageOfFlowFiles;

            // If sending to the remote, allocate more flowfiles to the less-stressed peers
            if (direction == TransferDirection.SEND) {
                cappedPercent = (1 - percentageOfFlowFiles) / (peerCount - 1);
            }
        }
        return new BigDecimal(cappedPercent * 100).setScale(2, RoundingMode.FLOOR).doubleValue();
    }

    /**
     * Returns an ordered map of peers sorted in descending order by value (relative weight).
     *
     * @param unsortedMap the unordered map of peers to weights
     * @return the sorted (desc) map (by value)
     */
    private static LinkedHashMap<PeerStatus, Double> sortMapByWeight(Map<PeerStatus, Double> unsortedMap) {
        List<Map.Entry<PeerStatus, Double>> list = new ArrayList<>(unsortedMap.entrySet());
        list.sort(Map.Entry.comparingByValue());

        LinkedHashMap<PeerStatus, Double> result = new LinkedHashMap<>();
        for (int i = list.size() - 1; i >= 0; i--) {
            Map.Entry<PeerStatus, Double> entry = list.get(i);
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * Prints the distribution of the peers to the logger.
     *
     * @param sortedPeerWorkloads the peers and relative weights
     */
    private static void printDistributionStatistics(Map<PeerStatus, Double> sortedPeerWorkloads, TransferDirection direction) {
        if (logger.isDebugEnabled() && sortedPeerWorkloads != null) {
            DecimalFormat df = new DecimalFormat("##.##");
            df.setRoundingMode(RoundingMode.FLOOR);
            final StringBuilder distributionDescription = new StringBuilder();
            distributionDescription.append("New weighted distribution of nodes:");
            for (final Map.Entry<PeerStatus, Double> entry : sortedPeerWorkloads.entrySet()) {
                final double percentage = entry.getValue();
                distributionDescription.append("\n").append(entry.getKey())
                        .append(" will").append(direction == TransferDirection.RECEIVE ? " send " : " receive ")
                        .append(df.format(percentage)).append("% of data");
            }
            logger.debug(distributionDescription.toString());
        }
    }

    /**
     * Returns the total of all values in the map. This method is frequently used to calculate the total number of
     * flowfiles in the instance from the respective peer flowfile counts or the total percentage from the relative weights.
     *
     * @param peerWeightMap the map of peers to flowfile counts or relative weights
     * @return the total of the map values
     */
    private static double sumMapValues(Map<PeerStatus, Double> peerWeightMap) {
        return peerWeightMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    /**
     * Resets all penalization states for the peers.
     */
    public void clear() {
        peerPenaltyExpirations.clear();
    }

    /**
     * Return status of a peer that will be used for the next communication.
     * The peers with lower workloads will be selected with higher probability.
     *
     * @param direction the amount of workload is calculated based on transaction direction,
     *                  for SEND, a peer with fewer flow files is preferred,
     *                  for RECEIVE, a peer with more flow files is preferred
     * @return a selected peer, if there is no available peer or all peers are penalized, then return null
     */
    public PeerStatus getNextPeerStatus(final TransferDirection direction) {
        Set<PeerStatus> peerStatuses = getPeerStatuses();
        Map<PeerStatus, Double> orderedPeerStatuses = buildWeightedPeerMap(peerStatuses, direction);

        return getAvailablePeerStatus(orderedPeerStatuses);
    }

    /**
     * Returns {@code true} if this peer is currently penalized and should not send/receive flowfiles.
     *
     * @param peerStatus the peer status identifying the peer
     * @return true if this peer is penalized
     */
    public boolean isPenalized(final PeerStatus peerStatus) {
        final Long expirationEnd = peerPenaltyExpirations.get(peerStatus.getPeerDescription());
        return (expirationEnd != null && expirationEnd > System.currentTimeMillis());
    }

    /**
     * Updates internal state map to penalize a PeerStatus that points to the
     * specified peer.
     *
     * @param peer               the peer
     * @param penalizationMillis period of time to penalize a given peer (relative time, not absolute)
     */
    public void penalize(final Peer peer, final long penalizationMillis) {
        penalize(peer.getDescription(), penalizationMillis);
    }

    /**
     * Updates internal state map to penalize a PeerStatus that points to the
     * specified peer.
     *
     * @param peerDescription    the peer description (identifies the peer)
     * @param penalizationMillis period of time to penalize a given peer (relative time, not absolute)
     */
    public void penalize(final PeerDescription peerDescription, final long penalizationMillis) {
        Long expiration = peerPenaltyExpirations.get(peerDescription);
        if (expiration == null) {
            expiration = 0L;
        }

        final long newExpiration = Math.max(expiration, System.currentTimeMillis() + penalizationMillis);
        peerPenaltyExpirations.put(peerDescription, newExpiration);
    }

    /**
     * Allows for external callers to trigger a refresh of the internal peer status cache. Performs the refresh if the cache has expired. If the cache is still valid, skips the refresh.
     */
    public void refresh() {
        long cacheAgeMs = getCacheAge();
        logger.debug("External refresh triggered. Last refresh was {} ms ago", cacheAgeMs);
        if (isPeerRefreshNeeded()) {
            logger.debug("Refreshing peer status cache");
            refreshPeerStatusCache();
        } else {
            logger.debug("Cache is still valid; skipping refresh");
        }
    }

    /**
     * Sets the event reporter instance.
     *
     * @param eventReporter the event reporter
     */
    public void setEventReporter(EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    /**
     * Returns a map of peers prepared for flowfile transfer in the specified direction. Each peer is a key and the value is a
     * weighted percentage of the total flowfiles in the remote instance. For example, in a cluster where the total number of flowfiles
     * is 100, distributed across three nodes 20 in A, 30 in B, and 50 in C, the resulting map for
     * {@code SEND} will be {@code [A:40.0, B:35.0, C:25.0]} (1 - .2 => .8 * 100 / (3-1)) => 40.0).
     *
     * @param statuses  the set of all peers
     * @param direction the direction of transfer ({@code SEND} weights the destinations higher if they have more flowfiles, {@code RECEIVE} weights them higher if they have fewer)
     * @return the ordered map of each peer to its relative weight
     */
    LinkedHashMap<PeerStatus, Double> buildWeightedPeerMap(final Set<PeerStatus> statuses, final TransferDirection direction) {
        // Get all the destinations with their relative weights
        final Map<PeerStatus, Double> peerWorkloads = createDestinationMap(statuses, direction);

        if (!peerWorkloads.isEmpty()) {
            // This map is sorted, but not by key, so it cannot use SortedMap
            LinkedHashMap<PeerStatus, Double> sortedPeerWorkloads = sortMapByWeight(peerWorkloads);

            // Print the expected distribution of the peers
            printDistributionStatistics(sortedPeerWorkloads, direction);

            return sortedPeerWorkloads;
        } else {
            logger.debug("No peers available");
            return new LinkedHashMap<>();
        }
    }

    /**
     * Returns a map indexed by a peer to the normalized weight (number of flowfiles currently being
     * processed by the peer as a percentage of the total). This is used to allocate flowfiles to
     * the various peers as destinations.
     *
     * @param peerStatuses the set of peers, along with their current workload (number of flowfiles)
     * @param direction    whether sending flowfiles to these peers or receiving them
     * @return the map of weighted peers
     */
    @NotNull
    private Map<PeerStatus, Double> createDestinationMap(Set<PeerStatus> peerStatuses, TransferDirection direction) {
        final Map<PeerStatus, Double> peerWorkloads = new HashMap<>();

        // Calculate the total number of flowfiles in the peers
        long totalFlowFileCount = peerStatuses.stream().mapToLong(PeerStatus::getFlowFileCount).sum();
        logger.debug("Building weighted map of peers with total remote NiFi flowfile count: {}", totalFlowFileCount);

        // For each node, calculate the relative weight and store it in the map
        for (final PeerStatus nodeInfo : peerStatuses) {
            final int flowFileCount = nodeInfo.getFlowFileCount();
            final double normalizedWeight = calculateNormalizedWeight(direction, totalFlowFileCount, flowFileCount, peerStatuses.size());
            peerWorkloads.put(nodeInfo, normalizedWeight);
        }

        return peerWorkloads;
    }

    /**
     * Returns a set of {@link PeerStatus} objects representing all remote peers for the provided
     * {@link PeerDescription}s. If a queried peer returns updated state on a peer which has already
     * been captured, the new state is used.
     * <p>
     * Example:
     * <p>
     * 3 node cluster with nodes A, B, C
     * <p>
     * Node A knows about Node B and Node C, B about A and C, etc.
     *
     * <pre>
     *     Action                           |   Statuses
     *     query(A) -> B.status, C.status   |   Bs1, Cs1
     *     query(B) -> A.status, C.status   |   As1, Bs1, Cs2
     *     query(C) -> A.status, B.status   |   As2, Bs2, Cs2
     * </pre>
     *
     * @param peersToRequestClusterInfoFrom the set of peers to query
     * @return the complete set of statuses for each collection of peers
     * @throws IOException if there is a problem fetching peer statuses
     */
    private Set<PeerStatus> fetchRemotePeerStatuses(Set<PeerDescription> peersToRequestClusterInfoFrom) throws IOException {
        logger.debug("Fetching remote peer statuses from: {}", peersToRequestClusterInfoFrom);
        Exception lastFailure = null;

        final Set<PeerStatus> allPeerStatuses = new HashSet<>();

        // Iterate through all peers, getting (sometimes multiple) status(es) from each
        for (final PeerDescription peerDescription : peersToRequestClusterInfoFrom) {
            try {
                // Retrieve the peer status(es) from each peer description
                final Set<PeerStatus> statusesForPeerDescription = peerStatusProvider.fetchRemotePeerStatuses(peerDescription);

                // Filter to remove any peers which are not queryable
                final Set<PeerStatus> filteredStatuses = statusesForPeerDescription.stream()
                        .filter(PeerStatus::isQueryForPeers)
                        .collect(Collectors.toSet());

                allPeerStatuses.addAll(filteredStatuses);
            } catch (final Exception e) {
                logger.warn("Could not communicate with {}:{} to determine which node(s) exist in the remote NiFi instance, due to {}",
                        peerDescription.getHostname(), peerDescription.getPort(), e.toString());
                lastFailure = e;
            }
        }

        // If no peers were fetched and an exception was the cause, throw an exception
        if (allPeerStatuses.isEmpty() && lastFailure != null) {
            throw new IOException("Unable to retrieve nodes from remote instance", lastFailure);
        }

        return allPeerStatuses;
    }

    /**
     * Returns the {@link PeerStatus} identifying the next peer to send/receive data. This uses random
     * selection of peers, weighted by the relative desirability (i.e. for {@code SEND}, peers with more
     * flowfiles are more likely to be selected, and for {@code RECEIVE}, peers with fewer flowfiles are
     * more likely).
     *
     * @param orderedPeerStatuses the map of peers to relative weights, sorted in descending order by weight
     * @return the peer to send/receive data
     */
    private PeerStatus getAvailablePeerStatus(Map<PeerStatus, Double> orderedPeerStatuses) {
        if (orderedPeerStatuses == null || orderedPeerStatuses.isEmpty()) {
            logger.warn("Available peers collection is empty; no peer available");
            return null;
        }

        // Only distribute to unpenalized peers
        Map<PeerStatus, Double> unpenalizedPeers = orderedPeerStatuses.entrySet().stream()
                .filter(e -> !isPenalized(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final double totalWeights = sumMapValues(unpenalizedPeers);
        logger.debug("Determining next available peer ({} peers with total weight {})", unpenalizedPeers.keySet().size(), totalWeights);

        final double random = Math.random() * Math.min(100, totalWeights);
        logger.debug("Generated random value {}", random);

        double threshold = 0.0;
        for (Map.Entry<PeerStatus, Double> e : unpenalizedPeers.entrySet()) {
            logger.debug("Initial threshold was {}; added peer value {}; total {}", threshold, e.getValue(), threshold + e.getValue());
            threshold += e.getValue();
            if (random <= threshold) {
                return e.getKey();
            }
        }

        logger.debug("Did not select a peer; r {}, t {}, w {}", random, threshold, orderedPeerStatuses.values());
        logger.debug("All peers appear to be penalized; returning null");
        return null;
    }

    /**
     * Returns the cache age in milliseconds. If the cache is null or not set, returns {@code -1}.
     *
     * @return the cache age in millis
     */
    private long getCacheAge() {
        if (peerStatusCache == null) {
            return -1;
        }
        return System.currentTimeMillis() - peerStatusCache.getTimestamp();
    }

    /**
     * Returns the set of queryable peers ({@link PeerStatus#isQueryForPeers()}) most recently fetched.
     *
     * @return the set of queryable peers (empty set if the cache is {@code null})
     */
    @NotNull
    private Set<PeerStatus> getLastFetchedQueryablePeers() {
        return peerStatusCache != null ? peerStatusCache.getStatuses() : Collections.emptySet();
    }

    /**
     * Returns the set of peer statuses. If the cache is {@code null} or empty, refreshes the cache first and then returns the new peer status set.
     *
     * @return the most recent peer statuses (empty set if the cache is {@code null})
     */
    @NotNull
    private Set<PeerStatus> getPeerStatuses() {
        if (isPeerRefreshNeeded()) {
            refreshPeerStatusCache();
        }

        return getLastFetchedQueryablePeers();
    }

    /**
     * Returns the set of {@link PeerDescription} objects uniquely identifying each NiFi node which should be queried for {@link PeerStatus}.
     *
     * @return the set of recently retrieved peers and the bootstrap peer
     * @throws IOException if there is a problem retrieving the list of peers to query
     */
    private Set<PeerDescription> getPeersToQuery() throws IOException {
        final Set<PeerDescription> peersToRequestClusterInfoFrom = new HashSet<>();

        // Use the peers fetched last time
        final Set<PeerStatus> lastFetched = getLastFetchedQueryablePeers();
        if (lastFetched != null && !lastFetched.isEmpty()) {
            for (PeerStatus peerStatus : lastFetched) {
                peersToRequestClusterInfoFrom.add(peerStatus.getPeerDescription());
            }
        }

        // Always add the configured node info to the list of peers
        peersToRequestClusterInfoFrom.add(peerStatusProvider.getBootstrapPeerDescription());

        return peersToRequestClusterInfoFrom;
    }

    /**
     * Returns {@code true} if this cache has expired.
     *
     * @param cache the peer status cache
     * @return true if the cache is expired
     */
    private boolean isCacheExpired(PeerStatusCache cache) {
        return cache == null || cache.getTimestamp() + PEER_CACHE_MILLIS < System.currentTimeMillis();
    }

    /**
     * Returns {@code true} if the internal collection of peers is empty or the refresh time has passed.
     *
     * @return true if the peer statuses should be refreshed
     */
    private boolean isPeerRefreshNeeded() {
        return (peerStatusCache == null || peerStatusCache.isEmpty() || isCacheExpired(peerStatusCache));
    }

    /**
     * Persists the provided cache instance (in memory and via the {@link PeerPersistence} (e.g. in cluster state or a local file)) for future retrieval.
     *
     * @param peerStatusCache the cache of current peer statuses to persist
     */
    private void persistPeerStatuses(PeerStatusCache peerStatusCache) {
        try {
            this.peerStatusCache = peerStatusCache;

            // The #save mechanism persists the cache to stateful or file-based storage
            peerPersistence.save(peerStatusCache);
        } catch (final IOException e) {
            error(logger, eventReporter, "Failed to persist list of peers due to {}; if restarted" +
                    " and the nodes specified at the remote instance are down," +
                    " may be unable to transfer data until communications with those nodes are restored", e.toString());
            logger.error("", e);
        }
    }

    /**
     * Refreshes the list of S2S peers that flowfiles can be sent to or received from. Uses the stateful
     * cache to reduce network overhead.
     */
    private void refreshPeerStatusCache() {
        try {
            // Splitting enumeration and querying into separate methods allows better testing and composition
            final Set<PeerDescription> peersToQuery = getPeersToQuery();
            final Set<PeerStatus> statuses = fetchRemotePeerStatuses(peersToQuery);

            if (statuses.isEmpty()) {
                logger.info("No peers were retrieved from the remote group {}", peersToQuery.stream().map(p -> p.getHostname() + ":" + p.getPort()).collect(Collectors.joining(",")));
            }

            // Persist the fetched peer statuses
            PeerStatusCache peerStatusCache = new PeerStatusCache(statuses, System.currentTimeMillis(), peerStatusProvider.getRemoteInstanceUris(), peerStatusProvider.getTransportProtocol());
            persistPeerStatuses(peerStatusCache);
            logger.info("Successfully refreshed peer status cache; remote group consists of {} peers", statuses.size());
        } catch (Exception e) {
            warn(logger, eventReporter, "Unable to refresh remote group peers due to: {}", e.getMessage());
            if (logger.isDebugEnabled() && e.getCause() != null) {
                logger.warn("Caused by: ", e);
            }
        }
    }
}