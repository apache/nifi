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

import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.PeerStatusCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PeerSelectorTest {
    private static final PeerDescription BOOTSTRAP_PEER_DESCRIPTION = new PeerDescription("localhost", -1, false);
    private static final List<String> DEFAULT_NODES = Arrays.asList("node1.nifi", "node2.nifi", "node3.nifi");
    private static final String DEFAULT_REMOTE_INSTANCE_URIS = buildRemoteInstanceUris(DEFAULT_NODES);
    private static final Set<PeerStatus> DEFAULT_PEER_STATUSES = buildPeerStatuses(DEFAULT_NODES);
    private static final Set<PeerDescription> DEFAULT_PEER_DESCRIPTIONS = DEFAULT_PEER_STATUSES.stream()
            .map(PeerStatus::getPeerDescription)
            .collect(Collectors.toSet());

    /**A map where each key (peer description) is aware of all of its peer nodes (peer statuses).*/
    private static final Map<PeerDescription, Set<PeerStatus>> DEFAULT_PEER_NODES = DEFAULT_PEER_STATUSES.stream()
            .collect(Collectors.toMap(PeerStatus::getPeerDescription,
                    ps -> DEFAULT_PEER_STATUSES.stream()
                            .filter(peerStatus -> !peerStatus.getPeerDescription().getHostname().equals(ps.getPeerDescription().getHostname()))
                            .collect(Collectors.toSet()), (key1, key2) -> key1, LinkedHashMap::new));

    private static String buildRemoteInstanceUris(List<String> nodes) {
        return "http://" + String.join(":8443/nifi-api,http://", nodes) + ":8443/nifi-api";
    }

    private static Set<PeerStatus> buildPeerStatuses(List<String> nodes) {
        return nodes.stream()
                .map(nodeHostname -> new PeerDescription(nodeHostname, -1, false))
                .map(pd -> new PeerStatus(pd, 0, true))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns a map representing the cluster architecture formed by each hostname having the provided number of flowfiles.
     * @param peersWithFlowfiles a map of hostnames to flowfile counts
     * @return the map with formed objects like PeerStatus and PeerDescription
     */
    private static Map<PeerStatus, Integer> buildCluster(Map<String, Integer> peersWithFlowfiles) {
        return peersWithFlowfiles.entrySet().stream()
                .collect(Collectors.toMap(entry -> new PeerStatus(new PeerDescription(entry.getKey(), -1, false), entry.getValue(), true), Map.Entry::getValue,
                        (key1, key2) -> key1, LinkedHashMap::new));
    }

    /**
     * Returns a map of nodes to expected percentage of flowfiles allocated to/from the node.
     * @param nodes the map of nodes to current flowfile count
     * @param direction the transfer direction
     * @return the map of nodes to expected allocation
     */
    private static Map<String, Double> determineExpectedPercents(Map<String, Integer> nodes, TransferDirection direction) {
        final long totalFFC = nodes.values().stream()
                .reduce(0, Integer::sum);
        return nodes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> PeerSelector.calculateNormalizedWeight(direction, totalFFC, entry.getValue(), nodes.size())));
    }

    /**
     * Asserts that the provided frequency results are within {@code TOLERANCE} % of the expected values.
     * @param resultsFrequency  the map of node to invocations/hits
     * @param expectedPercents the map of node to expected percent of hits
     * @param numTimes         the total number of hits (defaults to the sum of all results)
     * @param tolerance         the tolerance for error (default 0.05 = 5%)
     */
    private static void assertDistributionPercentages(Map<String, Integer> resultsFrequency, final Map<String, Double> expectedPercents, final int numTimes, final double tolerance) {
        assertEquals(expectedPercents.keySet(), resultsFrequency.keySet());
        double realTolerance = tolerance * numTimes;

        expectedPercents.forEach((key, value) -> {
            double expectedCount = (value / 100) * numTimes;
            double lowerBound = Math.max(0, Math.round((expectedCount - realTolerance) * 100.0) / 100.0);
            double upperBound = Math.min(numTimes, Math.round((expectedCount + realTolerance) * 100.0) / 100.0);
            int count = resultsFrequency.get(key);
            assertTrue(count >= lowerBound && count <= upperBound);
        });
    }

    /**
     * Asserts that the provided frequency results are within {@code TOLERANCE} % of the expected values.
     * @param resultsFrequency the map of node to invocations/hits
     * @param expectedPercents the map of node to expected percent of hits
     */
    private static void assertDistributionPercentages(Map<String, Integer> resultsFrequency, final Map<String, Double> expectedPercents) {
        PeerSelectorTest.assertDistributionPercentages(resultsFrequency, expectedPercents, 10000, 0.05);
    }

    /**
     * Asserts that the last N peer selections do not have N-1 consecutive selections of the same peer, where N is the total peer count. This is a legacy requirement.
     * @param recentPeerSelectionQueue the recently selected peers (the PeerQueue should have been initialized with N elements)
     * @param nextPeer                 the next peer
     */
    private static void assertConsecutiveSelections(PeerQueue<String> recentPeerSelectionQueue, PeerStatus nextPeer) {
        try {
            recentPeerSelectionQueue.append(nextPeer.getPeerDescription().getHostname());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        int consecutiveElements = recentPeerSelectionQueue.getMaxConsecutiveElements();
        assertTrue(consecutiveElements <= recentPeerSelectionQueue.getTotalSize() - 1);
    }

    private static void setRestoreExpectation(PeerPersistence peerPersistence, Set<PeerStatus> peerStatuses) {
        try {
            when(peerPersistence.restore()).thenReturn(new PeerStatusCache(peerStatuses, System.currentTimeMillis(), DEFAULT_REMOTE_INSTANCE_URIS, SiteToSiteTransportProtocol.HTTP));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static PeerSelector buildPeerSelectorForCluster(PeerStatusProvider mockPSP, PeerPersistence mockPP, Map<String, Integer> nodes) {
        // Map the nodes to a cluster
        final Map<PeerStatus, Integer> clusterMap = buildCluster(nodes);
        setRestoreExpectation(mockPP, clusterMap.keySet());
        return new PeerSelector(mockPSP, mockPP);
    }

    @Test
    public void testGetPeersToQueryShouldBeEmpty(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws IOException {
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        final Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        assertEquals(1, peersToQuery.size());
        assertEquals(BOOTSTRAP_PEER_DESCRIPTION, peersToQuery.iterator().next());
    }

    @Test
    public void testShouldGetPeersToQuery(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws Exception {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);
        Set<PeerStatus> restoredPeerStatuses = buildPeerStatuses(DEFAULT_NODES);
        setRestoreExpectation(mockPP, restoredPeerStatuses);
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        final Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        assertEquals(restoredPeerStatuses.size() + 1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
        assertTrue(peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS));
    }

    /**
     * Asserts that calling the {@code #getPeersToQuery( )} method repeatedly provides the same result because it does not modify {@code lastFetchedQueryablePeers} directly.
     */
    @Test
    public void testGetPeersToQueryShouldBeIdempotent(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws IOException {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);

        final int numTimes = 3;
        setRestoreExpectation(mockPP, DEFAULT_PEER_STATUSES);
        final PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        final Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        final List<Set<PeerDescription>> repeatedPeersToQuery = new ArrayList<>();
        for(int num = 0; num < numTimes; num++) {
            repeatedPeersToQuery.add(ps.getPeersToQuery());
        }

        assertEquals(DEFAULT_PEER_STATUSES.size() + 1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
        assertTrue(peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS));

        repeatedPeersToQuery.forEach(query -> assertEquals(peersToQuery, query));
    }

    @Test
    public void testShouldFetchRemotePeerStatuses(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws IOException {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        when(mockPSP.fetchRemotePeerStatuses(any(PeerDescription.class))).thenAnswer(invocationOnMock ->
                DEFAULT_PEER_NODES.getOrDefault(invocationOnMock.getArgument(0, PeerDescription.class), new HashSet<>()));
        setRestoreExpectation(mockPP, DEFAULT_PEER_STATUSES);
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        final Set<PeerStatus> remotePeerStatuses = ps.fetchRemotePeerStatuses(DEFAULT_PEER_DESCRIPTIONS);

        assertEquals(DEFAULT_PEER_STATUSES.size(), remotePeerStatuses.size());
        assertTrue(remotePeerStatuses.containsAll(DEFAULT_PEER_STATUSES));
    }

    /**
     * Iterates through test scenarios of 100, 1000, and 10_000 total flowfiles and calculates the relative send and receive weights for every percentage.
     */
    @Test
    public void testShouldCalculateNormalizedWeight() {
        final String sendStr = "send";
        final String receiveStr = "receive";
        final Map<Integer, Map<Integer, Map<Integer, Map<String, Double>>>> results = new LinkedHashMap<>();
        Arrays.asList(3, 5, 7).forEach(nodeCount -> {
            results.put(nodeCount, new LinkedHashMap<>());
            Arrays.asList(2, 3, 4).forEach(e -> {
                int totalFlowfileCount = (int)Math.pow(10, e);
                results.get(nodeCount).put(totalFlowfileCount, new LinkedHashMap<>());
                final Map<Integer, Map<String, Double>> thisScenario = results.get(nodeCount).get(totalFlowfileCount);
                Stream.iterate(0, n -> n + 1)
                        .limit(100)
                        .forEach(i -> {
                            int flowfileCount = (i / 100 * totalFlowfileCount);
                            thisScenario.put(flowfileCount, new LinkedHashMap<>());
                            double sendWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.SEND, totalFlowfileCount, flowfileCount, nodeCount);
                            double receiveWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.RECEIVE, totalFlowfileCount, flowfileCount, nodeCount);
                            thisScenario.get(flowfileCount).put(sendStr, sendWeight);
                            thisScenario.get(flowfileCount).put(receiveStr, receiveWeight);
                        });
            });
        });

        results.forEach((nodeCount, t) -> t.forEach((total, r) -> r.forEach((key, value) -> {
            // Assert that the send percentage is always between 0% and 80%
            double send = value.get(sendStr);
            assertTrue(send >= 0 && send <= 80);
            // Assert that the receive percentage is always between 0% and 100%
            double receive = value.get(receiveStr);
            assertTrue(receive >= 0 && receive <= 100);
        })));
    }

    /**
     * Iterates through test scenarios of 100, 1000, and 10_000 total flowfiles and calculates the relative send and receive weights for every percentage.
     */
    @Test
    public void testShouldCalculateNormalizedWeightForSingleRemote() {
        final int nodeCount = 1;
        Arrays.asList(2, 3, 4).forEach(e -> {
            int totalFlowfileCount = (int)Math.pow(10, e);
            Stream.iterate(0, n -> n + 1)
                    .limit(100)
                    .forEach(i -> {
                        int flowfileCount = (i / 100 * totalFlowfileCount);
                        double sendWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.SEND, totalFlowfileCount, flowfileCount, nodeCount);
                        double receiveWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.RECEIVE, totalFlowfileCount, flowfileCount, nodeCount);
                        assertEquals(100, sendWeight);
                        assertEquals(100, receiveWeight);
                    });
        });
    }

    @Test
    public void testShouldBuildWeightedPeerMapForSend(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        Map<String, Integer> nodes = new LinkedHashMap<>(3);
        nodes.put("node1.nifi", 20);
        nodes.put("node2.nifi", 30);
        nodes.put("node3.nifi", 50);
        final Map<PeerStatus, Integer> clusterMap = buildCluster(nodes).entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                        LinkedHashMap::new));
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        setRestoreExpectation(mockPP, clusterMap.keySet());
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        Set<PeerStatus> peerStatuses = ps.getPeerStatuses();
        final Map<PeerStatus, Double> weightedPeerMap = ps.buildWeightedPeerMap(peerStatuses, TransferDirection.SEND);

        assertEquals(clusterMap.keySet(), weightedPeerMap.keySet());
    }

    @Test
    public void testShouldBuildWeightedPeerMapForReceive(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        Map<String, Integer> nodes = new LinkedHashMap<>(3);
        nodes.put("node1.nifi", 20);
        nodes.put("node2.nifi", 30);
        nodes.put("node3.nifi", 50);
        // Sort the map in descending order by value (RECEIVE)
        final Map<PeerStatus, Integer> clusterMap = buildCluster(nodes).entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                        LinkedHashMap::new));

        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        setRestoreExpectation(mockPP, clusterMap.keySet());
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        Set<PeerStatus> peerStatuses = ps.getPeerStatuses();
        final Map<PeerStatus, Double> weightedPeerMap = ps.buildWeightedPeerMap(peerStatuses, TransferDirection.RECEIVE);

        assertEquals(clusterMap.keySet(), weightedPeerMap.keySet());
    }

    /**
     * This test ensures that regardless of the total flowfile count, the resulting map has
     * normalized weights (i.e. percentage of 100).
     */
    @Test
    public void testCreateDestinationMapForSendShouldBeNormalized(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        Map<String, Map<String, Integer>> scenarios = new LinkedHashMap<>(8);
        Map<String, Integer> map1 = new LinkedHashMap<>(3);
        map1.put("node1.nifi", 100);
        map1.put("node2.nifi", 0);
        map1.put("node3.nifi", 0);
        scenarios.put("100 ff 100/0/0", map1);
        Map<String, Integer> map2 = new LinkedHashMap<>(3);
        map2.put("node1.nifi", 50);
        map2.put("node2.nifi", 50);
        map2.put("node3.nifi", 0);
        scenarios.put("100 ff 50/50/0", map2);
        Map<String, Integer> map3 = new LinkedHashMap<>(2);
        map3.put("node1.nifi", 100);
        map3.put("node2.nifi", 0);
        scenarios.put("100 ff 100/0", map3);
        Map<String, Integer> map4 = new LinkedHashMap<>(3);
        map4.put("node1.nifi", 200);
        map4.put("node2.nifi", 300);
        map4.put("node3.nifi", 500);
        scenarios.put("1000 ff 200/300/500", map4);
        Map<String, Integer> map5 = new LinkedHashMap<>(3);
        map5.put("node1.nifi", 333);
        map5.put("node2.nifi", 333);
        map5.put("node3.nifi", 334);
        scenarios.put("1000 ff 333/333/334", map5);
        Map<String, Integer> map6 = new LinkedHashMap<>(5);
        map6.put("node1.nifi", 0);
        map6.put("node2.nifi", 250);
        map6.put("node3.nifi", 250);
        map6.put("node4.nifi", 250);
        map6.put("node5.nifi", 250);
        scenarios.put("1000 ff 0/250x4", map6);
        scenarios.put("1000 ff 142x7", Stream.iterate(1, n -> n + 1)
                .limit(7)
                .map(i -> new AbstractMap.SimpleEntry<>("node" + i + ".nifi", 1000/7))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (e1, e2) -> e2,
                        LinkedHashMap::new)));
        Map<String, Integer> map7 = new LinkedHashMap<>(1);
        map7.put("node1.nifi", 151);
        for(int i = 2; i <= 50; i++) {
            map7.put("node" + i + "nifi", 1);
        }
        scenarios.put("200 ff 151/1x49", map7);

        scenarios.forEach((name, nodes) -> {
            PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);
            Set<PeerStatus> peerStatuses = ps.getPeerStatuses();

            // Check both SEND and RECEIVE
            Arrays.stream(TransferDirection.values()).forEach(direction -> {
                Map<PeerStatus, Double> destinationMap = ps.createDestinationMap(peerStatuses, direction);
                assertEquals(peerStatuses, destinationMap.keySet());

                // For uneven splits, the resulting percentage should be within +/- 1%
                double totalPercentage = destinationMap.values().stream()
                        .reduce(0.0, Double::sum);
                assertTrue(totalPercentage >= 99 && totalPercentage <= 100);
            });
        });
    }

    /**
     * Test the edge case where there is a rounding error and the selected random number is not captured in the buckets
     */
    @Test
    public void testGetAvailablePeerStatusShouldHandleEdgeCase(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 10000;

        Map<String, Integer> nodes = new LinkedHashMap<>(3);
        nodes.put("node1.nifi", 2);
        nodes.put("node2.nifi", 1);
        nodes.put("node3.nifi", 1);

        // Make a map where the weights are artificially suppressed and total far less than 100% to make the edge case more likely
        final Map<PeerStatus, Double> suppressedPercentageMap =
                buildPeerStatuses(new ArrayList<>(nodes.keySet())).stream()
                        .collect(Collectors.toMap(peerStatus -> peerStatus,
                                peerStatus -> nodes.get(peerStatus.getPeerDescription().getHostname()) / 100.0));

        final PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);

        // Collect the results and analyze the resulting frequency distribution
        Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                .collect(Collectors.toMap(node -> node, node -> 0));

        Stream.iterate(0, n -> n + 1)
                .limit(numTimes)
                .forEach(i -> {
                            PeerStatus nextPeer = ps.getAvailablePeerStatus(suppressedPercentageMap);
                            resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                                    (key, value) -> value + 1);
                        }
                );

        // The actual distribution would be 50/25/25
        Map<String, Double> expectedPercents = new LinkedHashMap<>(3);
        expectedPercents.put("node1.nifi", 50.0);
        expectedPercents.put("node2.nifi", 25.0);
        expectedPercents.put("node3.nifi", 25.0);

        assertDistributionPercentages(resultsFrequency, expectedPercents, numTimes, 0.05);
    }

    @Test
    public void testShouldGetNextPeer(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 10000;

        Map<String, Integer> nodes = new LinkedHashMap<>(3);
        nodes.put("node1.nifi", 20);
        nodes.put("node2.nifi", 30);
        nodes.put("node3.nifi", 50);

        // Check both SEND and RECEIVE
        Arrays.stream(TransferDirection.values()).forEach(direction -> {
            PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);
            // Collect the results and analyze the resulting frequency distribution
            Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                    .collect(Collectors.toMap(node -> node, node -> 0));

            Stream.iterate(0, n -> n + 1)
                    .limit(numTimes)
                    .forEach(i -> {
                        PeerStatus nextPeer = ps.getNextPeerStatus(direction);
                        resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                                (key, value) -> value + 1);
                    });

            final Map<String, Double> determineExpectedPercents = determineExpectedPercents(nodes, direction);
            assertDistributionPercentages(resultsFrequency, determineExpectedPercents);
        });
    }

    /**
     * When the cluster is balanced, the consecutive selection of peers should not repeat {@code cluster.size( ) - 1} times.
     */
    @Test
    public void testGetNextPeerShouldNotRepeatPeersOnBalancedCluster(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 10000;

        final Map<String, Integer> nodes = Stream.iterate(1, n -> n + 1)
                .limit(10)
                .map(i -> "node" + i + ".nifi")
                .collect(Collectors.toMap(key -> key, value -> 100));

        final PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);

        // Check both SEND and RECEIVE
        Arrays.stream(TransferDirection.values()).forEach(direction -> {
            // Collect the results and analyze the resulting frequency distribution
            Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                    .collect(Collectors.toMap(key -> key, value -> 0));

            // Use the queue to track recent peers and observe repeated selections
            PeerQueue<String> lastN = new PeerQueue<>(nodes.size());
            Stream.iterate(1, n -> n + 1)
                    .limit(numTimes)
                    .forEach( i -> {
                        PeerStatus nextPeer = ps.getNextPeerStatus(direction);
                        resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                                (key, value) -> value + 1);

                        // Assert the consecutive selections are ok
                        assertConsecutiveSelections(lastN, nextPeer);
                    });

            final Map<String, Double> expectedPercents = nodes.keySet().stream()
                    .collect(Collectors.toMap(key -> key, value -> 10.0));

            // The tolerance should be a bit higher because of the high number of nodes and even distribution
            assertDistributionPercentages(resultsFrequency, expectedPercents, numTimes, 0.10);
        });
    }

    /**
     * When a remote has only one valid peer, that peer should be selected every time
     */
    @Test
    public void testGetNextPeerShouldRepeatPeersOnSingleValidDestination(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 100;

        // Single destination scenarios
        Map<String, Map<String, Integer>> scenarios = new LinkedHashMap<>(3);
        Map<String, Integer> map1 = new LinkedHashMap<>(1);
        map1.put("node1.nifi", 100);
        scenarios.put("single node", map1);
        Map<String, Integer> map2 = new LinkedHashMap<>(1);
        map2.put("node1.nifi", 0);
        scenarios.put("single empty node", map2);
        Map<String, Integer> map3 = new LinkedHashMap<>(2);
        map3.put("node1.nifi", 100);
        map3.put("node2.nifi", 0);
        scenarios.put("100 ff 100/0", map3);

        scenarios.forEach((name, nodes) -> {
            PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);

            Arrays.stream(TransferDirection.values()).forEach(direction -> {
                Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                        .collect(Collectors.toMap(key -> key, value -> 0));
                PeerQueue<String> lastN = new PeerQueue<>(nodes.size());
                Stream.iterate(1, n -> n + 1)
                        .limit(numTimes)
                        .forEach(i -> {
                            PeerStatus nextPeer = ps.getNextPeerStatus(direction);
                            resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                                    (key, value) -> value + 1);
                            // Assert the consecutive selections are ok (i.e. it IS selecting the same peer repeatedly)
                            if (lastN.remainingCapacity() == 0) {
                                lastN.remove();
                            }
                            try {
                                lastN.put(nextPeer.getPeerDescription().getHostname());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }

                            // Spot check consecutive selection
                            if (i % 10 == 0) {
                                int consecutiveElements = lastN.getMaxConsecutiveElements();
                                assertEquals(lastN.size(), consecutiveElements);
                            }
                        });

                final Map<String, Double> expectedPercents = determineExpectedPercents(nodes, direction);
                // The tolerance should be zero; exact matches only
                assertDistributionPercentages(resultsFrequency, expectedPercents, numTimes, 0.00);
            });
        });
    }

    /**
     * The legacy requirement that the next peer not repeat N-1 times where N is the size of the remote cluster does not apply to the following scenarios:
     * <p>
     * * A remote of size <= 3
     * * An unbalanced remote (33/33/33/0) <em>should</em> repeat the last peer multiple times
     */
    @Test
    public void testGetNextPeerShouldRepeatPeersOnUnbalancedCluster(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        // Using a higher iteration count smooths out outliers
        final int numTimes = 10000;

        // Scenarios where consecutively-selected peers are expected to sometimes repeat (small clusters, uneven clusters)
        Map<String, Map<String, Integer>> scenarios = new LinkedHashMap<>(10);
        Map<String, Integer> map1 = new LinkedHashMap<>(2);
        map1.put("node1.nifi", 50);
        map1.put("node2.nifi", 50);
        scenarios.put("100 ff 50/50", map1);
        Map<String, Integer> map2 = new LinkedHashMap<>(2);
        map2.put("node1.nifi", 75);
        map2.put("node2.nifi", 25);
        scenarios.put("100 ff 75/25", map2);
        Map<String, Integer> map3 = new LinkedHashMap<>(3);
        map3.put("node1.nifi", 50);
        map3.put("node2.nifi", 50);
        map3.put("node3.nifi", 0);
        scenarios.put("100 ff 50/50/0", map3);
        Map<String, Integer> map4 = new LinkedHashMap<>(3);
        map4.put("node1.nifi", 800);
        map4.put("node2.nifi", 200);
        map4.put("node3.nifi", 0);
        scenarios.put("1000 ff 800/200/0", map4);
        Map<String, Integer> map5 = new LinkedHashMap<>(3);
        map5.put("node1.nifi", 8);
        map5.put("node2.nifi", 2);
        map5.put("node3.nifi", 0);
        scenarios.put("10 ff 8/2/0", map5);
        Map<String, Integer> map6 = new LinkedHashMap<>(4);
        map6.put("node1.nifi", 66);
        map6.put("node2.nifi", 66);
        map6.put("node3.nifi", 66);
        map6.put("node4.nifi", 0);
        scenarios.put("200 ff 66x3/0", map6);
        Map<String, Integer> map7 = new LinkedHashMap<>(5);
        map7.put("node1.nifi", 0);
        map7.put("node2.nifi", 250);
        map7.put("node3.nifi", 250);
        map7.put("node4.nifi", 250);
        map7.put("node5.nifi", 250);
        scenarios.put("1000 ff 0/250x4", map7);
        Map<String, Integer> map8 = new LinkedHashMap<>(1);
        map8.put("node1.nifi", 0);
        for(int i = 2; i <= 10; i++) {
            map8.put("node" + i + ".nifi", 111);
        }
        scenarios.put("1000 ff 0/111x9", map8);

        Map<String, Integer> map9 = new LinkedHashMap<>(2);
        map9.put("node1.nifi", 1024);
        map9.put("node2.nifi", 10240);
        for(int i = 3; i <= 5; i++) {
            map9.put("node" + i + ".nifi", 4096);
        }
        scenarios.put("legacy 1024/10240/4096x3", map9);

        Map<String, Integer> map10 = new LinkedHashMap<>(2);
        map10.put("node1.nifi", 50_000);
        map10.put("node2.nifi", 50);
        scenarios.put("legacy 50k/500", map10);

        scenarios.forEach((name, nodes) -> {
            PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);
            // Check both SEND and RECEIVE
            Arrays.stream(TransferDirection.values()).forEach( direction -> {
                // Collect the results and analyze the resulting frequency distribution
                Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                        .collect(Collectors.toMap(key -> key, value -> 0));

                // Use the queue to track recent peers and observe repeated selections
                PeerQueue<String> lastN = new PeerQueue<>(nodes.size());
                Stream.iterate(1, n -> n + 1)
                        .limit(numTimes)
                        .forEach(i -> {
                            PeerStatus nextPeer = ps.getNextPeerStatus(direction);
                            resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                                    (key, value) -> value + 1);

                            // Assert the consecutive selections are ok (i.e. it IS selecting the same peer repeatedly)
                            if (lastN.remainingCapacity() == 0) {
                                lastN.remove();
                            }
                            try {
                                lastN.put(nextPeer.getPeerDescription().getHostname());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });

                Map<String, Double> expectedPercents = determineExpectedPercents(nodes, direction);
                assertDistributionPercentages(resultsFrequency, expectedPercents);
            });
        });
    }

    /**
     * Test the edge case where peers are penalized
     */
    @Test
    public void testGetAvailablePeerStatusShouldHandlePenalizedPeers(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 100;

        // Should prefer node1, but it will be penalized
        Map<String, Integer> nodes = new LinkedHashMap<>(2);
        nodes.put("node1.nifi", 10);
        nodes.put("node2.nifi", 90);

        // Make a map where the weights are normal
        Set<PeerStatus> peerStatuses = buildPeerStatuses(new ArrayList<>(nodes.keySet()));
        final Map<PeerStatus, Double> weightMap = peerStatuses.stream()
                .collect(Collectors.toMap(key -> key, value -> nodes.get(value.getPeerDescription().getHostname()).doubleValue()));
        final PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);

        // Penalize node1
        ps.penalize(peerStatuses.iterator().next().getPeerDescription(), 10_000);

        // Collect the results and analyze the resulting frequency distribution
        final Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                .collect(Collectors.toMap(key -> key, value -> 0));

        Stream.iterate(1, n -> n + 1)
                .limit(numTimes)
                .forEach(i -> {
                    PeerStatus nextPeer = ps.getAvailablePeerStatus(weightMap);
                    resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                            (key, value) -> value + 1);
                });

        // The actual distribution would be .9/.1, but because of the penalization, all selections will be node2
        Map<String, Double> expectedPercents = new LinkedHashMap<>(2);
        expectedPercents.put("node1.nifi", 0.0);
        expectedPercents.put("node2.nifi", 100.0);

        // The tolerance should be very tight as this will be almost exact every time
        assertDistributionPercentages(resultsFrequency, expectedPercents, numTimes, 0.00);
    }

    /**
     * Test the edge case where peers are penalized
     */
    @Test
    public void testGetAvailablePeerStatusShouldHandleMultiplePenalizedPeers(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);

        final int numTimes = 10_000;

        // Should distribute evenly, but 1/2 of the nodes will be penalized
        Map<String, Integer> nodes = new LinkedHashMap<>(4);
        nodes.put("node1.nifi", 25);
        nodes.put("node2.nifi", 25);
        nodes.put("node3.nifi", 25);
        nodes.put("node4.nifi", 25);

        // Make a map where the weights are normal
        Set<PeerStatus> peerStatuses = buildPeerStatuses(new ArrayList<>(nodes.keySet()));
        final Map<PeerStatus, Double> weightMap = peerStatuses.stream()
                .collect(Collectors.toMap(key -> key, value -> nodes.get(value.getPeerDescription().getHostname()).doubleValue()));
        final PeerSelector ps = buildPeerSelectorForCluster(mockPSP, mockPP, nodes);

        // Penalize node1 & node3
        Set<PeerStatus> penalizedPeerStatuses = peerStatuses.stream()
                        .filter(peerStatus -> Arrays.asList("node1.nifi", "node3.nifi").contains(peerStatus.getPeerDescription().getHostname()))
                        .collect(Collectors.toSet());

        penalizedPeerStatuses.forEach( penalizedPeerStatus -> ps.penalize(penalizedPeerStatus.getPeerDescription(), 10_000));

        // Collect the results and analyze the resulting frequency distribution
        final Map<String, Integer> resultsFrequency = nodes.keySet().stream()
                .collect(Collectors.toMap(key -> key, value -> 0));

        Stream.iterate(1, n -> n + 1)
                .limit(numTimes)
                .forEach(i -> {
                    PeerStatus nextPeer = ps.getAvailablePeerStatus(weightMap);
                    resultsFrequency.computeIfPresent(nextPeer.getPeerDescription().getHostname(),
                            (key, value) -> value + 1);
                });

        // The actual distribution would be .25 * 4, but because of the penalization, node2 and node4 will each have ~50%
        Map<String, Double> expectedPercents = new LinkedHashMap<>(4);
        expectedPercents.put("node1.nifi", 0.0);
        expectedPercents.put("node2.nifi", 50.0);
        expectedPercents.put("node3.nifi", 0.0);
        expectedPercents.put("node4.nifi", 50.0);

        assertDistributionPercentages(resultsFrequency, expectedPercents, numTimes, 0.05);
    }

    /**
     * Test that the cache is the source of peer statuses initially
     */
    @Test
    public void testInitializationShouldRestorePeerStatusFileCache(@Mock PeerStatusProvider mockPSP) throws Exception {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt");
        cacheFile.deleteOnExit();

        final String cacheContents = mockPSP.getTransportProtocol() + "\n" +
                AbstractPeerPersistence.REMOTE_INSTANCE_URIS_PREFIX + mockPSP.getRemoteInstanceUris() + "\n" +
                DEFAULT_PEER_STATUSES.stream()
                        .map(ps -> String.join(":", ps.getPeerDescription().getHostname(),
                                Integer.toString(ps.getPeerDescription().getPort()), Boolean.toString(ps.getPeerDescription().isSecure()),
                                Boolean.toString(ps.isQueryForPeers())))
                        .collect(Collectors.joining("\n"));
        Files.writeString(cacheFile.toPath(), cacheContents);
        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile);
        // The constructor should restore the initial cache
        PeerSelector ps = new PeerSelector(mockPSP, filePP);
        // PeerSelector should access peer statuses from cache
        final Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        assertEquals(DEFAULT_NODES.size() + 1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
        assertTrue(peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS));
    }

    /**
     * Test that if the cache is expired, it is not used
     */
    @Test
    public void testRefreshShouldHandleExpiredPeerStatusFileCache(@Mock PeerStatusProvider mockPSP) throws Exception {
        List<String> nodes = DEFAULT_NODES;
        String remoteInstanceUris = buildRemoteInstanceUris(nodes);
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(remoteInstanceUris);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);
        when(mockPSP.fetchRemotePeerStatuses(any(PeerDescription.class))).thenAnswer(invocationOnMock -> new HashSet<>());

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt");
        cacheFile.deleteOnExit();

        // Construct the cache contents and write to disk
        final String cacheContents = mockPSP.getTransportProtocol() + "\n" +
                AbstractPeerPersistence.REMOTE_INSTANCE_URIS_PREFIX + mockPSP.getRemoteInstanceUris() + "\n" +
                DEFAULT_PEER_STATUSES.stream()
                        .map(ps -> String.join(":", ps.getPeerDescription().getHostname(),
                                Integer.toString(ps.getPeerDescription().getPort()), Boolean.toString(ps.getPeerDescription().isSecure()),
                                Boolean.toString(ps.isQueryForPeers())))
                        .collect(Collectors.joining("\n"));
        Files.writeString(cacheFile.toPath(), cacheContents);


        // Mark the file as expired
        cacheFile.setLastModified(System.currentTimeMillis() - (PeerSelector.PEER_CACHE_MILLIS * 2));

        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile);

        // The constructor should restore the initial cache
        PeerSelector ps = new PeerSelector(mockPSP, filePP);

        // The loaded cache should be marked as expired and not used
        assertTrue(ps.isCacheExpired(ps.peerStatusCache));

        // This internal method does not refresh or check expiration
        Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        // The cache has (expired) peer statuses present
        assertEquals(nodes.size() + 1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
        assertTrue(peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS));

        // Trigger the cache expiration detection
        ps.refresh();

        peersToQuery = ps.getPeersToQuery();

        // The cache only contains the bootstrap node
        assertEquals(1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
    }

    public Throwable generateException(String message, int nestedLevel) {
        IOException e = new IOException(message);
        for(int i = 1; i < nestedLevel; i++) {
            e = new IOException(message + " " + i, e);
        }

        return e;
    }

    /**
     * Test that printing the exception does not cause an infinite loop
     */
    @Test
    public void testRefreshShouldHandleExceptions(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws Exception {
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(DEFAULT_REMOTE_INSTANCE_URIS);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);
        when(mockPP.restore()).thenReturn(new PeerStatusCache(new HashSet<>(), System.currentTimeMillis(),
                DEFAULT_REMOTE_INSTANCE_URIS, SiteToSiteTransportProtocol.HTTP));
        doThrow(generateException("Custom error message", 3)).when(mockPP).save(any(PeerStatusCache.class));


        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        ps.refreshPeerStatusCache();
        Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        assertEquals(1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
    }

    /**
     * Test that the cache is not used if it does not match the transport protocol
     */
    @Test
    public void testInitializationShouldIgnoreCacheWithWrongTransportProtocol(@Mock PeerStatusProvider mockPSP) throws Exception {
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(BOOTSTRAP_PEER_DESCRIPTION);

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt");
        cacheFile.deleteOnExit();

        // Construct the cache contents (with wrong TP - mockPSP uses HTTP) and write to disk
        final String cacheContents = SiteToSiteTransportProtocol.RAW + "\n" +
                DEFAULT_PEER_STATUSES.stream()
                        .map(ps -> String.join(":", ps.getPeerDescription().getHostname(),
                                Integer.toString(ps.getPeerDescription().getPort()), Boolean.toString(ps.getPeerDescription().isSecure()),
                                Boolean.toString(ps.isQueryForPeers())))
                        .collect(Collectors.joining("\n"));
        Files.writeString(cacheFile.toPath(), cacheContents);
        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile);

        PeerSelector ps = new PeerSelector(mockPSP, filePP);

        // The cache should be ignored because of the transport protocol mismatch
        final Set<PeerDescription> peersToQuery = ps.getPeersToQuery();

        assertEquals(1, peersToQuery.size());
        assertTrue(peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION));
    }

    /**
     * This test simulates a failure scenario of a remote NiFi cluster. It confirms that:
     * <ol>
     *     <li>PeerSelector uses the bootstrap node to fetch remote peer statuses at the initial attempt</li>
     *     <li>PeerSelector uses one of query-able nodes lastly fetched successfully</li>
     *     <li>PeerSelector can refresh remote peer statuses even if the bootstrap node is down</li>
     *     <li>PeerSelector returns null as next peer when there's no peer available</li>
     *     <li>PeerSelector always tries to fetch peer statuses at least from the bootstrap node, so that it can
     *     recover when the node gets back online</li>
     * </ol>
     */
    @Test
    public void testShouldFetchRemotePeerStatusesInFailureScenario(@Mock PeerStatusProvider mockPSP, @Mock PeerPersistence mockPP) throws IOException {
        final List<String> nodes = Arrays.asList("node1.nifi", "node2.nifi");
        final Set<PeerStatus> peerStatuses = buildPeerStatuses(nodes);
        // Need references to the bootstrap and node2 later
        final PeerStatus bootstrapStatus = peerStatuses.stream()
                .filter(peerStatus -> "node1.nifi".equals(peerStatus.getPeerDescription().getHostname()))
                .findFirst().get();
        final PeerStatus node2Status = peerStatuses.stream()
                .filter(peerStatus -> "node2.nifi".equals(peerStatus.getPeerDescription().getHostname()))
                .findFirst().get();
        final String remoteInstanceUris = buildRemoteInstanceUris(nodes);
        when(mockPSP.getTransportProtocol()).thenReturn(SiteToSiteTransportProtocol.HTTP);
        when(mockPSP.getRemoteInstanceUris()).thenReturn(remoteInstanceUris);
        when(mockPSP.getBootstrapPeerDescription()).thenReturn(bootstrapStatus.getPeerDescription());
        when(mockPSP.fetchRemotePeerStatuses(any(PeerDescription.class)))
                .thenReturn(new LinkedHashSet<>(Arrays.asList(bootstrapStatus, node2Status)))
                .thenReturn(Collections.singleton(node2Status))
                .thenReturn(Collections.singleton(node2Status))
                .thenReturn(new LinkedHashSet<>())
                .thenReturn(Collections.singleton(bootstrapStatus));

        // Mock the PP with only these statuses
        when(mockPP.restore()).thenReturn(new PeerStatusCache(peerStatuses, System.currentTimeMillis(), remoteInstanceUris, SiteToSiteTransportProtocol.HTTP));
        doNothing().when(mockPP).save(any(PeerStatusCache.class));
        PeerSelector ps = new PeerSelector(mockPSP, mockPP);
        ps.refresh();
        PeerStatus peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNotNull(peerStatus);
        // Force the selector to refresh the cache
        ps.refreshPeerStatusCache();

        // Attempt 2 & 3 - only node2 available (PSP will only return node2)
        for(int time = 2; time <= 3; time++) {
            ps.refresh();
            peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE);
            assertEquals(node2Status, peerStatus);
            // Force the selector to refresh the cache
            ps.refreshPeerStatusCache();
        }

        // Attempt 4 - no available nodes
        ps.refresh();
        peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNull(peerStatus);

        // Force the selector to refresh the cache
        ps.refreshPeerStatusCache();

        // Attempt 5 - bootstrap node available
        ps.refresh();
        peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE);
        assertEquals(bootstrapStatus, peerStatus);
    }

    /**
     * Tests the utility class {@link PeerQueue} used to track consecutive peer selection.
     */
    @Test
    public void testPeerQueueShouldGetMaxConsecutiveElements() throws Exception {
        final PeerQueue<String> peerQueue = new PeerQueue<>(10);
        final List<String> nodes = Stream.iterate(1, n -> n +1)
                .limit(5)
                .map(index -> "node" + index + "nifi")
                .collect(Collectors.toList());

        final List<PeerStatus> peerStatuses = new ArrayList<>(buildPeerStatuses(nodes));

        // Same node every time
        for(int index = 1; index <= 100; index ++) {
            peerQueue.append(nodes.get(0));
            assertEquals(peerQueue.size(), peerQueue.getMaxConsecutiveElements());
        }

        // Never repeating node
        peerQueue.clear();
        for(int index = 1; index <= 100; index ++) {
            peerQueue.append(nodes.get(index % peerStatuses.size()));
            assertEquals(1, peerQueue.getMaxConsecutiveElements());
        }

        // Repeat up to nodes.size() times but no more
        peerQueue.clear();
        for(int index = 1; index <= 100; index ++) {
            peerQueue.append( (index % nodes.size() == 0) ? nodes.get(nodes.size() - 1) : nodes.get(0));
            assertTrue(peerQueue.getMaxConsecutiveElements() <= peerStatuses.size());
        }
    }

    public static class PeerQueue<E> extends ArrayBlockingQueue<E> {
        public PeerQueue(int capacity) {
            super(capacity);
        }

        public int getTotalSize() {
            return this.size() + this.remainingCapacity();
        }

        public int getMaxConsecutiveElements() {
            int currentMax = 1;
            int current = 1;
            Iterator<E> iterator = this.iterator();
            Object prev = iterator.next();
            while (iterator.hasNext()) {
                Object curr = iterator.next();
                if (prev.equals(curr)) {
                    current++;
                    if (current > currentMax) {
                        currentMax = current;
                    }

                } else {
                    current = 1;
                }

                prev = curr;
            }

            return currentMax;
        }

        /**
         * Adds the new Object to the tail of the queue. If the queue was full before, removes the head to open capacity.
         *
         * @param o the object to append
         */
        public void append(E o) throws InterruptedException {
            if (this.remainingCapacity() == 0) {
                this.remove();
            }

            this.put(o);
        }

    }
}
