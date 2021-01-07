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
package org.apache.nifi.remote.client


import org.apache.nifi.remote.PeerDescription
import org.apache.nifi.remote.PeerStatus
import org.apache.nifi.remote.TransferDirection
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol
import org.apache.nifi.remote.util.PeerStatusCache
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security
import java.util.concurrent.ArrayBlockingQueue

@RunWith(JUnit4.class)
class PeerSelectorTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(PeerSelectorTest.class)

    private static final BOOTSTRAP_PEER_DESCRIPTION = new PeerDescription("localhost", -1, false)
    private static final List<String> DEFAULT_NODES = ["node1.nifi", "node2.nifi", "node3.nifi"]
    private static final String DEFAULT_REMOTE_INSTANCE_URIS = buildRemoteInstanceUris(DEFAULT_NODES)
    private static final Set<PeerStatus> DEFAULT_PEER_STATUSES = buildPeerStatuses(DEFAULT_NODES)
    private static final Set<PeerDescription> DEFAULT_PEER_DESCRIPTIONS = DEFAULT_PEER_STATUSES*.peerDescription
    private static final Map<PeerDescription, Set<PeerStatus>> DEFAULT_PEER_NODES = buildPeersMap(DEFAULT_PEER_STATUSES)

    // Default collaborators
    private static mockPSP
    private static mockPP


    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        // Mock collaborators
        mockPSP = mockPeerStatusProvider()
        mockPP = mockPeerPersistence()
    }

    @After
    void tearDown() {

    }

    private static String buildRemoteInstanceUris(List<String> nodes = DEFAULT_NODES) {
        String remoteInstanceUris = "http://" + nodes.join(":8443/nifi-api,http://") + ":8443/nifi-api";
        remoteInstanceUris
    }

    private static Set<PeerStatus> buildPeerStatuses(List<String> nodes = DEFAULT_NODES) {
        Set<PeerDescription> nodePeerDescriptions = nodes.collect { String nodeHostname ->
            new PeerDescription(nodeHostname, -1, false)
        }

        Set<PeerStatus> peerStatuses = nodePeerDescriptions.collect { PeerDescription pd ->
            new PeerStatus(pd, 0, true)
        }
        peerStatuses
    }

    /**
     * Returns a map representing the cluster architecture formed by each hostname having the provided number of flowfiles.
     *
     * @param peersWithFlowfiles a map of hostnames to flowfile counts
     * @return the map with formed objects like PeerStatus and PeerDescription
     */
    private static Map<PeerStatus, Integer> buildCluster(Map<String, Integer> peersWithFlowfiles = [:]) {
        peersWithFlowfiles.collectEntries { String hostname, Integer flowfileCount ->
            [new PeerStatus(new PeerDescription(hostname, -1, false), flowfileCount, true), flowfileCount]
        }
    }

    /**
     * Returns a map where each key (peer description) is aware of all of its peer nodes (peer statuses).
     *
     * @param peerStatuses the set of peer statuses
     * @return a map of PDs to sibling peers
     */
    private static Map<PeerDescription, Set<PeerStatus>> buildPeersMap(Set<PeerStatus> peerStatuses) {
        peerStatuses.collectEntries { PeerStatus ps ->
            [ps.peerDescription, peerStatuses.findAll { it.peerDescription.hostname != ps.peerDescription.hostname }]
        }
    }

    /**
     * Returns a map of nodes to expected percentage of flowfiles allocated to/from the node.
     *
     * @param nodes the map of nodes to current flowfile count
     * @param direction the transfer direction
     * @return the map of nodes to expected allocation
     */
    private static Map<String, Double> determineExpectedPercents(Map<String, Integer> nodes, TransferDirection direction = TransferDirection.SEND) {
        long totalFFC = nodes.values().sum() as long
        nodes.collectEntries { name, ffc ->
            [name, PeerSelector.calculateNormalizedWeight(direction, totalFFC, ffc, nodes.size())]
        }
    }

    /**
     * Asserts that the provided frequency results are within {@code TOLERANCE} % of the expected values.
     *
     * @param resultsFrequency the map of node to invocations/hits
     * @param EXPECTED_PERCENTS the map of node to expected percent of hits
     * @param NUM_TIMES the total number of hits (defaults to the sum of all results)
     * @param TOLERANCE the tolerance for error (default 0.05 = 5%)
     */
    private static void assertDistributionPercentages(Map<String, Integer> resultsFrequency,
                                                      final Map<String, Double> EXPECTED_PERCENTS,
                                                      final int NUM_TIMES = resultsFrequency.values().sum() as int,
                                                      final double TOLERANCE = 0.05) {
        assert resultsFrequency.keySet() == EXPECTED_PERCENTS.keySet()

        logger.info("  Actual results: ${resultsFrequency.sort()}")
        logger.info("Expected results: ${EXPECTED_PERCENTS.sort().collect { k, v -> "${k}: ${v}%" }}")

        def max = resultsFrequency.max { a, b -> a.value <=> b.value }
        def min = resultsFrequency.min { a, b -> a.value <=> b.value }
        logger.info("Max: ${max.key} (${max.value}) | Min: ${min.key} (${min.value})")
        def realTolerance = TOLERANCE * NUM_TIMES
        logger.debug("Tolerance is measured as a percent of total flowfiles (${TOLERANCE * 100}% of ${NUM_TIMES} = ${realTolerance.round(2)})")

        // TODO: Change percentages to be percentage points of total for even comparison
        EXPECTED_PERCENTS.each { k, v ->
            def expectedCount = (v / 100) * NUM_TIMES
            def lowerBound = Math.max(0, (expectedCount - realTolerance).round(2))
            def upperBound = Math.min(NUM_TIMES, (expectedCount + realTolerance).round(2))
            def count = resultsFrequency[k]
            def difference = Math.abs(expectedCount - count) / NUM_TIMES
            logger.debug("Checking that ${count} is within ±${TOLERANCE * 100}% of ${expectedCount} (${lowerBound}, ${upperBound}) | ${(difference * 100).round(2)}%")
            assert count >= lowerBound && count <= upperBound
        }
    }

    /**
     * Asserts that the last N peer selections do not have N-1 consecutive selections of the same peer, where N is the total peer count. This is a legacy requirement.
     *
     * @param recentPeerSelectionQueue the recently selected peers (the PeerQueue should have been initialized with N elements)
     * @param nextPeer the next peer
     */
    private static void assertConsecutiveSelections(PeerQueue recentPeerSelectionQueue, PeerStatus nextPeer) {
        recentPeerSelectionQueue.append(nextPeer.peerDescription.hostname)
        int consecutiveElements = recentPeerSelectionQueue.getMaxConsecutiveElements()
//        String mcce = recentPeerSelectionQueue.getMostCommonConsecutiveElement()
//        logger.debug("Most consecutive elements in recentPeerSelectionQueue: ${consecutiveElements} - ${mcce} | ${recentPeerSelectionQueue}")
        assert consecutiveElements <= recentPeerSelectionQueue.totalSize - 1
    }

    private static double calculateMean(Map resultsFrequency) {
        int n = resultsFrequency.size()
        Object meanIndex = n % 2 == 0 ? (n / 2 - 1)..(n / 2) : (n / 2).intValue()
        List meanElements = resultsFrequency.values().sort()[meanIndex] as List
        return meanElements.sum() / meanElements.size()
    }

    private static PeerStatusProvider mockPeerStatusProvider(PeerDescription bootstrapPeerDescription = BOOTSTRAP_PEER_DESCRIPTION, String remoteInstanceUris = DEFAULT_REMOTE_INSTANCE_URIS, Map<PeerDescription, Set<PeerStatus>> peersMap = DEFAULT_PEER_NODES) {
        [getTransportProtocol       : { ->
            SiteToSiteTransportProtocol.HTTP
        },
         getRemoteInstanceUris: { ->
             remoteInstanceUris
         },
         getBootstrapPeerDescription: { ->
             bootstrapPeerDescription
         },
         fetchRemotePeerStatuses    : { PeerDescription pd ->
             peersMap[pd] ?: [] as Set<PeerStatus>
         }] as PeerStatusProvider
    }

    private static PeerPersistence mockPeerPersistence(String remoteInstanceUris = DEFAULT_REMOTE_INSTANCE_URIS, Set<PeerStatus> peerStatuses = DEFAULT_PEER_STATUSES) {
        [restore: { ->
            new PeerStatusCache(peerStatuses, System.currentTimeMillis(), remoteInstanceUris, SiteToSiteTransportProtocol.HTTP)
        },
         save   : { PeerStatusCache psc ->
             logger.mock("Persisting PeerStatusCache: ${psc}")
         }] as PeerPersistence
    }

    private static PeerSelector buildPeerSelectorForCluster(String scenarioName, Map nodes) {
        // Map the nodes to a cluster
        def clusterMap = buildCluster(nodes)
        logger.info("Using cluster map (${scenarioName}): ${clusterMap.collectEntries { k, v -> [k.peerDescription.hostname, v] }}")

        // Build a peer selector with this cluster
        PeerStatusProvider mockPSP = mockPeerStatusProvider(BOOTSTRAP_PEER_DESCRIPTION, DEFAULT_REMOTE_INSTANCE_URIS, buildPeersMap(clusterMap.keySet()))
        PeerPersistence mockPP = mockPeerPersistence(DEFAULT_REMOTE_INSTANCE_URIS, clusterMap.keySet())

        new PeerSelector(mockPSP, mockPP)
    }

    @Test
    void testGetPeersToQueryShouldBeEmpty() {
        // Arrange

        // Mock collaborators with empty data
        mockPSP = mockPeerStatusProvider(BOOTSTRAP_PEER_DESCRIPTION, "", [:])
        mockPP = mockPeerPersistence("", [] as Set)

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)

        // Act
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // Assert
        assert peersToQuery.size() == 1
        assert peersToQuery.first() == BOOTSTRAP_PEER_DESCRIPTION
    }

    @Test
    void testShouldGetPeersToQuery() {
        // Arrange
        Set<PeerStatus> restoredPeerStatuses = buildPeerStatuses()

        // Mock collaborators
        mockPP = mockPeerPersistence(DEFAULT_REMOTE_INSTANCE_URIS, restoredPeerStatuses)

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)

        // Act
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // Assert
        assert peersToQuery.size() == restoredPeerStatuses.size() + 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
        assert peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS)
    }

    /**
     * Asserts that calling the {@code #getPeersToQuery( )} method repeatedly provides the same result because it does not modify {@code lastFetchedQueryablePeers} directly.
     *
     */
    @Test
    void testGetPeersToQueryShouldBeIdempotent() {
        // Arrange
        final int NUM_TIMES = 3

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)

        // Act
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        def repeatedPeersToQuery = []
        NUM_TIMES.times { int i ->
            repeatedPeersToQuery << ps.getPeersToQuery()
        }

        // Assert
        assert peersToQuery.size() == DEFAULT_PEER_STATUSES.size() + 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
        assert peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS)

        assert repeatedPeersToQuery.every { it == peersToQuery }
    }

    @Test
    void testShouldFetchRemotePeerStatuses() {
        // Arrange
        PeerSelector ps = new PeerSelector(mockPSP, mockPP)

        // Act
        Set<PeerStatus> remotePeerStatuses = ps.fetchRemotePeerStatuses(DEFAULT_PEER_DESCRIPTIONS)
        logger.info("Retrieved ${remotePeerStatuses.size()} peer statuses: ${remotePeerStatuses}")

        // Assert
        assert remotePeerStatuses.size() == DEFAULT_PEER_STATUSES.size()
        assert remotePeerStatuses.containsAll(DEFAULT_PEER_STATUSES)
    }

    /**
     * Iterates through test scenarios of 100, 1000, and 10_000 total flowfiles and calculates the relative send and receive weights for every percentage.
     */
    @Test
    void testShouldCalculateNormalizedWeight() {
        // Arrange
        def results = [:]

        // Act
        [3, 5, 7].each { int nodeCount ->
            results["$nodeCount"] = [:]
            (2..4).each { int e ->
                int totalFlowfileCount = 10**e
                results["$nodeCount"]["$totalFlowfileCount"] = [:]
                def thisScenario = results["$nodeCount"]["$totalFlowfileCount"]
                logger.info("Running ${nodeCount} node scenario for ${totalFlowfileCount} total flowfiles")
                (0..100).each { int i ->
                    int flowfileCount = (i / 100 * totalFlowfileCount).intValue()
                    thisScenario["$flowfileCount"] = [:]

                    double sendWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.SEND, totalFlowfileCount, flowfileCount, nodeCount)
                    double receiveWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.RECEIVE, totalFlowfileCount, flowfileCount, nodeCount)

                    thisScenario["$flowfileCount"]["send"] = sendWeight
                    thisScenario["$flowfileCount"]["receive"] = receiveWeight
                }
            }
        }

        // Assert
        results.each { nodeCount, t ->
            t.each { total, r ->
                total = Integer.valueOf(total)
                logger.info("Results for ${nodeCount} nodes with ${total} flowfiles: ")
                logger.info(["Count", "Send", "Receive"].collect { it.padLeft(10, " ") }.join())
                int step = total / 10 as int
                (0..total).step(step).each { int n ->
                    def data = r["$n"]
                    def line = [n, data.send, data.receive].collect { (it as String).padLeft(10, " ") }.join()
                    logger.debug(line)
                }

                // Assert that the send percentage is always between 0% and 80%
                assert r.every { k, v -> v.send >= 0 && v.send <= 80 }

                // Assert that the receive percentage is always between 0% and 100%
                assert r.every { k, v -> v.receive >= 0 && v.receive <= 100 }
            }
        }
    }

    /**
     * Iterates through test scenarios of 100, 1000, and 10_000 total flowfiles and calculates the relative send and receive weights for every percentage.
     */
    @Test
    void testShouldCalculateNormalizedWeightForSingleRemote() {
        // Arrange
        final int NODE_COUNT = 1

        // Act
        (2..4).each { int e ->
            int totalFlowfileCount = 10**e
            logger.info("Running single node scenario for ${totalFlowfileCount} total flowfiles")
            (0..100).each { int i ->
                int flowfileCount = (i / 100 * totalFlowfileCount).intValue()
                double sendWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.SEND, totalFlowfileCount, flowfileCount, NODE_COUNT)
                double receiveWeight = PeerSelector.calculateNormalizedWeight(TransferDirection.RECEIVE, totalFlowfileCount, flowfileCount, NODE_COUNT)

                // Assert
                assert sendWeight == 100
                assert receiveWeight == 100
            }
        }
    }

    @Test
    void testShouldBuildWeightedPeerMapForSend() {
        // Arrange
        def nodes = ["node1.nifi": 20, "node2.nifi": 30, "node3.nifi": 50]
        def clusterMap = buildCluster(nodes)

        // Sort the map in ascending order by value (SEND)
        clusterMap = clusterMap.sort { e1, e2 -> e1.value <=> e2.value }
        logger.info("Using cluster map: ${clusterMap.collectEntries { k, v -> [k.peerDescription.hostname, v] }}")

        mockPSP = mockPeerStatusProvider(BOOTSTRAP_PEER_DESCRIPTION, DEFAULT_REMOTE_INSTANCE_URIS, buildPeersMap(clusterMap.keySet()))
        mockPP = mockPeerPersistence(DEFAULT_REMOTE_INSTANCE_URIS, clusterMap.keySet())

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)
        Set<PeerStatus> peerStatuses = ps.getPeerStatuses()

        // Act
        LinkedHashMap<PeerStatus, Double> weightedPeerMap = ps.buildWeightedPeerMap(peerStatuses, TransferDirection.SEND)
        logger.info("Weighted peer map: ${weightedPeerMap}")

        // Assert
        assert new ArrayList<>(weightedPeerMap.keySet()) == new ArrayList(clusterMap.keySet())
    }

    @Test
    void testShouldBuildWeightedPeerMapForReceive() {
        // Arrange
        def nodes = ["node1.nifi": 20, "node2.nifi": 30, "node3.nifi": 50]
        def clusterMap = buildCluster(nodes)

        // Sort the map in descending order by value (RECEIVE)
        clusterMap = clusterMap.sort { e1, e2 -> e2.value <=> e1.value }
        logger.info("Using cluster map: ${clusterMap.collectEntries { k, v -> [k.peerDescription.hostname, v] }}")

        mockPSP = mockPeerStatusProvider(BOOTSTRAP_PEER_DESCRIPTION, DEFAULT_REMOTE_INSTANCE_URIS, buildPeersMap(clusterMap.keySet()))
        mockPP = mockPeerPersistence(DEFAULT_REMOTE_INSTANCE_URIS, clusterMap.keySet())

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)
        Set<PeerStatus> peerStatuses = ps.getPeerStatuses()

        // Act
        LinkedHashMap<PeerStatus, Double> weightedPeerMap = ps.buildWeightedPeerMap(peerStatuses, TransferDirection.RECEIVE)
        logger.info("Weighted peer map: ${weightedPeerMap}")

        // Assert
        assert new ArrayList<>(weightedPeerMap.keySet()) == new ArrayList(clusterMap.keySet())
    }

    /**
     * This test ensures that regardless of the total flowfile count, the resulting map has
     * normalized weights (i.e. percentage of 100).
     */
    @Test
    void testCreateDestinationMapForSendShouldBeNormalized() {
        // Arrange
        def scenarios = [
                "100 ff 100/0/0"     : ["node1.nifi": 100, "node2.nifi": 0, "node3.nifi": 0],
                "100 ff 50/50/0"     : ["node1.nifi": 50, "node2.nifi": 50, "node3.nifi": 0],
                "100 ff 100/0"       : ["node1.nifi": 100, "node2.nifi": 0],
                "1000 ff 200/300/500": ["node1.nifi": 200, "node2.nifi": 300, "node3.nifi": 500],
                "1000 ff 333/333/334": ["node1.nifi": 333, "node2.nifi": 333, "node3.nifi": 334],
                "1000 ff 0/250x4"    : ["node1.nifi": 0, "node2.nifi": 250, "node3.nifi": 250, "node4.nifi": 250, "node5.nifi": 250],
                "1000 ff 142x7"      : ((1..7).collectEntries { int i -> ["node${i}.nifi", 1000.intdiv(7)] }),
                "200 ff 151/1x49"    : ["node1.nifi": 151] + ((2..50).collectEntries { int i -> ["node${i}.nifi", 1] })
        ]

        scenarios.each { String name, Map nodes ->
            PeerSelector ps = buildPeerSelectorForCluster(name, nodes)
            Set<PeerStatus> peerStatuses = ps.getPeerStatuses()

            // Check both SEND and RECEIVE
            TransferDirection.values().each { TransferDirection direction ->
                logger.info("Retrieving peers for ${direction} in scenario ${name}")

                // Act
                Map<PeerStatus, Double> destinationMap = ps.createDestinationMap(peerStatuses, direction)
                logger.info("Destination map: ${destinationMap}")

                // Assert
                assert destinationMap.keySet() == peerStatuses

                // For uneven splits, the resulting percentage should be within +/- 1%
                def totalPercentage = destinationMap.values().sum()
                assert totalPercentage >= 99 && totalPercentage <= 100
            }
        }
    }

    /**
     * Test the edge case where there is a rounding error and the selected random number is not captured in the buckets
     */
    @Test
    void testGetAvailablePeerStatusShouldHandleEdgeCase() {
        // Arrange
        final int NUM_TIMES = 10000

        def nodes = ["node1.nifi": 2, "node2.nifi": 1, "node3.nifi": 1]

        // Make a map where the weights are artificially suppressed and total far less than 100% to make the edge case more likely
        Map<PeerStatus, Double> suppressedPercentageMap = buildPeerStatuses(new ArrayList<String>(nodes.keySet())).collectEntries { [it, nodes[it.peerDescription.hostname] / 100.0 as double] }

        PeerSelector ps = buildPeerSelectorForCluster("edge case cluster", nodes)

        // Collect the results and analyze the resulting frequency distribution
        Map<String, Integer> resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

        // Act
        NUM_TIMES.times { int i ->
            def nextPeer = ps.getAvailablePeerStatus(suppressedPercentageMap)
//            logger.debug("${(i as String).padLeft(Math.log10(NUM_TIMES).intValue())}: ${nextPeer.peerDescription.hostname}")
            resultsFrequency[nextPeer.peerDescription.hostname]++
        }
        logger.info("Peer frequency results (${NUM_TIMES}): ${resultsFrequency}")

        // Assert

        // The actual distribution would be 50/25/25
        final Map<String, Double> EXPECTED_PERCENTS = ["node1.nifi": 50.0, "node2.nifi": 25.0, "node3.nifi": 25.0]

        assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES, 0.05)
    }

    @Test
    void testShouldGetNextPeer() {
        // Arrange
        final int NUM_TIMES = 10000

        def nodes = ["node1.nifi": 20, "node2.nifi": 30, "node3.nifi": 50]

        // Check both SEND and RECEIVE
        TransferDirection.values().each { TransferDirection direction ->
            logger.info("Selecting ${NUM_TIMES} peers for ${direction}")

            PeerSelector ps = buildPeerSelectorForCluster("100 ff 20/30/50", nodes)

            // Collect the results and analyze the resulting frequency distribution
            Map<String, Integer> resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

            // Act
            NUM_TIMES.times { int i ->
                def nextPeer = ps.getNextPeerStatus(direction)
//                logger.debug("${(i as String).padLeft(Math.log10(NUM_TIMES).intValue())}: ${nextPeer.peerDescription.hostname}")
                resultsFrequency[nextPeer.peerDescription.hostname]++
            }
            logger.info("Peer frequency results (${NUM_TIMES}): ${resultsFrequency}")

            // Assert
            final Map<String, Double> EXPECTED_PERCENTS = determineExpectedPercents(nodes, direction)
            assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES)
        }
    }

    /**
     * When the cluster is balanced, the consecutive selection of peers should not repeat {@code cluster.size( ) - 1} times.
     */
    @Test
    void testGetNextPeerShouldNotRepeatPeersOnBalancedCluster() {
        // Arrange
        final int NUM_TIMES = 10000

        def nodes = ((1..10).collectEntries { int i -> ["node${i}.nifi".toString(), 100] })
        PeerSelector ps = buildPeerSelectorForCluster("1000 ff 100x10", nodes)

        // Check both SEND and RECEIVE
        TransferDirection.values().each { TransferDirection direction ->
            logger.info("Selecting ${NUM_TIMES} peers for ${direction}")

            // Collect the results and analyze the resulting frequency distribution
            def resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

            // Use the queue to track recent peers and observe repeated selections
            PeerQueue lastN = new PeerQueue(nodes.size())

            // Act
            NUM_TIMES.times { int i ->
                def nextPeer = ps.getNextPeerStatus(direction)
                resultsFrequency[nextPeer.peerDescription.hostname]++

                // Assert the consecutive selections are ok
                assertConsecutiveSelections(lastN, nextPeer)
            }

            // Assert
            final def EXPECTED_PERCENTS = nodes.collectEntries { [it.key, 10.0] }

            // The tolerance should be a bit higher because of the high number of nodes and even distribution
            assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES, 0.10)
        }
    }

    /**
     * When a remote has only one valid peer, that peer should be selected every time
     */
    @Test
    void testGetNextPeerShouldRepeatPeersOnSingleValidDestination() {
        // Arrange
        final int NUM_TIMES = 100

        // Single destination scenarios
        def scenarios = [
                "single node"      : ["node1.nifi": 100],
                "single empty node": ["node1.nifi": 0],
                "100 ff 100/0"     : ["node1.nifi": 100, "node2.nifi": 0],
        ]

        scenarios.each { String name, Map nodes ->
            PeerSelector ps = buildPeerSelectorForCluster(name, nodes)

            // Check both SEND and RECEIVE
            TransferDirection.values().each { TransferDirection direction ->
                logger.info("Selecting ${NUM_TIMES} peers for ${direction} in scenario ${name}")

                // Collect the results and analyze the resulting frequency distribution
                def resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

                // Use the queue to track recent peers and observe repeated selections
                PeerQueue lastN = new PeerQueue(nodes.size())

                // Act
                NUM_TIMES.times { int i ->
                    def nextPeer = ps.getNextPeerStatus(direction)
                    resultsFrequency[nextPeer.peerDescription.hostname]++

                    // Assert the consecutive selections are ok (i.e. it IS selecting the same peer repeatedly)
                    if (lastN.remainingCapacity() == 0) {
                        lastN.remove()
                    }
                    lastN.put(nextPeer.peerDescription.hostname)

                    // Spot check consecutive selection
                    if (i % 10 == 0) {
                        int consecutiveElements = lastN.getMaxConsecutiveElements()
                        assert consecutiveElements == lastN.size()
                    }
                }

                // Assert
                final def EXPECTED_PERCENTS = determineExpectedPercents(nodes, direction)
                logger.info("Expected percentages for ${name}: ${EXPECTED_PERCENTS}")

                // The tolerance should be zero; exact matches only
                assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES, 0.00)
            }
        }
    }

    /**
     * The legacy requirement that the next peer not repeat N-1 times where N is the size of the remote cluster does not apply to the following scenarios:
     *
     * * A remote of size <= 3
     * * An unbalanced remote (33/33/33/0) <em>should</em> repeat the last peer multiple times
     */
    @Test
    void testGetNextPeerShouldRepeatPeersOnUnbalancedCluster() {
        // Arrange

        // Using a higher iteration count smooths out outliers
        final int NUM_TIMES = 10000

        // Scenarios where consecutively-selected peers are expected to sometimes repeat (small clusters, uneven clusters)
        def scenarios = [
                "100 ff 50/50"            : ["node1.nifi": 50, "node2.nifi": 50],
                "100 ff 75/25"            : ["node1.nifi": 75, "node2.nifi": 25],
                "100 ff 50/50/0"          : ["node1.nifi": 50, "node2.nifi": 50, "node3.nifi": 0],
                "1000 ff 800/200/0"       : ["node1.nifi": 800, "node2.nifi": 200, "node3.nifi": 0],
                "10 ff 8/2/0"             : ["node1.nifi": 8, "node2.nifi": 2, "node3.nifi": 0],
                "200 ff 66x3/0"           : ["node1.nifi": 66, "node2.nifi": 66, "node3.nifi": 66, "node4.nifi": 0],
                "1000 ff 0/250x4"         : ["node1.nifi": 0, "node2.nifi": 250, "node3.nifi": 250, "node4.nifi": 250, "node5.nifi": 250],
                "1000 ff 0/111x9"         : ["node1.nifi": 0] + ((2..10).collectEntries { ["node${it}.nifi".toString(), 111] }),
                "legacy 1024/10240/4096x3": ["node1.nifi": 1024, "node2.nifi": 10240] + (3..5).collectEntries { ["node${it}.nifi".toString(), 4096] },
                "legacy 50k/500"          : ["node1.nifi": 50_000, "node2.nifi": 50],
        ]

        scenarios.each { String name, Map nodes ->
            PeerSelector ps = buildPeerSelectorForCluster(name, nodes)

            // Check both SEND and RECEIVE
            TransferDirection.values().each { TransferDirection direction ->
                logger.info("Selecting ${NUM_TIMES} peers for ${direction} in scenario ${name}")

                // Collect the results and analyze the resulting frequency distribution
                def resultsFrequency = nodes.keySet().collectEntries { [it, 0] }
                logger.debug("Initialized results map to ${resultsFrequency}")

                // Use the queue to track recent peers and observe repeated selections
                PeerQueue lastN = new PeerQueue(nodes.size())

                // Act
                NUM_TIMES.times { int i ->
                    def nextPeer = ps.getNextPeerStatus(direction)
//                logger.debug("${(i as String).padLeft(Math.log10(NUM_TIMES).intValue())}: ${nextPeer.peerDescription.hostname}")
                    resultsFrequency[nextPeer.peerDescription.hostname]++

                    // Assert the consecutive selections are ok (i.e. it IS selecting the same peer repeatedly)
                    if (lastN.remainingCapacity() == 0) {
                        lastN.remove()
                    }
                    lastN.put(nextPeer.peerDescription.hostname)

                    int consecutiveElements = lastN.getMaxConsecutiveElements()
                    if (consecutiveElements == nodes.size() && nodes.size() > 3) {
                        logger.debug("Most consecutive elements in recentPeerSelectionQueue: ${consecutiveElements} | ${lastN}")
                    }
                }

                // Assert
                final def EXPECTED_PERCENTS = determineExpectedPercents(nodes, direction)
                logger.info("Expected percentages for ${name}: ${EXPECTED_PERCENTS}")

                assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES)
            }
        }
    }

    /**
     * Test the edge case where peers are penalized
     */
    @Test
    void testGetAvailablePeerStatusShouldHandlePenalizedPeers() {
        // Arrange
        final int NUM_TIMES = 100

        // Should prefer node1, but it will be penalized
        def nodes = ["node1.nifi": 10, "node2.nifi": 90]

        // Make a map where the weights are normal
        def peerStatuses = buildPeerStatuses(new ArrayList<String>(nodes.keySet()))
        Map<PeerStatus, Double> weightMap = peerStatuses.collectEntries { [it, nodes[it.peerDescription.hostname] as double] }

        PeerSelector ps = buildPeerSelectorForCluster("penalized peer", nodes)

        // Penalize node1
        ps.penalize(peerStatuses.sort().first().peerDescription, 10_000)

        // Collect the results and analyze the resulting frequency distribution
        Map<String, Integer> resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

        // Act
        NUM_TIMES.times { int i ->
            def nextPeer = ps.getAvailablePeerStatus(weightMap)
//            logger.debug("${(i as String).padLeft(Math.log10(NUM_TIMES).intValue())}: ${nextPeer.peerDescription.hostname}")
            resultsFrequency[nextPeer.peerDescription.hostname]++
        }
        logger.info("Peer frequency results (${NUM_TIMES}): ${resultsFrequency}")

        // Assert

        // The actual distribution would be .9/.1, but because of the penalization, all selections will be node2
        final Map<String, Double> EXPECTED_PERCENTS = ["node1.nifi": 0.0, "node2.nifi": 100.0]

        // The tolerance should be very tight as this will be almost exact every time
        assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES, 0.00)
    }

    /**
     * Test the edge case where peers are penalized
     */
    @Test
    void testGetAvailablePeerStatusShouldHandleMultiplePenalizedPeers() {
        // Arrange
        final int NUM_TIMES = 10_000

        // Should distribute evenly, but 1/2 of the nodes will be penalized
        def nodes = ["node1.nifi": 25, "node2.nifi": 25, "node3.nifi": 25, "node4.nifi": 25]

        // Make a map where the weights are normal
        def peerStatuses = buildPeerStatuses(new ArrayList<String>(nodes.keySet()))
        Map<PeerStatus, Double> weightMap = peerStatuses.collectEntries { [it, nodes[it.peerDescription.hostname] as double] }

        PeerSelector ps = buildPeerSelectorForCluster("penalized peers", nodes)

        // Penalize node1 & node3
        def penalizedPeerStatuses = peerStatuses.findAll { ["node1.nifi", "node3.nifi"].contains(it.peerDescription.hostname) }
        penalizedPeerStatuses.each { ps.penalize(it.peerDescription, 10_000) }

        // Collect the results and analyze the resulting frequency distribution
        Map<String, Integer> resultsFrequency = nodes.keySet().collectEntries { [it, 0] }

        // Act
        NUM_TIMES.times { int i ->
            def nextPeer = ps.getAvailablePeerStatus(weightMap)
//            logger.debug("${(i as String).padLeft(Math.log10(NUM_TIMES).intValue())}: ${nextPeer.peerDescription.hostname}")
            resultsFrequency[nextPeer.peerDescription.hostname]++
        }
        logger.info("Peer frequency results (${NUM_TIMES}): ${resultsFrequency}")

        // Assert

        // The actual distribution would be .25 * 4, but because of the penalization, node2 and node4 will each have ~50%
        final Map<String, Double> EXPECTED_PERCENTS = ["node1.nifi": 0.0, "node2.nifi": 50.0, "node3.nifi": 0.0, "node4.nifi": 50.0]

        assertDistributionPercentages(resultsFrequency, EXPECTED_PERCENTS, NUM_TIMES, 0.05)
    }

    // Copied legacy tests from TestPeerSelector

    /**
     * Test that the cache is the source of peer statuses initially
     */
    @Test
    void testInitializationShouldRestorePeerStatusFileCache() {
        // Arrange
        def nodes = DEFAULT_NODES
        def peerStatuses = DEFAULT_PEER_STATUSES

        // Create the peer status provider
        mockPSP = mockPeerStatusProvider()

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt")
        cacheFile.deleteOnExit()

        // Construct the cache contents and write to disk
        final String CACHE_CONTENTS = "${mockPSP.getTransportProtocol()}\n" + "${AbstractPeerPersistence.REMOTE_INSTANCE_URIS_PREFIX}${mockPSP.getRemoteInstanceUris()}\n" + peerStatuses.collect { PeerStatus ps ->
            [ps.peerDescription.hostname, ps.peerDescription.port, ps.peerDescription.isSecure(), ps.isQueryForPeers()].join(":")
        }.join("\n")
        cacheFile.text = CACHE_CONTENTS

        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile)

        // Act

        // The constructor should restore the initial cache
        PeerSelector ps = new PeerSelector(mockPSP, filePP)

        // PeerSelector should access peer statuses from cache
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // Assert
        assert peersToQuery.size() == nodes.size() + 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
        assert peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS)
    }

    /**
     * Test that if the cache is expired, it is not used
     */
    @Test
    void testRefreshShouldHandleExpiredPeerStatusFileCache() {
        // Arrange
        def nodes = DEFAULT_NODES
        def peerStatuses = DEFAULT_PEER_STATUSES
        def remoteInstanceUris = buildRemoteInstanceUris(nodes)

        // Create the peer status provider with no actual remote peers
        mockPSP = mockPeerStatusProvider(BOOTSTRAP_PEER_DESCRIPTION, remoteInstanceUris, [:])

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt")
        cacheFile.deleteOnExit()

        // Construct the cache contents and write to disk
        final String CACHE_CONTENTS = "${mockPSP.getTransportProtocol()}\n" + "${AbstractPeerPersistence.REMOTE_INSTANCE_URIS_PREFIX}${mockPSP.getRemoteInstanceUris()}\n" + peerStatuses.collect { PeerStatus ps ->
            [ps.peerDescription.hostname, ps.peerDescription.port, ps.peerDescription.isSecure(), ps.isQueryForPeers()].join(":")
        }.join("\n")
        cacheFile.text = CACHE_CONTENTS

        // Mark the file as expired
        cacheFile.lastModified = System.currentTimeMillis() - (PeerSelector.PEER_CACHE_MILLIS * 2)

        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile)

        // Act

        // The constructor should restore the initial cache
        PeerSelector ps = new PeerSelector(mockPSP, filePP)

        // Assert

        // The loaded cache should be marked as expired and not used
        assert ps.isCacheExpired(ps.peerStatusCache)

        // This internal method does not refresh or check expiration
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // The cache has (expired) peer statuses present
        assert peersToQuery.size() == nodes.size() + 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
        assert peersToQuery.containsAll(DEFAULT_PEER_DESCRIPTIONS)

        // Trigger the cache expiration detection
        ps.refresh()

        peersToQuery = ps.getPeersToQuery()
        logger.info("After cache expiration, retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // The cache only contains the bootstrap node
        assert peersToQuery.size() == 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
    }

    Throwable generateException(String message, int nestedLevel = 0) {
        IOException e = new IOException(message)
        nestedLevel.times { int i ->
            e = new IOException("${message} ${i + 1}", e)
        }
        e
    }

    /**
     * Test that printing the exception does not cause an infinite loop
     */
    @Test
    void testRefreshShouldHandleExceptions() {
        // Arrange
        mockPP = [
                restore: { ->
                    new PeerStatusCache([] as Set<PeerStatus>, System.currentTimeMillis(), DEFAULT_REMOTE_INSTANCE_URIS, SiteToSiteTransportProtocol.HTTP)
                },
                // Create the peer persistence to throw an exception on save
                save   : { PeerStatusCache cache ->
                    throw generateException("Custom error message", 3)
                }
        ] as PeerPersistence

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)

        // Act
        ps.refreshPeerStatusCache()
        def peersToQuery = ps.getPeersToQuery()

        // Assert
        assert peersToQuery.size() == 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
    }

    /**
     * Test that the cache is not used if it does not match the transport protocol
     */
    @Test
    void testInitializationShouldIgnoreCacheWithWrongTransportProtocol() {
        // Arrange
        def nodes = DEFAULT_NODES
        def peerStatuses = DEFAULT_PEER_STATUSES

        // Create the peer status provider
        mockPSP = mockPeerStatusProvider()

        // Point to the persisted cache on disk
        final File cacheFile = File.createTempFile("peers", "txt")
        cacheFile.deleteOnExit()

        // Construct the cache contents (with wrong TP - mockPSP uses HTTP) and write to disk
        final String CACHE_CONTENTS = "${SiteToSiteTransportProtocol.RAW}\n" + peerStatuses.collect { PeerStatus ps ->
            [ps.peerDescription.hostname, ps.peerDescription.port, ps.peerDescription.isSecure(), ps.isQueryForPeers()].join(":")
        }.join("\n")
        cacheFile.text = CACHE_CONTENTS

        FilePeerPersistence filePP = new FilePeerPersistence(cacheFile)

        // Act
        PeerSelector ps = new PeerSelector(mockPSP, filePP)

        // The cache should be ignored because of the transport protocol mismatch
        def peersToQuery = ps.getPeersToQuery()
        logger.info("Retrieved ${peersToQuery.size()} peers to query: ${peersToQuery}")

        // Assert
        assert peersToQuery.size() == 1
        assert peersToQuery.contains(BOOTSTRAP_PEER_DESCRIPTION)
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
    void testShouldFetchRemotePeerStatusesInFailureScenario() throws IOException {
        // Arrange
        int currentAttempt = 1

        // The bootstrap node is node1.nifi
        List<String> nodes = ["node1.nifi", "node2.nifi"]
        Set<PeerStatus> peerStatuses = buildPeerStatuses(nodes)

        // Need references to the bootstrap and node2 later
        PeerStatus bootstrapStatus = peerStatuses.find { it.peerDescription.hostname == "node1.nifi" }
        PeerDescription bootstrapDescription = bootstrapStatus.peerDescription

        PeerStatus node2Status = peerStatuses.find { it.peerDescription.hostname == "node2.nifi" }
        PeerDescription node2Description = node2Status.peerDescription

        String remoteInstanceUris = buildRemoteInstanceUris(nodes)

        // Mock the PSP
        mockPSP = [
                getTransportProtocol       : { ->
                    SiteToSiteTransportProtocol.HTTP
                },
                getRemoteInstanceUris: { ->
                    remoteInstanceUris
                },
                getBootstrapPeerDescription: { ->
                    bootstrapDescription
                },
                fetchRemotePeerStatuses    : { PeerDescription pd ->
                    // Depending on the scenario, return given peer statuses
                    logger.mock("Scenario ${currentAttempt} fetchRemotePeerStatus for ${pd}")
                    switch (currentAttempt) {
                        case 1:
                            return [bootstrapStatus, node2Status] as Set<PeerStatus>
                        case 2..3:
                            return [node2Status] as Set<PeerStatus>
                        case 4:
                            return [] as Set<PeerStatus>
                        default:
                            return [bootstrapStatus] as Set<PeerStatus>
                    }
                }
        ] as PeerStatusProvider

        // Mock the PP with only these statuses
        mockPP = mockPeerPersistence(remoteInstanceUris, peerStatuses)

        PeerSelector ps = new PeerSelector(mockPSP, mockPP)
        ps.refresh()
        PeerStatus peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE)
        logger.info("Attempt ${currentAttempt} - ${peerStatus}")
        assert peerStatus

        // Force the selector to refresh the cache
        currentAttempt++
        ps.refreshPeerStatusCache()

        // Attempt 2 & 3 - only node2 available (PSP will only return node2)
        2.times {
            ps.refresh()
            peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE)
            logger.info("Attempt ${currentAttempt} - ${peerStatus}")
            assert peerStatus == node2Status

            // Force the selector to refresh the cache
            currentAttempt++
            ps.refreshPeerStatusCache()
        }

        // Attempt 4 - no available nodes
        ps.refresh()
        peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE)
        logger.info("Attempt ${currentAttempt} - ${peerStatus}")
        assert !peerStatus

        // Force the selector to refresh the cache
        currentAttempt = 5
        ps.refreshPeerStatusCache()

        // Attempt 5 - bootstrap node available
        ps.refresh()
        peerStatus = ps.getNextPeerStatus(TransferDirection.RECEIVE)
        logger.info("Attempt ${currentAttempt} - ${peerStatus}")
        assert peerStatus == bootstrapStatus
    }

    // PeerQueue definition and tests

    /**
     * Tests the utility class {@link PeerQueue} used to track consecutive peer selection.
     */
    @Test
    void testPeerQueueShouldGetMaxConsecutiveElements() {
        // Arrange
        PeerQueue peerQueue = new PeerQueue(10)
        List<String> nodes = (1..5).collect { "node${it}.nifi".toString() }
        List<PeerStatus> peerStatuses = new ArrayList<>(buildPeerStatuses(nodes))

        // Act

        // Same node every time
        100.times { int i ->
            peerQueue.append(nodes.first())

            // Assert
            assert peerQueue.getMaxConsecutiveElements() == peerQueue.size()
        }

        // Never repeating node
        peerQueue.clear()
        100.times { int i ->
            peerQueue.append(nodes.get(i % peerStatuses.size()))

            // Assert
            assert peerQueue.getMaxConsecutiveElements() == 1
        }

        // Repeat up to nodes.size() times but no more
        peerQueue.clear()
        100.times { int i ->
            // Puts the first node unless this is a multiple of the node count
            peerQueue.append((i % nodes.size() == 0) ? nodes.last() : nodes.first())

            // Assert
//            logger.debug("Most consecutive elements in queue: ${peerQueue.getMaxConsecutiveElements()} | ${peerQueue}")
            assert peerQueue.getMaxConsecutiveElements() <= peerStatuses.size()
        }
    }

    class PeerQueue extends ArrayBlockingQueue {
        PeerQueue(int capacity) {
            super(capacity)
        }

        int getTotalSize() {
            this.size() + this.remainingCapacity()
        }

        int getMaxConsecutiveElements() {
            int currentMax = 1, current = 1
            def iterator = this.iterator()
            Object prev = iterator.next()
            while (iterator.hasNext()) {
                def curr = iterator.next()
                if (prev == curr) {
                    current++
                    if (current > currentMax) {
                        currentMax = current
                    }
                } else {
                    current = 1
                }
                prev = curr
            }
            return currentMax
        }

        Object getMostFrequentElement() {
            def map = this.groupBy { it }
            map.max { a, b -> a.value.size() <=> b.value.size() }.key
        }

        Object getMostCommonConsecutiveElement() {
            int currentMax = 1, current = 1
            def iterator = this.iterator()
            Object prev = iterator.next()
            Object mcce = prev
            while (iterator.hasNext()) {
                def curr = iterator.next()
                if (prev == curr) {
                    current++
                    if (current > currentMax) {
                        currentMax = current
                        mcce = curr
                    }
                } else {
                    current = 1
                }
                prev = curr
            }
            return mcce
        }

        /**
         * Adds the new Object to the tail of the queue. If the queue was full before, removes the head to open capacity.
         *
         * @param o the object to append
         */
        void append(Object o) {
            if (this.remainingCapacity() == 0) {
                this.remove()
            }
            this.put(o)
        }
    }
}