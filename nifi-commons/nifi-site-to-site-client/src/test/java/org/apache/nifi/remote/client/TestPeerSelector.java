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
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

public class TestPeerSelector {

    private static final Logger logger = LoggerFactory.getLogger(TestPeerSelector.class);

    private Map<String, Integer> calculateAverageSelectedCount(Set<PeerStatus> collection, List<PeerStatus> destinations) {
        // Calculate hostname entry, for average calculation. Because there're multiple entry with same host name, different port.
        final Map<String, Integer> hostNameCounts
                = collection.stream().collect(groupingBy(p -> p.getPeerDescription().getHostname(), reducing(0, p -> 1, Integer::sum)));

        // Calculate how many times each hostname is selected.
        return destinations.stream().collect(groupingBy(p -> p.getPeerDescription().getHostname(), reducing(0, p -> 1, Integer::sum)))
                .entrySet().stream().collect(toMap(Map.Entry::getKey, e -> {
                    return e.getValue() / hostNameCounts.get(e.getKey());
                }));
    }

    @Test
    public void testFormulateDestinationListForOutputEven() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("Node1", 1111, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("Node2", 2222, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("Node3", 3333, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("Node4", 4444, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("Node5", 5555, true), 4096, true));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);

        int consecutiveSamePeerCount = 0;
        PeerStatus previousPeer = null;
        for (PeerStatus peer : destinations) {
            if (previousPeer != null && peer.getPeerDescription().equals(previousPeer.getPeerDescription())) {
                consecutiveSamePeerCount++;
                // The same peer shouldn't be used consecutively (number of nodes - 1) times or more.
                if (consecutiveSamePeerCount >= (collection.size() - 1)) {
                    fail("The same peer is returned consecutively too frequently.");
                }
            } else {
                consecutiveSamePeerCount = 0;
            }
            previousPeer = peer;
        }

    }

    @Test
    public void testFormulateDestinationListForOutput() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 1111, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 2222, true), 10240, true));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 3333, true), 1024, true));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 4444, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 5555, true), 4096, true));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);
        assertTrue("HasLots should send lots", selectedCounts.get("HasLots") > selectedCounts.get("HasMedium"));
        assertTrue("HasMedium should send medium", selectedCounts.get("HasMedium") > selectedCounts.get("HasLittle"));
    }

    @Test
    public void testFormulateDestinationListForOutputHugeDifference() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 1111, true), 500, true));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 2222, true), 50000, true));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);
        assertTrue("HasLots should send lots", selectedCounts.get("HasLots") > selectedCounts.get("HasLittle"));
    }

    @Test
    public void testFormulateDestinationListForInputPorts() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 1111, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 2222, true), 10240, true));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 3333, true), 1024, true));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 4444, true), 4096, true));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 5555, true), 4096, true));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);
        assertTrue("HasLots should get little", selectedCounts.get("HasLots") < selectedCounts.get("HasMedium"));
        assertTrue("HasMedium should get medium", selectedCounts.get("HasMedium") < selectedCounts.get("HasLittle"));
    }

    @Test
    public void testFormulateDestinationListForInputPortsHugeDifference() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("HasLots", 1111, true), 500, true));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 2222, true), 50000, true));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);
        assertTrue("HasLots should get little", selectedCounts.get("HasLots") < selectedCounts.get("HasLittle"));
    }

    private static class UnitTestSystemTime extends PeerSelector.SystemTime {
        private long offset = 0;

        @Override
        long currentTimeMillis() {
            return super.currentTimeMillis() + offset;
        }
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
    public void testFetchRemotePeerStatuses() throws IOException {

        final Set<PeerStatus> peerStatuses = new HashSet<>();
        final PeerDescription bootstrapNode = new PeerDescription("Node1", 1111, true);
        final PeerDescription node2 = new PeerDescription("Node2", 2222, true);
        final PeerStatus bootstrapNodeStatus = new PeerStatus(bootstrapNode, 10, true);
        final PeerStatus node2Status = new PeerStatus(node2, 10, true);
        peerStatuses.add(bootstrapNodeStatus);
        peerStatuses.add(node2Status);

        final PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        final PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);
        final UnitTestSystemTime systemTime = new UnitTestSystemTime();
        peerSelector.setSystemTime(systemTime);

        doReturn(bootstrapNode).when(peerStatusProvider).getBootstrapPeerDescription();
        doAnswer(invocation -> {
            final PeerDescription peerFetchStatusesFrom = invocation.getArgumentAt(0, PeerDescription.class);
            if (peerStatuses.stream().filter(ps -> ps.getPeerDescription().equals(peerFetchStatusesFrom)).collect(Collectors.toSet()).size() > 0) {
                // If the remote peer is running, then return available peer statuses.
                return peerStatuses;
            }
            throw new IOException("Connection refused. " + peerFetchStatusesFrom + " is not running.");
        }).when(peerStatusProvider).fetchRemotePeerStatuses(any(PeerDescription.class));

        // 1st attempt. It uses the bootstrap node.
        peerSelector.refreshPeers();
        PeerStatus peerStatus = peerSelector.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNotNull(peerStatus);

        // Proceed time so that peer selector refresh statuses.
        peerStatuses.remove(bootstrapNodeStatus);
        systemTime.offset += TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES) + 1;

        // 2nd attempt.
        peerSelector.refreshPeers();
        peerStatus = peerSelector.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNotNull(peerStatus);
        assertEquals("Node2 should be returned since node 2 is the only available node.", node2, peerStatus.getPeerDescription());

        // Proceed time so that peer selector refresh statuses.
        systemTime.offset += TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES) + 1;

        // 3rd attempt.
        peerSelector.refreshPeers();
        peerStatus = peerSelector.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNotNull(peerStatus);
        assertEquals("Node2 should be returned since node 2 is the only available node.", node2, peerStatus.getPeerDescription());

        // Remove node2 to simulate that it goes down. There's no available node at this point.
        peerStatuses.remove(node2Status);
        systemTime.offset += TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES) + 1;

        peerSelector.refreshPeers();
        peerStatus = peerSelector.getNextPeerStatus(TransferDirection.RECEIVE);
        assertNull("PeerSelector should return null as next peer status, since there's no available peer", peerStatus);

        // Add node1 back. PeerSelector should be able to fetch peer statuses because it always tries to fetch at least from the bootstrap node.
        peerStatuses.add(bootstrapNodeStatus);
        systemTime.offset += TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES) + 1;

        peerSelector.refreshPeers();
        peerStatus = peerSelector.getNextPeerStatus(TransferDirection.RECEIVE);
        assertEquals("Node1 should be returned since node 1 is the only available node.", bootstrapNode, peerStatus.getPeerDescription());
    }
}
