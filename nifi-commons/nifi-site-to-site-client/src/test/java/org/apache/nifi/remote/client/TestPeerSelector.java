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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertTrue;

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
    public void testFormulateDestinationListForOutput() throws IOException {
        final Set<PeerStatus> collection = new HashSet<>();
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 1111, true), 4096));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 2222, true), 10240));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 3333, true), 1024));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 4444, true), 4096));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 5555, true), 4096));

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
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 1111, true), 500));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 2222, true), 50000));

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
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 1111, true), 4096));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 2222, true), 10240));
        collection.add(new PeerStatus(new PeerDescription("HasLots", 3333, true), 1024));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 4444, true), 4096));
        collection.add(new PeerStatus(new PeerDescription("HasMedium", 5555, true), 4096));

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
        collection.add(new PeerStatus(new PeerDescription("HasLots", 1111, true), 500));
        collection.add(new PeerStatus(new PeerDescription("HasLittle", 2222, true), 50000));

        PeerStatusProvider peerStatusProvider = Mockito.mock(PeerStatusProvider.class);
        PeerSelector peerSelector = new PeerSelector(peerStatusProvider, null);

        final List<PeerStatus> destinations = peerSelector.formulateDestinationList(collection, TransferDirection.RECEIVE);
        final Map<String, Integer> selectedCounts = calculateAverageSelectedCount(collection, destinations);

        logger.info("selectedCounts={}", selectedCounts);
        assertTrue("HasLots should get little", selectedCounts.get("HasLots") < selectedCounts.get("HasLittle"));
    }
}
