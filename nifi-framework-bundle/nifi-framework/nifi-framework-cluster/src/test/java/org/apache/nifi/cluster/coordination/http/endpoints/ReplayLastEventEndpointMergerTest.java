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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventSnapshotDTO;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplayLastEventEndpointMergerTest {

    @Test
    public void testCanHandle() {
        final ReplayLastEventEndpointMerger merger = new ReplayLastEventEndpointMerger();
        assertTrue(merger.canHandle(URI.create("/nifi-api/provenance-events/latest/replays"), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/provenance-events/latest/replays"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/provenance-events"), "POST"));
    }

    /**
     * Verifies that when two nodes each replay their last event and both happen to have the same
     * local event ID (which is expected when both nodes have processed the same number of events),
     * the merged aggregate correctly reports 2 events replayed rather than 1.
     * Provenance event IDs are local counters per node, so ID collisions across nodes are normal.
     */
    @Test
    public void testMergeResponsesWithIdenticalEventIds() {
        final ReplayLastEventEndpointMerger merger = new ReplayLastEventEndpointMerger();

        final ReplayLastEventResponseEntity clientEntity = createEntity(7L, null, true);

        final NodeIdentifier node1 = new NodeIdentifier("node1", "host1", 8080, "host1", 8081, "host1", 8082, 8083, false);
        final NodeIdentifier node2 = new NodeIdentifier("node2", "host2", 8080, "host2", 8081, "host2", 8082, 8083, false);

        // Both nodes replay their last event; both happen to report local event ID 7
        final Map<NodeIdentifier, ReplayLastEventResponseEntity> entityMap = new HashMap<>();
        entityMap.put(node1, createEntity(7L, null, true));
        entityMap.put(node2, createEntity(7L, null, true));

        merger.mergeResponses(clientEntity, entityMap, Collections.emptySet(), Collections.emptySet());

        assertEquals(2, clientEntity.getAggregateSnapshot().getEventsReplayed().size(),
                "Both nodes replayed an event; aggregate must count them independently regardless of matching local event IDs");
        assertTrue(clientEntity.getAggregateSnapshot().getEventAvailable());
        assertNull(clientEntity.getAggregateSnapshot().getFailureExplanation());
        assertEquals(2, clientEntity.getNodeSnapshots().size());
    }

    @Test
    public void testMergeResponsesWithDistinctEventIds() {
        final ReplayLastEventEndpointMerger merger = new ReplayLastEventEndpointMerger();

        final ReplayLastEventResponseEntity clientEntity = createEntity(3L, null, true);

        final NodeIdentifier node1 = new NodeIdentifier("node1", "host1", 8080, "host1", 8081, "host1", 8082, 8083, false);
        final NodeIdentifier node2 = new NodeIdentifier("node2", "host2", 8080, "host2", 8081, "host2", 8082, 8083, false);

        final Map<NodeIdentifier, ReplayLastEventResponseEntity> entityMap = new HashMap<>();
        entityMap.put(node1, createEntity(3L, null, true));
        entityMap.put(node2, createEntity(5L, null, true));

        merger.mergeResponses(clientEntity, entityMap, Collections.emptySet(), Collections.emptySet());

        assertEquals(2, clientEntity.getAggregateSnapshot().getEventsReplayed().size());
        assertTrue(clientEntity.getAggregateSnapshot().getEventAvailable());
        assertNull(clientEntity.getAggregateSnapshot().getFailureExplanation());
    }

    @Test
    public void testMergeResponsesWithFailure() {
        final ReplayLastEventEndpointMerger merger = new ReplayLastEventEndpointMerger();

        final ReplayLastEventResponseEntity clientEntity = createEntity(1L, null, true);

        final NodeIdentifier node1 = new NodeIdentifier("node1", "host1", 8080, "host1", 8081, "host1", 8082, 8083, false);
        final NodeIdentifier node2 = new NodeIdentifier("node2", "host2", 8080, "host2", 8081, "host2", 8082, 8083, false);

        final Map<NodeIdentifier, ReplayLastEventResponseEntity> entityMap = new HashMap<>();
        entityMap.put(node1, createEntity(1L, null, true));
        entityMap.put(node2, createEntity(null, "Source FlowFile Queue", false));

        merger.mergeResponses(clientEntity, entityMap, Collections.emptySet(), Collections.emptySet());

        assertEquals(1, clientEntity.getAggregateSnapshot().getEventsReplayed().size());
        assertTrue(clientEntity.getAggregateSnapshot().getEventAvailable());
        assertTrue(clientEntity.getAggregateSnapshot().getFailureExplanation().contains("Source FlowFile Queue"));
    }

    private ReplayLastEventResponseEntity createEntity(final Long eventId, final String failureExplanation, final boolean eventAvailable) {
        final ReplayLastEventSnapshotDTO snapshot = new ReplayLastEventSnapshotDTO();
        snapshot.setEventAvailable(eventAvailable);
        snapshot.setFailureExplanation(failureExplanation);
        if (eventId != null) {
            snapshot.setEventsReplayed(Collections.singletonList(eventId));
        }

        final ReplayLastEventResponseEntity entity = new ReplayLastEventResponseEntity();
        entity.setAggregateSnapshot(snapshot);
        return entity;
    }
}
