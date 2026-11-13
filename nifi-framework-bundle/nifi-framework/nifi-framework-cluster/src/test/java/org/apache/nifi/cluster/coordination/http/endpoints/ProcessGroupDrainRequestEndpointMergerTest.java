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
import org.apache.nifi.web.api.dto.DrainBlockingComponentDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ProcessGroupDrainRequestEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessGroupDrainRequestEndpointMergerTest {
    private static final String DRAINING = "DRAINING";
    private static final String FAILED = "FAILED";
    private static final String ENDED = "ENDED";
    private static final String PROCESS_GROUP_ID = "abcdef00-abcd-abcd-abcd-abcdef000000";
    private static final String DRAIN_REQUEST_ID = "00000000-0000-0000-0000-000000000001";
    private static final String DRAIN_REQUESTS_URI = "/nifi-api/process-groups/" + PROCESS_GROUP_ID + "/drain-requests";
    private static final String DRAIN_REQUEST_URI = DRAIN_REQUESTS_URI + "/" + DRAIN_REQUEST_ID;
    private static final long BYTES_PER_FLOWFILE = 1000L;

    private final ProcessGroupDrainRequestEndpointMerger merger = new ProcessGroupDrainRequestEndpointMerger();

    @Test
    void testCanHandle() {
        assertTrue(merger.canHandle(URI.create(DRAIN_REQUESTS_URI), "POST"));
        assertTrue(merger.canHandle(URI.create(DRAIN_REQUEST_URI), "GET"));
        assertTrue(merger.canHandle(URI.create(DRAIN_REQUEST_URI), "DELETE"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/process-groups/root/drain-requests"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/process-groups/root/drain-requests/" + DRAIN_REQUEST_ID), "GET"));

        assertFalse(merger.canHandle(URI.create(DRAIN_REQUESTS_URI), "GET"));
        assertFalse(merger.canHandle(URI.create(DRAIN_REQUEST_URI), "POST"));
        assertFalse(merger.canHandle(URI.create(DRAIN_REQUEST_URI), "PUT"));
        assertFalse(merger.canHandle(URI.create(DRAIN_REQUESTS_URI + "/not-a-uuid"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/process-groups/" + PROCESS_GROUP_ID + "/empty-all-connections-requests"), "POST"));
    }

    @Test
    void testMergeSumsCountsAndSizes() {
        final ProcessGroupDrainRequestDTO merged = merge(createRequest(DRAINING, 10, 0), createRequest(DRAINING, 30, 20));
        assertEquals(DRAIN_REQUEST_ID, merged.getId());
        assertEquals(PROCESS_GROUP_ID, merged.getProcessGroupId());
        assertEquals(DRAIN_REQUEST_URI, merged.getUri());
        assertEquals(40, merged.getOriginalCount());
        assertEquals(40 * BYTES_PER_FLOWFILE, merged.getOriginalSize());
        assertEquals(20, merged.getCurrentCount());
        assertEquals(20 * BYTES_PER_FLOWFILE, merged.getCurrentSize());
        assertTrue(merged.getPrepared());

        // The drain is prepared only once it is prepared on every node
        final ProcessGroupDrainRequestDTO unpreparedRequest = createRequest(DRAINING, 30, 0);
        unpreparedRequest.setPrepared(false);
        assertFalse(merge(createRequest(DRAINING, 10, 0), unpreparedRequest).getPrepared());
    }

    @Test
    void testMergeStateAndFailureReason() {
        assertEquals(ENDED, merge(createRequest(ENDED, 0, 0), createRequest(ENDED, 0, 0)).getState());

        final ProcessGroupDrainRequestDTO draining = merge(createRequest(ENDED, 0, 0), createRequest(DRAINING, 0, 0));
        assertEquals(DRAINING, draining.getState());
        assertNull(draining.getFailureReason());

        final ProcessGroupDrainRequestDTO secondNodeRequest = createRequest(FAILED, 0, 0);
        secondNodeRequest.setFailureReason("Could not start processor");
        final ProcessGroupDrainRequestDTO thirdNodeRequest = createRequest(FAILED, 0, 0);
        thirdNodeRequest.setFailureReason("Could not enable service");

        final ProcessGroupDrainRequestDTO failed = merge(createRequest(DRAINING, 0, 0), secondNodeRequest, thirdNodeRequest);
        assertEquals(FAILED, failed.getState());
        assertEquals("localhost:8002 -- Could not start processor\nlocalhost:8003 -- Could not enable service", failed.getFailureReason());
    }

    @Test
    void testMergeBlockingComponents() {
        final ProcessGroupDrainRequestDTO firstNodeRequest = createRequest(DRAINING, 10, 10);
        firstNodeRequest.setBlockingComponents(List.of(createBlockingComponent("component-b", 1), createBlockingComponent("component-a", 3)));

        final ProcessGroupDrainRequestDTO secondNodeRequest = createRequest(DRAINING, 10, 10);
        secondNodeRequest.setBlockingComponents(List.of(createBlockingComponent("component-c", 2), createBlockingComponent("component-a", 4), createBlockingComponent("component-b", 5)));

        // The components are sorted by identifier, so their order does not depend on the order in which the nodes respond
        final List<DrainBlockingComponentDTO> blockingComponents = merge(firstNodeRequest, secondNodeRequest).getBlockingComponents();
        assertEquals(List.of("component-a", "component-b", "component-c"), blockingComponents.stream().map(DrainBlockingComponentDTO::getId).toList());
        assertEquals(List.of(7, 6, 2), blockingComponents.stream().map(DrainBlockingComponentDTO::getQueuedCount).toList());
    }

    @Test
    void testMergeReportsNodesWithoutRecordAsFailed() {
        // A node that has no record of the drain, such as a node that restarted, may still have data queued, so the drain cannot complete
        final ProcessGroupDrainRequestDTO merged = merge(null, createRequest(DRAINING, 10, 0), null);
        assertEquals(DRAIN_REQUEST_ID, merged.getId());
        assertEquals(FAILED, merged.getState());
        assertEquals(10, merged.getOriginalCount());
        assertEquals("localhost:8001 -- The node has no record of the drain, for example because it restarted, so the FlowFile counts do not include its FlowFiles\n"
            + "localhost:8003 -- The node has no record of the drain, for example because it restarted, so the FlowFile counts do not include its FlowFiles", merged.getFailureReason());

        // If no node has a record of the drain, for example because a new drain replaced it, the merged response has no drain request
        assertNull(merge(null, null, null));
    }

    /**
     * Merges the given requests as if each came from a different node. The first request is the response of the node that the client is connected to.
     * A null request represents a node that has no record of the drain.
     */
    private ProcessGroupDrainRequestDTO merge(final ProcessGroupDrainRequestDTO... nodeRequests) {
        final Map<NodeIdentifier, ProcessGroupDrainRequestEntity> entityMap = new LinkedHashMap<>();
        for (int index = 0; index < nodeRequests.length; index++) {
            final ProcessGroupDrainRequestEntity entity = new ProcessGroupDrainRequestEntity();
            entity.setDrainRequest(nodeRequests[index]);
            entityMap.put(nodeIdentifier(index + 1), entity);
        }

        final ProcessGroupDrainRequestEntity clientEntity = entityMap.values().iterator().next();
        merger.mergeResponses(clientEntity, entityMap, null, null);
        return clientEntity.getDrainRequest();
    }

    private static ProcessGroupDrainRequestDTO createRequest(final String state, final int originalCount, final int currentCount) {
        final ProcessGroupDrainRequestDTO request = new ProcessGroupDrainRequestDTO();
        request.setId(DRAIN_REQUEST_ID);
        request.setProcessGroupId(PROCESS_GROUP_ID);
        request.setUri(DRAIN_REQUEST_URI);
        request.setState(state);
        request.setPrepared(true);
        request.setOriginalCount(originalCount);
        request.setOriginalSize(originalCount * BYTES_PER_FLOWFILE);
        request.setCurrentCount(currentCount);
        request.setCurrentSize(currentCount * BYTES_PER_FLOWFILE);
        request.setBlockingComponents(List.of());
        return request;
    }

    private static DrainBlockingComponentDTO createBlockingComponent(final String id, final int queuedCount) {
        final DrainBlockingComponentDTO component = new DrainBlockingComponentDTO();
        component.setId(id);
        component.setQueuedCount(queuedCount);
        return component;
    }

    private static NodeIdentifier nodeIdentifier(final int index) {
        return new NodeIdentifier("node-" + index, "localhost", 8000 + index, "localhost", 8100 + index, "localhost", 8200 + index, 8300 + index, false);
    }
}
