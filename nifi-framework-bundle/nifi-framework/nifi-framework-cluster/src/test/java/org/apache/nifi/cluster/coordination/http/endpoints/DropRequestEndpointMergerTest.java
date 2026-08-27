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
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DropRequestEndpointMergerTest {
    private static final String CONNECTION_ID = "12345678-1234-1234-1234-123456789012";
    private static final String REQUEST_ID = "00000000-0000-0000-0000-000000000001";

    private final DropRequestEndpointMerger merger = new DropRequestEndpointMerger();

    @Test
    void testCanHandleFlowFileQueueDropRequests() {
        assertTrue(merger.canHandle(URI.create("/nifi-api/flowfile-queues/" + CONNECTION_ID + "/drop-requests"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flowfile-queues/" + CONNECTION_ID + "/drop-requests/" + REQUEST_ID), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flowfile-queues/" + CONNECTION_ID + "/drop-requests/" + REQUEST_ID), "DELETE"));
    }

    @Test
    void testCanHandleRejectsUnsupportedRequests() {
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/abcdef00-abcd-abcd-abcd-abcdef000000/purge-requests"), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/flowfile-queues/not-a-uuid/drop-requests"), "POST"));
    }

    @Test
    void testMergeWaitsForAllNodesAndAggregatesCounts() {
        final DropRequestDTO clientRequest = dropRequest(true, 0, 4, 40L, 6, 60L);
        final Map<NodeIdentifier, DropRequestDTO> requests = new LinkedHashMap<>();
        requests.put(nodeIdentifier(1), clientRequest);
        requests.put(nodeIdentifier(2), dropRequest(false, 3, 6, 60L, 7, 70L));

        merger.mergeResponses(clientRequest, requests, null, null);

        assertFalse(clientRequest.isFinished());
        assertEquals(3, clientRequest.getCurrentCount());
        assertEquals(30L, clientRequest.getCurrentSize());
        assertEquals(10, clientRequest.getDroppedCount());
        assertEquals(100L, clientRequest.getDroppedSize());
        assertEquals(13, clientRequest.getOriginalCount());
        assertEquals(130L, clientRequest.getOriginalSize());
        assertEquals(76, clientRequest.getPercentCompleted());
    }

    private static DropRequestDTO dropRequest(
            final boolean finished,
            final int currentCount,
            final int droppedCount,
            final long droppedSize,
            final int originalCount,
            final long originalSize
    ) {
        final DropRequestDTO request = new DropRequestDTO();
        request.setFinished(finished);
        request.setCurrentCount(currentCount);
        request.setCurrentSize(currentCount * 10L);
        request.setDroppedCount(droppedCount);
        request.setDroppedSize(droppedSize);
        request.setOriginalCount(originalCount);
        request.setOriginalSize(originalSize);
        request.setState(finished ? DropFlowFileState.COMPLETE.toString() : DropFlowFileState.DROPPING_FLOWFILES.toString());
        return request;
    }

    private static NodeIdentifier nodeIdentifier(final int index) {
        return new NodeIdentifier(
                "node-" + index,
                "localhost",
                8000 + index,
                "localhost",
                8100 + index,
                "localhost",
                8200 + index,
                8300 + index,
                false
        );
    }
}
