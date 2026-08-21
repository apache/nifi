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
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorPurgeRequestEndpointMergerTest {
    private static final String HTTP_DELETE = "DELETE";
    private static final String HTTP_GET = "GET";
    private static final String HTTP_POST = "POST";
    private static final String HTTP_PUT = "PUT";
    private static final String CONNECTOR_ID = "abcdef00-abcd-abcd-abcd-abcdef000000";
    private static final String REQUEST_ID = "00000000-0000-0000-0000-000000000001";
    private static final String PURGE_REQUESTS_URI = "/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests";
    private static final String PURGE_REQUEST_URI = PURGE_REQUESTS_URI + "/" + REQUEST_ID;
    private static final String FAILURE_REASON = "Failed to purge queue";
    private static final int NO_PERCENT_COMPLETED = 0;
    private static final int PARTIAL_PERCENT_COMPLETED = 40;
    private static final int TOTAL_PERCENT_COMPLETED = 100;

    private final ConnectorPurgeRequestEndpointMerger merger = new ConnectorPurgeRequestEndpointMerger();

    @Test
    void testCanHandleConnectorPurgeRequests() {
        assertTrue(merger.canHandle(URI.create(PURGE_REQUESTS_URI), HTTP_POST));
        assertTrue(merger.canHandle(URI.create(PURGE_REQUEST_URI), HTTP_GET));
        assertTrue(merger.canHandle(URI.create(PURGE_REQUEST_URI), HTTP_DELETE));
    }

    @Test
    void testCanHandleRejectsUnrelatedRequests() {
        assertFalse(merger.canHandle(URI.create(PURGE_REQUESTS_URI), HTTP_GET));
        assertFalse(merger.canHandle(URI.create(PURGE_REQUESTS_URI), HTTP_DELETE));
        assertFalse(merger.canHandle(URI.create(PURGE_REQUEST_URI), HTTP_POST));
        assertFalse(merger.canHandle(URI.create(PURGE_REQUEST_URI), HTTP_PUT));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/not-a-uuid/purge-requests"), HTTP_POST));
        assertFalse(merger.canHandle(URI.create(PURGE_REQUESTS_URI + "/not-a-uuid"), HTTP_GET));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/backlog-requests"), HTTP_POST));
        assertFalse(merger.canHandle(URI.create("/nifi-api/processors/" + CONNECTOR_ID + "/purge-requests"), HTTP_POST));
    }

    @Test
    void testMergeWaitsForSlowestNode() {
        final DropRequestDTO clientRequest = request(true, TOTAL_PERCENT_COMPLETED, null);
        final Map<NodeIdentifier, DropRequestDTO> requests = new LinkedHashMap<>();
        requests.put(nodeIdentifier(1), clientRequest);
        requests.put(nodeIdentifier(2), request(false, PARTIAL_PERCENT_COMPLETED, null));

        merger.mergeResponses(clientRequest, requests, null, null);

        assertFalse(clientRequest.isFinished());
        assertEquals(PARTIAL_PERCENT_COMPLETED, clientRequest.getPercentCompleted());
        assertEquals("In Progress", clientRequest.getState());
    }

    @Test
    void testMergeCompletesWhenAllNodesFinish() {
        final DropRequestDTO clientRequest = request(true, TOTAL_PERCENT_COMPLETED, null);
        final Map<NodeIdentifier, DropRequestDTO> requests = new LinkedHashMap<>();
        requests.put(nodeIdentifier(1), clientRequest);
        requests.put(nodeIdentifier(2), request(true, TOTAL_PERCENT_COMPLETED, null));

        merger.mergeResponses(clientRequest, requests, null, null);

        assertTrue(clientRequest.isFinished());
        assertEquals(TOTAL_PERCENT_COMPLETED, clientRequest.getPercentCompleted());
        assertEquals("Complete", clientRequest.getState());
    }

    @Test
    void testMergePropagatesFailure() {
        final DropRequestDTO clientRequest = request(false, PARTIAL_PERCENT_COMPLETED, null);
        final Map<NodeIdentifier, DropRequestDTO> requests = new LinkedHashMap<>();
        requests.put(nodeIdentifier(1), clientRequest);
        requests.put(nodeIdentifier(2), request(true, NO_PERCENT_COMPLETED, FAILURE_REASON));

        merger.mergeResponses(clientRequest, requests, null, null);

        assertTrue(clientRequest.isFinished());
        assertEquals(TOTAL_PERCENT_COMPLETED, clientRequest.getPercentCompleted());
        assertEquals(FAILURE_REASON, clientRequest.getFailureReason());
        assertEquals("Failed: " + FAILURE_REASON, clientRequest.getState());
    }

    private static DropRequestDTO request(
            final boolean finished,
            final int percentCompleted,
            final String failureReason
    ) {
        final DropRequestDTO request = new DropRequestDTO();
        request.setFinished(finished);
        request.setPercentCompleted(percentCompleted);
        request.setFailureReason(failureReason);
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
