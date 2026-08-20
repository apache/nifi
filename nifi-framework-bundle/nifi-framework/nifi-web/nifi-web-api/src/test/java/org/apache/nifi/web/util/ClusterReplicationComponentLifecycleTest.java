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

package org.apache.nifi.web.util;

import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.AsyncClusterResponse;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.QueueSizeDTO;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterReplicationComponentLifecycleTest {
    private static final NodeIdentifier NODE_1 = new NodeIdentifier("node-1", "localhost", 8081, "localhost", 9081, "localhost", 10081, 11081, false);
    private static final NodeIdentifier NODE_2 = new NodeIdentifier("node-2", "localhost", 8082, "localhost", 9082, "localhost", 10082, 11082, false);
    private static final Set<NodeIdentifier> EXPECTED_NODES = Set.of(NODE_1, NODE_2);
    private static final URI EXAMPLE_URI = URI.create("http://localhost:8080/nifi-api/flow/connections/connection-a/status");

    @Mock
    private ClusterCoordinator clusterCoordinator;
    @Mock
    private RequestReplicator requestReplicator;
    private NiFiUser user;

    @BeforeEach
    void setUpCurrentUser() {
        user = new StandardNiFiUser.Builder().identity("unit-test-user").build();
        SecurityContextHolder.getContext().setAuthentication(new NiFiAuthenticationToken(new NiFiUserDetails(user)));
    }

    @AfterEach
    void clearCurrentUser() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testWaitForConnectionQueuesEmptyReturnsTrueOnlyWhenAllConnectionsAreZeroAcrossTargetNodes() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(true);
        final AsyncClusterResponse firstConnectionNonEmpty = asyncResponse(mergedNodeResponse("connection-a", 1), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse firstConnectionDelete = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));
        final AsyncClusterResponse firstConnectionEmpty = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse firstConnectionSecondDelete = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));
        final AsyncClusterResponse secondConnectionEmpty = asyncResponse(mergedNodeResponse("connection-b", 0), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse secondConnectionDelete = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, firstConnectionNonEmpty, firstConnectionEmpty, secondConnectionEmpty);
        stubReplicate(HttpMethod.DELETE, firstConnectionDelete, firstConnectionSecondDelete, secondConnectionDelete);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a", "connection-b"), pause);

        assertTrue(result);
        verify(clusterCoordinator).getNodeIdentifiers(NodeConnectionState.CONNECTED);
        verifyReplicate(HttpMethod.POST, 3);
        verifyReplicate(HttpMethod.DELETE, 3);
        verify(requestReplicator, never()).forwardToCoordinator(eq(NODE_1), eq(user), any(), any(URI.class), any(), any());
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsMissingNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsExtraNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final NodeIdentifier extraNode = new NodeIdentifier("node-3", "localhost", 8083, "localhost", 9083, "localhost", 10083, 11083, false);
        final AsyncClusterResponse createResponse = asyncResponse(
                mergedNodeResponse("connection-a", 0),
                completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2), successfulNodeResponse(extraNode)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2), deleteNodeResponse(extraNode)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsDuplicateNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final NodeIdentifier duplicateNode = new NodeIdentifier("node-1", "localhost", 8083, "localhost", 9083, "localhost", 10083, 11083, false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(duplicateNode)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(duplicateNode)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsProblematicNodeResponses() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1), failedNodeResponse(NODE_2, 500)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsIncompleteAsyncResponse() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(
                mergedNodeResponse("connection-a", 0),
                completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)), false);
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsCompletedIdentifiersThatMissExpectedNodeId() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final NodeIdentifier unexpectedNode = new NodeIdentifier("node-3", "localhost", 8083, "localhost", 9083, "localhost", 10083, 11083, false);
        final AsyncClusterResponse createResponse = asyncResponse(
                mergedNodeResponse("connection-a", 0),
                Set.of(NODE_1, unexpectedNode),
                completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsNonEmptyAggregateEvenWithFullCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 1), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyReturnsFalseWhenPauseCancelsPolling() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 1), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
        assertTrue(pause.wasInvoked());
    }

    @Test
    void testWaitForConnectionQueuesEmptyReplicatesDirectlyToSnapshotNodesWhenCoordinatorActive() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse deleteResponse = asyncResponse(deleteResponse(), completedResponses(deleteNodeResponse(NODE_1), deleteNodeResponse(NODE_2)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertTrue(result);
        verifyReplicate(HttpMethod.POST, 1);
        verifyReplicate(HttpMethod.DELETE, 1);
    }

    @Test
    void testWaitForConnectionQueuesEmptyTreatsDeleteCleanupAsBestEffort() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);
        final AsyncClusterResponse createResponse = asyncResponse(mergedNodeResponse("connection-a", 0), completedResponses(successfulNodeResponse(NODE_1), successfulNodeResponse(NODE_2)));
        final AsyncClusterResponse deleteResponse = asyncResponse(failedDeleteResponse(), completedResponses(deleteNodeResponse(NODE_1), failedDeleteNodeResponse(NODE_2, 404)));

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(EXPECTED_NODES);
        stubReplicate(HttpMethod.POST, createResponse);
        stubReplicate(HttpMethod.DELETE, deleteResponse);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertTrue(result);
        verifyReplicate(HttpMethod.POST, 1);
        verifyReplicate(HttpMethod.DELETE, 1);
    }

    private ClusterReplicationComponentLifecycle createLifecycle() {
        final ClusterReplicationComponentLifecycle lifecycle = new ClusterReplicationComponentLifecycle();
        lifecycle.setClusterCoordinator(clusterCoordinator);
        lifecycle.setRequestReplicator(requestReplicator);
        lenient().when(clusterCoordinator.getElectedActiveCoordinatorNode()).thenReturn(NODE_1);
        return lifecycle;
    }

    private void stubReplicate(final String method, final AsyncClusterResponse... responses) {
        when(requestReplicator.replicate(eq(EXPECTED_NODES), eq(user), eq(method), any(URI.class), eq(Collections.emptyMap()), eq(Collections.emptyMap()), eq(true), eq(true)))
            .thenReturn(responses[0], java.util.Arrays.copyOfRange(responses, 1, responses.length));
    }

    private void verifyReplicate(final String method, final int times) {
        verify(requestReplicator, times(times)).replicate(eq(EXPECTED_NODES), eq(user), eq(method), any(URI.class), eq(Collections.emptyMap()), eq(Collections.emptyMap()), eq(true), eq(true));
    }

    private AsyncClusterResponse asyncResponse(final NodeResponse mergedResponse, final Set<NodeResponse> completedResponses) throws Exception {
        final Set<NodeIdentifier> completedNodeIdentifiers = completedResponses.stream()
            .map(NodeResponse::getNodeId)
            .collect(Collectors.toUnmodifiableSet());
        return asyncResponse(mergedResponse, completedNodeIdentifiers, completedResponses, true);
    }

    private AsyncClusterResponse asyncResponse(final NodeResponse mergedResponse, final Set<NodeResponse> completedResponses, final boolean complete) throws Exception {
        final Set<NodeIdentifier> completedNodeIdentifiers = completedResponses.stream()
            .map(NodeResponse::getNodeId)
            .collect(Collectors.toUnmodifiableSet());
        return asyncResponse(mergedResponse, completedNodeIdentifiers, completedResponses, complete);
    }

    private AsyncClusterResponse asyncResponse(final NodeResponse mergedResponse, final Set<NodeIdentifier> completedNodeIdentifiers,
                                               final Set<NodeResponse> completedResponses) throws Exception {
        return asyncResponse(mergedResponse, completedNodeIdentifiers, completedResponses, true);
    }

    private AsyncClusterResponse asyncResponse(final NodeResponse mergedResponse, final Set<NodeIdentifier> completedNodeIdentifiers,
                                               final Set<NodeResponse> completedResponses, final boolean complete) throws Exception {
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        when(asyncResponse.awaitMergedResponse()).thenReturn(mergedResponse);
        lenient().when(asyncResponse.getNodesInvolved()).thenReturn(completedNodeIdentifiers);
        lenient().when(asyncResponse.getCompletedNodeIdentifiers()).thenReturn(completedNodeIdentifiers);
        lenient().when(asyncResponse.getCompletedNodeResponses()).thenReturn(completedResponses);
        lenient().when(asyncResponse.isComplete()).thenReturn(complete);
        return asyncResponse;
    }

    private Set<NodeResponse> completedResponses(final NodeResponse... nodeResponses) {
        final Set<NodeResponse> completedResponses = new LinkedHashSet<>();
        for (final NodeResponse nodeResponse : nodeResponses) {
            completedResponses.add(nodeResponse);
        }
        return completedResponses;
    }

    private NodeResponse mergedNodeResponse(final String connectionId, final int aggregateQueued) {
        final QueueSizeDTO queueSize = new QueueSizeDTO();
        queueSize.setObjectCount(aggregateQueued);

        final ListingRequestDTO listingRequest = new ListingRequestDTO();
        listingRequest.setId(connectionId + "-request");
        listingRequest.setQueueSize(queueSize);

        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(listingRequest);

        final Response response = Response.accepted(entity).build();
        final NodeResponse nodeResponse = new NodeResponse(NODE_1, HttpMethod.POST, EXAMPLE_URI, response, 0L, connectionId);
        return new NodeResponse(nodeResponse, entity);
    }

    private NodeResponse deleteResponse() {
        final Response response = Response.ok().build();
        return new NodeResponse(NODE_1, HttpMethod.DELETE, EXAMPLE_URI, response, 0L, "delete");
    }

    private NodeResponse failedDeleteResponse() {
        final Response response = Response.status(404).build();
        return new NodeResponse(NODE_1, HttpMethod.DELETE, EXAMPLE_URI, response, 0L, "delete");
    }

    private NodeResponse successfulNodeResponse(final NodeIdentifier nodeIdentifier) {
        return new NodeResponse(nodeIdentifier, HttpMethod.POST, EXAMPLE_URI, Response.accepted().build(), 0L, nodeIdentifier.getId());
    }

    private NodeResponse deleteNodeResponse(final NodeIdentifier nodeIdentifier) {
        return new NodeResponse(nodeIdentifier, HttpMethod.DELETE, EXAMPLE_URI, Response.ok().build(), 0L, nodeIdentifier.getId() + "-delete");
    }

    private NodeResponse failedDeleteNodeResponse(final NodeIdentifier nodeIdentifier, final int status) {
        return new NodeResponse(nodeIdentifier, HttpMethod.DELETE, EXAMPLE_URI, Response.status(status).build(), 0L, nodeIdentifier.getId() + "-delete");
    }

    private NodeResponse failedNodeResponse(final NodeIdentifier nodeIdentifier, final int status) {
        return new NodeResponse(nodeIdentifier, HttpMethod.POST, EXAMPLE_URI, Response.status(status).build(), 0L, nodeIdentifier.getId());
    }

    private static final class TestPause implements Pause {
        private final List<Boolean> decisions;
        private int index = 0;
        private boolean invoked;

        private TestPause(final Boolean... decisions) {
            this.decisions = List.of(decisions);
        }

        @Override
        public boolean pause() {
            invoked = true;
            if (index >= decisions.size()) {
                return false;
            }

            return decisions.get(index++);
        }

        private boolean wasInvoked() {
            return invoked;
        }
    }
}
