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
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterReplicationComponentLifecycleTest {
    private static final NodeIdentifier NODE_1 = new NodeIdentifier("node-1", "localhost", 8081, "localhost", 9081, "localhost", 10081, 11081, false);
    private static final NodeIdentifier NODE_2 = new NodeIdentifier("node-2", "localhost", 8082, "localhost", 9082, "localhost", 10082, 11082, false);
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

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse firstConnectionNonEmpty = asyncResponse(mergedNodeResponse("connection-a", 1, List.of(snapshot(NODE_1, 1), snapshot(NODE_2, 0))));
        final AsyncClusterResponse firstConnectionEmpty = asyncResponse(mergedNodeResponse("connection-a", 0, List.of(snapshot(NODE_1, 0), snapshot(NODE_2, 0))));
        final AsyncClusterResponse secondConnectionEmpty = asyncResponse(mergedNodeResponse("connection-b", 0, List.of(snapshot(NODE_1, 0), snapshot(NODE_2, 0))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any()))
                .thenReturn(firstConnectionNonEmpty, firstConnectionEmpty, secondConnectionEmpty);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a", "connection-b"), pause);

        assertTrue(result);
        verify(clusterCoordinator).getNodeIdentifiers(NodeConnectionState.CONNECTED);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsMissingNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 0, List.of(snapshot(NODE_1, 0))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsExtraNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        final NodeIdentifier extraNode = new NodeIdentifier("node-3", "localhost", 8083, "localhost", 9083, "localhost", 10083, 11083, false);

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 0, List.of(snapshot(NODE_1, 0), snapshot(NODE_2, 0), snapshot(extraNode, 0))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsDuplicateNodeCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 0, List.of(snapshot(NODE_1, 0), snapshot(NODE_1, 0))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyRejectsNonEmptyAggregateEvenWithFullCoverage() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 1, List.of(snapshot(NODE_1, 0), snapshot(NODE_2, 1))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
    }

    @Test
    void testWaitForConnectionQueuesEmptyReturnsFalseWhenPauseCancelsPolling() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 1, List.of(snapshot(NODE_1, 1), snapshot(NODE_2, 0))));
        when(requestReplicator.forwardToCoordinator(eq(NODE_1), eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertFalse(result);
        assertTrue(pause.wasInvoked());
    }

    @Test
    void testWaitForConnectionQueuesEmptyReplicatesFromCoordinator() throws Exception {
        final ClusterReplicationComponentLifecycle lifecycle = createLifecycle();
        final TestPause pause = new TestPause(false);

        when(clusterCoordinator.isActiveClusterCoordinator()).thenReturn(true);
        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(NODE_1, NODE_2));
        final AsyncClusterResponse response = asyncResponse(mergedNodeResponse("connection-a", 0, List.of(snapshot(NODE_1, 0), snapshot(NODE_2, 0))));
        when(requestReplicator.replicate(eq(user), eq(HttpMethod.GET), any(URI.class), any(), any())).thenReturn(response);

        final boolean result = lifecycle.waitForConnectionQueuesEmpty(EXAMPLE_URI, Set.of("connection-a"), pause);

        assertTrue(result);
        verify(requestReplicator).replicate(eq(user), eq(HttpMethod.GET), any(URI.class), any(), any());
    }

    private ClusterReplicationComponentLifecycle createLifecycle() {
        final ClusterReplicationComponentLifecycle lifecycle = new ClusterReplicationComponentLifecycle();
        lifecycle.setClusterCoordinator(clusterCoordinator);
        lifecycle.setRequestReplicator(requestReplicator);
        lenient().when(clusterCoordinator.getElectedActiveCoordinatorNode()).thenReturn(NODE_1);
        return lifecycle;
    }

    private AsyncClusterResponse asyncResponse(final NodeResponse mergedResponse) throws Exception {
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        when(asyncResponse.awaitMergedResponse()).thenReturn(mergedResponse);
        return asyncResponse;
    }

    private NodeResponse mergedNodeResponse(final String connectionId, final int aggregateQueued, final List<NodeConnectionStatusSnapshotDTO> nodeSnapshots) {
        final ConnectionStatusSnapshotDTO aggregateSnapshot = new ConnectionStatusSnapshotDTO();
        aggregateSnapshot.setId(connectionId);
        aggregateSnapshot.setFlowFilesQueued(aggregateQueued);

        final ConnectionStatusDTO connectionStatus = new ConnectionStatusDTO();
        connectionStatus.setId(connectionId);
        connectionStatus.setAggregateSnapshot(aggregateSnapshot);
        connectionStatus.setNodeSnapshots(nodeSnapshots);

        final ConnectionStatusEntity entity = new ConnectionStatusEntity();
        entity.setConnectionStatus(connectionStatus);
        entity.setCanRead(true);

        final Response response = Response.ok(entity).build();
        final NodeResponse nodeResponse = new NodeResponse(NODE_1, HttpMethod.GET, EXAMPLE_URI, response, 0L, connectionId);
        return new NodeResponse(nodeResponse, entity);
    }

    private NodeConnectionStatusSnapshotDTO snapshot(final NodeIdentifier nodeIdentifier, final int queued) {
        final ConnectionStatusSnapshotDTO snapshot = new ConnectionStatusSnapshotDTO();
        snapshot.setId("connection");
        snapshot.setFlowFilesQueued(queued);

        final NodeConnectionStatusSnapshotDTO nodeSnapshot = new NodeConnectionStatusSnapshotDTO();
        nodeSnapshot.setNodeId(nodeIdentifier.getId());
        nodeSnapshot.setAddress(nodeIdentifier.getApiAddress());
        nodeSnapshot.setApiPort(nodeIdentifier.getApiPort());
        nodeSnapshot.setStatusSnapshot(snapshot);
        return nodeSnapshot;
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
