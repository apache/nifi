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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.AsyncClusterResponse;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.groups.ProcessGroupDrainState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ProcessGroupDrainRequestEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessGroupDrainMonitorTest {
    private static final String PROCESS_GROUP_ID = "process-group-1";
    private static final String DRAIN_REQUEST_ID = "drain-1";
    private static final URI DRAIN_REQUEST_URI = URI.create("http://localhost:8080/nifi-api/process-groups/process-group-1/drain-requests/drain-1");
    private static final NiFiUser USER = new StandardNiFiUser.Builder().identity("drain-user").build();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private ClusterCoordinator clusterCoordinator;

    @Mock
    private RequestReplicator requestReplicator;

    private ProcessGroupDrainMonitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new ProcessGroupDrainMonitor(clusterCoordinator, requestReplicator, serviceFacade);
    }

    @AfterEach
    void shutdownMonitor() {
        monitor.shutdown();
    }

    @Test
    void testLocalDrainEndsOnlyAfterTwoEmptyChecksInARow() {
        final List<String> checkingUsers = new ArrayList<>();
        when(serviceFacade.getProcessGroupDrainRequest(PROCESS_GROUP_ID, DRAIN_REQUEST_ID)).thenAnswer(invocation -> {
            checkingUsers.add(NiFiUserUtils.getNiFiUserIdentity());
            return Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 5));
        }).thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 0)))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 3)))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 0)))
            .thenThrow(new IllegalStateException("Flow is not ready"))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 0, false)))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 0)))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.DRAINING, 0)));

        // A check that finds data, that fails, or that finds a drain that is not yet prepared resets the count of empty checks
        final ProcessGroupDrainMonitor.DrainCheck check = monitor.new DrainCheck(null, PROCESS_GROUP_ID, DRAIN_REQUEST_ID, USER);
        for (int i = 0; i < 7; i++) {
            assertTrue(check.check());
        }

        verify(serviceFacade, never()).endProcessGroupDrain(PROCESS_GROUP_ID, DRAIN_REQUEST_ID);
        assertFalse(check.check());
        verify(serviceFacade).endProcessGroupDrain(PROCESS_GROUP_ID, DRAIN_REQUEST_ID);

        assertEquals(List.of(USER.getIdentity()), checkingUsers);
        assertNull(SecurityContextHolder.getContext().getAuthentication());
    }

    @Test
    void testLocalDrainStopsBeingWatchedOnceItIsNoLongerDraining() {
        when(serviceFacade.getProcessGroupDrainRequest(PROCESS_GROUP_ID, DRAIN_REQUEST_ID))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.ENDED, 0)))
            .thenThrow(new ResourceNotFoundException("Process Group was removed"))
            .thenReturn(Optional.of(createDrainRequest(ProcessGroupDrainState.FAILED, 0)));

        for (int i = 0; i < 3; i++) {
            final ProcessGroupDrainMonitor.DrainCheck check = monitor.new DrainCheck(null, PROCESS_GROUP_ID, DRAIN_REQUEST_ID, USER);
            assertFalse(check.check());
        }
        verify(serviceFacade, never()).endProcessGroupDrain(any(), any());

        // A drain that failed is ended, because in a cluster it may still be draining on the nodes where it did not fail
        assertFalse(monitor.new DrainCheck(null, PROCESS_GROUP_ID, DRAIN_REQUEST_ID, USER).check());
        verify(serviceFacade).endProcessGroupDrain(PROCESS_GROUP_ID, DRAIN_REQUEST_ID);
    }

    @Test
    void testClusteredDrainReadsMergedStatusAndEndsOnAllNodes() throws InterruptedException {
        final NodeResponse failedResponse = createNodeResponse(500, null);
        final NodeResponse emptyResponse = createNodeResponse(200, createDrainRequest(ProcessGroupDrainState.DRAINING, 0));
        final AsyncClusterResponse failedGet = createAsyncResponse(failedResponse);
        final AsyncClusterResponse emptyGet = createAsyncResponse(emptyResponse);
        final AsyncClusterResponse delete = createAsyncResponse(createNodeResponse(200, null));

        // The coordinator replicates the requests itself
        when(clusterCoordinator.isActiveClusterCoordinator()).thenReturn(true);
        when(requestReplicator.replicate(eq(USER), eq(HttpMethod.GET), eq(DRAIN_REQUEST_URI), any(), anyMap())).thenReturn(emptyGet, failedGet, emptyGet);

        // A node that is not connected does not report its data, so a check made while a node is not connected does not count as empty
        final NodeIdentifier disconnectedNode = mock(NodeIdentifier.class);
        when(clusterCoordinator.getNodeIdentifiers(any(NodeConnectionState[].class))).thenReturn(Set.of(disconnectedNode), Set.of());

        final ProcessGroupDrainMonitor.DrainCheck check = monitor.new DrainCheck(DRAIN_REQUEST_URI, PROCESS_GROUP_ID, DRAIN_REQUEST_ID, USER);
        assertTrue(check.check());
        assertTrue(check.check());
        assertTrue(check.check());
        assertTrue(check.check());
        verify(requestReplicator, never()).replicate(any(NiFiUser.class), eq(HttpMethod.DELETE), any(), any(), anyMap());

        // Any other node forwards the requests to the coordinator
        final NodeIdentifier coordinatorNode = mock(NodeIdentifier.class);
        when(clusterCoordinator.isActiveClusterCoordinator()).thenReturn(false);
        when(clusterCoordinator.getElectedActiveCoordinatorNode()).thenReturn(coordinatorNode);
        when(requestReplicator.forwardToCoordinator(eq(coordinatorNode), eq(USER), eq(HttpMethod.GET), eq(DRAIN_REQUEST_URI), any(), anyMap())).thenReturn(emptyGet);
        when(requestReplicator.forwardToCoordinator(eq(coordinatorNode), eq(USER), eq(HttpMethod.DELETE), eq(DRAIN_REQUEST_URI), any(), anyMap())).thenReturn(delete);

        assertFalse(check.check());
        verify(requestReplicator).forwardToCoordinator(eq(coordinatorNode), eq(USER), eq(HttpMethod.DELETE), eq(DRAIN_REQUEST_URI), any(), anyMap());
        verify(serviceFacade, never()).endProcessGroupDrain(any(), any());

        // A Process Group that no longer exists on the cluster is no longer watched
        final AsyncClusterResponse notFound = createAsyncResponse(createNodeResponse(404, null));
        when(requestReplicator.forwardToCoordinator(eq(coordinatorNode), eq(USER), eq(HttpMethod.GET), eq(DRAIN_REQUEST_URI), any(), anyMap())).thenReturn(notFound);
        assertFalse(monitor.new DrainCheck(DRAIN_REQUEST_URI, PROCESS_GROUP_ID, DRAIN_REQUEST_ID, USER).check());
    }

    private static ProcessGroupDrainRequestDTO createDrainRequest(final ProcessGroupDrainState state, final int currentCount) {
        return createDrainRequest(state, currentCount, true);
    }

    private static ProcessGroupDrainRequestDTO createDrainRequest(final ProcessGroupDrainState state, final int currentCount, final boolean prepared) {
        final ProcessGroupDrainRequestDTO drainRequest = new ProcessGroupDrainRequestDTO();
        drainRequest.setId(DRAIN_REQUEST_ID);
        drainRequest.setProcessGroupId(PROCESS_GROUP_ID);
        drainRequest.setState(state.name());
        drainRequest.setPrepared(prepared);
        drainRequest.setCurrentCount(currentCount);
        return drainRequest;
    }

    private static NodeResponse createNodeResponse(final int status, final ProcessGroupDrainRequestDTO drainRequest) {
        final NodeResponse nodeResponse = mock(NodeResponse.class);
        when(nodeResponse.getStatus()).thenReturn(status);
        if (drainRequest != null) {
            final ProcessGroupDrainRequestEntity entity = new ProcessGroupDrainRequestEntity();
            entity.setDrainRequest(drainRequest);
            when(nodeResponse.getUpdatedEntity()).thenReturn(entity);
        }

        return nodeResponse;
    }

    private static AsyncClusterResponse createAsyncResponse(final NodeResponse nodeResponse) throws InterruptedException {
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        return asyncResponse;
    }
}
