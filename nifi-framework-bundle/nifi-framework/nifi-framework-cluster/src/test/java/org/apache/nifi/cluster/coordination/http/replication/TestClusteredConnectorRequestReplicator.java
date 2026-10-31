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

package org.apache.nifi.cluster.coordination.http.replication;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestClusteredConnectorRequestReplicator {
    private static final String CONNECTOR_ID = "connector-1";

    @Test
    void testFlowEvolutionStatusWaitsForMembershipToSettle() {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
        when(clusterCoordinator.getConnectionStatuses()).thenReturn(List.of(connectionStatus));
        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(
                () -> requestReplicator, () -> clusterCoordinator, false);

        when(clusterCoordinator.isConnected()).thenReturn(false);
        assertThrows(NoConnectedNodesException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));

        when(clusterCoordinator.isConnected()).thenReturn(true);
        when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTING);
        assertThrows(NoConnectedNodesException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));

        when(connectionStatus.getState()).thenReturn(NodeConnectionState.DISCONNECTED);
        when(connectionStatus.getDisconnectCode()).thenReturn(DisconnectionCode.NOT_YET_CONNECTED);
        assertThrows(NoConnectedNodesException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));

        verify(requestReplicator, never()).replicate(isNull(), eq("GET"), any(), anyMap(), anyMap());
    }

    @Test
    void testFlowEvolutionStatusFailsWhenNodeDisconnected() {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
        when(clusterCoordinator.isConnected()).thenReturn(true);
        when(clusterCoordinator.getConnectionStatuses()).thenReturn(List.of(connectionStatus));
        when(connectionStatus.getState()).thenReturn(NodeConnectionState.DISCONNECTED);

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(
                () -> requestReplicator, () -> clusterCoordinator, false);

        assertThrows(IOException.class, () -> replicator.getFlowEvolutionStatus(CONNECTOR_ID));
        verify(requestReplicator, never()).replicate(isNull(), eq("GET"), any(), anyMap(), anyMap());
    }

    @Test
    void testFlowEvolutionStatusReturnsMergedFailure() throws Exception {
        final RequestReplicator requestReplicator = mock(RequestReplicator.class);
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        final NodeResponse nodeResponse = mock(NodeResponse.class);

        when(clusterCoordinator.isConnected()).thenReturn(true);
        when(clusterCoordinator.getConnectionStatuses()).thenReturn(List.of());
        when(requestReplicator.replicate(isNull(), eq("GET"), any(), anyMap(), anyMap())).thenReturn(asyncResponse);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        when(nodeResponse.getUpdatedEntity()).thenReturn(connectorEntity(FlowEvolutionStatus.FAILED));

        final ClusteredConnectorRequestReplicator replicator = new ClusteredConnectorRequestReplicator(
                () -> requestReplicator, () -> clusterCoordinator, false);

        assertEquals(FlowEvolutionStatus.FAILED, replicator.getFlowEvolutionStatus(CONNECTOR_ID));
    }

    private static ConnectorEntity connectorEntity(final FlowEvolutionStatus status) {
        final ConnectorDTO connector = new ConnectorDTO();
        connector.setFlowEvolutionStatus(status.name());
        final ConnectorEntity entity = new ConnectorEntity();
        entity.setComponent(connector);
        return entity;
    }
}
