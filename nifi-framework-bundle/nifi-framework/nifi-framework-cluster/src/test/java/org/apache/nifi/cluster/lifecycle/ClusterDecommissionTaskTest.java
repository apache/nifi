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

package org.apache.nifi.cluster.lifecycle;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterDecommissionTaskTest {
    private static final NodeIdentifier LOCAL_NODE_ID = new NodeIdentifier("local", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false);

    private static final NodeIdentifier OTHER_NODE_ID = new NodeIdentifier("other", "localhost", 8100, "localhost", 8101, "localhost", 8102, 8103, false);

    @Mock
    private ClusterCoordinator clusterCoordinator;

    @Mock
    private FlowController flowController;

    private ClusterDecommissionTask decommissionTask;

    @BeforeEach
    void setTask() {
        when(clusterCoordinator.getLocalNodeIdentifier()).thenReturn(LOCAL_NODE_ID);
        decommissionTask = new ClusterDecommissionTask(clusterCoordinator, flowController);
    }

    @Test
    void testDecommissionOnlyConnectedNode() {
        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(LOCAL_NODE_ID));

        assertThrows(IllegalStateException.class, decommissionTask::decommission);

        verify(flowController, never()).stopHeartbeating();
        verify(flowController, never()).setClustered(anyBoolean(), any());
        verify(clusterCoordinator, never()).requestNodeDisconnect(any(), any(), any());
    }

    @Test
    void testDecommissionDisconnectRejected() {
        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(LOCAL_NODE_ID, OTHER_NODE_ID));
        when(clusterCoordinator.requestNodeDisconnect(LOCAL_NODE_ID, DisconnectionCode.USER_DISCONNECTED, "Node is being decommissioned"))
                .thenThrow(new IllegalNodeDisconnectionException("Only connected node"));

        assertThrows(IllegalNodeDisconnectionException.class, decommissionTask::decommission);

        verify(flowController).stopHeartbeating();
        verify(flowController).setClustered(false, null);
        verify(flowController).setClustered(true, null);
        verify(flowController).startHeartbeating();
    }

    @Test
    void testDecommission() throws InterruptedException {
        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTED)).thenReturn(Set.of(LOCAL_NODE_ID, OTHER_NODE_ID));
        when(clusterCoordinator.requestNodeDisconnect(LOCAL_NODE_ID, DisconnectionCode.USER_DISCONNECTED, "Node is being decommissioned")).thenReturn(CompletableFuture.completedFuture(null));
        when(clusterCoordinator.requestNodeOffload(LOCAL_NODE_ID, OffloadCode.OFFLOADED, "Node is being decommissioned")).thenReturn(CompletableFuture.completedFuture(null));

        final NodeConnectionStatus disconnectedStatus = new NodeConnectionStatus(LOCAL_NODE_ID, NodeConnectionState.DISCONNECTED);
        final NodeConnectionStatus offloadedStatus = new NodeConnectionStatus(LOCAL_NODE_ID, NodeConnectionState.OFFLOADED);
        when(clusterCoordinator.getConnectionStatus(LOCAL_NODE_ID)).thenReturn(disconnectedStatus, offloadedStatus, offloadedStatus, null);

        decommissionTask.decommission();

        verify(flowController).stopHeartbeating();
        verify(flowController).setClustered(false, null);
        verify(flowController, never()).setClustered(true, null);
        verify(flowController, never()).startHeartbeating();
        verify(clusterCoordinator).removeNode(LOCAL_NODE_ID, "<Local Decommission>");
    }
}
