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
package org.apache.nifi.controller;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TestFlowControllerNodeConnectionState {

    @Test
    void testGetNodeConnectionStateStandaloneReturnsStandalone() throws Exception {
        final FlowController controller = createFlowController(false, null);

        assertEquals(NodeConnectionState.STANDALONE, controller.getNodeConnectionState());
    }

    @Test
    void testGetNodeConnectionStateClusterConfiguredWithMissingNodeIdReturnsDisconnected() throws Exception {
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final FlowController controller = createFlowController(true, clusterCoordinator);

        assertEquals(NodeConnectionState.DISCONNECTED, controller.getNodeConnectionState());
        verifyNoInteractions(clusterCoordinator);
    }

    @Test
    void testGetNodeConnectionStateClusterConfiguredWithMissingStatusReturnsDisconnected() throws Exception {
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final FlowController controller = createFlowController(true, clusterCoordinator);
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        controller.setNodeId(nodeIdentifier);

        when(clusterCoordinator.getConnectionStatus(nodeIdentifier)).thenReturn(null);

        assertEquals(NodeConnectionState.DISCONNECTED, controller.getNodeConnectionState());
    }

    @Test
    void testGetNodeConnectionStateMapsAllProtocolStates() throws Exception {
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final FlowController controller = createFlowController(true, clusterCoordinator);
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        controller.setNodeId(nodeIdentifier);

        for (final org.apache.nifi.cluster.coordination.node.NodeConnectionState protocolState
                : org.apache.nifi.cluster.coordination.node.NodeConnectionState.values()) {
            when(clusterCoordinator.getConnectionStatus(nodeIdentifier)).thenReturn(new NodeConnectionStatus(nodeIdentifier, protocolState));

            assertEquals(NodeConnectionState.valueOf(protocolState.name()), controller.getNodeConnectionState());
        }
    }

    @Test
    void testGetNodeConnectionStateReturnsConnectingBeforeClusteredFlagBecomesTrue() throws Exception {
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final FlowController controller = createFlowController(true, clusterCoordinator);
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        controller.setNodeId(nodeIdentifier);

        when(clusterCoordinator.getConnectionStatus(nodeIdentifier)).thenReturn(new NodeConnectionStatus(
                nodeIdentifier, org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING));

        assertFalse(controller.isClustered());
        assertEquals(NodeConnectionState.CONNECTING, controller.getNodeConnectionState());
    }

    private FlowController createFlowController(final boolean configuredForClustering, final ClusterCoordinator clusterCoordinator) throws Exception {
        final FlowController controller = mock(FlowController.class, CALLS_REAL_METHODS);
        doReturn(configuredForClustering).when(controller).isConfiguredForClustering();
        doReturn(clusterCoordinator).when(controller).getClusterCoordinator();
        return controller;
    }

    private NodeIdentifier createNodeIdentifier() {
        return new NodeIdentifier("node-1", "localhost", 8443, "localhost", 9090, "localhost", 6342, 10443, false);
    }
}
