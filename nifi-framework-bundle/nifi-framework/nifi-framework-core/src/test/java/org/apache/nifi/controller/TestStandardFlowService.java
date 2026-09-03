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

import org.apache.nifi.asset.AssetSynchronizer;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.ComponentRevisionSnapshot;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.state.MockStateMap;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFlowService {

    private FlowController controller;
    private ClusterCoordinator clusterCoordinator;
    private StandardFlowService flowService;
    private NiFiProperties nifiProperties;

    @TempDir
    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        controller = mock(FlowController.class);
        clusterCoordinator = mock(ClusterCoordinator.class);
        final NodeProtocolSenderListener senderListener = mock(NodeProtocolSenderListener.class);
        final RevisionManager revisionManager = mock(RevisionManager.class);
        final NarManager narManager = mock(NarManager.class);
        final AssetSynchronizer parameterContextAssetSynchronizer = mock(AssetSynchronizer.class);
        final AssetSynchronizer connectorAssetSynchronizer = mock(AssetSynchronizer.class);
        final Authorizer authorizer = mock(Authorizer.class);

        final StateManagerProvider stateManagerProvider = mock(StateManagerProvider.class);
        final StateManager stateManager = mock(StateManager.class);
        when(stateManager.getState(any(Scope.class))).thenReturn(new MockStateMap(Collections.emptyMap(), 1));
        when(stateManagerProvider.getStateManager(anyString())).thenReturn(stateManager);
        when(controller.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(controller.getExtensionManager()).thenReturn(mock(ExtensionManager.class));

        final Path flowConfigFile = tempDir.resolve("flow.json.gz");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
                NiFiProperties.FLOW_CONFIGURATION_FILE, flowConfigFile.toString(),
                NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 secs",
                NiFiProperties.WEB_HTTPS_HOST, "localhost",
                NiFiProperties.WEB_HTTPS_PORT, "8443",
                NiFiProperties.CLUSTER_NODE_ADDRESS, "localhost",
                NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "9090",
                NiFiProperties.LOAD_BALANCE_HOST, "localhost",
                NiFiProperties.LOAD_BALANCE_PORT, "6342",
                NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"
        ));

        flowService = StandardFlowService.createClusteredInstance(controller, nifiProperties, senderListener,
                clusterCoordinator, revisionManager, narManager, parameterContextAssetSynchronizer,
                connectorAssetSynchronizer, authorizer);
    }

    @Test
    public void testDisconnectionRequestWithMatchingGeneration() {
        assertEquals(0, flowService.getConnectionGeneration());

        final DisconnectMessage disconnectMessage = createDisconnectMessage("Test disconnect");
        flowService.handleDisconnectionRequest(disconnectMessage, 0);

        verify(controller).setConnectionStatus(any(NodeConnectionStatus.class));
        verify(controller).setClustered(false, null);
        verify(clusterCoordinator).setConnected(false);
    }

    @Test
    public void testDisconnectionRequestWithStaleGeneration() {
        assertEquals(0, flowService.getConnectionGeneration());

        final DisconnectMessage disconnectMessage = createDisconnectMessage("Test disconnect");
        flowService.handleDisconnectionRequest(disconnectMessage, 0);

        verify(controller).setClustered(false, null);

        reset(controller, clusterCoordinator);

        final DisconnectMessage staleDisconnectMessage = createDisconnectMessage("Stale disconnect from before reconnect");
        flowService.handleDisconnectionRequest(staleDisconnectMessage, 5);

        verify(controller, never()).setClustered(false, null);
        verify(clusterCoordinator, never()).setConnected(false);
    }

    @Test
    public void testNodeConnectionStateConnectingBeforeFlowSynchronization() throws Exception {
        final FlowController clusteredController = mock(FlowController.class);
        final StateManagerProvider stateManagerProvider = controller.getStateManagerProvider();
        final ExtensionManager extensionManager = controller.getExtensionManager();
        final NarManager narManager = mock(NarManager.class);
        when(clusteredController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(clusteredController.getExtensionManager()).thenReturn(extensionManager);

        final StandardFlowService clusteredFlowService = spy(StandardFlowService.createClusteredInstance(clusteredController, nifiProperties,
                mock(NodeProtocolSenderListener.class), clusterCoordinator, mock(RevisionManager.class), narManager,
                mock(AssetSynchronizer.class), mock(AssetSynchronizer.class), mock(Authorizer.class)));
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        final NodeConnectionStatus connectingStatus = new NodeConnectionStatus(nodeIdentifier, NodeConnectionState.CONNECTING);
        final ConnectionResponse response = new ConnectionResponse(nodeIdentifier,
                new StandardDataFlow(new byte[0], new byte[0], new byte[0], Collections.emptySet()), "instance-1",
                List.of(connectingStatus), mock(ComponentRevisionSnapshot.class));
        final ArgumentCaptor<NodeConnectionStatus> statusCaptor = ArgumentCaptor.forClass(NodeConnectionStatus.class);
        doAnswer(invocation -> {
            verify(clusteredController, never()).setClustered(eq(true), any());
            throw new FlowSynchronizationException("Stop after observing the flow synchronization boundary");
        }).when(clusteredFlowService).loadFromBytes(any(), eq(true), eq(BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST));

        assertThrows(FlowSynchronizationException.class, () -> clusteredFlowService.loadFromConnectionResponse(response));

        final InOrder inOrder = inOrder(clusterCoordinator, clusteredController, narManager, clusteredFlowService);
        inOrder.verify(clusterCoordinator).resetNodeStatuses(any(Map.class));
        inOrder.verify(clusteredController).setNodeId(nodeIdentifier);
        inOrder.verify(clusteredController).setConnectionStatus(statusCaptor.capture());
        inOrder.verify(narManager).syncWithClusterCoordinator();
        inOrder.verify(clusteredFlowService).loadFromBytes(any(), eq(true), eq(BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST));
        assertEquals(nodeIdentifier, statusCaptor.getValue().getNodeIdentifier());
        assertEquals(NodeConnectionState.CONNECTING, statusCaptor.getValue().getState());
        verify(clusteredController, never()).setClustered(anyBoolean(), any());
    }

    private DisconnectMessage createDisconnectMessage(final String explanation) {
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        final DisconnectMessage message = new DisconnectMessage();
        message.setNodeId(nodeIdentifier);
        message.setExplanation(explanation);
        return message;
    }

    private NodeIdentifier createNodeIdentifier() {
        return new NodeIdentifier("node-1", "localhost", 8443, "localhost", 9090, "localhost", 6342, 10443, false);
    }

}
