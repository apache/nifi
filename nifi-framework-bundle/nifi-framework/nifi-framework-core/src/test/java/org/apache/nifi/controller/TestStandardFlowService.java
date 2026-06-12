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
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.state.MockStateMap;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFlowService {

    private FlowController controller;
    private ClusterCoordinator clusterCoordinator;
    private StandardFlowService flowService;

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
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
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

    private DisconnectMessage createDisconnectMessage(final String explanation) {
        final NodeIdentifier nodeIdentifier = new NodeIdentifier("node-1", "localhost", 8443,
                "localhost", 9090, "localhost", 6342, 10443, false);
        final DisconnectMessage message = new DisconnectMessage();
        message.setNodeId(nodeIdentifier);
        message.setExplanation(explanation);
        return message;
    }
}
