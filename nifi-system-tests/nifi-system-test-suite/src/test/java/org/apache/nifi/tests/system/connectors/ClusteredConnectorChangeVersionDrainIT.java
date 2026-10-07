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
package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two-node version-change drain scenarios.
 */
public class ClusteredConnectorChangeVersionDrainIT extends AbstractChangeVersionDrainIT {

    private static final String LOADED_BUNDLE_MISMATCH_REASON = "Cluster nodes load different Connector versions: ";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    @Timeout(300)
    public void testBarrierWaitsForBothNodes() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        requestStart(connectorId);
        waitForNodeConnectorState(1, connectorId, ConnectorState.DRAINING);
        waitForNodeConnectorState(2, connectorId, ConnectorState.DRAINING);

        createGate(1);
        waitForNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.PHASE_ONE_COMPLETE);
        assertNodeRemainsBeforePhaseTwo(1, connectorId);
        assertNodeConnectorState(2, connectorId, ConnectorState.DRAINING);
        assertNodeConnectionPresent(2, connectorId);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.PHASE_ONE_PENDING);

        createGate(2);
        waitForNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.PHASE_ONE_COMPLETE);
        waitForNodeConnectorState(1, connectorId, ConnectorState.RUNNING);
        waitForNodeConnectorState(2, connectorId, ConnectorState.RUNNING);
        assertNodeConnectionRemoved(1, connectorId);
        assertNodeConnectionRemoved(2, connectorId);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.INACTIVE);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.INACTIVE);
    }

    @Test
    @Timeout(300)
    public void testNodeLocalFailureKeepsBothStoppedBeforePhaseTwo() throws Exception {
        final String connectorId = createOutgoingConnector("NO_OP", true, null);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        // Gate only node 1 so its generator stops while TerminateFlowFile, already running with the connector, empties
        // that node. Node 2 keeps generating. The status API aggregates every node, so read each node without replication.
        createGate(1);
        waitForNodeLocalQueueEmpty(1, connectorId);
        assertTrue(nodeLocalQueuedCount(2, connectorId) > 0);
        getClientUtil().stopConnector(connectorId);
        assertEquals(0, nodeLocalQueuedCount(1, connectorId));
        assertTrue(nodeLocalQueuedCount(2, connectorId) > 0);

        triggerUpgrade(connectorId);

        requestStart(connectorId);
        waitForNodeConnectorState(2, connectorId, ConnectorState.STOPPED);
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);
        assertNodeStoppedPendingWithOldFlow(1, connectorId);
        assertNodeStoppedPendingWithOldFlow(2, connectorId);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.FAILED);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.FAILED);
    }

    @Test
    @Timeout(300)
    public void testDisconnectedDrainingNodeBlocksPhaseTwoAndKeepsPendingFlowOnReconnect() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        requestStart(connectorId);
        waitForNodeConnectorState(1, connectorId, ConnectorState.DRAINING);
        waitForNodeConnectorState(2, connectorId, ConnectorState.DRAINING);

        disconnectNode(2);
        createGates();
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);
        assertNodeStoppedPendingWithOldFlow(1, connectorId);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.FAILED);

        reconnectNode(2);
        waitForAllNodesConnected();
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);
        waitForNodeConnectorState(2, connectorId, ConnectorState.STOPPED);
        assertNodeStoppedPendingWithOldFlow(1, connectorId);
        assertNodeStoppedPendingWithOldFlow(2, connectorId);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.FAILED);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.FAILED);
    }

    /**
     * A node that drops out partway through a version change keeps the outgoing version. The change is applied to node 1
     * alone to reproduce that state deterministically.
     */
    @Test
    @Timeout(300)
    public void testNodeDisconnectDuringChangeVersionPreventsStart() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);

        changeNodeVersion(1, connectorId, DRAIN_V2);
        assertEquals(DRAIN_V2, nodeBundleVersion(1, connectorId));
        assertEquals(DRAIN_V1, nodeBundleVersion(2, connectorId));

        final String startReason = actionReason(connectorId, "START");
        assertNotNull(startReason);
        assertTrue(startReason.startsWith(LOADED_BUNDLE_MISMATCH_REASON), startReason);
        createGates();
        final NiFiClientException failure = assertThrows(NiFiClientException.class, () -> requestStart(connectorId));
        assertTrue(failure.getMessage().contains(LOADED_BUNDLE_MISMATCH_REASON), failure.getMessage());
        assertNodeStoppedPendingWithOldFlow(1, connectorId);
        assertNodeConnectorState(2, connectorId, ConnectorState.STOPPED);
        assertNodeConnectionPresent(2, connectorId);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.INACTIVE);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.INACTIVE);

        changeNodeVersion(2, connectorId, DRAIN_V2);
        assertNodeStoppedPendingWithOldFlow(2, connectorId);

        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNodeRunningWithoutRemovedConnection(1, connectorId);
        assertNodeRunningWithoutRemovedConnection(2, connectorId);
    }

    @Test
    @Timeout(240)
    public void testApplyFailureRetriesApplyOnEveryNodeWithoutRerunningHook() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, "original");
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("NO_OP", false, "edited"));
        writeNodeFile(1, APPLY_FAIL_FILE, "");

        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertManagedFlowBundle(FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId), outgoingBundle());

        deleteNodeFiles(APPLY_FAIL_FILE);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.INACTIVE);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.INACTIVE);
        assertEquals("edited", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
        assertNotPending(connectorId);
    }

    private void changeNodeVersion(final int nodeIndex, final String connectorId, final String version) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            changeVersion(getNifiClient().getConnectorClient(DO_NOT_REPLICATE), connectorId, version, false);
        } finally {
            setupClient();
        }
    }

    private String nodeBundleVersion(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            return getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId).getComponent().getBundle().getVersion();
        } finally {
            setupClient();
        }
    }

    private void createGate(final int nodeIndex) throws IOException {
        Files.writeString(new File(nodeDirectory(nodeIndex), GATE_FILE).toPath(), "");
    }

    private int nodeLocalQueuedCount(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            final ProcessGroupStatusEntity status = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getStatus(connectorId, true);
            return status.getProcessGroupStatus().getAggregateSnapshot().getFlowFilesQueued();
        } finally {
            setupClient();
        }
    }

    private void waitForNodeLocalQueueEmpty(final int nodeIndex, final String connectorId) throws InterruptedException {
        switchClientToNode(nodeIndex);
        try {
            waitFor(() -> {
                try {
                    final ProcessGroupStatusEntity status = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getStatus(connectorId, true);
                    return status.getProcessGroupStatus().getAggregateSnapshot().getFlowFilesQueued() == 0;
                } catch (final Exception e) {
                    return false;
                }
            });
        } finally {
            setupClient();
        }
    }

    private void waitForNodeConnectorState(final int nodeIndex, final String connectorId, final ConnectorState expectedState) throws InterruptedException {
        switchClientToNode(nodeIndex);
        try {
            waitFor(() -> {
                try {
                    return expectedState.name().equals(
                        getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId).getComponent().getState());
                } catch (final Exception e) {
                    return false;
                }
            });
        } finally {
            setupClient();
        }
    }

    private void assertNodeConnectorState(final int nodeIndex, final String connectorId, final ConnectorState expectedState)
            throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            final ConnectorEntity connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId);
            assertEquals(expectedState.name(), connector.getComponent().getState());
        } finally {
            setupClient();
        }
    }

    private void assertNodeRemainsBeforePhaseTwo(final int nodeIndex, final String connectorId)
            throws NiFiClientException, IOException, InterruptedException {
        switchClientToNode(nodeIndex);
        try {
            final ConnectorClient connectorClient = getNifiClient().getConnectorClient(DO_NOT_REPLICATE);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                final ConnectorEntity connector = connectorClient.getConnector(connectorId);
                assertEquals(ConnectorState.DRAINING.name(), connector.getComponent().getState());
                assertEquals(FlowEvolutionStatus.PHASE_ONE_COMPLETE.name(), connector.getComponent().getFlowEvolutionStatus());
                Thread.sleep(100L);
            }

            assertNotNull(findNodeLocalConnection(connectorClient, connectorId));
        } finally {
            setupClient();
        }
    }

    private void assertNodeConnectionPresent(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            assertNotNull(findNodeLocalConnection(getNifiClient().getConnectorClient(DO_NOT_REPLICATE), connectorId));
        } finally {
            setupClient();
        }
    }

    private void assertNodeConnectionRemoved(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            assertNull(findNodeLocalConnection(getNifiClient().getConnectorClient(DO_NOT_REPLICATE), connectorId));
        } finally {
            setupClient();
        }
    }

    private void assertNodeRunningWithoutRemovedConnection(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            final ConnectorClient connectorClient = getNifiClient().getConnectorClient(DO_NOT_REPLICATE);
            assertEquals(ConnectorState.RUNNING.name(), connectorClient.getConnector(connectorId).getComponent().getState());
            assertNull(findNodeLocalConnection(connectorClient, connectorId));
        } finally {
            setupClient();
        }
    }

    private void assertNodeStoppedPendingWithOldFlow(final int nodeIndex, final String connectorId) throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            final ConnectorClient connectorClient = getNifiClient().getConnectorClient(DO_NOT_REPLICATE);
            final ConnectorEntity connector = connectorClient.getConnector(connectorId);
            assertEquals(ConnectorState.STOPPED.name(), connector.getComponent().getState());
            assertNotNull(findNodeLocalConnection(connectorClient, connectorId));
            connector.setDisconnectedNodeAcknowledged(true);
            final NiFiClientException failure = assertThrows(NiFiClientException.class, () -> connectorClient.applyUpdate(connector));
            assertTrue(failure.getMessage().contains("start the connector instead"), failure.getMessage());
        } finally {
            setupClient();
        }
    }

    private ConnectionEntity findNodeLocalConnection(final ConnectorClient connectorClient, final String connectorId)
            throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = connectorClient.getFlow(connectorId);
        if (flow.getProcessGroupFlow().getFlow().getConnections() == null) {
            return null;
        }

        for (final ConnectionEntity connection : flow.getProcessGroupFlow().getFlow().getConnections()) {
            if (REMOVED_CONNECTION.equals(connection.getComponent().getName())) {
                return connection;
            }
        }

        return null;
    }

    private void waitForNodeFlowEvolutionStatus(final int nodeIndex, final String connectorId, final FlowEvolutionStatus expectedStatus)
            throws InterruptedException {
        switchClientToNode(nodeIndex);
        try {
            waitFor(() -> {
                try {
                    return expectedStatus.name().equals(getNifiClient().getConnectorClient(DO_NOT_REPLICATE)
                        .getConnector(connectorId).getComponent().getFlowEvolutionStatus());
                } catch (final Exception e) {
                    return false;
                }
            });
        } finally {
            setupClient();
        }
    }

    private void assertNodeFlowEvolutionStatus(final int nodeIndex, final String connectorId, final FlowEvolutionStatus expectedStatus)
            throws NiFiClientException, IOException {
        switchClientToNode(nodeIndex);
        try {
            assertEquals(expectedStatus.name(), getNifiClient().getConnectorClient(DO_NOT_REPLICATE)
                .getConnector(connectorId).getComponent().getFlowEvolutionStatus());
        } finally {
            setupClient();
        }
    }
}
