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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two-node version-change drain scenarios. The REST API merges every node's view of a Connector, so per-node assertions
 * read each node directly without replication.
 */
public class ClusteredConnectorChangeVersionDrainIT extends AbstractChangeVersionDrainIT {

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

        requestStart(connectorId);
        assertNodeConnectorState(1, connectorId, ConnectorState.DRAINING);
        assertNodeConnectorState(2, connectorId, ConnectorState.DRAINING);

        releaseFlowFiles(1);
        waitForNode(1, connectorClient -> FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE.name().equals(connectorClient.getConnector(connectorId).getComponent().getFlowEvolutionStatus()));
        // Node 1 has drained but stays at the drain barrier while node 2 still holds FlowFiles.
        onNode(1, connectorClient -> {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                final ConnectorEntity connector = connectorClient.getConnector(connectorId);
                assertEquals(ConnectorState.DRAINING.name(), connector.getComponent().getState());
                assertEquals(FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE.name(), connector.getComponent().getFlowEvolutionStatus());
                Thread.sleep(100L);
            }

            return null;
        });
        assertNotNull(nodeConnection(1, connectorId));
        assertNodeConnectorState(2, connectorId, ConnectorState.DRAINING);
        assertNotNull(nodeConnection(2, connectorId));
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.EVOLVE_DRAIN_PENDING);

        releaseFlowFiles(2);
        waitForNodeConnectorState(1, connectorId, ConnectorState.RUNNING);
        waitForNodeConnectorState(2, connectorId, ConnectorState.RUNNING);
        assertNull(nodeConnection(1, connectorId));
        assertNull(nodeConnection(2, connectorId));
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.INACTIVE);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.INACTIVE);
    }

    @Test
    @Timeout(300)
    public void testNodeLocalFailureKeepsBothStoppedAtDrainBarrier() throws Exception {
        final String connectorId = createOutgoingConnector("NO_OP", true, null);
        holdFlowFiles();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        // Release only node 1 so its generator stops while TerminateFlowFile, already running with the connector, empties that node. Node 2 keeps generating.
        releaseFlowFiles(1);
        waitForNode(1, connectorClient -> localQueuedCount(connectorClient, connectorId) == 0);
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
        final String nodeTwoErrors = onNode(2, connectorClient -> connectorClient.getConnector(connectorId).getComponent().getValidationErrors().toString());
        assertTrue(nodeTwoErrors.contains("does not contain a match for"), nodeTwoErrors);
    }

    @Test
    @Timeout(300)
    public void testDisconnectedDrainingNodeBlocksDrainBarrierAndKeepsPendingFlowOnReconnect() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        requestStart(connectorId);
        waitForNodeConnectorState(1, connectorId, ConnectorState.DRAINING);
        waitForNodeConnectorState(2, connectorId, ConnectorState.DRAINING);

        disconnectNode(2);
        releaseFlowFiles();
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

        // Node 1 drained before its attempt failed at the barrier, but the upgraded flow was never applied, so a retry
        // prepares and drains again. A preparation failure on node 1 therefore fails the retry.
        injectFault(1, UpgradeDrainFault.PREPARATION_FAILS);
        requestStart(connectorId);
        waitForNode(1, connectorClient -> connectorClient.getConnector(connectorId).getComponent().getValidationErrors().toString().contains(PREPARATION_FAILED));
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);
        waitForNodeConnectorState(2, connectorId, ConnectorState.STOPPED);
        assertNodeStoppedPendingWithOldFlow(1, connectorId);
        assertNodeStoppedPendingWithOldFlow(2, connectorId);

        clearFault(UpgradeDrainFault.PREPARATION_FAILS);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        for (int nodeIndex = 1; nodeIndex <= 2; nodeIndex++) {
            assertNodeConnectorState(nodeIndex, connectorId, ConnectorState.RUNNING);
            assertNull(nodeConnection(nodeIndex, connectorId));
            assertNodeFlowEvolutionStatus(nodeIndex, connectorId, FlowEvolutionStatus.INACTIVE);
        }
    }

    @Test
    @Timeout(240)
    public void testApplyFailureRetriesApplyWithoutRepeatingEvolutionPreparation() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, "original");
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("NO_OP", false, "edited"));
        injectFault(1, UpgradeDrainFault.APPLY_FAILS);

        requestStart(connectorId);
        // The merged cluster state reports UPDATE_FAILED as soon as one node fails. A node that is still finishing its
        // failed evolution treats a new start request as part of that evolution, so wait for every node to fail first.
        waitForNodeConnectorState(1, connectorId, ConnectorState.UPDATE_FAILED);
        waitForNodeConnectorState(2, connectorId, ConnectorState.UPDATE_FAILED);
        assertManagedFlowBundle(FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId), outgoingBundle());

        // Node 2 already applied the upgraded flow, so the retry only applies it on node 1. Flow evolution preparation
        // already completed on both nodes, so the retry does not run it again.
        clearFault(UpgradeDrainFault.APPLY_FAILS);
        injectFault(2, UpgradeDrainFault.APPLY_FAILS);
        injectFault(UpgradeDrainFault.PREPARATION_FAILS);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNodeFlowEvolutionStatus(1, connectorId, FlowEvolutionStatus.INACTIVE);
        assertNodeFlowEvolutionStatus(2, connectorId, FlowEvolutionStatus.INACTIVE);
        assertEquals("edited", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
        stopAndAssertNotPending(connectorId);
    }

    private <T> T onNode(final int nodeIndex, final NodeCall<T> call) throws Exception {
        switchClientToNode(nodeIndex);
        try {
            return call.call(getNifiClient().getConnectorClient(DO_NOT_REPLICATE));
        } finally {
            setupClient();
        }
    }

    private void waitForNode(final int nodeIndex, final NodeCall<Boolean> condition) throws InterruptedException {
        switchClientToNode(nodeIndex);
        try {
            final ConnectorClient connectorClient = getNifiClient().getConnectorClient(DO_NOT_REPLICATE);
            waitFor(() -> condition.call(connectorClient));
        } finally {
            setupClient();
        }
    }

    private void waitForNodeConnectorState(final int nodeIndex, final String connectorId, final ConnectorState expectedState) throws InterruptedException {
        waitForNode(nodeIndex, connectorClient -> expectedState.name().equals(connectorClient.getConnector(connectorId).getComponent().getState()));
    }

    private void assertNodeConnectorState(final int nodeIndex, final String connectorId, final ConnectorState expectedState) throws Exception {
        assertEquals(expectedState.name(), onNode(nodeIndex, connectorClient -> connectorClient.getConnector(connectorId)).getComponent().getState());
    }

    private void assertNodeFlowEvolutionStatus(final int nodeIndex, final String connectorId, final FlowEvolutionStatus expectedStatus) throws Exception {
        assertEquals(expectedStatus.name(), onNode(nodeIndex, connectorClient -> connectorClient.getConnector(connectorId)).getComponent().getFlowEvolutionStatus());
    }

    private ConnectionEntity nodeConnection(final int nodeIndex, final String connectorId) throws Exception {
        return onNode(nodeIndex, connectorClient -> findConnection(connectorClient, connectorId, REMOVED_CONNECTION));
    }

    private int nodeLocalQueuedCount(final int nodeIndex, final String connectorId) throws Exception {
        return onNode(nodeIndex, connectorClient -> localQueuedCount(connectorClient, connectorId));
    }

    private static int localQueuedCount(final ConnectorClient connectorClient, final String connectorId) throws NiFiClientException, IOException {
        return connectorClient.getStatus(connectorId, true).getProcessGroupStatus().getAggregateSnapshot().getFlowFilesQueued();
    }

    private void assertNodeStoppedPendingWithOldFlow(final int nodeIndex, final String connectorId) throws Exception {
        onNode(nodeIndex, connectorClient -> {
            final ConnectorEntity connector = connectorClient.getConnector(connectorId);
            assertEquals(ConnectorState.STOPPED.name(), connector.getComponent().getState());
            assertNotNull(findConnection(connectorClient, connectorId, REMOVED_CONNECTION));
            connector.setDisconnectedNodeAcknowledged(true);
            final NiFiClientException failure = assertThrows(NiFiClientException.class, () -> connectorClient.applyUpdate(connector));
            assertTrue(failure.getMessage().contains("start the connector instead"), failure.getMessage());
            return null;
        });
    }

    @FunctionalInterface
    private interface NodeCall<T> {
        T call(ConnectorClient connectorClient) throws Exception;
    }
}
