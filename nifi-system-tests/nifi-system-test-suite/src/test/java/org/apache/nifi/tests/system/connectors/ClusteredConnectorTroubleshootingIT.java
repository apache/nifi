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

import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Clustered variant of {@link ConnectorTroubleshootingIT}. All Troubleshooting lifecycle tests defined on the parent
 * class are re-executed against a two-node NiFi cluster so that the cluster-replication and node-restart paths are
 * exercised in addition to the standalone paths.
 *
 * <p>This class also adds clustered-only tests that cannot be expressed in the standalone parent, namely scenarios
 * that exercise node disconnect / reconnect while a Connector is in Troubleshooting mode.</p>
 */
public class ClusteredConnectorTroubleshootingIT extends ConnectorTroubleshootingIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    /**
     * Exercises the path where {@code syncConnector} runs against a node whose in-memory Connector is already in
     * Troubleshooting mode. Disconnecting and then reconnecting a node leaves the in-memory Connector in place and
     * triggers a sync that invokes {@code connector.setName(...)} and configuration handling on a Connector that is
     * already TROUBLESHOOTING.
     */
    @Test
    public void testSyncAgainstExistingTroubleshootingConnectorOnReconnect() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();
        final String originalName = connector.getComponent().getName();

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        disconnectNode(2);
        reconnectNode(2);
        waitForAllNodesConnected();

        switchClientToNode(2);
        try {
            final ConnectorEntity node2Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId);
            assertEquals(ConnectorState.TROUBLESHOOTING.name(), node2Connector.getComponent().getState(),
                    "Connector on the reconnected node must remain in TROUBLESHOOTING after sync");
            assertEquals(originalName, node2Connector.getComponent().getName(),
                    "Connector name must survive the sync that runs against the reconnected node's existing TROUBLESHOOTING connector");
        } finally {
            switchClientToNode(1);
        }

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
    }

    /**
     * Verifies that a node refuses to rejoin the cluster when it holds a RUNNING Connector in Troubleshooting locally
     * while the cluster's authoritative state for that Connector is not Troubleshooting.
     *
     * <p>The sequence is:</p>
     * <ol>
     *   <li>Start the Connector so the cluster's authoritative state for it is RUNNING.</li>
     *   <li>Disconnect Node 2.</li>
     *   <li>Issue a node-local request against the disconnected Node 2 to enter Troubleshooting, so only Node 2 is in
     *       Troubleshooting while the connected coordinator (Node 1) keeps the Connector RUNNING.</li>
     *   <li>Reconnect Node 2.</li>
     * </ol>
     *
     * <p>Node 2 has the Connector in Troubleshooting locally but the cluster's authoritative state is not Troubleshooting,
     * so the troubleshooting-state mismatch makes the proposed cluster flow uninheritable. Node 2 returns to DISCONNECTED
     * and keeps its own local Troubleshooting flow rather than adopting the cluster's flow.</p>
     */
    @Test
    public void testNodeCannotRejoinWhenRunningConnectorWentTroubleshootingWhileDisconnected()
            throws NiFiClientException, IOException, InterruptedException {

        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);

        disconnectNode(2);
        enterTroubleshootingOnDisconnectedNode2(connectorId);

        reconnectNode(2);

        assertNode2FailsToRejoinAndKeepsLocalState(connectorId, ConnectorState.TROUBLESHOOTING);
    }

    /**
     * Companion to {@link #testNodeCannotRejoinWhenRunningConnectorWentTroubleshootingWhileDisconnected()} for a STOPPED
     * Connector. The cluster's authoritative state is STOPPED while Node 2 is locally in Troubleshooting. As with the
     * RUNNING case, the troubleshooting-state mismatch makes the cluster flow uninheritable, so Node 2 must refuse to
     * join and remain DISCONNECTED with its local Troubleshooting flow intact.
     */
    @Test
    public void testNodeCannotRejoinWhenStoppedConnectorWentTroubleshootingWhileDisconnected()
            throws NiFiClientException, IOException, InterruptedException {

        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        disconnectNode(2);
        enterTroubleshootingOnDisconnectedNode2(connectorId);

        reconnectNode(2);

        assertNode2FailsToRejoinAndKeepsLocalState(connectorId, ConnectorState.TROUBLESHOOTING);
    }

    /**
     * Companion to {@link #testNodeCannotRejoinWhenStoppedConnectorWentTroubleshootingWhileDisconnected()} for the
     * reverse mismatch: Node 2 is not in Troubleshooting locally while the cluster's authoritative state is
     * Troubleshooting.
     *
     * <p>The Connector is left STOPPED, then Node 2 is disconnected and the Connector is taken into Troubleshooting on the
     * remaining coordinator, so the cluster's authoritative state is Troubleshooting while Node 2 stays STOPPED locally.
     * When Node 2 reconnects, its local non-Troubleshooting state conflicts with the cluster's Troubleshooting state, so
     * the cluster flow is uninheritable and Node 2 must refuse to join, remaining DISCONNECTED with its local STOPPED flow
     * intact.</p>
     */
    @Test
    public void testNodeCannotRejoinWhenClusterEnteredTroubleshootingWhileNodeDisconnected()
            throws NiFiClientException, IOException, InterruptedException {

        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        disconnectNode(2);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        reconnectNode(2);

        assertNode2FailsToRejoinAndKeepsLocalState(connectorId, ConnectorState.STOPPED);
    }

    private void assertNode2FailsToRejoinAndKeepsLocalState(final String connectorId, final ConnectorState expectedNode2State)
            throws InterruptedException, NiFiClientException, IOException {
        // The reconnect must be rejected because Node 2's local Troubleshooting state does not match the cluster, so Node 2
        // returns to DISCONNECTED rather than reaching CONNECTED.
        waitForNodeState(2, NodeConnectionState.DISCONNECTED);

        // Only the coordinator remains connected.
        assertEquals(1, getNifiClient().getFlowClient().getClusterSummary().getClusterSummary().getConnectedNodeCount().intValue());

        // Node 2 kept its own local flow rather than adopting the cluster's conflicting flow.
        waitForNode2ConnectorState(connectorId, expectedNode2State);
    }

    private void enterTroubleshootingOnDisconnectedNode2(final String connectorId) throws NiFiClientException, IOException, InterruptedException {
        switchClientToNode(2);
        try {
            final ConnectorEntity node2Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId);
            node2Connector.setDisconnectedNodeAcknowledged(true);
            getNifiClient().getConnectorClient(DO_NOT_REPLICATE).enterTroubleshooting(node2Connector);

            waitFor(() -> ConnectorState.TROUBLESHOOTING.name().equals(
                    getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId).getComponent().getState()));
        } finally {
            switchClientToNode(1);
        }
    }

    private void waitForNode2ConnectorState(final String connectorId, final ConnectorState expected) throws InterruptedException {
        switchClientToNode(2);
        try {
            waitFor(() -> expected.name().equals(
                    getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId).getComponent().getState()));
        } finally {
            switchClientToNode(1);
        }
    }

    private void assertConnectorState(final String connectorId, final ConnectorState expected) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(expected.name(), entity.getComponent().getState());
    }
}
