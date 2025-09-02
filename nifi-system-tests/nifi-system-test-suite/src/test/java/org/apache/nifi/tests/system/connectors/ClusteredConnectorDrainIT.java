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
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cluster-specific tests for connector draining functionality.
 * These tests verify that draining works correctly when nodes complete draining at different times.
 */
public class ClusteredConnectorDrainIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredConnectorDrainIT.class);

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    /**
     * Tests that when draining in a cluster:
     * 1. Create gate file on Node 1 only - Node 1 finishes draining (STOPPED), Node 2 still DRAINING
     * 2. Aggregate state should be DRAINING (since at least one node is still draining)
     * 3. Create gate file on Node 2 - both nodes finish draining
     * 4. Aggregate state should be STOPPED
     */
    @Test
    public void testDrainWithNodeCompletingAtDifferentTimes() throws NiFiClientException, IOException, InterruptedException {
        final File node1InstanceDir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File node2InstanceDir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File node1GateFile = new File(node1InstanceDir, "gate-file.txt");
        final File node2GateFile = new File(node2InstanceDir, "gate-file.txt");
        node1GateFile.deleteOnExit();
        node2GateFile.deleteOnExit();
        deleteIfExists(node1GateFile);
        deleteIfExists(node2GateFile);

        logger.info("Creating GatedDataQueuingConnector");
        final ConnectorEntity connector = getClientUtil().createConnector("GatedDataQueuingConnector");
        assertNotNull(connector);
        final String connectorId = connector.getId();

        final String gateFilePath = "./gate-file.txt";
        logger.info("Configuring connector {} with gate file path: {}", connectorId, gateFilePath);
        getClientUtil().configureConnector(connector, "Gate Configuration", Map.of("Gate File Path", gateFilePath));
        getClientUtil().applyConnectorUpdate(connector);

        logger.info("Starting connector {}", connectorId);
        getClientUtil().startConnector(connectorId);

        logger.info("Waiting for at least 5 FlowFiles to queue");
        waitForConnectorMinQueueCount(connectorId, 5);

        logger.info("Stopping connector {}", connectorId);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        final int queuedCountBeforeDrain = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Queued FlowFile count before drain: {}", queuedCountBeforeDrain);
        assertTrue(queuedCountBeforeDrain > 0);

        logger.info("Initiating drain for connector {}", connectorId);
        getClientUtil().drainConnector(connectorId);

        logger.info("Waiting for aggregate connector state to be DRAINING");
        getClientUtil().waitForConnectorDraining(connectorId);

        logger.info("Creating gate file for Node 1 only: {}", node1GateFile.getAbsolutePath());
        assertTrue(node1GateFile.createNewFile());

        logger.info("Waiting for Node 1 to finish draining and become STOPPED");
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);
        waitForNodeConnectorState(2, connectorId, ConnectorState.DRAINING);

        logger.info("Verifying aggregate state is still DRAINING (Node 2 still draining)");
        final ConnectorEntity aggregateConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(ConnectorState.DRAINING.name(), aggregateConnector.getComponent().getState());

        logger.info("Creating gate file for Node 2: {}", node2GateFile.getAbsolutePath());
        assertTrue(node2GateFile.createNewFile());

        logger.info("Waiting for aggregate state to become STOPPED");
        waitFor(() -> {
            try {
                final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
                return ConnectorState.STOPPED.name().equals(entity.getComponent().getState());
            } catch (final Exception e) {
                return false;
            }
        });

        final ConnectorEntity finalConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Final aggregate connector state: {}", finalConnector.getComponent().getState());
        assertEquals(ConnectorState.STOPPED.name(), finalConnector.getComponent().getState());

        final int finalQueuedCount = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Final queued FlowFile count: {}", finalQueuedCount);
        assertEquals(0, finalQueuedCount);

        logger.info("testDrainWithNodeCompletingAtDifferentTimes completed successfully");
    }

    /**
     * Tests that when canceling drain in a cluster where one node has already finished:
     * 1. Create gate file on Node 1 only - Node 1 finishes draining (STOPPED), Node 2 still DRAINING
     * 2. Aggregate state should be DRAINING
     * 3. Cancel drain - Node 2 should stop draining
     * 4. Aggregate state should be STOPPED
     * 5. Data should still be queued (from Node 2)
     */
    @Test
    public void testCancelDrainWithOneNodeAlreadyComplete() throws NiFiClientException, IOException, InterruptedException {
        final File node1InstanceDir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File node2InstanceDir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File node1GateFile = new File(node1InstanceDir, "gate-file-cancel.txt");
        final File node2GateFile = new File(node2InstanceDir, "gate-file-cancel.txt");
        node1GateFile.deleteOnExit();
        node2GateFile.deleteOnExit();
        deleteIfExists(node1GateFile);
        deleteIfExists(node2GateFile);

        logger.info("Creating GatedDataQueuingConnector");
        final ConnectorEntity connector = getClientUtil().createConnector("GatedDataQueuingConnector");
        assertNotNull(connector);
        final String connectorId = connector.getId();

        final String gateFilePath = "./gate-file-cancel.txt";
        logger.info("Configuring connector {} with gate file path: {}", connectorId, gateFilePath);
        getClientUtil().configureConnector(connector, "Gate Configuration", Map.of("Gate File Path", gateFilePath));
        getClientUtil().applyConnectorUpdate(connector);

        logger.info("Starting connector {}", connectorId);
        getClientUtil().startConnector(connectorId);

        logger.info("Waiting for at least 5 FlowFiles to queue");
        waitForConnectorMinQueueCount(connectorId, 5);

        logger.info("Stopping connector {}", connectorId);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        final int queuedCountBeforeDrain = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Queued FlowFile count before drain: {}", queuedCountBeforeDrain);
        assertTrue(queuedCountBeforeDrain > 0);

        logger.info("Initiating drain for connector {}", connectorId);
        getClientUtil().drainConnector(connectorId);

        logger.info("Waiting for aggregate connector state to be DRAINING");
        getClientUtil().waitForConnectorDraining(connectorId);

        logger.info("Creating gate file for Node 1 only: {}", node1GateFile.getAbsolutePath());
        assertTrue(node1GateFile.createNewFile());

        logger.info("Waiting for Node 1 to finish draining and become STOPPED");
        waitForNodeConnectorState(1, connectorId, ConnectorState.STOPPED);

        logger.info("Verifying aggregate state is still DRAINING (Node 2 still draining)");
        final ConnectorEntity aggregateBeforeCancel = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(ConnectorState.DRAINING.name(), aggregateBeforeCancel.getComponent().getState());

        logger.info("Canceling drain for connector {}", connectorId);
        getClientUtil().cancelDrain(connectorId);

        logger.info("Waiting for aggregate state to become STOPPED after cancel");
        getClientUtil().waitForConnectorStopped(connectorId);

        final ConnectorEntity finalConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Final aggregate connector state: {}", finalConnector.getComponent().getState());
        assertEquals(ConnectorState.STOPPED.name(), finalConnector.getComponent().getState());

        final int finalQueuedCount = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Final queued FlowFile count after cancel (should still have data from Node 2): {}", finalQueuedCount);
        assertTrue(finalQueuedCount > 0);

        logger.info("testCancelDrainWithOneNodeAlreadyComplete completed successfully");
    }

    private void waitForNodeConnectorState(final int nodeIndex, final String connectorId, final ConnectorState expectedState) throws InterruptedException {
        logger.info("Waiting for Node {} connector {} to reach state {}", nodeIndex, connectorId, expectedState);
        waitFor(() -> {
            try {
                switchClientToNode(nodeIndex);
                final ConnectorEntity entity = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connectorId);
                return expectedState.name().equals(entity.getComponent().getState());
            } catch (final Exception e) {
                return false;
            }
        });
        logger.info("Node {} connector {} reached state {}", nodeIndex, connectorId, expectedState);
    }

    private void deleteIfExists(final File file) {
        if (file.exists()) {
            assertTrue(file.delete());
        }
    }
}
