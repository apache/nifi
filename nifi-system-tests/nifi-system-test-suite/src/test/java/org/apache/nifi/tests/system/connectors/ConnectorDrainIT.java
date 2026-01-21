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

public class ConnectorDrainIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorDrainIT.class);

    @Test
    public void testDrainFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        final File gateFile = new File(getNiFiInstance().getInstanceDirectory(), "gate-file.txt");
        gateFile.deleteOnExit();

        if (gateFile.exists()) {
            assertTrue(gateFile.delete());
        }

        logger.info("Creating GatedDataQueuingConnector");
        final ConnectorEntity connector = getClientUtil().createConnector("GatedDataQueuingConnector");
        assertNotNull(connector);
        final String connectorId = connector.getId();

        logger.info("Configuring connector {} with gate file path: {}", connectorId, gateFile.getAbsolutePath());
        getClientUtil().configureConnector(connector, "Gate Configuration", Map.of("Gate File Path", gateFile.getAbsolutePath()));
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

        logger.info("Waiting for connector to enter DRAINING state");
        getClientUtil().waitForConnectorDraining(connectorId);

        logger.info("Sleeping for 2 seconds to verify connector remains in DRAINING state");
        Thread.sleep(2000L);

        ConnectorEntity drainingConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Connector state after 2 seconds: {}", drainingConnector.getComponent().getState());
        assertEquals(ConnectorState.DRAINING.name(), drainingConnector.getComponent().getState());

        final int queuedWhileDraining = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Queued FlowFile count while draining (gate file absent): {}", queuedWhileDraining);
        assertTrue(queuedWhileDraining > 0);

        logger.info("Creating gate file to allow draining to complete: {}", gateFile.getAbsolutePath());
        assertTrue(gateFile.createNewFile());

        logger.info("Waiting for connector to complete draining and return to STOPPED state");
        waitFor(() -> {
            try {
                final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
                return ConnectorState.STOPPED.name().equals(entity.getComponent().getState());
            } catch (final Exception e) {
                return false;
            }
        });

        final ConnectorEntity finalConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Final connector state: {}", finalConnector.getComponent().getState());
        assertEquals(ConnectorState.STOPPED.name(), finalConnector.getComponent().getState());

        final int finalQueuedCount = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Final queued FlowFile count: {}", finalQueuedCount);
        assertEquals(0, finalQueuedCount);

        logger.info("testDrainFlowFiles completed successfully");
    }

    @Test
    public void testCancelDrainFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        final File gateFile = new File(getNiFiInstance().getInstanceDirectory(), "gate-file-cancel.txt");
        gateFile.deleteOnExit();

        if (gateFile.exists()) {
            assertTrue(gateFile.delete());
        }

        logger.info("Creating GatedDataQueuingConnector");
        final ConnectorEntity connector = getClientUtil().createConnector("GatedDataQueuingConnector");
        assertNotNull(connector);
        final String connectorId = connector.getId();

        logger.info("Configuring connector {} with gate file path: {}", connectorId, gateFile.getAbsolutePath());
        getClientUtil().configureConnector(connector, "Gate Configuration", Map.of("Gate File Path", gateFile.getAbsolutePath()));
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

        logger.info("Waiting for connector to enter DRAINING state");
        getClientUtil().waitForConnectorDraining(connectorId);

        logger.info("Sleeping for 2 seconds to verify connector remains in DRAINING state");
        Thread.sleep(2000L);

        ConnectorEntity drainingConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Connector state after 2 seconds: {}", drainingConnector.getComponent().getState());
        assertEquals(ConnectorState.DRAINING.name(), drainingConnector.getComponent().getState());

        final int queuedCountWhileDraining = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Queued FlowFile count while draining: {}", queuedCountWhileDraining);
        assertTrue(queuedCountWhileDraining > 0);

        logger.info("Canceling drain for connector {}", connectorId);
        getClientUtil().cancelDrain(connectorId);

        logger.info("Waiting for connector to return to STOPPED state after cancel");
        getClientUtil().waitForConnectorStopped(connectorId);

        final ConnectorEntity finalConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        logger.info("Final connector state: {}", finalConnector.getComponent().getState());
        assertEquals(ConnectorState.STOPPED.name(), finalConnector.getComponent().getState());

        final int queuedCountAfterCancel = getConnectorQueuedFlowFileCount(connectorId);
        logger.info("Queued FlowFile count after cancel (should still have data): {}", queuedCountAfterCancel);
        assertTrue(queuedCountAfterCancel > 0);

        logger.info("testCancelDrainFlowFiles completed successfully");
    }
}
