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
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies a restart onto a new Connector version when flow.json.gz holds no managed flow for the Connector, as a runtime that does not
 * persist managed flows saves it. The version 2 Connector supplies the flow that version 1 installed, so FlowFiles queued on the
 * connection that version 2 removes rejoin that connection and are drained before the upgrade completes.
 */
public class ConnectorPreUpgradeFlowIT extends AbstractConnectorUpgradeDrainIT {

    @Override
    protected String createOutgoingConnector(final Map<String, String> properties) throws Exception {
        return createDrainConnector(DRAIN_V1, properties);
    }

    @Override
    protected ExpectedBundle outgoingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V1);
    }

    @Override
    protected ExpectedBundle incomingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V2);
    }

    @Test
    @Timeout(300)
    public void testRestartWithoutManagedFlowDrainsPreUpgradeQueue() throws Exception {
        uploadDrainNars(DRAIN_V1, DRAIN_V2);
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final ConnectionEntity queuedConnection = findConnection(connectorId, REMOVED_CONNECTION);
        assertNotNull(queuedConnection);
        final int queued = getConnectionQueuedCount(connectorId, REMOVED_CONNECTION);
        assertTrue(queued > 0);
        waitForPersistedConnector(1, connectorId, connector -> connector.getScheduledState() == VersionedConnectorState.ENABLED);

        getNiFiInstance().stop();
        FlowDefinitionFiles.removeManagedFlow(getNiFiInstance(), connectorId);
        installOnlyDrainVersionTwo();
        getNiFiInstance().start(true);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());
        assertPending(connectorId);
        final ConnectionEntity restoredConnection = findConnection(connectorId, REMOVED_CONNECTION);
        assertNotNull(restoredConnection);
        assertEquals(queuedConnection.getId(), restoredConnection.getId());
        assertEquals(queued, getConnectionQueuedCount(connectorId, REMOVED_CONNECTION));

        requestStart(connectorId);
        waitUntilDraining(connectorId);
        releaseFlowFiles();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertNull(findProcessor(connectorId, "TerminateFlowFile"));
        waitUntilUpgradedFlowSaved(connectorId);
    }

    /**
     * Leaves the uploaded version 2 NAR as the only bundle that provides the drain Connector on the stopped node, so the Connector
     * saved by version 1 loads with version 2. The system test extensions NAR also packages version 1 of the Connector.
     */
    private void installOnlyDrainVersionTwo() throws IOException {
        final File instanceDirectory = nodeDirectory(1);
        final File installedVersionOne = new File(instanceDirectory, String.join(File.separator, "nar_repository", "installed", DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, DRAIN_V1));
        assertTrue(installedVersionOne.isDirectory(), installedVersionOne.getAbsolutePath());
        deleteRecursively(installedVersionOne);

        final File lib = new File(instanceDirectory, "lib");
        moveNars(lib, "nifi-system-test-extensions-nar-.*", new File(lib, "alternate-config"));
    }
}
