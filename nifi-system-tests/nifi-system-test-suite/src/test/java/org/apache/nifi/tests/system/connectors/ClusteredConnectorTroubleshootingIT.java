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

    private void assertConnectorState(final String connectorId, final ConnectorState expected) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(expected.name(), entity.getComponent().getState());
    }
}
