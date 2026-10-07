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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies clustered startup after a Connector NAR swap. The pre-upgrade flow queues FlowFiles on the connection
 * from the processor the upgrade removes to the terminator that stays, so the connector stays draining until that
 * connection is empty. The processor before the removed one is replaced.
 */
public class ClusteredConnectorUpgradeDrainIT extends AbstractNarSwapUpgradeDrainIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    @Timeout(240)
    public void testRunningConnectorAutoResumes() throws Exception {
        final Map<String, String> properties = properties("DRAIN", true, null);
        properties.put(REMOVED_PROCESSOR_CHAIN, "true");
        final String connectorId = createOutgoingConnector(properties);
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectionMinQueueCount(connectorId, REMOVED_CONNECTION, 1);
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForConnectorState(connectorId, ConnectorState.DRAINING);
        assertNotNull(findProcessor(connectorId, RELAY_PROCESSOR));
        assertNotNull(findProcessor(connectorId, REMOVED_PROCESSOR));
        assertNotNull(findProcessor(connectorId, "TerminateFlowFile"));
        assertTrue(getConnectionQueuedCount(connectorId, REMOVED_CONNECTION) > 0);

        createGates();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findProcessor(connectorId, RELAY_PROCESSOR));
        assertNull(findProcessor(connectorId, REMOVED_PROCESSOR));
        assertNotNull(findProcessor(connectorId, SWAPPED_PROCESSOR));
        assertNotNull(findProcessor(connectorId, "TerminateFlowFile"));
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        assertNotPending(connectorId);
    }
}
