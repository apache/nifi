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

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ClusteredConnectorIT extends ConnectorCrudIT {
    private static final Logger logger = LoggerFactory.getLogger(ClusteredConnectorIT.class);

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testModifiedWhenDisconnected() throws NiFiClientException, IOException, InterruptedException {
        // Create a new connector
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        assertNotNull(connector);
        logger.info("Created Connector with ID {}", connector.getId());

        // Configure the connector with an initial value and apply the update
        getClientUtil().configureConnector(connector, "Ignored Step", Map.of("Ignored Property", "Initial Value"));
        getClientUtil().applyConnectorUpdate(connector);
        logger.info("Configured Connector with initial value and applied the update");

        // Make requests to each node to ensure that the config has been applied
        switchClientToNode(1);
        ConnectorEntity node1Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connector.getId());
        assertActiveConfigurationValue(node1Connector, "Ignored Property", "Initial Value");
        assertWorkingConfigurationValue(node1Connector, "Ignored Property", "Initial Value");

        switchClientToNode(2);
        ConnectorEntity node2Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connector.getId());
        assertActiveConfigurationValue(node2Connector, "Ignored Property", "Initial Value");
        assertWorkingConfigurationValue(node2Connector, "Ignored Property", "Initial Value");
        logger.info("Validated initial configuration on both nodes");

        // Disconnect node 2
        switchClientToNode(1);
        disconnectNode(2);
        logger.info("Disconnected Node 2");

        // Make changes on node 1 and apply the change
        getClientUtil().configureConnector(connector.getId(), "Ignored Step", Map.of("Ignored Property", "Applied While Disconnected"));
        getNifiClient().getConnectorClient().applyUpdate(connector);
        logger.info("Applied configuration change on Node 1 while Node 2 is disconnected");

        // Configure the connector to yet another value on node 1 but do NOT apply the change
        getClientUtil().configureConnector(connector.getId(), "Ignored Step", Map.of("Ignored Property", "Working Only Value"));
        logger.info("Configured working configuration on Node 1 without applying the change");

        // Verify node 1 has the expected active and working configurations
        node1Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connector.getId());
        assertActiveConfigurationValue(node1Connector, "Ignored Property", "Applied While Disconnected");
        assertWorkingConfigurationValue(node1Connector, "Ignored Property", "Working Only Value");
        logger.info("Validated active and working configuration on Node 1");

        // Reconnect Node 2
        reconnectNode(2);
        waitForAllNodesConnected();
        logger.info("Reconnected Node 2");

        // Make requests to node 2 to ensure that it properly inherited both the working and active context configuration
        switchClientToNode(2);
        waitFor(() -> {
            try {
                final ConnectorEntity latestNode2Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connector.getId());
                final String activeValue = getConfigurationValue(latestNode2Connector.getComponent().getActiveConfiguration(), "Ignored Property");
                final String workingValue = getConfigurationValue(latestNode2Connector.getComponent().getWorkingConfiguration(), "Ignored Property");
                return "Applied While Disconnected".equals(activeValue) && "Working Only Value".equals(workingValue);
            } catch (final Exception e) {
                return false;
            }
        });
        logger.info("Validated that Node 2 has received updated configuration after reconnection");

        final ConnectorEntity finalNode2Connector = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getConnector(connector.getId());
        assertActiveConfigurationValue(finalNode2Connector, "Ignored Property", "Applied While Disconnected");
        assertWorkingConfigurationValue(finalNode2Connector, "Ignored Property", "Working Only Value");
        logger.info("Validated active and working configuration on Node 2");
    }

    private void assertActiveConfigurationValue(final ConnectorEntity connector, final String propertyName, final String expectedValue) {
        final String actualValue = getConfigurationValue(connector.getComponent().getActiveConfiguration(), propertyName);
        assertEquals(expectedValue, actualValue, "Active configuration property '" + propertyName + "' did not match expected value");
    }

    private void assertWorkingConfigurationValue(final ConnectorEntity connector, final String propertyName, final String expectedValue) {
        final String actualValue = getConfigurationValue(connector.getComponent().getWorkingConfiguration(), propertyName);
        assertEquals(expectedValue, actualValue, "Working configuration property '" + propertyName + "' did not match expected value");
    }

    private String getConfigurationValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
        final Map<String, ConnectorValueReferenceDTO> propertyValues = configuration.getConfigurationStepConfigurations().getFirst()
            .getPropertyGroupConfigurations().getFirst().getPropertyValues();
        final ConnectorValueReferenceDTO valueRef = propertyValues.get(propertyName);
        return valueRef == null ? null : valueRef.getValue();
    }
}
