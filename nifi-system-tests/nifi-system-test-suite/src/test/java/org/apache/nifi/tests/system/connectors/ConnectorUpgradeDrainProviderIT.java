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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A NAR-swap upgrade applies configuration supplied by the external provider.
 */
public class ConnectorUpgradeDrainProviderIT extends AbstractNarSwapUpgradeDrainIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String COMMENT = "Comment";
    private static final String LEGACY_NOTE = "Legacy Note";
    private static final String DRAIN_LABEL = "Drain Label";
    private static final String SUPPLIED_COMMENT = "supplied-by-provider";

    private final Path providerState = createProviderState();

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(NiFiProperties.CONNECTOR_CONFIGURATION_PROVIDER_IMPLEMENTATION, "org.apache.nifi.connectors.tests.system.FileConnectorConfigurationProvider", NiFiProperties
            .CONNECTOR_CONFIGURATION_PROVIDER_PROPERTIES_PREFIX + "state.directory", providerState.toAbsolutePath().toString());
    }

    @Test
    @Timeout(240)
    public void testProviderSuppliedConfigurationIsAppliedByUpgrade() throws Exception {
        final Map<String, String> initial = properties("DRAIN", true, "shared");
        initial.put(LEGACY_NOTE, "v1-only");
        final String connectorId = createConnector("UpgradeDrainConnector", initial);

        getNiFiInstance().stop();
        supplyComment(connectorId, SUPPLIED_COMMENT);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        final ConnectorEntity pending = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals("shared", configurationValue(pending.getComponent().getActiveConfiguration(), COMMENT));
        assertEquals(SUPPLIED_COMMENT, configurationValue(pending.getComponent().getWorkingConfiguration(), COMMENT));
        assertNull(configurationValue(pending.getComponent().getActiveConfiguration(), LEGACY_NOTE));
        assertNull(configurationValue(pending.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(pending.getComponent().getActiveConfiguration(), DRAIN_LABEL));
        assertEquals("upgraded", configurationValue(pending.getComponent().getWorkingConfiguration(), DRAIN_LABEL));

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertEquals(SUPPLIED_COMMENT, findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    private void supplyComment(final String connectorId, final String comment) throws IOException {
        final Path liveConfiguration = providerState.resolve(connectorId).resolve("live-config.json");
        final ObjectNode configuration = (ObjectNode) OBJECT_MAPPER.readTree(liveConfiguration.toFile());
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        assertNotNull(steps, "Provider configuration has no working flow configuration");
        boolean updated = false;
        for (final JsonNode step : steps) {
            final ObjectNode properties = step.get("properties") instanceof ObjectNode objectNode ? objectNode : null;
            final ObjectNode property = properties == null || !(properties.get(COMMENT) instanceof ObjectNode) ? null : (ObjectNode) properties.get(COMMENT);
            if (property != null) {
                property.put("value", comment);
                updated = true;
            }
        }
        if (!updated) {
            throw new AssertionError("Provider configuration has no " + COMMENT + " property");
        }
        OBJECT_MAPPER.writeValue(liveConfiguration.toFile(), configuration);
    }

    private static String configurationValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
        if (configuration == null || configuration.getConfigurationStepConfigurations() == null) {
            return null;
        }
        for (final ConfigurationStepConfigurationDTO step : configuration.getConfigurationStepConfigurations()) {
            if (!"Upgrade".equals(step.getConfigurationStepName()) || step.getPropertyGroupConfigurations() == null) {
                continue;
            }
            for (final PropertyGroupConfigurationDTO group : step.getPropertyGroupConfigurations()) {
                if (group.getPropertyValues() != null && group.getPropertyValues().containsKey(propertyName)) {
                    return group.getPropertyValues().get(propertyName).getValue();
                }
            }
        }
        return null;
    }

    private static Path createProviderState() {
        try {
            return Files.createTempDirectory("file-connector-provider");
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create the provider state directory", e);
        }
    }
}
