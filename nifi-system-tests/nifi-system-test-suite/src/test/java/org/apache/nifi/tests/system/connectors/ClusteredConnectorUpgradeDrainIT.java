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
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies clustered startup after a Connector NAR swap, with Connector configuration held by an external provider.
 */
public class ClusteredConnectorUpgradeDrainIT extends AbstractNarSwapUpgradeDrainIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String UPGRADE_STEP = "Upgrade";
    private static final String COMMENT_PROPERTY = "Comment";
    private static final String LEGACY_NOTE = "Legacy Note";
    private static final String DRAIN_LABEL = "Drain Label";
    private static final String PROVIDER_EDITED_COMMENT = "edited-in-provider";

    private final List<Path> providerStates = List.of(createProviderState(), createProviderState());

    /**
     * Each node gets its own provider state directory because the file-based provider writes through a fixed temporary file name.
     */
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                .instanceDirectory("target/node1")
                .overrideNifiProperties(providerProperties(providerStates.get(0)))
                .build(),
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                .instanceDirectory("target/node2")
                .overrideNifiProperties(providerProperties(providerStates.get(1)))
                .build());
    }

    /**
     * Swaps the Connector NARs with two Connectors. The running Connector queues FlowFiles on the connection from the processor
     * the upgrade removes to the terminator that stays, so it resumes draining until that connection is empty. The stopped
     * Connector stays pending with the configuration the provider supplied while NiFi was down.
     */
    @Test
    @Timeout(300)
    public void testNarSwapResumesRunningConnectorAndAppliesProviderConfiguration() throws Exception {
        final Map<String, String> runningProperties = properties("DRAIN", true, null);
        runningProperties.put(REMOVED_PROCESSOR_CHAIN, "true");
        final String runningConnectorId = createOutgoingConnector(runningProperties);
        holdFlowFiles();
        getClientUtil().startConnector(runningConnectorId);
        waitForConnectionMinQueueCount(runningConnectorId, REMOVED_CONNECTION, 1);

        final Map<String, String> providedProperties = properties("DRAIN", true, "shared");
        providedProperties.put(LEGACY_NOTE, "v1-only");
        final String providedConnectorId = createOutgoingConnector(providedProperties);

        getNiFiInstance().stop();
        editWorkingCommentInProvider(providedConnectorId);
        switchOutNars();
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().waitForConnectorState(runningConnectorId, ConnectorState.DRAINING);
        assertNotNull(findProcessor(runningConnectorId, RELAY_PROCESSOR));
        assertNotNull(findProcessor(runningConnectorId, REMOVED_PROCESSOR));
        assertNotNull(findProcessor(runningConnectorId, "TerminateFlowFile"));
        assertTrue(getConnectionQueuedCount(runningConnectorId, REMOVED_CONNECTION) > 0);

        assertConnectorState(providedConnectorId, ConnectorState.STOPPED);
        assertPending(providedConnectorId);
        final ConnectorEntity pending = getNifiClient().getConnectorClient().getConnector(providedConnectorId);
        assertEquals("shared", configurationValue(pending.getComponent().getActiveConfiguration(), COMMENT_PROPERTY));
        assertEquals(PROVIDER_EDITED_COMMENT, configurationValue(pending.getComponent().getWorkingConfiguration(), COMMENT_PROPERTY));
        assertNull(configurationValue(pending.getComponent().getActiveConfiguration(), LEGACY_NOTE));
        assertNull(configurationValue(pending.getComponent().getWorkingConfiguration(), LEGACY_NOTE));
        assertEquals("upgraded", configurationValue(pending.getComponent().getActiveConfiguration(), DRAIN_LABEL));
        assertEquals("upgraded", configurationValue(pending.getComponent().getWorkingConfiguration(), DRAIN_LABEL));

        releaseFlowFiles();
        getClientUtil().waitForConnectorState(runningConnectorId, ConnectorState.RUNNING);
        assertNull(findProcessor(runningConnectorId, RELAY_PROCESSOR));
        assertNull(findProcessor(runningConnectorId, REMOVED_PROCESSOR));
        assertNotNull(findProcessor(runningConnectorId, SWAPPED_PROCESSOR));
        assertNotNull(findProcessor(runningConnectorId, "TerminateFlowFile"));
        assertNull(findConnection(runningConnectorId, REMOVED_CONNECTION));

        requestStart(providedConnectorId);
        getClientUtil().waitForConnectorState(providedConnectorId, ConnectorState.RUNNING);
        assertEquals(PROVIDER_EDITED_COMMENT, findProcessor(providedConnectorId, "GenerateFlowFile").getComponent().getConfig().getComments());

        waitUntilUpgradedFlowSaved(runningConnectorId);
        waitUntilUpgradedFlowSaved(providedConnectorId);
    }

    /**
     * Simulates an out-of-band edit while NiFi is stopped: sets the Comment property of the Upgrade step in the working
     * configuration that each node's file-based provider stores. The active configuration is left unchanged.
     */
    private void editWorkingCommentInProvider(final String connectorId) throws IOException {
        for (final Path providerState : providerStates) {
            setWorkingCommentProperty(providerState.resolve(connectorId).resolve("live-config.json"));
        }
    }

    private void setWorkingCommentProperty(final Path liveConfiguration) throws IOException {
        final ObjectNode configuration = (ObjectNode) OBJECT_MAPPER.readTree(liveConfiguration.toFile());
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        assertNotNull(steps, "Provider configuration has no working flow configuration");
        for (final JsonNode step : steps) {
            if (!UPGRADE_STEP.equals(step.path("name").asText()) || !(step.path("properties").get(COMMENT_PROPERTY) instanceof final ObjectNode property)) {
                continue;
            }

            property.put("value", PROVIDER_EDITED_COMMENT);
            OBJECT_MAPPER.writeValue(liveConfiguration.toFile(), configuration);
            return;
        }

        throw new AssertionError("Provider working configuration has no " + COMMENT_PROPERTY + " property in the " + UPGRADE_STEP + " step");
    }

    private static String configurationValue(final ConnectorConfigurationDTO configuration, final String propertyName) {
        if (configuration == null || configuration.getConfigurationStepConfigurations() == null) {
            return null;
        }

        for (final ConfigurationStepConfigurationDTO step : configuration.getConfigurationStepConfigurations()) {
            if (!UPGRADE_STEP.equals(step.getConfigurationStepName()) || step.getPropertyGroupConfigurations() == null) {
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

    private static Map<String, String> providerProperties(final Path providerState) {
        return Map.of(
            NiFiProperties.CONNECTOR_CONFIGURATION_PROVIDER_IMPLEMENTATION, "org.apache.nifi.connectors.tests.system.FileConnectorConfigurationProvider",
            NiFiProperties.CONNECTOR_CONFIGURATION_PROVIDER_PROPERTIES_PREFIX + "state.directory", providerState.toAbsolutePath().toString());
    }

    private static Path createProviderState() {
        try {
            return Files.createTempDirectory("file-connector-provider");
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create the provider state directory", e);
        }
    }
}
