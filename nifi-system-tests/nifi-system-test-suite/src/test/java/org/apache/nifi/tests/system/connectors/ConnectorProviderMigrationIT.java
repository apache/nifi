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
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedStandaloneNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Loads a provider document that still uses the previous connector shape and verifies migration plus default handling.
 */
public class ConnectorProviderMigrationIT extends NiFiSystemIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PROVIDER_CLASS = "org.apache.nifi.connectors.tests.system.ProviderMigrationConnectorConfigurationProvider";
    private static final File STATE_DIRECTORY = new File("target/provider-migration").getAbsoluteFile();

    private static final String LEGACY_STEP_NAME = "Legacy Connection";
    private static final String CONNECTION_STEP_NAME = "Connection";
    private static final String LEGACY_HOST_NAME = "legacy-host";
    private static final String HOST_NAME = "Host";
    private static final String HOST_VALUE = "db.example.com";
    private static final String LEGACY_OBSOLETE_NAME = "legacy-obsolete";
    private static final String CLIENT_ID_NAME = "Client Id";
    private static final String CLIENT_ID_VALUE = "auto-migrated";
    private static final String POOL_SIZE_NAME = "Pool Size";
    private static final String POOL_SIZE_DEFAULT = "8";

    private static final String REMOVED_STEP_NAME = "Removed Step";
    private static final String REMOVED_PROPERTY_NAME = "Removed Property";

    private static final String ADDED_STEP_NAME = "Added Step";
    private static final String ADDED_PROPERTY_NAME = "Added Property";
    private static final String ADDED_PROPERTY_DEFAULT = "added-default";

    private static final String OPTIONAL_STEP_NAME = "Optional Step";
    private static final String OPTIONAL_PROPERTY_NAME = "Note";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        deleteRecursively(STATE_DIRECTORY);

        return new SpawnedStandaloneNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/default/bootstrap.conf")
                .instanceDirectory("target/standalone-instance")
                .overrideNifiProperties(Map.of(
                    "nifi.connector.configuration.provider.implementation", PROVIDER_CLASS,
                    "nifi.connector.configuration.provider.properties.state.directory", STATE_DIRECTORY.getAbsolutePath()
                ))
                .build());
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testProviderDocumentIsMigratedAndDefaultsAreApplied() throws NiFiClientException, IOException {
        final JsonNode initialConfiguration = readConfiguration("initial-config.json");
        assertEquals(HOST_VALUE, propertyValue(initialConfiguration, LEGACY_STEP_NAME, LEGACY_HOST_NAME));
        assertEquals("gone", propertyValue(initialConfiguration, LEGACY_STEP_NAME, LEGACY_OBSOLETE_NAME));
        assertEquals("drop-me", propertyValue(initialConfiguration, REMOVED_STEP_NAME, REMOVED_PROPERTY_NAME));
        assertFalse(containsStep(initialConfiguration, CONNECTION_STEP_NAME));
        assertFalse(containsStep(initialConfiguration, ADDED_STEP_NAME));
        assertFalse(containsStep(initialConfiguration, OPTIONAL_STEP_NAME));
        assertFalse(containsProperty(initialConfiguration, POOL_SIZE_NAME));
        assertFalse(containsProperty(initialConfiguration, CLIENT_ID_NAME));

        final ConnectorEntity connector = getClientUtil().createConnector("ProviderMigrationConnector");
        assertNotNull(connector);

        final ConnectorEntity loaded = getNifiClient().getConnectorClient().getConnector(connector.getId());
        final ConfigurationStepConfigurationDTO connection = findStep(loaded, CONNECTION_STEP_NAME);
        assertNotNull(connection);
        assertEquals(HOST_VALUE, propertyValue(connection, HOST_NAME));
        assertEquals(CLIENT_ID_VALUE, propertyValue(connection, CLIENT_ID_NAME));
        assertEquals(POOL_SIZE_DEFAULT, propertyValue(connection, POOL_SIZE_NAME));
        assertFalse(containsProperty(connection, LEGACY_HOST_NAME));
        assertFalse(containsProperty(connection, LEGACY_OBSOLETE_NAME));
        assertNull(findStep(loaded, LEGACY_STEP_NAME));

        final ConfigurationStepConfigurationDTO addedStep = findStep(loaded, ADDED_STEP_NAME);
        assertNotNull(addedStep);
        assertEquals(ADDED_PROPERTY_DEFAULT, propertyValue(addedStep, ADDED_PROPERTY_NAME));

        final ConfigurationStepConfigurationDTO optionalStep = findStep(loaded, OPTIONAL_STEP_NAME);
        assertNotNull(optionalStep);
        assertFalse(containsProperty(optionalStep, OPTIONAL_PROPERTY_NAME));

        final ConfigurationStepConfigurationDTO removedStep = findStep(loaded, REMOVED_STEP_NAME);
        assertNotNull(removedStep);
        assertFalse(containsProperty(removedStep, REMOVED_PROPERTY_NAME));

        // Loading the connector migrates the provider document in memory. The provider writes saved-config.json only from save(), so a read must leave that file absent.
        assertFalse(new File(STATE_DIRECTORY, "saved-config.json").exists());
    }

    private JsonNode readConfiguration(final String fileName) throws IOException {
        return OBJECT_MAPPER.readTree(new File(STATE_DIRECTORY, fileName));
    }

    private ConfigurationStepConfigurationDTO findStep(final ConnectorEntity connector, final String stepName) {
        final List<ConfigurationStepConfigurationDTO> steps = connector.getComponent().getWorkingConfiguration().getConfigurationStepConfigurations();
        if (steps == null) {
            return null;
        }

        for (final ConfigurationStepConfigurationDTO step : steps) {
            if (stepName.equals(step.getConfigurationStepName())) {
                return step;
            }
        }

        return null;
    }

    private String propertyValue(final ConfigurationStepConfigurationDTO step, final String propertyName) {
        final ConnectorValueReferenceDTO valueReference = propertyValues(step).get(propertyName);
        assertNotNull(valueReference);
        return valueReference.getValue();
    }

    private boolean containsProperty(final ConfigurationStepConfigurationDTO step, final String propertyName) {
        return propertyValues(step).containsKey(propertyName);
    }

    private Map<String, ConnectorValueReferenceDTO> propertyValues(final ConfigurationStepConfigurationDTO step) {
        final List<PropertyGroupConfigurationDTO> groups = step.getPropertyGroupConfigurations();
        assertNotNull(groups);
        assertFalse(groups.isEmpty());
        final Map<String, ConnectorValueReferenceDTO> propertyValues = groups.getFirst().getPropertyValues();
        assertNotNull(propertyValues);
        return propertyValues;
    }

    private String propertyValue(final JsonNode configuration, final String stepName, final String propertyName) {
        final JsonNode property = findProperty(configuration, stepName, propertyName);
        assertNotNull(property);
        return property.get("value").asText();
    }

    private boolean containsProperty(final JsonNode configuration, final String propertyName) {
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        if (steps == null) {
            return false;
        }

        for (final JsonNode step : steps) {
            final JsonNode properties = step.get("properties");
            if (properties != null && properties.has(propertyName)) {
                return true;
            }
        }

        return false;
    }

    private boolean containsStep(final JsonNode configuration, final String stepName) {
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        if (steps == null) {
            return false;
        }

        for (final JsonNode step : steps) {
            if (stepName.equals(step.path("name").asText())) {
                return true;
            }
        }

        return false;
    }

    private JsonNode findProperty(final JsonNode configuration, final String stepName, final String propertyName) {
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        if (steps == null) {
            return null;
        }

        for (final JsonNode step : steps) {
            if (!stepName.equals(step.path("name").asText())) {
                continue;
            }
            final JsonNode properties = step.get("properties");
            if (properties != null && properties.has(propertyName)) {
                return properties.get(propertyName);
            }
        }

        return null;
    }

    private void deleteRecursively(final File file) {
        if (!file.exists()) {
            return;
        }

        final File[] children = file.listFiles();
        if (children != null) {
            for (final File child : children) {
                deleteRecursively(child);
            }
        }

        if (!file.delete()) {
            throw new IllegalStateException("Unable to delete " + file.getAbsolutePath());
        }
    }
}
