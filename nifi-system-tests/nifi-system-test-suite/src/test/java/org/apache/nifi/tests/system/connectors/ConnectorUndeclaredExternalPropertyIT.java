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
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that saving a connector drops properties and steps that the current connector does not declare,
 * even when the external configuration provider loaded them.
 */
public class ConnectorUndeclaredExternalPropertyIT extends NiFiSystemIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PROVIDER_CLASS = "org.apache.nifi.connectors.tests.system.UndeclaredPropertyConnectorConfigurationProvider";
    private static final File STATE_DIRECTORY = new File("target/undeclared-external-property").getAbsoluteFile();
    private static final String STEP_NAME = "Ignored Step";
    private static final String DECLARED_PROPERTY_NAME = "Ignored Property";
    private static final String LEGACY_PROPERTY_NAME = "Legacy Ignored Property";
    private static final String DECLARED_PROPERTY_VALUE = "from-external-config";
    private static final String UPDATED_PROPERTY_VALUE = "saved-by-test";
    private static final String RELOADED_PROPERTY_VALUE = "saved-after-reload";
    private static final String UNDECLARED_PROPERTY_NAME = "Removed Property";
    private static final String REQUIRED_DEFAULT_PROPERTY_NAME = "Required Default Property";
    private static final String REQUIRED_DEFAULT_PROPERTY_VALUE = "default-value";

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
    public void testReadMigratesInMemoryAndExplicitSavePersistsMigratedConfiguration() throws NiFiClientException, IOException {
        final JsonNode initialConfiguration = readConfiguration("initial-config.json");
        assertEquals(DECLARED_PROPERTY_VALUE, propertyValue(initialConfiguration, LEGACY_PROPERTY_NAME));
        assertFalse(containsProperty(initialConfiguration, DECLARED_PROPERTY_NAME));
        assertEquals("should-be-dropped", propertyValue(initialConfiguration, UNDECLARED_PROPERTY_NAME));

        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        assertNotNull(connector);

        final ConnectorEntity beforeSave = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(DECLARED_PROPERTY_VALUE, workingPropertyValue(beforeSave, DECLARED_PROPERTY_NAME));
        assertEquals(REQUIRED_DEFAULT_PROPERTY_VALUE, workingPropertyValue(beforeSave, REQUIRED_DEFAULT_PROPERTY_NAME));
        final ConnectorEntity repeatedRead = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(DECLARED_PROPERTY_VALUE, workingPropertyValue(repeatedRead, DECLARED_PROPERTY_NAME));
        assertEquals(REQUIRED_DEFAULT_PROPERTY_VALUE, workingPropertyValue(repeatedRead, REQUIRED_DEFAULT_PROPERTY_NAME));
        assertFalse(new File(STATE_DIRECTORY, "saved-config.json").exists());

        getClientUtil().configureConnector(connector, STEP_NAME, Map.of(DECLARED_PROPERTY_NAME, UPDATED_PROPERTY_VALUE));

        final JsonNode savedConfiguration = readConfiguration("saved-config.json");
        assertEquals(UPDATED_PROPERTY_VALUE, propertyValue(savedConfiguration, DECLARED_PROPERTY_NAME));
        assertFalse(containsProperty(savedConfiguration, LEGACY_PROPERTY_NAME));
        assertFalse(containsProperty(savedConfiguration, UNDECLARED_PROPERTY_NAME));
        assertFalse(containsProperty(savedConfiguration, REQUIRED_DEFAULT_PROPERTY_NAME));

        final ConnectorEntity afterSave = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(UPDATED_PROPERTY_VALUE, workingPropertyValue(afterSave, DECLARED_PROPERTY_NAME));
        assertEquals(REQUIRED_DEFAULT_PROPERTY_VALUE, workingPropertyValue(afterSave, REQUIRED_DEFAULT_PROPERTY_NAME));

        getClientUtil().configureConnector(afterSave, STEP_NAME, Map.of(DECLARED_PROPERTY_NAME, RELOADED_PROPERTY_VALUE));

        final JsonNode reloadedConfiguration = readConfiguration("saved-config.json");
        assertEquals(RELOADED_PROPERTY_VALUE, propertyValue(reloadedConfiguration, DECLARED_PROPERTY_NAME));
        assertFalse(containsProperty(reloadedConfiguration, LEGACY_PROPERTY_NAME));
        assertFalse(containsProperty(reloadedConfiguration, UNDECLARED_PROPERTY_NAME));
        assertFalse(containsProperty(reloadedConfiguration, REQUIRED_DEFAULT_PROPERTY_NAME));
    }

    private JsonNode readConfiguration(final String fileName) throws IOException {
        return OBJECT_MAPPER.readTree(new File(STATE_DIRECTORY, fileName));
    }

    private String workingPropertyValue(final ConnectorEntity connector, final String propertyName) {
        final ConnectorConfigurationDTO workingConfiguration = connector.getComponent().getWorkingConfiguration();
        final Map<String, ConnectorValueReferenceDTO> propertyValues = workingConfiguration.getConfigurationStepConfigurations().getFirst()
            .getPropertyGroupConfigurations().getFirst()
            .getPropertyValues();
        return propertyValues.get(propertyName).getValue();
    }

    private String propertyValue(final JsonNode configuration, final String propertyName) {
        final JsonNode property = findProperty(configuration, propertyName);
        assertNotNull(property);
        return property.get("value").asText();
    }

    private boolean containsProperty(final JsonNode configuration, final String propertyName) {
        return findProperty(configuration, propertyName) != null;
    }

    private JsonNode findProperty(final JsonNode configuration, final String propertyName) {
        final JsonNode steps = configuration.get("workingFlowConfiguration");
        if (steps == null) {
            return null;
        }

        for (final JsonNode step : steps) {
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
