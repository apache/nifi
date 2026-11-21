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
 * Verifies that a required property with a default is populated when the external configuration provider omits it.
 */
public class ConnectorOmittedDefaultPropertyIT extends NiFiSystemIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PROVIDER_CLASS = "org.apache.nifi.connectors.tests.system.OmittingDefaultConnectorConfigurationProvider";
    private static final File STATE_DIRECTORY = new File("target/omitted-default-property").getAbsoluteFile();
    private static final String SUPPLIED_PROPERTY_NAME = "Source";
    private static final String SUPPLIED_PROPERTY_VALUE = "from-external-config";
    private static final String OMITTED_PROPERTY_NAME = "Greeting";
    private static final String OMITTED_PROPERTY_DEFAULT = "Hello";

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
    public void testOmittedRequiredDefaultIsPopulated() throws NiFiClientException, IOException {
        final JsonNode initialConfiguration = readConfiguration("initial-config.json");
        assertEquals(SUPPLIED_PROPERTY_VALUE, propertyValue(initialConfiguration, SUPPLIED_PROPERTY_NAME));
        assertFalse(containsProperty(initialConfiguration, OMITTED_PROPERTY_NAME));

        final ConnectorEntity connector = getClientUtil().createConnector("RequiredDefaultConnector");
        assertNotNull(connector);

        final ConnectorEntity loaded = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertEquals(SUPPLIED_PROPERTY_VALUE, workingPropertyValue(loaded, SUPPLIED_PROPERTY_NAME));
        assertEquals(OMITTED_PROPERTY_DEFAULT, workingPropertyValue(loaded, OMITTED_PROPERTY_NAME));
    }

    private JsonNode readConfiguration(final String fileName) throws IOException {
        return OBJECT_MAPPER.readTree(new File(STATE_DIRECTORY, fileName));
    }

    private String workingPropertyValue(final ConnectorEntity connector, final String propertyName) {
        final ConnectorConfigurationDTO workingConfiguration = connector.getComponent().getWorkingConfiguration();
        final Map<String, ConnectorValueReferenceDTO> propertyValues = workingConfiguration.getConfigurationStepConfigurations().getFirst()
            .getPropertyGroupConfigurations().getFirst()
            .getPropertyValues();
        final ConnectorValueReferenceDTO valueReference = propertyValues.get(propertyName);
        assertNotNull(valueReference);
        return valueReference.getValue();
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
