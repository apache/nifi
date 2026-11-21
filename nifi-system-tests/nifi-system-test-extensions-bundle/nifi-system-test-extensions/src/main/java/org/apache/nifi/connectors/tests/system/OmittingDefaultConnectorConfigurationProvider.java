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
package org.apache.nifi.connectors.tests.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.connector.ConnectorConfigurationProvider;
import org.apache.nifi.components.connector.ConnectorConfigurationProviderException;
import org.apache.nifi.components.connector.ConnectorConfigurationProviderInitializationContext;
import org.apache.nifi.components.connector.ConnectorWorkingConfiguration;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration provider whose initial document includes a required property and omits a required property that has
 * a default. The initial document is written to disk so a system test can confirm the default was not supplied.
 */
public class OmittingDefaultConnectorConfigurationProvider implements ConnectorConfigurationProvider {
    public static final String STATE_DIRECTORY_PROPERTY = "state.directory";
    public static final String INITIAL_CONFIG_FILE_NAME = "initial-config.json";
    public static final String SAVED_CONFIG_FILE_NAME = "saved-config.json";

    static final String SUPPLIED_PROPERTY_VALUE = "from-external-config";

    private static final Logger logger = LoggerFactory.getLogger(OmittingDefaultConnectorConfigurationProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, ConnectorWorkingConfiguration> savedConfigurations = new ConcurrentHashMap<>();

    private File stateDirectory;

    @Override
    public void initialize(final ConnectorConfigurationProviderInitializationContext context) {
        final String stateDirectoryPath = context.getProperties().get(STATE_DIRECTORY_PROPERTY);
        if (stateDirectoryPath == null || stateDirectoryPath.isBlank()) {
            throw new ConnectorConfigurationProviderException("Connector configuration provider property [%s] is required".formatted(STATE_DIRECTORY_PROPERTY));
        }

        stateDirectory = new File(stateDirectoryPath);
        if (!stateDirectory.isDirectory() && !stateDirectory.mkdirs()) {
            throw new ConnectorConfigurationProviderException("Unable to create connector configuration provider state directory [%s]".formatted(stateDirectoryPath));
        }

        writeConfiguration(new File(stateDirectory, INITIAL_CONFIG_FILE_NAME), seedConfiguration());
    }

    @Override
    public Optional<ConnectorWorkingConfiguration> load(final String connectorId) {
        final ConnectorWorkingConfiguration savedConfiguration = savedConfigurations.get(connectorId);
        if (savedConfiguration != null) {
            return Optional.of(copy(savedConfiguration));
        }

        return Optional.of(seedConfiguration());
    }

    @Override
    public void save(final String connectorId, final ConnectorWorkingConfiguration configuration) {
        final ConnectorWorkingConfiguration savedConfiguration = copy(configuration);
        savedConfigurations.put(connectorId, savedConfiguration);
        writeConfiguration(new File(stateDirectory, SAVED_CONFIG_FILE_NAME), savedConfiguration);
    }

    @Override
    public void discard(final String connectorId) {
        logger.debug("Discard is a no-op for connector {}", connectorId);
    }

    @Override
    public void delete(final String connectorId) {
        savedConfigurations.remove(connectorId);
    }

    @Override
    public void verifyCreate(final String connectorId) {
        logger.debug("Verify create is a no-op for connector {}", connectorId);
    }

    @Override
    public void storeAsset(final String connectorId, final String nifiUuid, final String assetName, final InputStream content) {
        logger.debug("Store asset is a no-op for connector {} and asset {}", connectorId, nifiUuid);
    }

    @Override
    public void deleteAsset(final String connectorId, final String nifiUuid) {
        logger.debug("Delete asset is a no-op for connector {} and asset {}", connectorId, nifiUuid);
    }

    @Override
    public void syncAssets(final String connectorId) {
        logger.debug("Sync assets is a no-op for connector {}", connectorId);
    }

    private ConnectorWorkingConfiguration seedConfiguration() {
        final VersionedConnectorValueReference suppliedValue = new VersionedConnectorValueReference();
        suppliedValue.setValueType("STRING_LITERAL");
        suppliedValue.setValue(SUPPLIED_PROPERTY_VALUE);

        final Map<String, VersionedConnectorValueReference> properties = new HashMap<>();
        properties.put(RequiredDefaultConnector.SUPPLIED_PROPERTY_NAME, suppliedValue);

        final VersionedConfigurationStep step = new VersionedConfigurationStep();
        step.setName(RequiredDefaultConnector.STEP_NAME);
        step.setProperties(properties);

        final ConnectorWorkingConfiguration configuration = new ConnectorWorkingConfiguration();
        configuration.setName("RequiredDefaultConnector");
        configuration.setWorkingFlowConfiguration(new ArrayList<>(List.of(step)));
        return configuration;
    }

    private ConnectorWorkingConfiguration copy(final ConnectorWorkingConfiguration configuration) {
        try {
            return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(configuration), ConnectorWorkingConfiguration.class);
        } catch (final IOException e) {
            throw new ConnectorConfigurationProviderException("Unable to copy connector working configuration", e);
        }
    }

    private void writeConfiguration(final File destination, final ConnectorWorkingConfiguration configuration) {
        try {
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(destination, configuration);
        } catch (final IOException e) {
            throw new ConnectorConfigurationProviderException("Unable to write connector working configuration to [%s]".formatted(destination.getAbsolutePath()), e);
        }
    }
}
