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
import org.apache.nifi.components.connector.ConnectorConfigurationProviderInitializationContext;
import org.apache.nifi.components.connector.ConnectorSyncDirective;
import org.apache.nifi.components.connector.ConnectorWorkingConfiguration;
import org.apache.nifi.flow.VersionedConnectorState;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * File-backed provider used by upgrade-drain system tests. {@code load} and {@code getSyncDirective} return the live
 * configuration when present and the committed configuration otherwise. {@code save} writes live configuration and
 * {@code discard} removes it.
 */
public class FileConnectorConfigurationProvider implements ConnectorConfigurationProvider {

    public static final String STATE_DIRECTORY_PROPERTY = "state.directory";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Path stateDirectory;

    @Override
    public synchronized void initialize(final ConnectorConfigurationProviderInitializationContext context) {
        final String configuredDirectory = context.getProperties().get(STATE_DIRECTORY_PROPERTY);
        if (configuredDirectory == null || configuredDirectory.isBlank()) {
            throw new IllegalStateException("FileConnectorConfigurationProvider requires " + STATE_DIRECTORY_PROPERTY);
        }
        stateDirectory = Path.of(configuredDirectory);
        try {
            Files.createDirectories(stateDirectory);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create provider state directory " + stateDirectory, e);
        }
    }

    @Override
    public synchronized Optional<ConnectorWorkingConfiguration> load(final String connectorId) {
        return readCurrent(connectorId);
    }

    @Override
    public synchronized void save(final String connectorId, final ConnectorWorkingConfiguration configuration) {
        writeAtomically(liveConfiguration(connectorId), configuration);
    }

    @Override
    public synchronized void discard(final String connectorId) {
        try {
            Files.deleteIfExists(liveConfiguration(connectorId));
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to discard live configuration for " + connectorId, e);
        }
    }

    @Override
    public synchronized void delete(final String connectorId) {
        try {
            Files.deleteIfExists(liveConfiguration(connectorId));
            Files.deleteIfExists(committedConfiguration(connectorId));
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to delete configuration for " + connectorId, e);
        }
    }

    @Override
    public void verifyCreate(final String connectorId) {
    }

    @Override
    public synchronized ConnectorSyncDirective getSyncDirective(final String connectorId, final VersionedConnectorState proposedScheduledState) {
        return readCurrent(connectorId)
            .map(ConnectorSyncDirective::allow)
            .orElseGet(ConnectorSyncDirective::allow);
    }

    @Override
    public void storeAsset(final String connectorId, final String nifiUuid, final String assetName, final InputStream content) {
    }

    @Override
    public void deleteAsset(final String connectorId, final String nifiUuid) {
    }

    @Override
    public void syncAssets(final String connectorId) {
    }

    private Optional<ConnectorWorkingConfiguration> readCurrent(final String connectorId) {
        final Path live = liveConfiguration(connectorId);
        if (Files.exists(live)) {
            return Optional.of(read(live));
        }
        final Path committed = committedConfiguration(connectorId);
        if (Files.exists(committed)) {
            return Optional.of(read(committed));
        }
        return Optional.empty();
    }

    private ConnectorWorkingConfiguration read(final Path path) {
        try {
            return objectMapper.readValue(path.toFile(), ConnectorWorkingConfiguration.class);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to read provider configuration " + path, e);
        }
    }

    private void writeAtomically(final Path path, final ConnectorWorkingConfiguration configuration) {
        try {
            Files.createDirectories(path.getParent());
            final Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
            objectMapper.writeValue(temporary.toFile(), configuration);
            try {
                Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (final IOException atomicMoveFailure) {
                Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to write provider configuration " + path, e);
        }
    }

    private Path liveConfiguration(final String connectorId) {
        return connectorDirectory(connectorId).resolve("live-config.json");
    }

    private Path committedConfiguration(final String connectorId) {
        return connectorDirectory(connectorId).resolve("committed-config.json");
    }

    private Path connectorDirectory(final String connectorId) {
        return stateDirectory.resolve(connectorId);
    }
}
