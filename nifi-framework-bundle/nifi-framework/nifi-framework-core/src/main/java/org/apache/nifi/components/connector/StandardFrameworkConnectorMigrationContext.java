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
package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class StandardFrameworkConnectorMigrationContext implements FrameworkConnectorMigrationContext {
    private static final Logger logger = LoggerFactory.getLogger(StandardFrameworkConnectorMigrationContext.class);

    /**
     * Tracks the phase the migration is currently in. The framework transitions the phase via
     * {@link #setPhase(Phase)} between calls to the two {@code MigratableConnector} methods so the context can
     * enforce phase-scoping on its write APIs and refuse any out-of-phase calls. Once a migration finishes (success
     * or failure), the phase is moved to {@link #COMPLETED} and every write call throws.
     */
    enum Phase {
        CONFIGURATION,
        STATE,
        COMPLETED
    }

    private final String connectorId;
    private final VersionedExternalFlow sourceFlow;
    private final boolean localMigration;
    private final FrameworkFlowContext activeFlowContext;
    private final FrameworkFlowContext migrationFlowContext;
    private final MutableConnectorConfigurationContext workingConfiguration;
    private final AssetManager sourceAssetManager;
    private final ConnectorRepository connectorRepository;
    private final StateManagerProvider stateManagerProvider;
    private final ClusterTopologyProvider clusterTopologyProvider;
    private final Set<String> copiedAssetIds = Collections.synchronizedSet(new java.util.LinkedHashSet<>());

    private final Object stagingLock = new Object();
    private final Map<String, VersionedComponentState> stagedComponentStates = new LinkedHashMap<>();
    private volatile Phase phase = Phase.CONFIGURATION;

    public StandardFrameworkConnectorMigrationContext(final String connectorId, final VersionedExternalFlow sourceFlow, final boolean localMigration,
            final FrameworkFlowContext activeFlowContext, final MutableConnectorConfigurationContext workingConfiguration, final AssetManager sourceAssetManager,
            final ConnectorRepository connectorRepository, final StateManagerProvider stateManagerProvider, final ClusterTopologyProvider clusterTopologyProvider) {
        this.connectorId = connectorId;
        this.sourceFlow = sourceFlow;
        this.localMigration = localMigration;
        this.activeFlowContext = activeFlowContext;
        // Hand the connector a wrapped active flow context so any updateFlow(...) attempt from either migration
        // phase fails immediately. The framework still has access to the unwrapped activeFlowContext for the
        // applyUpdate(...) it drives between the two phases.
        this.migrationFlowContext = activeFlowContext == null ? null : new MigrationFlowContext(activeFlowContext);
        // The connector's configuration writes during migrateConfiguration(...) are applied directly to this working
        // configuration, which is seeded by the framework with a clone of the connector's active configuration. The
        // working configuration always holds the fully-merged result, so there is no separate per-step delta to track
        // or replay. The framework reads the merged result back via getMergedConfiguration() and never mutates the
        // active configuration until both migration phases have succeeded.
        this.workingConfiguration = workingConfiguration;
        this.sourceAssetManager = sourceAssetManager;
        this.connectorRepository = connectorRepository;
        this.stateManagerProvider = stateManagerProvider;
        this.clusterTopologyProvider = clusterTopologyProvider;
    }

    @Override
    public VersionedExternalFlow getSourceFlow() {
        return sourceFlow;
    }

    @Override
    public boolean isLocalMigration() {
        return localMigration;
    }

    @Override
    public FrameworkFlowContext getActiveFlowContext() {
        return migrationFlowContext;
    }

    /**
     * Returns the unwrapped active flow context owned by the framework. The connector never sees this object; only
     * the migration manager calls it to feed the real {@code FrameworkFlowContext} into
     * {@link Connector#applyUpdate(org.apache.nifi.components.connector.components.FlowContext,
     *   org.apache.nifi.components.connector.components.FlowContext) Connector.applyUpdate(...)}.
     */
    FrameworkFlowContext getUnwrappedActiveFlowContext() {
        return activeFlowContext;
    }

    @Override
    public AssetReference copyAssetFromSource(final String sourceAssetId) {
        if (sourceAssetId == null || sourceAssetId.isBlank()) {
            throw new IllegalArgumentException("Source asset identifier must be specified.");
        }
        if (!localMigration) {
            throw new IllegalStateException("Source assets can only be copied for migrations from a local Versioned Process Group.");
        }

        final String copiedAssetId = UUID.nameUUIDFromBytes((connectorId + ":" + sourceAssetId).getBytes(StandardCharsets.UTF_8)).toString();
        final Optional<Asset> existingAsset = connectorRepository.getAsset(copiedAssetId);
        if (existingAsset.isPresent() && existingAsset.get().getFile().isFile() && existingAsset.get().getFile().length() > 0L) {
            copiedAssetIds.add(copiedAssetId);
            return new AssetReference(Set.of(copiedAssetId));
        }

        final Optional<Asset> sourceAsset = sourceAssetManager.getAsset(sourceAssetId);
        if (sourceAsset.isEmpty()) {
            logger.warn("Connector [{}] migration could not locate source asset [{}]; the asset will not be copied and the affected parameter "
                            + "will be left without an asset reference for the user to re-attach after migration.", connectorId, sourceAssetId);
            return new AssetReference(Set.of());
        }

        try (final InputStream sourceContents = new FileInputStream(sourceAsset.get().getFile())) {
            final Asset copiedAsset = connectorRepository.storeAsset(connectorId, copiedAssetId, sourceAsset.get().getName(), sourceContents);
            copiedAssetIds.add(copiedAsset.getIdentifier());
            return new AssetReference(Set.of(copiedAsset.getIdentifier()));
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to copy source asset with identifier " + sourceAssetId, e);
        }
    }

    @Override
    public void setProperties(final String stepName, final Map<String, String> propertyValues) {
        requireConfigurationPhase("setProperties");
        Objects.requireNonNull(stepName, "Configuration step name is required");
        Objects.requireNonNull(propertyValues, "Property values are required");

        // Merge the requested values onto the step as it currently stands on the working configuration. The working
        // configuration's setProperties(...) overlays the new values on top of the existing ones, so properties the
        // connector does not mention (including asset and secret references it inherited from its prior configuration)
        // are preserved.
        synchronized (stagingLock) {
            workingConfiguration.setProperties(stepName, toStepConfiguration(propertyValues));
        }
    }

    @Override
    public void replaceProperties(final String stepName, final Map<String, String> propertyValues) {
        requireConfigurationPhase("replaceProperties");
        Objects.requireNonNull(stepName, "Configuration step name is required");
        Objects.requireNonNull(propertyValues, "Property values are required");

        // Replace the entire step on the working configuration, dropping any property the connector does not include.
        synchronized (stagingLock) {
            workingConfiguration.replaceProperties(stepName, toStepConfiguration(propertyValues));
        }
    }

    private static StepConfiguration toStepConfiguration(final Map<String, String> propertyValues) {
        final Map<String, ConnectorValueReference> converted = new HashMap<>();
        for (final Map.Entry<String, String> entry : propertyValues.entrySet()) {
            converted.put(entry.getKey(), new StringLiteralValue(entry.getValue()));
        }
        return new StepConfiguration(converted);
    }

    @Override
    public void setComponentState(final String managedComponentId, final VersionedComponentState state) {
        requireStatePhase("setComponentState");
        if (managedComponentId == null || managedComponentId.isBlank()) {
            throw new IllegalArgumentException("Managed component identifier is required");
        }
        Objects.requireNonNull(state, "Versioned component state is required");

        synchronized (stagingLock) {
            stagedComponentStates.put(managedComponentId, state);
        }
    }

    @Override
    public AssetManager getSourceAssetManager() {
        return sourceAssetManager;
    }

    @Override
    public ConnectorRepository getConnectorRepository() {
        return connectorRepository;
    }

    @Override
    public StateManagerProvider getStateManagerProvider() {
        return stateManagerProvider;
    }

    @Override
    public ClusterTopologyProvider getClusterTopologyProvider() {
        return clusterTopologyProvider;
    }

    @Override
    public Set<String> getCopiedAssetIds() {
        synchronized (copiedAssetIds) {
            return Set.copyOf(copiedAssetIds);
        }
    }

    /**
     * Returns the current migration phase. Visible for use by {@link StandardConnectorMigrationManager} and unit
     * tests that exercise phase enforcement.
     */
    Phase getPhase() {
        return phase;
    }

    /**
     * Transitions the migration phase. Only {@link StandardConnectorMigrationManager} should call this. Each
     * transition moves the phase strictly forward (CONFIGURATION -> STATE -> COMPLETED, with CONFIGURATION -> COMPLETED
     * also allowed as the failure shortcut). Backward or self transitions are rejected so a connector cannot re-enter
     * an earlier write phase after the framework has moved on.
     */
    void setPhase(final Phase nextPhase) {
        Objects.requireNonNull(nextPhase, "Phase is required");
        final Phase currentPhase = this.phase;
        if (!isAllowedTransition(currentPhase, nextPhase)) {
            throw new IllegalStateException("Cannot transition migration phase from " + currentPhase + " to " + nextPhase
                    + "; phases must move strictly forward (CONFIGURATION -> STATE -> COMPLETED)");
        }
        this.phase = nextPhase;
    }

    private static boolean isAllowedTransition(final Phase currentPhase, final Phase nextPhase) {
        return switch (currentPhase) {
            case CONFIGURATION -> nextPhase == Phase.STATE || nextPhase == Phase.COMPLETED;
            case STATE -> nextPhase == Phase.COMPLETED;
            case COMPLETED -> false;
        };
    }

    /**
     * Returns the merged working configuration the connector produced during {@code migrateConfiguration(...)}. The
     * framework hands this to {@code ConnectorNode.applyMigratedConfiguration(...)} so the managed Process Group can be
     * rebuilt from it. This is a live reference to the working configuration, not a copy; the framework does not write
     * it onto the active configuration until both migration phases have succeeded.
     */
    MutableConnectorConfigurationContext getMergedConfiguration() {
        synchronized (stagingLock) {
            return workingConfiguration;
        }
    }

    /**
     * Returns the staged component-state writes and clears the internal buffer. Called by the framework once after
     * {@code migrateState(...)} returns so it can apply the recorded state to the managed components.
     */
    Map<String, VersionedComponentState> drainStagedComponentStates() {
        synchronized (stagingLock) {
            final Map<String, VersionedComponentState> snapshot = new LinkedHashMap<>(stagedComponentStates);
            stagedComponentStates.clear();
            return snapshot;
        }
    }

    private void requireConfigurationPhase(final String methodName) {
        final Phase currentPhase = this.phase;
        if (currentPhase != Phase.CONFIGURATION) {
            throw new IllegalStateException(methodName + "() is only valid during MigratableConnector.migrateConfiguration(); current migration phase is " + currentPhase);
        }
    }

    private void requireStatePhase(final String methodName) {
        final Phase currentPhase = this.phase;
        if (currentPhase != Phase.STATE) {
            throw new IllegalStateException(methodName + "() is only valid during MigratableConnector.migrateState(); current migration phase is " + currentPhase);
        }
    }
}
