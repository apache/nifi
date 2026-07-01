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

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardConnectorRepository implements ConnectorRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorRepository.class);
    private static final Duration SYNC_POLL_INTERVAL = Duration.ofSeconds(2);

    /**
     * Soft cardinality limit beyond which a WARN is emitted when a {@link ConnectorConfigurationProvider}
     * supplies logging attributes for a connector. Exceeding this threshold signals a potential metric/MDC
     * cardinality risk for downstream observability backends but does not reject the attributes.
     */
    private static final int CUSTOM_LOGGING_ATTRIBUTE_CARDINALITY_WARN_THRESHOLD = 5;

    private final Map<String, ConnectorNode> connectors = new ConcurrentHashMap<>();
    private final FlowEngine lifecycleExecutor = new FlowEngine(8, "NiFi Connector Lifecycle");

    private volatile FlowManager flowManager;
    private volatile ExtensionManager extensionManager;
    private volatile ConnectorRequestReplicator requestReplicator;
    private volatile SecretsManager secretsManager;
    private volatile AssetManager assetManager;
    private volatile ConnectorConfigurationProvider configurationProvider;
    private volatile Duration syncTimeout;

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        logger.debug("Initializing ConnectorRepository");
        this.flowManager = context.getFlowManager();
        this.extensionManager = context.getExtensionManager();
        this.requestReplicator = context.getRequestReplicator();
        this.secretsManager = context.getSecretsManager();
        this.assetManager = context.getAssetManager();
        this.configurationProvider = context.getConnectorConfigurationProvider();
        this.syncTimeout = context.getConnectorSyncTimeout();
        logger.debug("Successfully initialized ConnectorRepository with configurationProvider={}, syncTimeout={}",
                configurationProvider != null ? configurationProvider.getClass().getSimpleName() : "null", syncTimeout);
    }

    @Override
    public void verifyCreate(final String connectorId) {
        if (connectors.containsKey(connectorId)) {
            throw new IllegalStateException("A Connector already exists with ID %s".formatted(connectorId));
        }
        if (configurationProvider != null) {
            configurationProvider.verifyCreate(connectorId);
        }
    }

    @Override
    public void addConnector(final ConnectorNode connector) {
        applyProviderLoggingAttributes(connector);
        syncFromProvider(connector);
        connectors.put(connector.getIdentifier(), connector);
    }

    @Override
    public void restoreConnector(final ConnectorNode connector) {
        applyProviderLoggingAttributes(connector);
        connectors.put(connector.getIdentifier(), connector);
        logger.debug("Successfully restored {}", connector);
    }

    @Override
    public ConnectorSyncResult syncConnector(final VersionedConnector versionedConnector) {
        final String connectorId = versionedConnector.getInstanceIdentifier();
        final VersionedConnectorState proposedScheduledState = versionedConnector.getScheduledState();
        logger.debug("syncConnector called for connector [{}]", connectorId);

        // Consult the provider for external state checks and working config
        final ConnectorSyncDirective directive;
        if (configurationProvider != null) {
            try {
                directive = configurationProvider.getSyncDirective(connectorId, proposedScheduledState);
            } catch (final Exception e) {
                logger.error("Configuration provider threw exception during getSyncDirective for connector [{}]: {}", connectorId, e.getMessage(), e);
                final ConnectorNode existingNode = ensureConnectorNodeExists(versionedConnector);
                existingNode.markInvalid("Flow Synchronization Failure",
                        "Configuration provider error during synchronization: " + e.getMessage());
                return ConnectorSyncResult.failed(existingNode);
            }
        } else {
            directive = ConnectorSyncDirective.allow();
        }

        logger.debug("Connector [{}] sync directive: {}", connectorId, directive);

        // Handle REMOVE: connector should not exist on this node
        if (directive.getAction() == ConnectorSyncDirective.Action.REMOVE) {
            final ConnectorNode existingNode = connectors.get(connectorId);
            if (existingNode != null) {
                logger.info("Removing connector [{}] (state={}) from local repository per provider REMOVE directive",
                        connectorId, existingNode.getCurrentState());
                stopConnectorAndAwait(existingNode);
                purgeConnectorAndAwait(existingNode);
                logger.debug("Connector [{}] stopped and purged (state={}); calling removeConnector", connectorId, existingNode.getCurrentState());
                removeConnector(connectorId);
                logger.info("Successfully removed connector [{}] per REMOVE directive", connectorId);
            } else {
                logger.debug("Connector [{}] not present locally; REMOVE directive is a no-op", connectorId);
            }
            return ConnectorSyncResult.removed();
        }

        // Handle REJECT: create if needed (for background repair), mark invalid
        if (directive.getAction() == ConnectorSyncDirective.Action.REJECT) {
            final ConnectorNode node = ensureConnectorNodeExists(versionedConnector);
            logger.warn("Connector [{}] rejected by provider during sync; marking invalid", connectorId);
            node.markInvalid("Flow Synchronization Failure",
                    "Configuration provider rejected synchronization for this connector");
            return ConnectorSyncResult.rejected(node);
        }

        // ALLOW: proceed with sync
        // Look up or create the connector node
        ConnectorNode connector = connectors.get(connectorId);
        final boolean isNewConnector = connector == null;
        if (isNewConnector) {
            connector = createConnectorNode(versionedConnector);
            connectors.put(connectorId, connector);
            logger.info("Created new connector node [{}] of type [{}]", connectorId, versionedConnector.getType());
        }

        // Check local ConnectorState and handle per the agreed behavior matrix
        ConnectorState currentState = connector.getCurrentState();

        if (isWaitableState(currentState)) {
            logger.info("{} is in transient state {}; waiting for stable state (timeout={})", connector, currentState, syncTimeout);
            currentState = waitForStableState(connector, currentState);
            if (isWaitableState(currentState)) {
                logger.error("{} timed out waiting for transient state {} to resolve", connector, currentState);
                connector.markInvalid("Flow Synchronization Failure",
                        "Timed out waiting for connector to leave transient state " + currentState);
                return ConnectorSyncResult.rejected(connector);
            }
        }

        if (isRejectableState(currentState)) {
            logger.warn("{} is in state {} which cannot be synchronized; marking invalid", connector, currentState);
            connector.markInvalid("Flow Synchronization Failure",
                    "Connector cannot be synchronized while in state " + currentState);
            return ConnectorSyncResult.rejected(connector);
        }

        // Determine effective name, working config, and ScheduledState
        final ConnectorWorkingConfiguration providerConfig = directive.getWorkingConfiguration();

        // Enrich provider-sourced SECRET_REFERENCE values with providerId before they are compared
        // against the in-memory configuration or passed into inheritConfiguration.
        if (providerConfig != null) {
            resolveSecretReferencesFromProvider(providerConfig.getWorkingFlowConfiguration());
        }

        final String effectiveName = (providerConfig != null && providerConfig.getName() != null)
                ? providerConfig.getName()
                : versionedConnector.getName();

        final List<VersionedConfigurationStep> effectiveWorkingConfig = (providerConfig != null && providerConfig.getWorkingFlowConfiguration() != null)
                ? providerConfig.getWorkingFlowConfiguration()
                : versionedConnector.getWorkingFlowConfiguration();

        final List<VersionedConfigurationStep> effectiveActiveConfig = versionedConnector.getActiveFlowConfiguration();

        final VersionedConnectorState effectiveScheduledState = (directive.getScheduledStateOverride() != null)
                ? directive.getScheduledStateOverride()
                : proposedScheduledState;

        // Set name locally (no provider.save())
        connector.setName(effectiveName);

        final boolean wasRunning = currentState == ConnectorState.RUNNING;
        final boolean configChanged = isNewConnector || isConfigurationUpdated(connector, effectiveActiveConfig, effectiveWorkingConfig);
        final boolean restoringTroubleshooting = effectiveScheduledState == VersionedConnectorState.TROUBLESHOOTING;

        // Configuration must be inherited even when the effective state is TROUBLESHOOTING. The Connector's managed
        // Parameter Context is intentionally not registered with the global ParameterContextManager (see
        // StandardFlowManager#createConnector) and therefore is not persisted in flow.json. The framework can only
        // re-populate that Parameter Context by letting the Connector's applyUpdate run with the persisted active
        // configuration, which is exactly what inheritConfiguration does.
        //
        // For a TROUBLESHOOTING restoration, the Connector's authoritative active flow that inheritConfiguration
        // produces is then immediately overlaid by restoreTroubleshootingFlow with the user's persisted Managed
        // Process Group snapshot. The structural overlay preserves the in-memory Parameter Context binding, so the
        // Connector-supplied parameter values remain available, while the user's flow modifications are the ones
        // running. The transient Connector-supplied flow shape is invisible outside this synchronization cycle: flow
        // synchronization completes before the FlowFile Repository attaches FlowFiles to queues, so no FlowFile
        // movement can observe it.
        if (configChanged || restoringTroubleshooting) {
            logger.info("{} configuration needs synchronization", connector);
            try {
                inheritConfiguration(connector, effectiveActiveConfig, effectiveWorkingConfig, versionedConnector.getBundle());
            } catch (final Exception e) {
                logger.error("{} failed to inherit configuration", connector, e);
                if (wasRunning) {
                    try {
                        stopConnector(connector);
                    } catch (final Exception stopEx) {
                        logger.error("{} also failed to stop after sync failure", connector, stopEx);
                    }
                }
                return ConnectorSyncResult.failed(connector);
            }
        } else {
            logger.debug("{} configuration is up to date, no update necessary", connector);
        }

        if (restoringTroubleshooting) {
            final VersionedProcessGroup persistedManagedGroup = versionedConnector.getManagedProcessGroup();
            if (persistedManagedGroup == null) {
                logger.warn("{} effective scheduled state is TROUBLESHOOTING but no Managed Process Group snapshot was persisted; leaving Managed Process Group unchanged", connector);
            } else {
                logger.info("{} was persisted in Troubleshooting mode; restoring Managed Process Group from persisted snapshot", connector);
                try {
                    connector.getActiveFlowContext().restoreTroubleshootingFlow(persistedManagedGroup);
                } catch (final Exception e) {
                    logger.error("{} failed to restore Managed Process Group from Troubleshooting snapshot", connector, e);
                    connector.markInvalid("Flow Synchronization Failure",
                            "Failed to restore Managed Process Group from Troubleshooting snapshot: " + e.getMessage());
                    return ConnectorSyncResult.failed(connector);
                }
            }

            if (connector.getCurrentState() != ConnectorState.TROUBLESHOOTING) {
                // Use restoreTroubleshootingState rather than enterTroubleshooting because the former does not stop
                // running components within the Managed Process Group. Components inside the Managed Process Group that
                // were persisted as RUNNING have just been restored to the RUNNING state by restoreTroubleshootingFlow above;
                // calling enterTroubleshooting here would immediately stop them, violating the contract that persisted
                // user modifications (including running processors) should survive a NiFi restart while in Troubleshooting.
                connector.restoreTroubleshootingState();
            }

            return ConnectorSyncResult.syncedConfigUnchanged(connector, effectiveScheduledState);
        }

        return configChanged
                ? ConnectorSyncResult.synced(connector, effectiveScheduledState)
                : ConnectorSyncResult.syncedConfigUnchanged(connector, effectiveScheduledState);
    }

    private ConnectorNode ensureConnectorNodeExists(final VersionedConnector versionedConnector) {
        final String connectorId = versionedConnector.getInstanceIdentifier();
        ConnectorNode node = connectors.get(connectorId);
        if (node == null) {
            node = createConnectorNode(versionedConnector);
            connectors.put(connectorId, node);
            logger.info("Created connector node [{}] for reject/error handling", connectorId);
        }
        return node;
    }

    private ConnectorNode createConnectorNode(final VersionedConnector versionedConnector) {
        final BundleCoordinate coordinate = resolveBundleCoordinate(versionedConnector.getBundle(), versionedConnector.getType());
        return flowManager.createConnector(versionedConnector.getType(), versionedConnector.getInstanceIdentifier(), coordinate, false, true);
    }

    private BundleCoordinate resolveBundleCoordinate(final Bundle bundle, final String componentType) {
        try {
            final BundleDTO bundleDto = new BundleDTO(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            return BundleUtils.getCompatibleBundle(extensionManager, componentType, bundleDto);
        } catch (final IllegalStateException e) {
            return new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        }
    }

    private boolean isWaitableState(final ConnectorState state) {
        return state == ConnectorState.STARTING
                || state == ConnectorState.STOPPING
                || state == ConnectorState.PURGING;
    }

    private boolean isRejectableState(final ConnectorState state) {
        return state == ConnectorState.DRAINING
                || state == ConnectorState.PREPARING_FOR_UPDATE
                || state == ConnectorState.UPDATING;
    }

    private ConnectorState waitForStableState(final ConnectorNode connector, final ConnectorState initialState) {
        final long deadlineNanos = System.nanoTime() + syncTimeout.toNanos();
        int iterations = 0;
        ConnectorState current = initialState;

        while (isWaitableState(current) && System.nanoTime() < deadlineNanos) {
            try {
                Thread.sleep(SYNC_POLL_INTERVAL.toMillis());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("{} interrupted while waiting for stable state", connector);
                return current;
            }

            current = connector.getCurrentState();
            iterations++;
            if (iterations % 10 == 0) {
                final long elapsedSeconds = Duration.ofNanos(System.nanoTime() - (deadlineNanos - syncTimeout.toNanos())).toSeconds();
                logger.info("Still waiting for {} to leave state {}; current state is {}; elapsed={}s",
                        connector, initialState, current, elapsedSeconds);
            }
        }

        return current;
    }

    // --- Configuration comparison ---

    private boolean isConfigurationUpdated(final ConnectorNode existingConnector,
                                           final List<VersionedConfigurationStep> effectiveActiveConfig,
                                           final List<VersionedConfigurationStep> effectiveWorkingConfig) {
        final ConnectorConfiguration activeConfig = existingConnector.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration();
        final boolean activeContextChanged = isConfigurationUpdated(activeConfig, effectiveActiveConfig);

        final ConnectorConfiguration workingConfig = existingConnector.getWorkingFlowContext().getConfigurationContext().toConnectorConfiguration();
        final boolean workingContextChanged = isConfigurationUpdated(workingConfig, effectiveWorkingConfig);

        return activeContextChanged || workingContextChanged;
    }

    private boolean isConfigurationUpdated(final ConnectorConfiguration existingConfiguration, final List<VersionedConfigurationStep> versionedConfigurationSteps) {
        if (versionedConfigurationSteps == null || versionedConfigurationSteps.isEmpty()) {
            return existingConfiguration != null && !existingConfiguration.getNamedStepConfigurations().isEmpty();
        }

        final Set<NamedStepConfiguration> existingStepConfigurations = existingConfiguration.getNamedStepConfigurations();
        if (existingStepConfigurations.size() != versionedConfigurationSteps.size()) {
            return true;
        }

        final Map<String, NamedStepConfiguration> existingStepsByName = existingStepConfigurations.stream()
                .collect(Collectors.toMap(NamedStepConfiguration::stepName, Function.identity()));

        for (final VersionedConfigurationStep versionedStep : versionedConfigurationSteps) {
            final NamedStepConfiguration existingStep = existingStepsByName.get(versionedStep.getName());
            if (existingStep == null) {
                return true;
            }

            if (isConfigurationStepUpdated(existingStep, versionedStep)) {
                return true;
            }
        }

        return false;
    }

    private boolean isConfigurationStepUpdated(final NamedStepConfiguration existingStep, final VersionedConfigurationStep versionedStep) {
        final Map<String, ConnectorValueReference> existingProperties = existingStep.configuration().getPropertyValues();

        final Map<String, VersionedConnectorValueReference> versionedProperties = versionedStep.getProperties();
        if (versionedProperties == null || versionedProperties.isEmpty()) {
            return existingProperties != null && !existingProperties.isEmpty();
        }

        if (existingProperties == null || existingProperties.size() != versionedProperties.size()) {
            return true;
        }

        for (final Map.Entry<String, VersionedConnectorValueReference> versionedEntry : versionedProperties.entrySet()) {
            final String propertyName = versionedEntry.getKey();
            final VersionedConnectorValueReference versionedRef = versionedEntry.getValue();
            final ConnectorValueReference existingRef = existingProperties.get(propertyName);

            if (!valueReferencesEqual(versionedRef, existingRef)) {
                return true;
            }
        }

        return false;
    }

    private boolean valueReferencesEqual(final VersionedConnectorValueReference versionedReference, final ConnectorValueReference existingReference) {
        if (versionedReference == null && existingReference == null) {
            return true;
        }
        if (versionedReference == null || existingReference == null) {
            return false;
        }

        final String versionedValueType = versionedReference.getValueType();
        final String existingValueType = existingReference.getValueType().name();
        if (!Objects.equals(versionedValueType, existingValueType)) {
            return false;
        }

        return switch (existingReference) {
            case StringLiteralValue stringLiteral -> Objects.equals(stringLiteral.getValue(), versionedReference.getValue());
            case AssetReference assetRef -> Objects.equals(assetRef.getAssetIdentifiers(), versionedReference.getAssetIds());
            case SecretReference secretRef -> Objects.equals(secretRef.getProviderId(), versionedReference.getProviderId())
                    && Objects.equals(secretRef.getSecretName(), versionedReference.getSecretName());
        };
    }

    @Override
    public void verifyDelete(final ConnectorNode connector) {
        connector.verifyCanDelete();
    }

    @Override
    public void removeConnector(final String connectorId) {
        logger.debug("Removing connector [{}]", connectorId);
        final ConnectorNode connectorNode = connectors.get(connectorId);
        if (connectorNode == null) {
            throw new IllegalStateException("No connector found with ID " + connectorId);
        }

        logger.debug("Verifying connector [{}] (state={}) can be deleted", connectorId, connectorNode.getCurrentState());
        verifyDelete(connectorNode);
        if (configurationProvider != null) {
            logger.debug("Notifying configuration provider of connector [{}] deletion", connectorId);
            configurationProvider.delete(connectorId);
        }
        connectors.remove(connectorId);

        final Class<?> taskClass = connectorNode.getConnector().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskClass, connectorId)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, connectorNode.getConnector());
        }

        extensionManager.removeInstanceClassLoader(connectorId);
        logger.info("Connector [{}] removed from repository", connectorId);
    }

    private void stopConnectorAndAwait(final ConnectorNode connector) {
        final String connectorId = connector.getIdentifier();
        final ConnectorState currentState = connector.getCurrentState();
        if (currentState == ConnectorState.STOPPED || currentState == ConnectorState.UPDATE_FAILED || currentState == ConnectorState.UPDATED) {
            logger.debug("Connector [{}] is already in state {}; no stop needed", connectorId, currentState);
            return;
        }

        logger.info("Stopping connector [{}] (current state: {}) and awaiting completion", connectorId, currentState);
        try {
            final Future<Void> stopFuture = stopConnector(connector);
            stopFuture.get(syncTimeout.toMillis(), TimeUnit.MILLISECONDS);
            logger.debug("Connector [{}] stopped successfully", connectorId);
        } catch (final TimeoutException e) {
            logger.warn("Timed out waiting for connector [{}] to stop", connectorId);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for connector [{}] to stop", connectorId);
        } catch (final Exception e) {
            logger.warn("Failed to stop connector [{}]: {}", connectorId, e.getMessage(), e);
        }
    }

    private void purgeConnectorAndAwait(final ConnectorNode connector) {
        final String connectorId = connector.getIdentifier();
        try {
            logger.debug("Purging FlowFiles for connector [{}] before removal", connectorId);
            final Future<Void> purgeFuture = connector.purgeFlowFiles("Flow Synchronization");
            purgeFuture.get(syncTimeout.toMillis(), TimeUnit.MILLISECONDS);
            logger.debug("Connector [{}] purged successfully", connectorId);
        } catch (final TimeoutException e) {
            logger.warn("Timed out waiting for connector [{}] to purge FlowFiles", connectorId);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for connector [{}] to purge FlowFiles", connectorId);
        } catch (final Exception e) {
            logger.warn("Failed to purge FlowFiles for connector [{}]: {}", connectorId, e.getMessage(), e);
        }
    }

    @Override
    public ConnectorNode getConnector(final String identifier, final ConnectorSyncMode syncMode) {
        Objects.requireNonNull(syncMode, "syncMode is required");
        final ConnectorNode connector = connectors.get(identifier);
        if (connector != null && syncMode == ConnectorSyncMode.SYNC_WITH_PROVIDER) {
            syncFromProviderForRead(connector);
        }
        return connector;
    }

    @Override
    public List<ConnectorNode> getConnectors(final ConnectorSyncMode syncMode) {
        Objects.requireNonNull(syncMode, "syncMode is required");
        final List<ConnectorNode> connectorList = List.copyOf(connectors.values());
        if (syncMode == ConnectorSyncMode.SYNC_WITH_PROVIDER) {
            for (final ConnectorNode connector : connectorList) {
                syncFromProviderForRead(connector);
            }
        }
        return connectorList;
    }

    /**
     * Sync a connector from the provider for a read operation, tolerating a configuration-load failure.
     * A configuration that cannot be loaded or parsed (e.g. a transient provider error, or a corrupt
     * stored configuration left by a failed commit) must not make a connector unreadable — and therefore
     * undeletable. On failure we log and return the in-memory node without updating its working
     * configuration; the connector's own state is left untouched, so a subsequent successful read recovers
     * normally. Write paths ({@code addConnector}, {@code applyUpdate}) call
     * {@link #syncFromProvider(ConnectorNode)} directly and continue to propagate the exception so they do
     * not proceed on a configuration that could not be loaded.
     */
    private void syncFromProviderForRead(final ConnectorNode connector) {
        try {
            syncFromProvider(connector);
        } catch (final ConnectorConfigurationProviderException e) {
            logger.error("Failed to load configuration from provider for connector [{}] during a read operation; "
                    + "returning the connector with its existing configuration so it remains readable and deletable",
                    connector.getIdentifier(), e);
        }
    }

    @Override
    public Future<Void> startConnector(final ConnectorNode connector) {
        return connector.start(lifecycleExecutor);
    }

    @Override
    public Future<Void> stopConnector(final ConnectorNode connector) {
        return connector.stop(lifecycleExecutor);
    }

    @Override
    public Future<Void> restartConnector(final ConnectorNode connector) {
        final CompletableFuture<Void> restartCompleteFuture = new CompletableFuture<>();
        restartConnector(connector, restartCompleteFuture);
        return restartCompleteFuture;
    }

    private void restartConnector(final ConnectorNode connector, final CompletableFuture<Void> restartCompleteFuture) {
        try {
            final Future<Void> stopFuture = connector.stop(lifecycleExecutor);
            stopFuture.get();

            final Future<Void> startFuture = connector.start(lifecycleExecutor);
            startFuture.get();

            logger.info("Successfully restarted connector [{}]", connector.getIdentifier());
            restartCompleteFuture.complete(null);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while restarting connector [{}]", connector.getIdentifier(), e);
            restartCompleteFuture.completeExceptionally(e);
        } catch (final ExecutionException e) {
            logger.error("Failed to restart connector [{}]", connector.getIdentifier(), e.getCause());
            restartCompleteFuture.completeExceptionally(e);
        }
    }

    @Override
    public void applyUpdate(final ConnectorNode connector, final ConnectorUpdateContext context) throws FlowUpdateException {
        logger.debug("Applying update to {}", connector);

        // Refuse the update before any provider interaction or asset sync. Once a Connector enters Troubleshooting the
        // user owns the managed flow (NIP-28) and the Connector configuration is locked. Deferring this check until
        // transitionStateForUpdating() lets a provider that returns shouldApplyUpdate()=false cause the framework to
        // silently treat the request as successful while the Connector is still in Troubleshooting, and also lets the
        // provider observe and act on an update request that should never reach it in this state.
        if (connector.getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot apply an update to " + connector + " while it is in Troubleshooting mode; "
                + "exit Troubleshooting mode before applying updates.");
        }

        if (configurationProvider != null && !configurationProvider.shouldApplyUpdate(connector.getIdentifier())) {
            logger.info("ConnectorConfigurationProvider indicated framework should not apply update for {}; skipping framework update process", connector);
            return;
        }

        final ConnectorState initialDesiredState = connector.getDesiredState();
        logger.info("Applying update to Connector {}", connector);

        // Sync asset binaries first (provider downloads changed assets, assigns new UUIDs),
        // then load the updated configuration (which will reflect any new NiFi UUIDs).
        syncAssetsFromProvider(connector);

        // Transition the connector's state to PREPARING_FOR_UPDATE before starting the background process.
        // This allows us to ensure that if we poll and see the state in the same state it was in before that
        // we know the update has already completed (successfully or otherwise).
        logger.debug("Transitioning {} to PREPARING_FOR_UPDATE state before applying update", connector);
        connector.transitionStateForUpdating();
        logger.debug("{} is now in PREPARING_FOR_UPDATE state", connector);

        // Update connector in a background thread. This will handle transitioning the Connector state appropriately
        // so that it's clear when the update has completed.
        lifecycleExecutor.submit(() -> {
            updateConnector(connector, initialDesiredState, context);
            cleanUpAssets(connector);
        });
    }

    private void updateConnector(final ConnectorNode connector, final ConnectorState initialDesiredState, final ConnectorUpdateContext context) {
        logger.debug("Updating {}", connector);
        try {
            // Perform whatever preparation is necessary for the update. Default implementation is to stop the connector.
            logger.debug("Preparing {} for update", connector);
            connector.prepareForUpdate();

            // Wait for Connector State to become UPDATING
            logger.debug("Waiting for {} to transition to UPDATING state", connector);
            waitForState(connector, Set.of(ConnectorState.UPDATING), Set.of(ConnectorState.PREPARING_FOR_UPDATE));

            // Apply the update to the connector.
            logger.info("{} has now completed preparations for update; applying update now", connector);
            connector.applyUpdate();
            logger.info("{} has successfully applied update", connector);

            // Now that the update has been applied, save the flow so that the updated configuration is persisted.
            context.saveFlow();

            // Wait for all nodes to complete the update.
            waitForState(connector, Set.of(ConnectorState.UPDATED), Set.of(ConnectorState.UPDATING));
            logger.info("{} has successfully completed update on all nodes", connector);

            // If the initial desired state was RUNNING, start the connector again. Otherwise, stop it.
            // We don't simply leave it be as the prepareForUpdate / update may have changed the state of some components.
            if (initialDesiredState == ConnectorState.RUNNING) {
                logger.info("{} has been successfully updated; starting to resume initial state", connector);
                connector.start(lifecycleExecutor);
            } else {
                logger.info("{} has been successfully updated; stopping to resume initial state", connector);
                connector.stop(lifecycleExecutor);
            }

            // We've updated the state of the connector so save flow again
            context.saveFlow();
        } catch (final Exception e) {
            logger.error("Failed to apply update for {}", connector, e);
            connector.abortUpdate(e);
        }
    }

    private void waitForState(final ConnectorNode connector, final Set<ConnectorState> desiredStates, final Set<ConnectorState> allowableStates)
                throws FlowUpdateException, IOException, InterruptedException {

        // Wait for Connector State to become the desired state
        int iterations = 0;
        final long startNanos = System.nanoTime();
        while (true) {
            final ConnectorState clusterState = requestReplicator.getState(connector.getIdentifier());
            if (desiredStates.contains(clusterState)) {
                logger.info("State for {} is now {}", connector, clusterState);
                break;
            } else if (allowableStates.contains(clusterState)) {
                final long elapsedSeconds = Duration.ofNanos(System.nanoTime() - startNanos).toSeconds();
                if (++iterations % 10 == 0) {
                    logger.info("Waiting for {} to transition to one of {}. Current state is {}; elapsed time = {} secs", connector, desiredStates, clusterState, elapsedSeconds);
                } else {
                    logger.debug("Waiting for {} to transition to one of {}. Current state is {}; elapsed time = {} secs", connector, desiredStates, clusterState, elapsedSeconds);
                }

                Thread.sleep(Duration.ofSeconds(1));
                continue;
            } else if (clusterState == ConnectorState.UPDATE_FAILED) {
                throw new FlowUpdateException("State of " + connector + " transitioned to UPDATE_FAILED while waiting for state to become one of " + desiredStates);
            }

            throw new FlowUpdateException("While waiting for %s to transition to state in set %s, connector transitioned to unexpected state: %s".formatted(connector, desiredStates, clusterState));
        }
    }

    private void cleanUpAssets(final ConnectorNode connector) {
        final String connectorId = connector.getIdentifier();
        final Set<String> referencedAssetIds = new HashSet<>();
        collectReferencedAssetIds(connector.getActiveFlowContext(), referencedAssetIds);
        collectReferencedAssetIds(connector.getWorkingFlowContext(), referencedAssetIds);

        logger.debug("Found {} assets referenced for Connector [{}]", referencedAssetIds.size(), connectorId);

        final List<Asset> allConnectorAssets = assetManager.getAssets(connectorId);
        for (final Asset asset : allConnectorAssets) {
            final String assetId = asset.getIdentifier();
            if (!referencedAssetIds.contains(assetId)) {
                try {
                    logger.info("Deleting unreferenced asset [id={},name={}] for connector [{}]", assetId, asset.getName(), connectorId);

                    if (configurationProvider != null) {
                        // Provider deletes from external store and local AssetManager
                        configurationProvider.deleteAsset(connectorId, assetId);
                    } else {
                        assetManager.deleteAsset(assetId);
                    }
                } catch (final Exception e) {
                    logger.warn("Unable to delete unreferenced asset [id={},name={}] for connector [{}]", assetId, asset.getName(), connectorId, e);
                }
            }
        }
    }

    private void collectReferencedAssetIds(final FrameworkFlowContext flowContext, final Set<String> referencedAssetIds) {
        if (flowContext == null) {
            return;
        }

        final ConnectorConfiguration configuration = flowContext.getConfigurationContext().toConnectorConfiguration();
        for (final NamedStepConfiguration namedStepConfiguration : configuration.getNamedStepConfigurations()) {
            final StepConfiguration stepConfiguration = namedStepConfiguration.configuration();
            final Map<String, ConnectorValueReference> stepPropertyValues = stepConfiguration.getPropertyValues();
            if (stepPropertyValues == null) {
                continue;
            }
            for (final ConnectorValueReference valueReference : stepPropertyValues.values()) {
                if (valueReference instanceof final AssetReference assetReference) {
                    referencedAssetIds.addAll(assetReference.getAssetIdentifiers());
                }
            }
        }
    }

    @Override
    public void updateConnector(final ConnectorNode connector, final String name) {
        if (connector.getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot update the configuration of " + connector + " while it is in Troubleshooting mode; "
                + "exit Troubleshooting mode before modifying the Connector configuration.");
        }

        if (configurationProvider != null) {
            // Load the latest provider state so that other in-flight working changes are not overwritten by a rename.
            final Optional<ConnectorWorkingConfiguration> externalConfig = configurationProvider.load(connector.getIdentifier());
            final ConnectorWorkingConfiguration workingConfiguration = externalConfig.orElseGet(() -> buildWorkingConfiguration(connector));
            workingConfiguration.setName(name);
            configurationProvider.save(connector.getIdentifier(), workingConfiguration);
        }
        connector.setName(name);
    }

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        if (connector.getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot modify the configuration of " + connector + " while it is in Troubleshooting mode; "
                + "exit Troubleshooting mode before modifying the Connector configuration.");
        }

        if (configurationProvider != null) {
            final ConnectorWorkingConfiguration mergedConfiguration = buildMergedWorkingConfiguration(connector, stepName, configuration);
            configurationProvider.save(connector.getIdentifier(), mergedConfiguration);
        }

        connector.setConfiguration(stepName, configuration);
        logger.info("Successfully configured {} for step {}", connector, stepName);
    }

    @Override
    public void inheritConfiguration(final ConnectorNode connector, final List<VersionedConfigurationStep> activeFlowConfiguration,
                final List<VersionedConfigurationStep> workingFlowConfiguration, final Bundle flowContextBundle) throws FlowUpdateException {

        logger.debug("Inheriting configuration for {}", connector);
        connector.transitionStateForUpdating();
        connector.prepareForUpdate();

        try {
            connector.inheritConfiguration(activeFlowConfiguration, workingFlowConfiguration, flowContextBundle);
            logger.debug("Successfully inherited configuration for {}", connector);
        } catch (final Exception e) {
            logger.error("Failed to inherit configuration for {}", connector, e);
            connector.abortUpdate(e);
            connector.markInvalid("Flow Update Failure", "The flow could not be updated: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void discardWorkingConfiguration(final ConnectorNode connector) {
        if (connector.getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot discard the working configuration of " + connector + " while it is in Troubleshooting mode; "
                + "exit Troubleshooting mode before modifying the Connector configuration.");
        }

        if (configurationProvider != null) {
            configurationProvider.discard(connector.getIdentifier());
        }
        connector.discardWorkingConfiguration();
        cleanUpAssets(connector);
    }

    @Override
    public void verifyEnterTroubleshooting(final ConnectorNode connector) {
        connector.verifyCanEnterTroubleshooting();
        if (configurationProvider != null) {
            configurationProvider.verifyEnterTroubleshooting(connector.getIdentifier());
        }
    }

    @Override
    public void enterTroubleshooting(final ConnectorNode connector) {
        verifyEnterTroubleshooting(connector);
        logger.info("Transitioning Connector [{}] into Troubleshooting mode", connector.getIdentifier());
        connector.enterTroubleshooting();
    }

    @Override
    public void verifyEndTroubleshooting(final ConnectorNode connector) {
        connector.verifyCanEndTroubleshooting();
        if (configurationProvider != null) {
            configurationProvider.verifyEndTroubleshooting(connector.getIdentifier());
        }
    }

    @Override
    public void endTroubleshooting(final ConnectorNode connector) throws FlowUpdateException {
        // Consult the optional provider hook for end-of-troubleshooting symmetry with enterTroubleshooting.
        // The Connector's own pre-conditions (verifyCanEndTroubleshooting) are evaluated inside
        // connector.endTroubleshooting(), so we do not call them a second time here in order to avoid
        // double-evaluating the (relatively expensive) authoritative-flow preflight against the live managed group.
        if (configurationProvider != null) {
            configurationProvider.verifyEndTroubleshooting(connector.getIdentifier());
        }
        logger.info("Exiting Troubleshooting mode on Connector [{}]", connector.getIdentifier());
        connector.endTroubleshooting();
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
    }

    /**
     * Sources provider-supplied logging attributes for the given connector and applies them to the node so that
     * they are merged into the MDC of the connector's {@link org.apache.nifi.logging.ComponentLog} and the loggers
     * of every component running inside its managed flow (and surfaced on connector metrics). Invoked when the
     * connector node is added or restored. The framework-managed identity keys are always present regardless of
     * what the provider supplies.
     */
    private void applyProviderLoggingAttributes(final ConnectorNode connector) {
        connector.setCustomLoggingAttributes(resolveProviderLoggingAttributes(connector.getIdentifier()));
    }

    private Map<String, String> resolveProviderLoggingAttributes(final String connectorId) {
        if (configurationProvider == null) {
            return Map.of();
        }
        try {
            final Map<String, String> attributes = configurationProvider.getLoggingAttributes(connectorId);
            if (attributes == null || attributes.isEmpty()) {
                return Map.of();
            }
            if (attributes.size() > CUSTOM_LOGGING_ATTRIBUTE_CARDINALITY_WARN_THRESHOLD) {
                logger.warn("ConnectorConfigurationProvider [{}] supplied {} logging attributes for connector [{}] which exceeds the soft threshold of {}; "
                                + "high-cardinality attributes can degrade MDC logging and OpenTelemetry metric backends",
                        configurationProvider.getClass().getSimpleName(), attributes.size(), connectorId, CUSTOM_LOGGING_ATTRIBUTE_CARDINALITY_WARN_THRESHOLD);
            }
            return attributes;
        } catch (final Exception e) {
            logger.warn("ConnectorConfigurationProvider [{}] threw while computing logging attributes for connector [{}]; using empty map: {}",
                    configurationProvider.getClass().getSimpleName(), connectorId, e.getMessage(), e);
            return Map.of();
        }
    }

    @Override
    public ConnectorStateTransition createStateTransition(final String type, final String id) {
        final String componentDescription = "StandardConnectorNode[id=" + id + ", type=" + type + "]";
        return new StandardConnectorStateTransition(componentDescription);
    }

    @Override
    public FrameworkConnectorInitializationContextBuilder createInitializationContextBuilder() {
        return new StandardConnectorInitializationContext.Builder();
    }

    @Override
    public Asset storeAsset(final String connectorId, final String assetId, final String assetName, final InputStream content) throws IOException {
        if (configurationProvider == null) {
            return assetManager.saveAsset(connectorId, assetId, assetName, content);
        }

        // When a provider is configured, delegate entirely to the provider, which should write to the AssetManager and sync to the external store.
        try {
            configurationProvider.storeAsset(connectorId, assetId, assetName, content);
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException("Failed to store asset [" + assetName + "] to provider for connector [" + connectorId + "]", e);
        }
        logger.debug("Stored asset [nifiId={}, name={}] for connector [{}]", assetId, assetName, connectorId);

        return assetManager.getAsset(assetId)
                .orElseThrow(() -> new IOException("Asset [" + assetId + "] not found after storing for connector [" + connectorId + "]"));
    }

    @Override
    public Optional<Asset> getAsset(final String assetId) {
        return assetManager.getAsset(assetId);
    }

    @Override
    public List<Asset> getAssets(final String connectorId) {
        return assetManager.getAssets(connectorId);
    }

    @Override
    public void deleteAssets(final String connectorId) {
        final List<Asset> assets = assetManager.getAssets(connectorId);
        for (final Asset asset : assets) {
            try {
                if (configurationProvider != null) {
                    // When a provider is configured, delegate entirely to the provider, which should delete from the AssetManager and sync to the external store.
                    configurationProvider.deleteAsset(connectorId, asset.getIdentifier());
                } else {
                    assetManager.deleteAsset(asset.getIdentifier());
                }
            } catch (final Exception e) {
                logger.warn("Failed to delete asset [nifiUuid={}] for connector [{}]", asset.getIdentifier(), connectorId, e);
            }
        }
    }

    @Override
    public void syncAssetsFromProvider(final ConnectorNode connector) {
        if (configurationProvider == null) {
            return;
        }

        final String connectorId = connector.getIdentifier();

        // Step 1: Sync Connector Assets from Provider
        logger.debug("Syncing assets from provider for connector [{}]", connectorId);
        configurationProvider.syncAssets(connectorId);

        // Step 2: Sync ConnectorNode Configuration from Provider
        syncFromProvider(connector);
    }

    private void syncFromProvider(final ConnectorNode connector) {
        if (configurationProvider == null) {
            return;
        }

        final String connectorId = connector.getIdentifier();
        final Optional<ConnectorWorkingConfiguration> externalConfig = configurationProvider.load(connectorId);
        if (externalConfig.isEmpty()) {
            return;
        }

        final ConnectorWorkingConfiguration config = externalConfig.get();
        if (config.getName() != null) {
            connector.setName(config.getName());
        }

        final List<VersionedConfigurationStep> workingFlowConfiguration = config.getWorkingFlowConfiguration();

        if (workingFlowConfiguration == null) {
            return;
        }

        // Enrich provider-sourced SECRET_REFERENCE values with providerId before they are
        // converted into the in-memory ConnectorValueReference graph.
        resolveSecretReferencesFromProvider(workingFlowConfiguration);

        // Replace each step's working configuration on the connector. Routing through the connector
        // (rather than touching the configuration context directly) ensures it is notified via
        // onConfigurationStepConfigured when raw or resolved property values changed, so the embedded
        // flow's Parameter Context picks up new values (e.g., rotated secrets) without an explicit save.
        for (final VersionedConfigurationStep step : workingFlowConfiguration) {
            final StepConfiguration stepConfiguration = toStepConfiguration(step);
            try {
                connector.replaceWorkingConfiguration(step.getName(), stepConfiguration);
            } catch (final Exception e) {
                logger.warn("Failed to replace working configuration for step [{}] on {} during sync from provider; continuing with remaining steps",
                        step.getName(), connector, e);
            }
        }
    }

    private ConnectorWorkingConfiguration buildWorkingConfiguration(final ConnectorNode connector) {
        final ConnectorWorkingConfiguration config = new ConnectorWorkingConfiguration();
        config.setName(connector.getName());
        config.setWorkingFlowConfiguration(buildVersionedConfigurationSteps(connector.getWorkingFlowContext()));
        return config;
    }

    private List<VersionedConfigurationStep> buildVersionedConfigurationSteps(final FrameworkFlowContext flowContext) {
        if (flowContext == null) {
            return List.of();
        }

        final ConnectorConfiguration configuration = flowContext.getConfigurationContext().toConnectorConfiguration();
        final List<VersionedConfigurationStep> configurationSteps = new ArrayList<>();

        for (final NamedStepConfiguration namedStepConfiguration : configuration.getNamedStepConfigurations()) {
            final VersionedConfigurationStep versionedConfigurationStep = new VersionedConfigurationStep();
            versionedConfigurationStep.setName(namedStepConfiguration.stepName());
            versionedConfigurationStep.setProperties(toVersionedProperties(namedStepConfiguration.configuration()));
            configurationSteps.add(versionedConfigurationStep);
        }

        return configurationSteps;
    }

    private ConnectorWorkingConfiguration buildMergedWorkingConfiguration(final ConnectorNode connector, final String stepName, final StepConfiguration incomingConfiguration) {
        final ConnectorWorkingConfiguration existingConfig;
        if (configurationProvider != null) {
            final Optional<ConnectorWorkingConfiguration> externalConfig = configurationProvider.load(connector.getIdentifier());
            existingConfig = externalConfig.orElseGet(() -> buildWorkingConfiguration(connector));
        } else {
            existingConfig = buildWorkingConfiguration(connector);
        }

        final List<VersionedConfigurationStep> existingSteps = existingConfig.getWorkingFlowConfiguration() != null
            ? new ArrayList<>(existingConfig.getWorkingFlowConfiguration())
            : new ArrayList<>();

        VersionedConfigurationStep targetStep = null;
        for (final VersionedConfigurationStep step : existingSteps) {
            if (stepName.equals(step.getName())) {
                targetStep = step;
                break;
            }
        }

        if (targetStep == null) {
            targetStep = new VersionedConfigurationStep();
            targetStep.setName(stepName);
            targetStep.setProperties(new HashMap<>());
            existingSteps.add(targetStep);
        }

        final Map<String, VersionedConnectorValueReference> mergedProperties = targetStep.getProperties() != null
            ? new HashMap<>(targetStep.getProperties())
            : new HashMap<>();

        for (final Map.Entry<String, ConnectorValueReference> entry : incomingConfiguration.getPropertyValues().entrySet()) {
            if (entry.getValue() != null) {
                mergedProperties.put(entry.getKey(), toVersionedValueReference(entry.getValue()));
            }
        }
        targetStep.setProperties(mergedProperties);

        existingConfig.setWorkingFlowConfiguration(existingSteps);
        return existingConfig;
    }

    private Map<String, VersionedConnectorValueReference> toVersionedProperties(final StepConfiguration configuration) {
        final Map<String, VersionedConnectorValueReference> versionedProperties = new HashMap<>();
        for (final Map.Entry<String, ConnectorValueReference> entry : configuration.getPropertyValues().entrySet()) {
            final ConnectorValueReference valueReference = entry.getValue();
            if (valueReference != null) {
                versionedProperties.put(entry.getKey(), toVersionedValueReference(valueReference));
            }
        }
        return versionedProperties;
    }

    private VersionedConnectorValueReference toVersionedValueReference(final ConnectorValueReference valueReference) {
        final VersionedConnectorValueReference versionedReference = new VersionedConnectorValueReference();
        versionedReference.setValueType(valueReference.getValueType().name());

        switch (valueReference) {
            case StringLiteralValue stringLiteral -> versionedReference.setValue(stringLiteral.getValue());
            case AssetReference assetRef -> versionedReference.setAssetIds(assetRef.getAssetIdentifiers());
            case SecretReference secretRef -> {
                versionedReference.setProviderId(secretRef.getProviderId());
                versionedReference.setProviderName(secretRef.getProviderName());
                versionedReference.setSecretName(secretRef.getSecretName());
                versionedReference.setFullyQualifiedSecretName(secretRef.getFullyQualifiedName());
            }
        }

        return versionedReference;
    }

    private StepConfiguration toStepConfiguration(final VersionedConfigurationStep step) {
        final Map<String, ConnectorValueReference> propertyValues = new HashMap<>();
        final Map<String, VersionedConnectorValueReference> versionedProperties = step.getProperties();
        if (versionedProperties != null) {
            for (final Map.Entry<String, VersionedConnectorValueReference> entry : versionedProperties.entrySet()) {
                final VersionedConnectorValueReference versionedRef = entry.getValue();
                if (versionedRef != null) {
                    propertyValues.put(entry.getKey(), toConnectorValueReference(versionedRef));
                }
            }
        }
        return new StepConfiguration(propertyValues);
    }

    private ConnectorValueReference toConnectorValueReference(final VersionedConnectorValueReference versionedReference) {
        final ConnectorValueType valueType = ConnectorValueType.valueOf(versionedReference.getValueType());
        return switch (valueType) {
            case STRING_LITERAL -> new StringLiteralValue(versionedReference.getValue());
            case ASSET_REFERENCE -> new AssetReference(versionedReference.getAssetIds());
            case SECRET_REFERENCE -> new SecretReference(versionedReference.getProviderId(), versionedReference.getProviderName(),
                versionedReference.getSecretName(), versionedReference.getFullyQualifiedSecretName());
        };
    }

    /**
     * Resolves the {@code providerId} on SECRET_REFERENCE entries that arrive from a
     * {@link ConnectorConfigurationProvider} with only {@code providerName} populated. Provider
     * implementations may legitimately omit {@code providerId} because it is a runtime-assigned
     * UUID; the framework alone has the {@link FlowManager} state needed to map a provider name
     * back to its identifier. Other fields (such as {@code secretName}) are the responsibility of
     * the provider and pass through unchanged.
     *
     * <p>This is invoked at the two boundaries where provider-sourced configuration enters the
     * framework (the working config returned by {@link ConnectorConfigurationProvider#load(String)}
     * and the working config returned by {@link ConnectorConfigurationProvider#getSyncDirective}).</p>
     *
     * <p>Mutates the supplied references in place. References that already have a non-null
     * {@code providerId} are left alone. References whose {@code providerName} cannot be
     * unambiguously resolved are also left with a {@code null} {@code providerId} so that the UI
     * can surface the SECRET_REFERENCE as invalid; see {@link #findProviderIdByName(String)}.</p>
     *
     * @param steps the configuration steps loaded from the provider; may be {@code null} or empty
     */
    private void resolveSecretReferencesFromProvider(final List<VersionedConfigurationStep> steps) {
        if (steps == null) {
            return;
        }
        logger.debug("Resolving SECRET_REFERENCE providerIds across {} configuration step(s)", steps.size());
        steps.stream()
                .filter(Objects::nonNull)
                .map(VersionedConfigurationStep::getProperties)
                .filter(Objects::nonNull)
                .flatMap(properties -> properties.values().stream())
                .filter(Objects::nonNull)
                .filter(ref -> ConnectorValueType.SECRET_REFERENCE.name().equals(ref.getValueType()))
                .filter(ref -> ref.getProviderId() == null)
                .filter(ref -> ref.getProviderName() != null)
                .forEach(ref -> ref.setProviderId(findProviderIdByName(ref.getProviderName())));
    }

    /**
     * Resolves a parameter provider id by name, but only when the name unambiguously identifies a
     * single provider in the current flow. If zero or more than one parameter provider matches the
     * given name, returns {@code null} so that the SECRET_REFERENCE will be surfaced as invalid in
     * the UI and the user can re-configure it explicitly.
     *
     * <p>Package-private for testing. Production callers should access this method via
     * {@link #resolveSecretReferencesFromProvider}.</p>
     *
     * @param providerName the parameter provider name to resolve; may be {@code null}
     * @return the matching parameter provider identifier when exactly one provider has the given
     *         name; {@code null} when {@code providerName} is {@code null}, no provider matches,
     *         or multiple providers share the name
     */
    String findProviderIdByName(final String providerName) {
        if (flowManager == null || providerName == null) {
            return null;
        }
        final List<String> matches = new ArrayList<>();
        for (final ParameterProviderNode parameterProviderNode : flowManager.getAllParameterProviders()) {
            if (providerName.equals(parameterProviderNode.getName())) {
                matches.add(parameterProviderNode.getIdentifier());
            }
        }
        if (matches.isEmpty()) {
            logger.warn("No parameter provider found with name [{}]; SECRET_REFERENCE providerId will remain null", providerName);
            return null;
        }
        if (matches.size() > 1) {
            logger.warn("Multiple ({}) parameter providers found with name [{}] (ids={}); SECRET_REFERENCE providerId cannot be unambiguously resolved and will remain null",
                    matches.size(), providerName, matches);
            return null;
        }
        final String resolvedId = matches.getFirst();
        logger.debug("Resolved parameter provider name [{}] to id [{}]", providerName, resolvedId);
        return resolvedId;
    }
}
