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
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class StandardConnectorRepository implements ConnectorRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorRepository.class);

    private final Map<String, ConnectorNode> connectors = new ConcurrentHashMap<>();
    private final FlowEngine lifecycleExecutor = new FlowEngine(8, "NiFi Connector Lifecycle");

    private volatile ExtensionManager extensionManager;
    private volatile ConnectorRequestReplicator requestReplicator;
    private volatile SecretsManager secretsManager;
    private volatile ConnectorAssetRepository assetRepository;
    private volatile ConnectorConfigurationProvider configurationProvider;

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        logger.debug("Initializing ConnectorRepository");
        this.extensionManager = context.getExtensionManager();
        this.requestReplicator = context.getRequestReplicator();
        this.secretsManager = context.getSecretsManager();
        this.assetRepository = new StandardConnectorAssetRepository(context.getAssetManager());
        this.configurationProvider = context.getConnectorConfigurationProvider();
        logger.debug("Successfully initialized ConnectorRepository with configurationProvider={}", configurationProvider != null ? configurationProvider.getClass().getSimpleName() : "null");
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
        syncFromProvider(connector);
        connectors.put(connector.getIdentifier(), connector);
    }

    @Override
    public void restoreConnector(final ConnectorNode connector) {
        connectors.put(connector.getIdentifier(), connector);
        logger.debug("Successfully restored {}", connector);
    }

    @Override
    public void removeConnector(final String connectorId) {
        logger.debug("Removing {}", connectorId);
        final ConnectorNode connectorNode = connectors.get(connectorId);
        if (connectorNode == null) {
            throw new IllegalStateException("No connector found with ID " + connectorId);
        }

        connectorNode.verifyCanDelete();
        if (configurationProvider != null) {
            configurationProvider.delete(connectorId);
        }
        connectors.remove(connectorId);

        final Class<?> taskClass = connectorNode.getConnector().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskClass, connectorId)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, connectorNode.getConnector());
        }

        extensionManager.removeInstanceClassLoader(connectorId);
    }

    @Override
    public ConnectorNode getConnector(final String identifier) {
        final ConnectorNode connector = connectors.get(identifier);
        if (connector != null) {
            syncFromProvider(connector);
        }
        return connector;
    }

    @Override
    public List<ConnectorNode> getConnectors() {
        final List<ConnectorNode> connectorList = List.copyOf(connectors.values());
        for (final ConnectorNode connector : connectorList) {
            syncFromProvider(connector);
        }
        return connectorList;
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
        final ConnectorState initialDesiredState = connector.getDesiredState();
        logger.info("Applying update to Connector {}", connector);

        // Sync the working configuration from the external provider before applying the update,
        // so that the update is applied using the latest externally managed configuration.
        syncFromProvider(connector);

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
        final Set<String> referencedAssetIds = new HashSet<>();
        collectReferencedAssetIds(connector.getActiveFlowContext(), referencedAssetIds);
        collectReferencedAssetIds(connector.getWorkingFlowContext(), referencedAssetIds);

        logger.debug("Found {} assets referenced for Connector [{}]", referencedAssetIds.size(), connector.getIdentifier());

        final ConnectorAssetRepository assetRepository = getAssetRepository();
        final List<Asset> allConnectorAssets = assetRepository.getAssets(connector.getIdentifier());
        for (final Asset asset : allConnectorAssets) {
            final String assetId = asset.getIdentifier();
            if (!referencedAssetIds.contains(assetId)) {
                try {
                    logger.info("Deleting unreferenced asset [id={},name={}] for connector [{}]", assetId, asset.getName(), connector.getIdentifier());
                    assetRepository.deleteAsset(assetId);
                } catch (final Exception e) {
                    logger.warn("Unable to delete unreferenced asset [id={},name={}] for connector [{}]", assetId, asset.getName(), connector.getIdentifier(), e);
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
                if (valueReference instanceof AssetReference assetReference) {
                    referencedAssetIds.addAll(assetReference.getAssetIdentifiers());
                }
            }
        }
    }

    @Override
    public void updateConnector(final ConnectorNode connector, final String name) {
        if (configurationProvider != null) {
            final ConnectorWorkingConfiguration workingConfiguration = buildWorkingConfiguration(connector);
            workingConfiguration.setName(name);
            configurationProvider.save(connector.getIdentifier(), workingConfiguration);
        }
        connector.setName(name);
    }

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
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
            throw e;
        }
    }

    @Override
    public void discardWorkingConfiguration(final ConnectorNode connector) {
        if (configurationProvider != null) {
            configurationProvider.discard(connector.getIdentifier());
        }
        connector.discardWorkingConfiguration();
        cleanUpAssets(connector);
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
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
    public ConnectorAssetRepository getAssetRepository() {
        return assetRepository;
    }

    private void syncFromProvider(final ConnectorNode connector) {
        if (configurationProvider == null) {
            return;
        }

        final Optional<ConnectorWorkingConfiguration> externalConfig = configurationProvider.load(connector.getIdentifier());
        if (externalConfig.isEmpty()) {
            return;
        }

        final ConnectorWorkingConfiguration config = externalConfig.get();
        if (config.getName() != null) {
            connector.setName(config.getName());
        }

        final List<VersionedConfigurationStep> workingFlowConfiguration = config.getWorkingFlowConfiguration();
        if (workingFlowConfiguration != null) {
            final MutableConnectorConfigurationContext workingConfigContext = connector.getWorkingFlowContext().getConfigurationContext();
            for (final VersionedConfigurationStep step : workingFlowConfiguration) {
                final StepConfiguration stepConfiguration = toStepConfiguration(step);
                workingConfigContext.replaceProperties(step.getName(), stepConfiguration);
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
}
