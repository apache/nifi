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
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        this.extensionManager = context.getExtensionManager();
        this.requestReplicator = context.getRequestReplicator();
        this.secretsManager = context.getSecretsManager();
        this.assetRepository = new StandardConnectorAssetRepository(context.getAssetManager());
    }

    @Override
    public void addConnector(final ConnectorNode connector) {
        connectors.put(connector.getIdentifier(), connector);
    }

    @Override
    public void restoreConnector(final ConnectorNode connector) {
        addConnector(connector);
    }

    @Override
    public void removeConnector(final String connectorId) {
        final ConnectorNode connectorNode = connectors.get(connectorId);
        if (connectorNode == null) {
            throw new IllegalStateException("No connector found with ID " + connectorId);
        }

        connectorNode.verifyCanDelete();
        connectors.remove(connectorId);

        final Class<?> taskClass = connectorNode.getConnector().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskClass, connectorId)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, connectorNode.getConnector());
        }

        extensionManager.removeInstanceClassLoader(connectorId);
    }

    @Override
    public ConnectorNode getConnector(final String identifier) {
        return connectors.get(identifier);
    }

    @Override
    public List<ConnectorNode> getConnectors() {
        return List.copyOf(connectors.values());
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
        final ConnectorState initialDesiredState = connector.getDesiredState();
        logger.info("Applying update to Connector {}", connector);

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
                logger.info("Connector {} has been successfully updated; starting Connector to resume initial state", connector);
                connector.start(lifecycleExecutor);
            } else {
                logger.info("Connector {} has been successfully updated; stopping Connector to resume initial state", connector);
                connector.stop(lifecycleExecutor);
            }

            // We've updated the state of the connector so save flow again
            context.saveFlow();
        } catch (final Exception e) {
            logger.error("Failed to apply connector update for {}", connector, e);
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
        final FrameworkFlowContext activeFlowContext = connector.getActiveFlowContext();
        final ConnectorConfiguration activeConfiguration = activeFlowContext.getConfigurationContext().toConnectorConfiguration();

        final Set<String> referencedAssetIds = new HashSet<>();
        for (final NamedStepConfiguration namedStepConfiguration : activeConfiguration.getNamedStepConfigurations()) {
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

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        connector.setConfiguration(stepName, configuration);
    }

    @Override
    public void inheritConfiguration(final ConnectorNode connector, final List<VersionedConfigurationStep> activeFlowConfiguration,
                final List<VersionedConfigurationStep> workingFlowConfiguration, final Bundle flowContextBundle) throws FlowUpdateException {

        connector.transitionStateForUpdating();
        connector.prepareForUpdate();

        try {
            connector.inheritConfiguration(activeFlowConfiguration, workingFlowConfiguration, flowContextBundle);
        } catch (final Exception e) {
            connector.abortUpdate(e);
            throw e;
        }
    }

    @Override
    public void discardWorkingConfiguration(final ConnectorNode connector) {
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
}
