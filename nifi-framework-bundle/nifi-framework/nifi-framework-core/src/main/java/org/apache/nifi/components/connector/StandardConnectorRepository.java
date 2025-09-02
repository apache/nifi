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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public class StandardConnectorRepository implements ConnectorRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorRepository.class);

    private final Map<String, ConnectorNode> connectors = new HashMap<>();
    private final FlowEngine lifecycleExecutor = new FlowEngine(8, "NiFi Connector Lifecycle");

    private volatile ExtensionManager extensionManager;
    private volatile ConnectorRequestReplicator requestReplicator;

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        this.extensionManager = context.getExtensionManager();
        this.requestReplicator = context.getRequestReplicator();
    }

    @Override
    public synchronized void addConnector(final ConnectorNode connector) {
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
    public void applyUpdate(final ConnectorNode connector) throws FlowUpdateException {
        final ConnectorState initialDesiredState = connector.getDesiredState();

        // Perform whatever preparation is necessary for the update. Default implementation is to stop the connector.
        connector.prepareForUpdate();

        try {
            // Wait for Connector State to become UPDATING
            waitForState(connector, ConnectorState.UPDATING, Set.of(ConnectorState.PREPARING_FOR_UPDATE));

            // Apply the update to the connector.
            connector.applyUpdate();

            // Wait for Connector State to become UPDATED
            waitForState(connector, ConnectorState.UPDATED, Set.of(ConnectorState.UPDATING));

            // If the initial desired state was RUNNING, start the connector again. Otherwise, stop it.
            // We don't simply leave it be as the prepareForUpdate / update may have changed the state of some components.
            if (initialDesiredState == ConnectorState.RUNNING) {
                connector.start(lifecycleExecutor);
            } else {
                connector.stop(lifecycleExecutor);
            }
        } catch (final Exception e) {
            connector.abortUpdate(e);
        }
    }

    private void waitForState(final ConnectorNode connector, final ConnectorState desiredState, final Set<ConnectorState> allowableStates)
                throws FlowUpdateException, IOException, InterruptedException {

        // Wait for Connector State to become the desired state
        int iterations = 0;
        final long startNanos = System.nanoTime();
        while (true) {
            final ConnectorState clusterState = requestReplicator.getState(connector.getIdentifier());
            if (clusterState == desiredState) {
                logger.info("State for {} is now {}", connector, desiredState);
                break;
            } else if (allowableStates.contains(clusterState)) {
                final long elapsedSeconds = Duration.ofNanos(System.nanoTime() - startNanos).toSeconds();
                if (++iterations % 10 == 0) {
                    logger.info("Waiting for {} to transition to {}. Current state is {}; elapsed time = {} secs", connector, desiredState, clusterState, elapsedSeconds);
                } else {
                    logger.debug("Waiting for {} to transition to {}. Current state is {}; elapsed time = {} secs", connector, desiredState, clusterState, elapsedSeconds);
                }

                Thread.sleep(Duration.ofSeconds(1));
                continue;
            } else if (clusterState == ConnectorState.UPDATE_FAILED) {
                throw new FlowUpdateException("State of " + connector + " transitioned to UPDATE_FAILED while waiting for state " + desiredState);
            }

            throw new FlowUpdateException("While waiting for %s to transition to state of %s, connector transitioned to unexpected state: %s".formatted(connector, desiredState, clusterState) );
        }
    }

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final List<PropertyGroupConfiguration> stepConfiguration) throws FlowUpdateException {
        connector.setConfiguration(stepName, stepConfiguration);
    }

    @Override
    public void inheritConfiguration(final ConnectorNode connector, final List<VersionedConfigurationStep> flowConfiguration, final Bundle flowContextBundle) throws FlowUpdateException {
        connector.prepareForUpdate();

        try {
            connector.inheritConfiguration(flowConfiguration, flowContextBundle);
        } catch (final Exception e) {
            connector.abortUpdate(e);
            throw e;
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

}