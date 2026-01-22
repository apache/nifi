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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Standard implementation of {@link ConnectorLifecycleManager}.
 * </p>
 *
 * <p>
 * Note: The current implementation provides state tracking and coordination utilities.
 * The actual lifecycle operations (start, stop, update) on ConnectorNode instances
 * are performed by the {@link StandardConnectorManager} because ConnectorNode is a
 * framework-internal type that cannot be exposed to the pluggable interface.
 * </p>
 *
 * <p>
 * Third-party implementations of ConnectorLifecycleManager can customize:
 * <ul>
 *     <li>State tracking and storage</li>
 *     <li>Cluster coordination during updates</li>
 *     <li>Custom lifecycle policies</li>
 * </ul>
 * </p>
 */
public class StandardConnectorLifecycleManager implements ConnectorLifecycleManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorLifecycleManager.class);

    private final Map<String, ConnectorState> connectorStates = new ConcurrentHashMap<>();
    private final Map<String, ConnectorState> desiredStates = new ConcurrentHashMap<>();

    private volatile ConnectorLifecycleContext context;
    private volatile ScheduledExecutorService executorService;

    @Override
    public void initialize(final ConnectorLifecycleContext context) throws IOException {
        this.context = context;
        this.executorService = context.getScheduledExecutorService();
        logger.info("Initialized StandardConnectorLifecycleManager");
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down StandardConnectorLifecycleManager");
        connectorStates.clear();
        desiredStates.clear();
        // Note: We don't shutdown the executorService here as it's owned by the context
    }

    @Override
    public Future<Void> start(final String connectorId, final ConnectorStateCallback stateCallback) throws ConnectorLifecycleException {
        logger.info("Lifecycle manager notified of start request for connector {}", connectorId);
        desiredStates.put(connectorId, ConnectorState.RUNNING);

        // The actual start operation is performed by StandardConnectorManager on the ConnectorNode.
        // This method tracks the state transition.
        return CompletableFuture.runAsync(() -> {
            try {
                final ConnectorState previousState = connectorStates.getOrDefault(connectorId, ConnectorState.STOPPED);
                stateCallback.onStateChange(connectorId, previousState, ConnectorState.STARTING);
                connectorStates.put(connectorId, ConnectorState.STARTING);
                // The actual start is handled by ConnectorManager; we just track state here
            } catch (final Exception e) {
                logger.error("Error during start state transition for connector {}", connectorId, e);
                stateCallback.onOperationFailed(connectorId, "start", e);
            }
        }, executorService);
    }

    @Override
    public Future<Void> stop(final String connectorId, final ConnectorStateCallback stateCallback) throws ConnectorLifecycleException {
        logger.info("Lifecycle manager notified of stop request for connector {}", connectorId);
        desiredStates.put(connectorId, ConnectorState.STOPPED);

        return CompletableFuture.runAsync(() -> {
            try {
                final ConnectorState previousState = connectorStates.getOrDefault(connectorId, ConnectorState.RUNNING);
                stateCallback.onStateChange(connectorId, previousState, ConnectorState.STOPPING);
                connectorStates.put(connectorId, ConnectorState.STOPPING);
            } catch (final Exception e) {
                logger.error("Error during stop state transition for connector {}", connectorId, e);
                stateCallback.onOperationFailed(connectorId, "stop", e);
            }
        }, executorService);
    }

    @Override
    public void prepareForUpdate(final String connectorId, final ConnectorStateCallback stateCallback) throws ConnectorLifecycleException {
        logger.info("Preparing connector {} for update", connectorId);
        final ConnectorState previousState = connectorStates.getOrDefault(connectorId, ConnectorState.STOPPED);
        stateCallback.onStateChange(connectorId, previousState, ConnectorState.PREPARING_FOR_UPDATE);
        connectorStates.put(connectorId, ConnectorState.PREPARING_FOR_UPDATE);
    }

    @Override
    public void applyUpdate(final String connectorId, final ConnectorUpdateCallback updateCallback,
                            final ConnectorStateCallback stateCallback) throws ConnectorLifecycleException {
        logger.info("Applying update to connector {}", connectorId);

        try {
            final ConnectorState previousState = connectorStates.getOrDefault(connectorId, ConnectorState.PREPARING_FOR_UPDATE);
            stateCallback.onStateChange(connectorId, previousState, ConnectorState.UPDATING);
            connectorStates.put(connectorId, ConnectorState.UPDATING);

            // Persist the update
            updateCallback.persist();

            stateCallback.onStateChange(connectorId, ConnectorState.UPDATING, ConnectorState.UPDATED);
            connectorStates.put(connectorId, ConnectorState.UPDATED);

            logger.info("Successfully applied update to connector {}", connectorId);
        } catch (final Exception e) {
            logger.error("Failed to apply update to connector {}", connectorId, e);
            abortUpdate(connectorId, e);
            throw new ConnectorLifecycleException("Failed to apply update to connector " + connectorId, e);
        }
    }

    @Override
    public void abortUpdate(final String connectorId, final Throwable cause) {
        logger.warn("Aborting update for connector {} due to: {}", connectorId, cause.getMessage());
        connectorStates.put(connectorId, ConnectorState.UPDATE_FAILED);
    }

    @Override
    public ConnectorState getState(final String connectorId) {
        return connectorStates.getOrDefault(connectorId, ConnectorState.STOPPED);
    }

    @Override
    public ConnectorState getDesiredState(final String connectorId) {
        return desiredStates.getOrDefault(connectorId, ConnectorState.STOPPED);
    }

    @Override
    public void setDesiredState(final String connectorId, final ConnectorState desiredState) {
        desiredStates.put(connectorId, desiredState);
        logger.debug("Set desired state for connector {} to {}", connectorId, desiredState);
    }

    /**
     * Updates the current state of a connector. Called by the framework when connector
     * state changes.
     *
     * @param connectorId the connector identifier
     * @param state the new state
     */
    public void updateState(final String connectorId, final ConnectorState state) {
        connectorStates.put(connectorId, state);
        logger.debug("Updated state for connector {} to {}", connectorId, state);
    }

    /**
     * Removes state tracking for a connector. Called when a connector is removed.
     *
     * @param connectorId the connector identifier
     */
    public void removeConnector(final String connectorId) {
        connectorStates.remove(connectorId);
        desiredStates.remove(connectorId);
        logger.debug("Removed state tracking for connector {}", connectorId);
    }
}
