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

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * <p>
 * Manages the runtime lifecycle of connectors, including starting, stopping, and
 * applying configuration updates. Implementations of this interface are responsible
 * for orchestrating the state transitions of connectors.
 * </p>
 *
 * <p>
 * The default implementation coordinates lifecycle operations with the cluster
 * and uses the {@link ConnectorRepository} for persistence. Third-party implementations
 * may provide custom lifecycle behavior.
 * </p>
 *
 * <p>
 * Implementations must be thread-safe.
 * </p>
 */
public interface ConnectorLifecycleManager {

    /**
     * Initializes the lifecycle manager with the given context. This method is called once
     * before any other methods are called.
     *
     * @param context the initialization context providing configuration and dependencies
     * @throws IOException if an I/O error occurs during initialization
     */
    void initialize(ConnectorLifecycleContext context) throws IOException;

    /**
     * Shuts down the lifecycle manager and releases any resources. After this method is called,
     * the manager may be initialized again via {@link #initialize(ConnectorLifecycleContext)}.
     */
    void shutdown();

    /**
     * Starts a connector, transitioning it from STOPPED to RUNNING state.
     *
     * @param connectorId the identifier of the connector to start
     * @param stateCallback callback to report state changes during the operation
     * @return a Future that completes when the connector has started
     * @throws ConnectorLifecycleException if the connector cannot be started
     */
    Future<Void> start(String connectorId, ConnectorStateCallback stateCallback) throws ConnectorLifecycleException;

    /**
     * Stops a connector, transitioning it from RUNNING to STOPPED state.
     *
     * @param connectorId the identifier of the connector to stop
     * @param stateCallback callback to report state changes during the operation
     * @return a Future that completes when the connector has stopped
     * @throws ConnectorLifecycleException if the connector cannot be stopped
     */
    Future<Void> stop(String connectorId, ConnectorStateCallback stateCallback) throws ConnectorLifecycleException;

    /**
     * Prepares a connector for a configuration update. This may involve stopping the connector,
     * draining queues, or other preparatory actions.
     *
     * @param connectorId the identifier of the connector to prepare
     * @param stateCallback callback to report state changes during the operation
     * @throws ConnectorLifecycleException if the connector cannot be prepared for update
     */
    void prepareForUpdate(String connectorId, ConnectorStateCallback stateCallback) throws ConnectorLifecycleException;

    /**
     * Applies a configuration update to a connector. This method coordinates the update
     * with the cluster (if applicable) and ensures the connector's state is properly
     * transitioned and persisted.
     *
     * @param connectorId the identifier of the connector to update
     * @param updateCallback callback for persistence and cluster coordination
     * @param stateCallback callback to report state changes during the operation
     * @throws ConnectorLifecycleException if the update cannot be applied
     */
    void applyUpdate(String connectorId, ConnectorUpdateCallback updateCallback,
                     ConnectorStateCallback stateCallback) throws ConnectorLifecycleException;

    /**
     * Aborts an in-progress update operation. This is called when an update fails
     * or is cancelled, allowing the lifecycle manager to clean up and restore
     * the connector to a stable state.
     *
     * @param connectorId the identifier of the connector
     * @param cause the reason for aborting the update
     */
    void abortUpdate(String connectorId, Throwable cause);

    /**
     * Gets the current state of a connector.
     *
     * @param connectorId the identifier of the connector
     * @return the current state of the connector
     * @throws IllegalArgumentException if no connector exists with the given identifier
     */
    ConnectorState getState(String connectorId);

    /**
     * Gets the desired state of a connector.
     *
     * @param connectorId the identifier of the connector
     * @return the desired state of the connector
     * @throws IllegalArgumentException if no connector exists with the given identifier
     */
    ConnectorState getDesiredState(String connectorId);

    /**
     * Sets the desired state of a connector. The lifecycle manager will work to
     * transition the connector to the desired state.
     *
     * @param connectorId the identifier of the connector
     * @param desiredState the desired state
     * @throws IllegalArgumentException if no connector exists with the given identifier
     */
    void setDesiredState(String connectorId, ConnectorState desiredState);
}
