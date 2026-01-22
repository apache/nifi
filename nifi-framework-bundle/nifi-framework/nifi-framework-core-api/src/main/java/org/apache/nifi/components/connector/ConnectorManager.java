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

import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;

import java.util.List;
import java.util.concurrent.Future;

/**
 * <p>
 * Internal interface that manages connector runtime operations and orchestrates
 * the pluggable {@link org.apache.nifi.components.connector.ConnectorRepository} and
 * {@link org.apache.nifi.components.connector.ConnectorLifecycleManager} implementations.
 * </p>
 *
 * <p>
 * This interface is internal to the NiFi framework and is not intended to be
 * implemented by third parties. Third parties should instead implement the
 * {@link org.apache.nifi.components.connector.ConnectorRepository} or
 * {@link org.apache.nifi.components.connector.ConnectorLifecycleManager} interfaces
 * in nifi-framework-api.
 * </p>
 */
public interface ConnectorManager {

    /**
     * Initializes the ConnectorManager with the given context.
     *
     * @param context the initialization context
     */
    void initialize(ConnectorManagerInitializationContext context);

    /**
     * Adds the given Connector to the Manager.
     *
     * @param connector the Connector to add
     */
    void addConnector(ConnectorNode connector);

    /**
     * Restores a previously added Connector to the Manager on restart.
     * This is differentiated from addConnector in that this method is not called
     * for newly created Connectors during the typical lifecycle of NiFi, but rather
     * only to notify the Manager of Connectors that were present when NiFi was last shutdown.
     *
     * @param connector the Connector to restore
     */
    void restoreConnector(ConnectorNode connector);

    /**
     * Removes the given Connector from the Manager.
     *
     * @param connectorId the identifier of the Connector to remove
     */
    void removeConnector(String connectorId);

    /**
     * Gets the Connector with the given identifier.
     *
     * @param identifier the identifier of the Connector to get
     * @return the Connector with the given identifier, or null if no such Connector exists
     */
    ConnectorNode getConnector(String identifier);

    /**
     * Returns all Connectors managed by this ConnectorManager.
     *
     * @return all Connectors
     */
    List<ConnectorNode> getConnectors();

    /**
     * Starts the given Connector, managing any appropriate lifecycle events.
     *
     * @param connector the Connector to start
     * @return a Future that will be completed when the Connector has started
     */
    Future<Void> startConnector(ConnectorNode connector);

    /**
     * Stops the given Connector, managing any appropriate lifecycle events.
     *
     * @param connector the Connector to stop
     * @return a Future that will be completed when the Connector has stopped
     */
    Future<Void> stopConnector(ConnectorNode connector);

    /**
     * Restarts the given Connector, managing any appropriate lifecycle events.
     *
     * @param connector the Connector to restart
     * @return a Future that will be completed when the Connector has restarted
     */
    Future<Void> restartConnector(ConnectorNode connector);

    /**
     * Configures a connector with the given step configuration.
     *
     * @param connector the Connector to configure
     * @param stepName the name of the configuration step
     * @param configuration the configuration for the step
     * @throws FlowUpdateException if the configuration cannot be applied
     */
    void configureConnector(ConnectorNode connector, String stepName, StepConfiguration configuration) throws FlowUpdateException;

    /**
     * Applies a pending update to a connector.
     *
     * @param connector the Connector to update
     * @param context the update context providing persistence callbacks
     * @throws FlowUpdateException if the update cannot be applied
     */
    void applyUpdate(ConnectorNode connector, ConnectorUpdateContext context) throws FlowUpdateException;

    /**
     * Inherits configuration from a versioned flow into a connector.
     *
     * @param connector the Connector to update
     * @param activeFlowConfiguration the active flow configuration to inherit
     * @param workingFlowConfiguration the working flow configuration to inherit
     * @param flowContextBundle the bundle associated with the configuration
     * @throws FlowUpdateException if the configuration cannot be inherited
     */
    void inheritConfiguration(ConnectorNode connector, List<VersionedConfigurationStep> activeFlowConfiguration,
                              List<VersionedConfigurationStep> workingFlowConfiguration, Bundle flowContextBundle) throws FlowUpdateException;

    /**
     * Discards the working configuration for a connector and reverts to the active configuration.
     *
     * @param connector the Connector to discard working configuration for
     */
    void discardWorkingConfiguration(ConnectorNode connector);

    /**
     * Returns the SecretsManager for accessing secrets.
     *
     * @return the SecretsManager
     */
    SecretsManager getSecretsManager();

    /**
     * Creates a new ConnectorStateTransition instance for managing the lifecycle state of a connector.
     *
     * @param type the connector type
     * @param id the connector identifier
     * @return a new ConnectorStateTransition instance
     */
    ConnectorStateTransition createStateTransition(String type, String id);

    /**
     * Creates a new builder for connector initialization contexts.
     *
     * @return a new initialization context builder
     */
    FrameworkConnectorInitializationContextBuilder createInitializationContextBuilder();

    /**
     * Returns the ConnectorAssetRepository for managing connector assets.
     *
     * @return the ConnectorAssetRepository
     */
    ConnectorAssetRepository getAssetRepository();
}
