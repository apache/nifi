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

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;

import java.util.List;
import java.util.concurrent.Future;

public interface ConnectorRepository {

    void initialize(ConnectorRepositoryInitializationContext context);

    /**
     * Adds the given Connector to the Repository
     * @param connector the Connector to add
     */
    void addConnector(ConnectorNode connector);

    /**
     * Restores a previously added Connector to the Repository on restart.
     * This is differentiated from addConnector in that this method is not called
     * for newly created Connectors during the typical lifecycle of NiFi, but rather
     * only to notify the Repository of Connectors that were present when NiFi was last shutdown.
     *
     * @param connector the Connector to restore
     */
    void restoreConnector(ConnectorNode connector);

    /**
     * Removes the given Connector from the Repository
     * @param connectorId the identifier of the Connector to remove
     */
    void removeConnector(String connectorId);

    /**
     * Gets the Connector with the given identifier
     * @param identifier the identifier of the Connector to get
     * @return the Connector with the given identifier, or null if no such Connector exists
     */
    ConnectorNode getConnector(String identifier);

    /**
     * @return all Connectors in the Repository
     */
    List<ConnectorNode> getConnectors();

    /**
     * Starts the given Connector, managing any appropriate lifecycle events.
     * @param connector the Connector to start
     * @return a CompletableFuture that will be completed when the Connector has started
     */
    Future<Void> startConnector(ConnectorNode connector);

    /**
     * Stops the given Connector, managing any appropriate lifecycle events.
     * @param connector the Connector to stop
     * @return a CompletableFuture that will be completed when the Connector has stopped
     */
    Future<Void> stopConnector(ConnectorNode connector);

    void configureConnector(ConnectorNode connector, String stepName, List<PropertyGroupConfiguration> stepConfiguration) throws FlowUpdateException;

    void applyUpdate(ConnectorNode connector) throws FlowUpdateException;

    void inheritConfiguration(ConnectorNode connector, List<VersionedConfigurationStep> flowConfiguration, Bundle flowContextBundle) throws FlowUpdateException;

    /**
     * Creates a new ConnectorStateTransition instance for managing the lifecycle state of a connector.
     *
     * @param type the connector type
     * @param id the connector identifier
     * @return a new ConnectorStateTransition instance
     */
    ConnectorStateTransition createStateTransition(String type, String id);

    FrameworkConnectorInitializationContextBuilder createInitializationContextBuilder();

}
