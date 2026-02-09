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

import java.util.Optional;

/**
 * Extension point interface for external management of Connector working configuration.
 * Implementations of this interface allow a Connector's name and working flow configuration
 * to be persisted in an external store (such as a database) and to be externally modified.
 *
 * <p>When a ConnectorConfigurationProvider is configured, the framework will:
 * <ul>
 *   <li>On reads (e.g., getConnector): load configuration from the provider and override the in-memory working configuration</li>
 *   <li>On writes (e.g., configureConnector): save configuration to the provider before modifying the in-memory state</li>
 *   <li>On discard: notify the provider that the working configuration has been discarded</li>
 *   <li>On delete: notify the provider that the connector has been removed</li>
 * </ul>
 */
public interface ConnectorConfigurationProvider {

    /**
     * Initializes the ConnectorConfigurationProvider with the given context.
     *
     * @param context the initialization context providing configuration properties
     */
    void initialize(ConnectorConfigurationProviderInitializationContext context);

    /**
     * Loads the externally managed working configuration for the connector with the given identifier.
     *
     * @param connectorId the identifier of the connector
     * @return an Optional containing the working configuration if one exists in the external store,
     *         or an empty Optional if no external configuration exists (in which case the in-memory configuration is used)
     */
    Optional<ConnectorWorkingConfiguration> load(String connectorId);

    /**
     * Saves the working configuration for the connector with the given identifier to the external store.
     * This is called when working configuration properties are modified, such as during configureConnector
     * or when connector metadata (e.g., name) is updated.
     *
     * @param connectorId the identifier of the connector
     * @param configuration the working configuration to save
     */
    void save(String connectorId, ConnectorWorkingConfiguration configuration);

    /**
     * Notifies the provider that the working configuration for the given connector has been discarded
     * (i.e., reset to match the active configuration). This is semantically distinct from {@link #save}
     * because the external store may need to handle this differently, such as deleting the working copy
     * rather than overwriting it.
     *
     * @param connectorId the identifier of the connector whose working configuration was discarded
     */
    void discard(String connectorId);

    /**
     * Notifies the provider that the connector with the given identifier has been removed entirely.
     * The provider should clean up any stored configuration for this connector.
     *
     * @param connectorId the identifier of the connector that was removed
     */
    void delete(String connectorId);

    /**
     * Verifies that the provider can support creating a connector with the given identifier.
     * This is called before the connector is actually created, giving the provider an opportunity
     * to reject the operation (for example, if it has reached a capacity limit or the connector
     * already exists in the external store in an incompatible state).
     *
     * <p>If the provider cannot support the create operation, it should throw a
     * {@link ConnectorConfigurationProviderException}.</p>
     *
     * @param connectorId the identifier of the connector to be created
     */
    void verifyCreate(String connectorId);
}
