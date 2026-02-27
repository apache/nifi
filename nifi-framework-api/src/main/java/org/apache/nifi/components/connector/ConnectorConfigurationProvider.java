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

import java.io.InputStream;
import java.util.Optional;

/**
 * Extension point interface for external management of Connector working configuration and assets.
 * Implementations allow a Connector's name, working flow configuration, and binary assets to be
 * persisted in an external store and to be externally modified.
 *
 * <p>When a ConnectorConfigurationProvider is configured, the framework will:
 * <ul>
 *   <li>On reads (e.g., getConnector): load configuration from the provider and override the in-memory working configuration</li>
 *   <li>On writes (e.g., configureConnector): save configuration to the provider before modifying the in-memory state</li>
 *   <li>On discard: notify the provider that the working configuration has been discarded</li>
 *   <li>On delete: notify the provider that the connector has been removed</li>
 * </ul>
 *
 * <p>Asset management: the provider owns the mapping between NiFi asset UUIDs and external asset
 * identifiers. The {@link #load} method must return {@link ConnectorWorkingConfiguration} with
 * NiFi UUIDs in {@code assetIds} fields. The {@link #save} method receives NiFi UUIDs and the
 * provider translates to external identifiers when persisting. The local state of assets
 * (NiFi UUID mapping and digest tracking) is managed by the provider using the
 * {@link org.apache.nifi.asset.AssetManager} provided via the initialization context.</p>
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

    /**
     * Stores an asset to the local {@link org.apache.nifi.asset.AssetManager} and to the external
     * store. The provider records the NiFi UUID to external identifier mapping in its local state.
     * If the external store upload fails, the provider must roll back the local asset and throw.
     *
     * @param connectorId the identifier of the connector that owns the asset
     * @param nifiUuid the NiFi-assigned UUID for this asset
     * @param assetName the filename of the asset (e.g., "postgresql-42.6.0.jar")
     * @param content the binary content of the asset
     * @throws java.io.IOException if the asset cannot be stored
     */
    void storeAsset(String connectorId, String nifiUuid, String assetName, InputStream content) throws java.io.IOException;

    /**
     * Deletes an asset from the local {@link org.apache.nifi.asset.AssetManager} and from the
     * external store. The provider uses the NiFi UUID to look up the external identifier from its
     * local state, then cleans up both stores and removes the mapping entry.
     *
     * @param connectorId the identifier of the connector that owns the asset
     * @param nifiUuid the NiFi-assigned UUID of the asset to delete
     */
    void deleteAsset(String connectorId, String nifiUuid);

    /**
     * Called when a connector update is requested (e.g., applying a committed configuration change)
     * to determine whether the framework should proceed with the standard internal update process
     * (stopping, re-configuring, and restarting the connector).
     *
     * <p>Returning {@code true} (the default) indicates the framework should proceed normally.</p>
     *
     * <p>Returning {@code false} indicates the framework should skip the update and return
     * immediately -- this is not a failure; the provider may have handled the update externally
     * by doing some bookkeeping logic and the provider may re-trigger the update process by starting
     * a new request to the nifi framework once it is ready to proceed. If the provider wants to fail
     * the request, it should throw a runtime exception instead.</p>
     *
     * @param connectorId the identifier of the connector to update
     * @return {@code true} if the framework should proceed with the standard update process,
     *         {@code false} if the framework should skip the update (no-op)
     */
    default boolean shouldApplyUpdate(final String connectorId) {
        return true;
    }

    /**
     * Ensures that local asset binaries are up to date with the external store. For each asset
     * tracked in the provider's local state, this method compares the external store's current
     * content digest to the last-known digest. If changed or missing locally, the binary is
     * downloaded via the {@link org.apache.nifi.asset.AssetManager} using a new UUID so that
     * any existing local file for a running connector is not overwritten. The local state file
     * is updated with the new UUID and digest. This method does not modify the external store's
     * configuration.
     *
     * <p>After this method returns, callers should invoke {@link #load} to obtain the updated
     * working configuration reflecting any new NiFi UUIDs assigned during sync.</p>
     *
     * @param connectorId the identifier of the connector whose assets should be synced
     */
    void syncAssets(String connectorId);
}
