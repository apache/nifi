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
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * Provides persistence for Connector state and configuration. Implementations of this interface
 * are responsible for storing and retrieving connector records, which include the connector's
 * metadata, desired state, and configuration.
 * </p>
 *
 * <p>
 * The default implementation embeds connector data within the NiFi flow file (flow.json.gz).
 * Third-party implementations may store connector data in external systems such as databases
 * or cloud object storage (e.g., S3).
 * </p>
 *
 * <p>
 * Implementations must be thread-safe.
 * </p>
 */
public interface ConnectorRepository {

    /**
     * Initializes the repository with the given context. This method is called once
     * before any other methods are called.
     *
     * @param context the initialization context providing configuration and dependencies
     * @throws IOException if an I/O error occurs during initialization
     */
    void initialize(ConnectorRepositoryContext context) throws IOException;

    /**
     * Shuts down the repository and releases any resources. After this method is called,
     * the repository may be initialized again via {@link #initialize(ConnectorRepositoryContext)}.
     */
    void shutdown();

    /**
     * Persists a connector record. If a record with the same identifier already exists,
     * it will be replaced.
     *
     * @param record the connector record to persist
     * @throws IOException if an I/O error occurs during persistence
     */
    void save(PersistedConnectorRecord record) throws IOException;

    /**
     * Loads all persisted connector records. This method is typically called during
     * NiFi startup to restore connectors.
     *
     * @return a list of all persisted connector records
     * @throws IOException if an I/O error occurs during loading
     */
    List<PersistedConnectorRecord> loadAll() throws IOException;

    /**
     * Loads a specific connector record by its identifier.
     *
     * @param connectorId the identifier of the connector to load
     * @return an Optional containing the connector record if found, or empty if not found
     * @throws IOException if an I/O error occurs during loading
     */
    Optional<PersistedConnectorRecord> load(String connectorId) throws IOException;

    /**
     * Deletes a connector record from the repository.
     *
     * @param connectorId the identifier of the connector to delete
     * @throws IOException if an I/O error occurs during deletion
     */
    void delete(String connectorId) throws IOException;

    /**
     * Indicates whether a connector with the given identifier exists in the repository.
     *
     * @param connectorId the identifier of the connector to check
     * @return true if a connector with the given identifier exists, false otherwise
     */
    boolean exists(String connectorId);

    /**
     * Performs a checkpoint operation, ensuring any buffered writes are persisted.
     * The default implementation does nothing. Implementations that buffer writes
     * should override this method to flush their buffers.
     *
     * @throws IOException if an I/O error occurs during checkpointing
     */
    default void checkpoint() throws IOException {
        // Default implementation does nothing
    }

    /**
     * Indicates whether this repository supports transactional operations.
     * If true, the framework may batch multiple operations into a single transaction.
     *
     * @return true if transactional operations are supported, false otherwise
     */
    default boolean supportsTransactions() {
        return false;
    }

    /**
     * <p>
     * Indicates whether this repository supports external modification of connector data.
     * External modification means that connector configuration (particularly the working
     * configuration) can be changed outside of the NiFi REST API, such as by directly
     * modifying files in S3 or records in a database.
     * </p>
     *
     * <p>
     * If this method returns {@code true}, the framework will:
     * <ul>
     *     <li>Load from the repository on every read to sync the working configuration</li>
     *     <li>Call {@link #prepareForUpdate(String)} before applying updates</li>
     *     <li>Call {@link #completeUpdate(String, boolean)} after updates complete</li>
     * </ul>
     * </p>
     *
     * <p>
     * The default implementation returns {@code false}, which is appropriate for the
     * {@code FlowJsonConnectorRepository} where NiFi controls the flow.json.gz file.
     * </p>
     *
     * @return true if external modification is supported, false otherwise
     */
    default boolean supportsExternalModification() {
        return false;
    }

    /**
     * <p>
     * Prepares the repository for an update operation on the specified connector.
     * This method is called before the connector enters the PREPARING_FOR_UPDATE state,
     * giving the repository a chance to:
     * </p>
     * <ul>
     *     <li>Ensure the working configuration is stable and won't change during the update</li>
     *     <li>Acquire any necessary locks on the external storage</li>
     *     <li>"Vote" on whether it's safe to proceed with the update</li>
     * </ul>
     *
     * <p>
     * If this method returns {@code false}, the update will be rejected and the connector
     * will remain in its current state. This typically results in a 409 Conflict HTTP response.
     * </p>
     *
     * <p>
     * The default implementation always returns {@code true}, which is appropriate for
     * repositories that don't support external modification.
     * </p>
     *
     * @param connectorId the identifier of the connector being updated
     * @return true if it's safe to proceed with the update, false to reject the update
     * @throws IOException if an I/O error occurs
     */
    default boolean prepareForUpdate(String connectorId) throws IOException {
        return true;
    }

    /**
     * <p>
     * Called after an update operation completes, whether successfully or not.
     * This allows the repository to release any locks acquired during
     * {@link #prepareForUpdate(String)} or perform other cleanup operations.
     * </p>
     *
     * <p>
     * The default implementation does nothing, which is appropriate for repositories
     * that don't support external modification.
     * </p>
     *
     * @param connectorId the identifier of the connector that was updated
     * @param success true if the update completed successfully, false if it failed
     * @throws IOException if an I/O error occurs during cleanup
     */
    default void completeUpdate(String connectorId, boolean success) throws IOException {
        // Default: no-op
    }
}
