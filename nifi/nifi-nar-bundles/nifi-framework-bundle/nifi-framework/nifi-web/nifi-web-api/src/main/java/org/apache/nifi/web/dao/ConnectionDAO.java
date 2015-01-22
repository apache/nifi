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
package org.apache.nifi.web.dao;

import java.util.Set;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.web.api.dto.ConnectionDTO;

/**
 *
 */
public interface ConnectionDAO {

    /**
     * Gets the specified Connection.
     *
     * @param groupId
     * @param id The connection id
     * @return The connection
     */
    Connection getConnection(String groupId, String id);

    /**
     * Gets the connections for the specified source processor.
     *
     * @param groupId
     * @param processorId
     * @return
     */
    Set<Connection> getConnectionsForSource(String groupId, String processorId);

    /**
     * Determines if the specified connection exists.
     *
     * @param groupId
     * @param id
     * @return
     */
    boolean hasConnection(String groupId, String id);

    /**
     * Gets all of the connections.
     *
     * @param groupId
     * @return The connections
     */
    Set<Connection> getConnections(String groupId);

    /**
     * Creates a new Connection.
     *
     * @param groupId
     * @param connectionDTO The connection DTO
     * @return The connection
     */
    Connection createConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Verifies the create request can be processed.
     *
     * @param groupId
     * @param connectionDTO
     */
    void verifyCreate(String groupId, ConnectionDTO connectionDTO);

    /**
     * Verifies the update request can be processed.
     *
     * @param groupId
     * @param connectionDTO
     */
    void verifyUpdate(String groupId, ConnectionDTO connectionDTO);

    /**
     * Updates the specified Connection.
     *
     * @param groupId
     * @param connectionDTO The connection DTO
     * @return The connection
     */
    Connection updateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Verifies the delete request can be processed.
     *
     * @param groupId
     * @param id
     */
    void verifyDelete(String groupId, String id);

    /**
     * Deletes the specified Connection.
     *
     * @param groupId
     * @param id The id of the connection
     */
    void deleteConnection(String groupId, String id);
}
