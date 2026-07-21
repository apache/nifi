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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ClusterTopologyProvider;

import java.util.Set;

/**
 * Framework-only extension of {@link ConnectorMigrationContext} that exposes the managers needed to
 * execute migration and rollback logic.
 */
public interface FrameworkConnectorMigrationContext extends ConnectorMigrationContext {

    @Override
    FrameworkFlowContext getActiveFlowContext();

    AssetManager getSourceAssetManager();

    ConnectorRepository getConnectorRepository();

    StateManagerProvider getStateManagerProvider();

    ClusterTopologyProvider getClusterTopologyProvider();

    /**
     * Returns the identifiers of assets that the running migration attempt has copied into the
     * Connector asset namespace via {@link ConnectorMigrationContext#copyAssetFromSource(String)}.
     * Used by the framework to scope rollback so it deletes only assets created during the attempt
     * rather than every asset on the Connector.
     *
     * @return the identifiers of assets created during the current migration attempt
     */
    Set<String> getCopiedAssetIds();
}
