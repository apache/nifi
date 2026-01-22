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
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.nar.ExtensionManager;

/**
 * Provides context information for initializing a {@link ConnectorManager}.
 * This is an internal interface that provides access to framework components
 * needed by the ConnectorManager implementation.
 */
public interface ConnectorManagerInitializationContext {

    /**
     * Returns the FlowManager for managing flow components.
     *
     * @return the FlowManager
     */
    FlowManager getFlowManager();

    /**
     * Returns the ExtensionManager for loading and managing extensions.
     *
     * @return the ExtensionManager
     */
    ExtensionManager getExtensionManager();

    /**
     * Returns the SecretsManager for accessing secrets.
     *
     * @return the SecretsManager
     */
    SecretsManager getSecretsManager();

    /**
     * Returns the AssetManager for managing assets.
     *
     * @return the AssetManager
     */
    AssetManager getAssetManager();

    /**
     * Returns the NodeTypeProvider for node cluster information.
     *
     * @return the NodeTypeProvider
     */
    NodeTypeProvider getNodeTypeProvider();

    /**
     * Returns the ConnectorRequestReplicator for cluster request replication.
     *
     * @return the ConnectorRequestReplicator
     */
    ConnectorRequestReplicator getRequestReplicator();

    /**
     * Returns the pluggable ConnectorRepository for connector persistence.
     * This is the extension point that third parties can implement for
     * custom persistence backends.
     *
     * @return the ConnectorRepository
     */
    ConnectorRepository getConnectorRepository();

    /**
     * Returns the pluggable ConnectorLifecycleManager for connector lifecycle management.
     * This is the extension point that third parties can implement for
     * custom lifecycle handling.
     *
     * @return the ConnectorLifecycleManager
     */
    ConnectorLifecycleManager getConnectorLifecycleManager();
}
