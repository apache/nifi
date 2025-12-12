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
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.nar.ExtensionManager;

public class StandardConnectorRepoInitializationContext implements ConnectorRepositoryInitializationContext {
    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;
    private final SecretsManager secretsManager;
    private final NodeTypeProvider nodeTypeProvider;
    private final ConnectorRequestReplicator requestReplicator;

    public StandardConnectorRepoInitializationContext(final FlowManager flowManager,
                                                     final ExtensionManager extensionManager,
                                                     final SecretsManager secretsManager,
                                                     final NodeTypeProvider nodeTypeProvider,
                                                     final ConnectorRequestReplicator requestReplicator) {
        this.flowManager = flowManager;
        this.extensionManager = extensionManager;
        this.secretsManager = secretsManager;
        this.nodeTypeProvider = nodeTypeProvider;
        this.requestReplicator = requestReplicator;
    }

    @Override
    public FlowManager getFlowManager() {
        return flowManager;
    }

    @Override
    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }

    @Override
    public ConnectorRequestReplicator getRequestReplicator() {
        return requestReplicator;
    }
}
