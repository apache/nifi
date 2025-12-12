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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.FrameworkConnectorInitializationContextBuilder;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.FrameworkConnectorInitializationContext;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.logging.ComponentLog;

public class MockConnectorInitializationContext implements FrameworkConnectorInitializationContext {

    private final String identifier;
    private final String name;
    private final ComponentLog componentLog;
    private final SecretsManager secretsManager;
    private final AssetManager assetManager;

    private final MockExtensionMapper mockExtensionMapper;

    private MockConnectorInitializationContext(final Builder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.componentLog = builder.componentLog;
        this.secretsManager = builder.secretsManager;
        this.assetManager = builder.assetManager;
        this.mockExtensionMapper = builder.mockExtensionMapper;
    }


    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ComponentLog getLogger() {
        return componentLog;
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
    }

    @Override
    public AssetManager getAssetManager() {
        return assetManager;
    }

    @Override
    public void updateFlow(final FlowContext flowContext, final VersionedExternalFlow versionedExternalFlow) throws FlowUpdateException {
        if (!(flowContext instanceof final FrameworkFlowContext frameworkFlowContext)) {
            throw new IllegalArgumentException("FlowContext is not an instance provided by the framework");
        }

        replaceMocks(versionedExternalFlow.getFlowContents());

        // TODO: Probably should eliminate this method and instead move AssetManager to the FlowContext and add a method there
        //       to update the flow.
        frameworkFlowContext.updateFlow(versionedExternalFlow, assetManager);
    }

    private void replaceMocks(final VersionedProcessGroup group) {
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                mockExtensionMapper.mapProcessor(processor);
            }
        }

        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                replaceMocks(childGroup);
            }
        }
    }

    private void updateParameterContext(final VersionedProcessGroup group, final String parameterContextName) {
        group.setParameterContextName(parameterContextName);
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                updateParameterContext(childGroup, parameterContextName);
            }
        }
    }


    public static class Builder implements FrameworkConnectorInitializationContextBuilder {
        private final MockExtensionMapper mockExtensionMapper;
        private String identifier;
        private String name;
        private ComponentLog componentLog;
        private SecretsManager secretsManager;
        private AssetManager assetManager;

        public Builder(final MockExtensionMapper mockExtensionMapper) {
            this.mockExtensionMapper = mockExtensionMapper;
        }

        @Override
        public Builder identifier(final String identifier) {
            this.identifier = identifier;
            return this;
        }

        @Override
        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        @Override
        public Builder componentLog(final ComponentLog componentLog) {
            this.componentLog = componentLog;
            return this;
        }

        @Override
        public Builder secretsManager(final SecretsManager secretsManager) {
            this.secretsManager = secretsManager;
            return this;
        }

        @Override
        public Builder assetManager(final AssetManager assetManager) {
            this.assetManager = assetManager;
            return this;
        }

        @Override
        public MockConnectorInitializationContext build() {
            return new MockConnectorInitializationContext(this);
        }

    }
}
