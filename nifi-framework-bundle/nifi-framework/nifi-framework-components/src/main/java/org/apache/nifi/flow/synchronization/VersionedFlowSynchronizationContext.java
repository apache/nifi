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

package org.apache.nifi.flow.synchronization;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.ComponentScheduler;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class VersionedFlowSynchronizationContext {
    private final ComponentIdGenerator componentIdGenerator;
    private final FlowManager flowManager;
    private final ReloadComponent reloadComponent;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ExtensionManager extensionManager;
    private final ComponentScheduler componentScheduler;
    private final FlowMappingOptions flowMappingOptions;
    private final Function<ProcessorNode, ProcessContext> processContextFactory;
    private final Function<ComponentNode, ConfigurationContext> configurationContextFactory;
    private final AssetManager assetManager;


    private VersionedFlowSynchronizationContext(final Builder builder) {
        this.componentIdGenerator = builder.componentIdGenerator;
        this.flowManager = builder.flowManager;
        this.reloadComponent = builder.reloadComponent;
        this.controllerServiceProvider = builder.controllerServiceProvider;
        this.extensionManager = builder.extensionManager;
        this.componentScheduler = builder.componentScheduler;
        this.flowMappingOptions = builder.flowMappingOptions;
        this.processContextFactory = builder.processContextFactory;
        this.configurationContextFactory = builder.configurationContextFactory;
        this.assetManager = builder.assetManager;
    }

    public ComponentIdGenerator getComponentIdGenerator() {
        return componentIdGenerator;
    }

    public FlowManager getFlowManager() {
        return flowManager;
    }

    public ReloadComponent getReloadComponent() {
        return reloadComponent;
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }

    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    public ComponentScheduler getComponentScheduler() {
        return componentScheduler;
    }

    public FlowMappingOptions getFlowMappingOptions() {
        return flowMappingOptions;
    }

    public Function<ProcessorNode, ProcessContext> getProcessContextFactory() {
        return processContextFactory;
    }

    public Function<ComponentNode, ConfigurationContext> getConfigurationContextFactory() {
        return configurationContextFactory;
    }

    public AssetManager getAssetManager() {
        return assetManager;
    }

    public static class Builder {
        private ComponentIdGenerator componentIdGenerator;
        private FlowManager flowManager;
        private ReloadComponent reloadComponent;
        private ControllerServiceProvider controllerServiceProvider;
        private ExtensionManager extensionManager;
        private ComponentScheduler componentScheduler;
        private FlowMappingOptions flowMappingOptions;
        private Function<ProcessorNode, ProcessContext> processContextFactory;
        private Function<ComponentNode, ConfigurationContext> configurationContextFactory;
        private AssetManager assetManager;


        public Builder componentIdGenerator(final ComponentIdGenerator componentIdGenerator) {
            this.componentIdGenerator = componentIdGenerator;
            return this;
        }

        public Builder flowManager(final FlowManager flowManager) {
            this.flowManager = flowManager;
            return this;
        }

        public Builder reloadComponent(final ReloadComponent reloadComponent) {
            this.reloadComponent = reloadComponent;
            return this;
        }

        public Builder controllerServiceProvider(final ControllerServiceProvider provider) {
            this.controllerServiceProvider = provider;
            return this;
        }

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder componentScheduler(final ComponentScheduler scheduler) {
            this.componentScheduler = scheduler;
            return this;
        }

        public Builder flowMappingOptions(final FlowMappingOptions flowMappingOptions) {
            this.flowMappingOptions = flowMappingOptions;
            return this;
        }

        public Builder processContextFactory(final Function<ProcessorNode, ProcessContext> processContextFactory) {
            this.processContextFactory = processContextFactory;
            return this;
        }

        public Builder configurationContextFactory(final Function<ComponentNode, ConfigurationContext> configurationContextFactory) {
            this.configurationContextFactory = configurationContextFactory;
            return this;
        }

        public Builder assetManager(final AssetManager assetManager) {
            this.assetManager = assetManager;
            return this;
        }

        public VersionedFlowSynchronizationContext build() {
            requireNonNull(componentIdGenerator, "Component ID Generator must be set");
            requireNonNull(flowManager, "Flow Manager must be set");
            requireNonNull(reloadComponent, "Reload Component must be set");
            requireNonNull(controllerServiceProvider, "Controller Service Provider must be set");
            requireNonNull(extensionManager, "Extension Manager must be set");
            requireNonNull(componentScheduler, "Component Scheduler must be set");
            requireNonNull(flowMappingOptions, "Flow Mapping Options must be set");
            requireNonNull(processContextFactory, "Process Context Factory must be set");
            requireNonNull(configurationContextFactory, "Configuration Context Factory must be set");
            if (flowMappingOptions.isMapAssetReferences() && assetManager == null) {
                throw new IllegalStateException("Asset Manager must be set when Flow Mapping Options specified that Asset references must be mapped");
            }

            return new VersionedFlowSynchronizationContext(this);
        }
    }

}
