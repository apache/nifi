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
package org.apache.nifi;

import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.init.ConfigurableComponentInitializer;
import org.apache.nifi.init.ReflectionUtils;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.mock.MockConfigurationContext;
import org.apache.nifi.mock.MockFlowRegistryClientInitializationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientInitializationContext;
import org.apache.nifi.reporting.InitializationException;

/**
 * Initializes a FlowRegistryClient using a MockFlowRegistryClientInitializationContext;
 */
public class FlowRegistryClientInitializer implements ConfigurableComponentInitializer {

    private final ExtensionManager extensionManager;


    public FlowRegistryClientInitializer(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void initialize(final ConfigurableComponent component) throws InitializationException {
        FlowRegistryClient flowRegistryClient = (FlowRegistryClient) component;
        FlowRegistryClientInitializationContext context = new MockFlowRegistryClientInitializationContext();
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), context.getIdentifier())) {
            flowRegistryClient.initialize(context);
        }
    }

    @Override
    public void teardown(final ConfigurableComponent component) {
        FlowRegistryClient flowRegistryClient = (FlowRegistryClient) component;
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), component.getIdentifier())) {
            final MockConfigurationContext context = new MockConfigurationContext();
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, flowRegistryClient, new MockComponentLogger(), context);
        } finally {
            extensionManager.removeInstanceClassLoader(component.getIdentifier());
        }
    }
}
