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
package org.apache.nifi.init;

import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.mock.MockConfigurationContext;
import org.apache.nifi.mock.MockParameterProviderInitializationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterProviderInitializationContext;
import org.apache.nifi.reporting.InitializationException;

/**
 * Initializes a ParameterProvider using a MockReportingInitializationContext;
 *
 *
 */
public class ParameterProviderInitializer implements ConfigurableComponentInitializer {

    private final ExtensionManager extensionManager;

    public ParameterProviderInitializer(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void initialize(final ConfigurableComponent component) throws InitializationException {
        ParameterProvider parameterProvider = (ParameterProvider) component;
        ParameterProviderInitializationContext context = new MockParameterProviderInitializationContext();
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), context.getIdentifier())) {
            parameterProvider.initialize(context);
        }
    }

    @Override
    public void teardown(final ConfigurableComponent component) {
        ParameterProvider parameterProvider = (ParameterProvider) component;
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), component.getIdentifier())) {

            final MockConfigurationContext context = new MockConfigurationContext();
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, parameterProvider, new MockComponentLogger(), context);
        } finally {
            extensionManager.removeInstanceClassLoader(component.getIdentifier());
        }
    }
}
