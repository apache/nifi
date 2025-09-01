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
package org.apache.nifi.controller.state.providers;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.annotation.StateProviderContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Managed State Provider encapsulates method invocations using NarClassLoader
 */
public class ManagedStateProvider implements StateProvider {
    private final StateProvider stateProvider;

    private ManagedStateProvider(final StateProvider provider) {
        this.stateProvider = Objects.requireNonNull(provider, "State Provider required");
    }

    /**
     * Create StateProvider from ExtensionManager
     *
     * @param extensionManager Extension Manager
     * @param type StateProvider Class
     * @param properties Application Properties
     * @return StateProvider
     */
    public static StateProvider create(final ExtensionManager extensionManager, final String type, final NiFiProperties properties) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> bundles = extensionManager.getBundles(type);
            if (bundles.isEmpty()) {
                throw new IllegalStateException("StateProvider Class [%s] not found".formatted(type));
            }
            if (bundles.size() > 1) {
                throw new IllegalStateException("Multiple bundles found containing StateProvider class [%s]".formatted(type));
            }

            final Bundle bundle = bundles.getFirst();
            final ClassLoader bundleClassLoader = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, bundleClassLoader);

            Thread.currentThread().setContextClassLoader(bundleClassLoader);
            final Class<? extends StateProvider> providerClass = rawClass.asSubclass(StateProvider.class);
            final StateProvider provider = providerClass.getDeclaredConstructor().newInstance();
            injectProperties(provider, providerClass, properties);

            return new ManagedStateProvider(provider);
        } catch (final Exception e) {
            throw new IllegalStateException("StateProvider Class [%s] creation failed".formatted(type), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static void injectProperties(
            final Object instance,
            final Class<?> stateProviderClass,
            final NiFiProperties properties
    ) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : stateProviderClass.getMethods()) {
            if (method.isAnnotationPresent(StateProviderContext.class)) {
                final Class<?>[] argumentTypes = method.getParameterTypes();

                if (argumentTypes.length == 1) {
                    final Class<?> argumentType = argumentTypes[0];

                    if (NiFiProperties.class.isAssignableFrom(argumentType)) {
                        method.invoke(instance, properties);
                    }
                }
            }
        }
    }

    @Override
    public void initialize(final StateProviderInitializationContext context) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.initialize(context);
        }
    }

    @Override
    public void shutdown() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.shutdown();
        }
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.setState(state, componentId);
        }
    }

    @Override
    public StateMap getState(final String componentId) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getState(componentId);
        }
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.replace(oldValue, newValue, componentId);
        }
    }

    @Override
    public void clear(final String componentId) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.clear(componentId);
        }
    }

    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.onComponentRemoved(componentId);
        }
    }

    @Override
    public void enable() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.enable();
        }
    }

    @Override
    public void disable() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.disable();
        }
    }

    @Override
    public boolean isEnabled() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.isEnabled();
        }
    }

    @Override
    public Scope[] getSupportedScopes() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getSupportedScopes();
        }
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.validate(context);
        }
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getPropertyDescriptor(name);
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            stateProvider.onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getPropertyDescriptors();
        }
    }

    @Override
    public String getIdentifier() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getIdentifier();
        }
    }

    @Override
    public boolean isComponentEnumerationSupported() {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.isComponentEnumerationSupported();
        }
    }

    @Override
    public Collection<String> getStoredComponentIds() throws IOException {
        try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
            return stateProvider.getStoredComponentIds();
        }
    }
}
