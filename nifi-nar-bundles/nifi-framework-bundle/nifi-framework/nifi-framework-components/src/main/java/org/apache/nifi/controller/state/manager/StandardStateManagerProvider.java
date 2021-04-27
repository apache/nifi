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

package org.apache.nifi.controller.state.manager;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.ResourceReferenceFactory;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.annotation.StateProviderContext;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.state.ConfigParseException;
import org.apache.nifi.controller.state.StandardStateManager;
import org.apache.nifi.controller.state.StandardStateProviderInitializationContext;
import org.apache.nifi.controller.state.config.StateManagerConfiguration;
import org.apache.nifi.controller.state.config.StateProviderConfiguration;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardValidationContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StandardStateManagerProvider implements StateManagerProvider {
    private static final Logger logger = LoggerFactory.getLogger(StandardStateManagerProvider.class);

    private static StateManagerProvider provider;
    private static NiFiProperties nifiProperties;

    private final ConcurrentMap<String, StateManager> stateManagers = new ConcurrentHashMap<>();
    private final StateProvider localStateProvider;
    private final StateProvider clusterStateProvider;

    public StandardStateManagerProvider(final StateProvider localStateProvider, final StateProvider clusterStateProvider) {
        this.localStateProvider = localStateProvider;
        this.clusterStateProvider = clusterStateProvider;
    }

    protected StateProvider getLocalStateProvider() {
        return localStateProvider;
    }

    protected StateProvider getClusterStateProvider() {
        return clusterStateProvider;
    }

    public static synchronized StateManagerProvider create(final NiFiProperties properties, final VariableRegistry variableRegistry, final ExtensionManager extensionManager,
                                                           final ParameterLookup parameterLookup) throws ConfigParseException, IOException {
        nifiProperties = properties;

        if (provider != null) {
            return provider;
        }

        final StateProvider localProvider = createLocalStateProvider(properties,variableRegistry, extensionManager, parameterLookup);

        final StateProvider clusterProvider;
        if (properties.isNode()) {
            clusterProvider = createClusteredStateProvider(properties,variableRegistry, extensionManager, parameterLookup);
        } else {
            clusterProvider = null;
        }

        provider = new StandardStateManagerProvider(localProvider, clusterProvider);
        return provider;
    }

    public static synchronized void resetProvider() {
        provider = null;
    }

    private static StateProvider createLocalStateProvider(final NiFiProperties properties, final VariableRegistry variableRegistry, final ExtensionManager extensionManager,
                                                          final ParameterLookup parameterLookup) throws IOException, ConfigParseException {
        final File configFile = properties.getStateManagementConfigFile();
        return createStateProvider(configFile, Scope.LOCAL, properties, variableRegistry, extensionManager, parameterLookup);
    }

    private static StateProvider createClusteredStateProvider(final NiFiProperties properties, final VariableRegistry variableRegistry, final ExtensionManager extensionManager,
                                                              final ParameterLookup parameterLookup) throws IOException, ConfigParseException {
        final File configFile = properties.getStateManagementConfigFile();
        return createStateProvider(configFile, Scope.CLUSTER, properties, variableRegistry, extensionManager, parameterLookup);
    }

    private static StateProvider createStateProvider(final File configFile, final Scope scope, final NiFiProperties properties, final VariableRegistry variableRegistry,
                                                     final ExtensionManager extensionManager, final ParameterLookup parameterLookup) throws ConfigParseException, IOException {
        final String providerId;
        final String providerIdPropertyName;
        final String providerDescription;
        final String providerXmlElementName;
        final String oppositeScopeXmlElementName;

        switch (scope) {
            case CLUSTER:
                providerId = properties.getClusterStateProviderId();
                providerIdPropertyName = NiFiProperties.STATE_MANAGEMENT_CLUSTER_PROVIDER_ID;
                providerDescription = "Cluster State Provider";
                providerXmlElementName = "cluster-provider";
                oppositeScopeXmlElementName = "local-provider";
                break;
            case LOCAL:
                providerId = properties.getLocalStateProviderId();
                providerIdPropertyName = NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID;
                providerDescription = "Local State Provider";
                providerXmlElementName = "local-provider";
                oppositeScopeXmlElementName = "cluster-provider";
                break;
            default:
                throw new AssertionError("Attempted to create State Provider for unknown Scope: " + scope);
        }

        if (!configFile.exists()) {
            throw new IllegalStateException("Cannot create " + providerDescription + " because the State Management Configuration File " + configFile + " does not exist");
        }
        if (!configFile.canRead()) {
            throw new IllegalStateException("Cannot create " + providerDescription + " because the State Management Configuration File " + configFile + " cannot be read");
        }

        if (providerId == null) {
            if (scope == Scope.CLUSTER) {
                throw new IllegalStateException("Cannot create Cluster State Provider because the '" + providerIdPropertyName
                    + "' property is missing from the NiFi Properties file. In order to run NiFi in a cluster, the " + providerIdPropertyName
                    + " property must be configured in nifi.properties");
            }

            throw new IllegalStateException("Cannot create " + providerDescription + " because the '" + providerIdPropertyName
                + "' property is missing from the NiFi Properties file");
        }

        if (providerId.trim().isEmpty()) {
            throw new IllegalStateException("Cannot create " + providerDescription + " because the '" + providerIdPropertyName
                + "' property in the NiFi Properties file has no value set. This is a required property and must reference the identifier of one of the "
                + providerXmlElementName + " elements in the State Management Configuration File (" + configFile + ")");
        }

        final StateManagerConfiguration config = StateManagerConfiguration.parse(configFile);
        final StateProviderConfiguration providerConfig = config.getStateProviderConfiguration(providerId);
        if (providerConfig == null) {
            throw new IllegalStateException("Cannot create " + providerDescription + " because the '" + providerIdPropertyName
                + "' property in the NiFi Properties file is set to '" + providerId + "', but there is no " + providerXmlElementName
                + " entry in the State Management Configuration File (" + configFile + ") with this id");
        }

        if (providerConfig.getScope() != scope) {
            throw new IllegalStateException("Cannot create " + providerDescription + " because the '" + providerIdPropertyName
                + "' property in the NiFi Properties file is set to '" + providerId + "', but this id is assigned to a " + oppositeScopeXmlElementName
                + " entry in the State Management Configuration File (" + configFile + "), rather than a " + providerXmlElementName + " entry");
        }

        final String providerClassName = providerConfig.getClassName();

        final StateProvider provider;
        try {
            provider = instantiateStateProvider(extensionManager, providerClassName);
        } catch (final Exception e) {
            throw new RuntimeException("Cannot create " + providerDescription + " of type " + providerClassName, e);
        }

        if (!ArrayUtils.contains(provider.getSupportedScopes(), scope)) {
            throw new RuntimeException("Cannot use " + providerDescription + " ("+providerClassName+") as it only supports scope(s) " + ArrayUtils.toString(provider.getSupportedScopes()) + " but " +
                "instance"
                + " is configured to use scope " + scope);
        }

        final SSLContext sslContext;
        TlsConfiguration standardTlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);
        try {
            sslContext = SslContextFactory.createSslContext(standardTlsConfiguration);
        } catch (TlsException e) {
            logger.error("Encountered an error configuring TLS for state manager: ", e);
            throw new IllegalStateException("Error configuring TLS for state manager", e);
        }

        //create variable registry
        final ParameterParser parser = new ExpressionLanguageAwareParameterParser();
        final Map<PropertyDescriptor, PropertyValue> propertyMap = new HashMap<>();
        final Map<PropertyDescriptor, PropertyConfiguration> propertyStringMap = new HashMap<>();

        final ResourceReferenceFactory resourceReferenceFactory = new StandardResourceReferenceFactory();
        //set default configuration
        for (final PropertyDescriptor descriptor : provider.getPropertyDescriptors()) {
            final ResourceContext resourceContext = new StandardResourceContext(resourceReferenceFactory, descriptor);
            propertyMap.put(descriptor, new StandardPropertyValue(resourceContext, descriptor.getDefaultValue(),null, parameterLookup, variableRegistry));

            final ParameterTokenList references = parser.parseTokens(descriptor.getDefaultValue());
            final PropertyConfiguration configuration = new PropertyConfiguration(descriptor.getDefaultValue(), references, references.toReferenceList());

            propertyStringMap.put(descriptor, configuration);
        }

        //set properties from actual configuration
        for (final Map.Entry<String, String> entry : providerConfig.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = provider.getPropertyDescriptor(entry.getKey());

            final ParameterTokenList references = parser.parseTokens(entry.getValue());
            final PropertyConfiguration configuration = new PropertyConfiguration(entry.getValue(), references, references.toReferenceList());

            propertyStringMap.put(descriptor, configuration);
            final ResourceContext resourceContext = new StandardResourceContext(resourceReferenceFactory, descriptor);
            propertyMap.put(descriptor, new StandardPropertyValue(resourceContext, entry.getValue(),null, parameterLookup, variableRegistry));
        }

        final ComponentLog logger = new SimpleProcessLogger(providerId, provider);
        final StateProviderInitializationContext initContext = new StandardStateProviderInitializationContext(providerId, propertyMap, sslContext, logger);

        synchronized (provider) {
            provider.initialize(initContext);
        }

        final ValidationContext validationContext = new StandardValidationContext(null, propertyStringMap, null, null, null, variableRegistry, null);
        final Collection<ValidationResult> results = provider.validate(validationContext);
        final StringBuilder validationFailures = new StringBuilder();

        int invalidCount = 0;
        for (final ValidationResult result : results) {
            if (!result.isValid()) {
                validationFailures.append(result.toString()).append("\n");
                invalidCount++;
            }
        }

        if (invalidCount > 0) {
            throw new IllegalStateException("Could not initialize State Providers because the " + providerDescription + " is not valid. The following "
                + invalidCount + " Validation Errors occurred:\n" + validationFailures.toString() + "\nPlease check the configuration of the " + providerDescription + " with ID ["
                + providerId.trim() + "] in the file " + configFile.getAbsolutePath());
        }

        return provider;
    }

    // Inject NiFi Properties to state providers that use the StateProviderContext annotation
    private static void performMethodInjection(final Object instance, final Class stateProviderClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : stateProviderClass.getMethods()) {
            if (method.isAnnotationPresent(StateProviderContext.class)) {
                // make the method accessible
                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
                    final Class<?>[] argumentTypes = method.getParameterTypes();

                    // look for setters (single argument)
                    if (argumentTypes.length == 1) {
                        final Class<?> argumentType = argumentTypes[0];

                        // look for well known types
                        if (NiFiProperties.class.isAssignableFrom(argumentType)) {
                            // nifi properties injection
                            method.invoke(instance, nifiProperties);
                        }
                    }
                } finally {
                    method.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = stateProviderClass.getSuperclass();
        if (parentClass != null && StateProvider.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    private static StateProvider instantiateStateProvider(final ExtensionManager extensionManager, final String type) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> bundles = extensionManager.getBundles(type);
            if (bundles.size() == 0) {
                throw new IllegalStateException(String.format("The specified class '%s' is not known to this nifi.", type));
            }
            if (bundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple bundles found for the specified class '%s', only one is allowed.", type));
            }

            final Bundle bundle = bundles.get(0);
            final ClassLoader detectedClassLoaderForType = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForType);

            Thread.currentThread().setContextClassLoader(detectedClassLoaderForType);
            final Class<? extends StateProvider> mgrClass = rawClass.asSubclass(StateProvider.class);
            StateProvider provider = mgrClass.newInstance();
            try {
                performMethodInjection(provider, mgrClass);
            } catch (InvocationTargetException e) {
                logger.error(String.format("Failed to inject nifi.properties to the '%s' state provider.", type), e);
            }
            return withNarClassLoader(provider);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    /**
     * Wrap the provider so that all method calls set the context class loader to the NAR's class loader before
     * executing the actual provider.
     *
     * @param stateProvider the base provider to wrap
     * @return the wrapped provider
     */
    private static StateProvider withNarClassLoader(final StateProvider stateProvider) {
        return new StateProvider() {
            @Override
            public void initialize(StateProviderInitializationContext context) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.initialize(context);
                }
            }

            @Override
            public void shutdown() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.shutdown();
                }
            }

            @Override
            public void setState(Map<String, String> state, String componentId) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.setState(state, componentId);
                }
            }

            @Override
            public StateMap getState(String componentId) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.getState(componentId);
                }
            }

            @Override
            public boolean replace(StateMap oldValue, Map<String, String> newValue, String componentId) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.replace(oldValue, newValue, componentId);
                }
            }

            @Override
            public void clear(String componentId) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.clear(componentId);
                }
            }

            @Override
            public void onComponentRemoved(String componentId) throws IOException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.onComponentRemoved(componentId);
                }
            }

            @Override
            public void enable() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.enable();
                }
            }

            @Override
            public void disable() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.disable();
                }
            }

            @Override
            public boolean isEnabled() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.isEnabled();
                }
            }

            @Override
            public Scope[] getSupportedScopes() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.getSupportedScopes();
                }
            }

            @Override
            public Collection<ValidationResult> validate(ValidationContext context) {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.validate(context);
                }
            }

            @Override
            public PropertyDescriptor getPropertyDescriptor(String name) {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.getPropertyDescriptor(name);
                }
            }

            @Override
            public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    stateProvider.onPropertyModified(descriptor, oldValue, newValue);
                }
            }

            @Override
            public List<PropertyDescriptor> getPropertyDescriptors() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.getPropertyDescriptors();
                }
            }

            @Override
            public String getIdentifier() {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return stateProvider.getIdentifier();
                }
            }
        };
    }

    /**
     * Returns the State Manager that has been created for the given component ID, or <code>null</code> if none exists
     *
     * @return the StateManager that can be used by the component with the given ID, or <code>null</code> if none exists
     */
    @Override
    public synchronized StateManager getStateManager(final String componentId) {
        StateManager stateManager = stateManagers.get(componentId);
        if (stateManager != null) {
            return stateManager;
        }

        stateManager = new StandardStateManager(localStateProvider, clusterStateProvider, componentId);
        stateManagers.put(componentId, stateManager);
        return stateManager;
    }

    @Override
    public synchronized void shutdown() {
        localStateProvider.shutdown();
        if (clusterStateProvider != null) {
            clusterStateProvider.shutdown();
        }
    }

    @Override
    public void enableClusterProvider() {
        clusterStateProvider.enable();
    }

    @Override
    public void disableClusterProvider() {
        clusterStateProvider.disable();
    }

    @Override
    public void onComponentRemoved(final String componentId) {
        final StateManager mgr = stateManagers.remove(componentId);
        if (mgr == null) {
            return;
        }

        try {
            mgr.clear(Scope.CLUSTER);
        } catch (final Exception e) {
            logger.warn("Component with ID {} was removed from NiFi instance but failed to clear clustered state for the component", componentId, e);
        }

        try {
            mgr.clear(Scope.LOCAL);
        } catch (final Exception e) {
            logger.warn("Component with ID {} was removed from NiFi instance but failed to clear local state for the component", componentId, e);
        }

        try {
            localStateProvider.onComponentRemoved(componentId);
        } catch (final Exception e) {
            logger.warn("Component with ID {} was removed from NiFi instance but failed to cleanup resources used to maintain its local state", componentId, e);
        }

        if (clusterStateProvider != null) {
            try {
                clusterStateProvider.onComponentRemoved(componentId);
            } catch (final Exception e) {
                logger.warn("Component with ID {} was removed from NiFi instance but failed to cleanup resources used to maintain its clustered state", componentId, e);
            }
        }
    }
}
