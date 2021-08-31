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
package org.apache.nifi.controller.parameter;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.SensitiveParameterProvider;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractParameterProviderNode extends AbstractComponentNode implements ParameterProviderNode {

    private final AtomicReference<ParameterProviderDetails> parameterProviderRef;
    private final ControllerServiceLookup serviceLookup;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ParameterContext> referencingParameterContexts;

    private final Map<ParameterDescriptor, Parameter> fetchedParameters = new LinkedHashMap<>();

    private volatile String comment;

    public AbstractParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent,
                                         final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(parameterProvider, id, controllerServiceProvider, validationContextFactory,
                parameterProvider.getComponent().getClass().getSimpleName(), parameterProvider.getComponent().getClass().getCanonicalName(),
                variableRegistry, reloadComponent, extensionManager, validationTrigger, false);
    }

    public AbstractParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                                         final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger,
                                         final boolean isExtensionMissing) {

        super(id, validationContextFactory, controllerServiceProvider, componentType, componentCanonicalClass, variableRegistry, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.parameterProviderRef = new AtomicReference<>(new ParameterProviderDetails(parameterProvider));
        this.serviceLookup = controllerServiceProvider;
        this.referencingParameterContexts = new HashSet<>();
    }

    @Override
    public ConfigurableComponent getComponent() {
        return parameterProviderRef.get().getParameterProvider();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return parameterProviderRef.get().getBundleCoordinate();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return parameterProviderRef.get().getComponentLog();
    }

    @Override
    public ParameterProvider getParameterProvider() {
        return parameterProviderRef.get().getParameterProvider();
    }

    @Override
    public void setParameterProvider(final LoggableComponent<ParameterProvider> parameterProvider) {
        this.parameterProviderRef.set(new ParameterProviderDetails(parameterProvider));
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws ParameterProviderInstantiationException {
        final String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
        getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
    }

    @Override
    public boolean isValidationNecessary() {
        return getValidationStatus() != ValidationStatus.VALID;
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup, null, getVariableRegistry());
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {

    }

    @Override
    public String getComments() {
        return comment;
    }

    @Override
    public void setComments(final String comment) {
        this.comment = CharacterFilterUtils.filterInvalidXmlCharacters(comment);
    }

    @Override
    public void verifyCanClearState() {

    }

    @Override
    public boolean isSensitiveParameterProvider() {
        return parameterProviderRef.get().getParameterProvider() instanceof SensitiveParameterProvider;
    }

    @Override
    public String toString() {
        return "ParameterProvider[id=" + getIdentifier() + "]";
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return ParameterLookup.EMPTY;
    }
    @Override
    public Set<ParameterContext> getReferences() {
        readLock.lock();
        try {
            return Collections.unmodifiableSet(referencingParameterContexts);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addReference(final ParameterContext parameterContext) {
        writeLock.lock();
        try {
            referencingParameterContexts.add(parameterContext);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeReference(final ParameterContext parameterContext) {
        writeLock.lock();
        try {
            referencingParameterContexts.remove(parameterContext);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void verifyCanFetchParameters() {
        final ValidationStatus validationStatus = performValidation();
        if (validationStatus != ValidationStatus.VALID) {
            throw new IllegalStateException(String.format("Parameter Provider [%s] cannot fetch parameters while validation state is %s",
                    getIdentifier(), validationStatus));
        }
    }

    @Override
    public void fetchParameters() {
        final ParameterProvider parameterProvider = parameterProviderRef.get().getParameterProvider();
        final ConfigurationContext configurationContext = getConfigurationContext();
        Map<ParameterDescriptor, Parameter> fetchedParameters;
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), parameterProvider.getClass(), parameterProvider.getIdentifier())) {
            fetchedParameters = parameterProvider.fetchParameters(configurationContext);
        }

        for(final Map.Entry<ParameterDescriptor, Parameter> entry : fetchedParameters.entrySet()) {
            final ParameterDescriptor descriptor = entry.getKey();
            if (descriptor.isSensitive() != isSensitiveParameterProvider()) {
                throw new IllegalStateException(String.format("Fetched parameter [%s] does not match the sensitivity of Parameter Provider [%s]",
                        descriptor.getName(), configurationContext.getName()));
            }
        }

        writeLock.lock();
        try {
            this.fetchedParameters.clear();
            this.fetchedParameters.putAll(toProvidedParameters(fetchedParameters));
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void verifyCanApplyParameters(final Set<String> parameterNames) {
        if (fetchedParameters.isEmpty()) {
            throw new IllegalStateException("No parameters have been fetched from Parameter Provider " + getName());
        }
        readLock.lock();
        try {
            for (final ParameterContext parameterContext : getReferences()) {
                parameterContext.verifyCanSetParameters(getFetchedParameterUpdateMap(parameterContext, parameterNames));
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDelete() {
        try {
            for (final ParameterContext parameterContext : getReferences()) {
                parameterContext.getSensitiveParameterProvider().ifPresent(sensitiveProvider -> {
                    if (getParameterProvider().getIdentifier().equals(sensitiveProvider.getIdentifier())) {
                        parameterContext.verifyCanSetSensitiveParameterProvider(null);
                    }
                });
                parameterContext.getNonSensitiveParameterProvider().ifPresent(nonSensitiveProvider -> {
                    if (getParameterProvider().getIdentifier().equals(nonSensitiveProvider.getIdentifier())) {
                        parameterContext.verifyCanSetNonSensitiveParameterProvider(null);
                    }
                });
            }
        } catch (final IllegalStateException e) {
            throw new IllegalStateException(String.format("Cannot delete Parameter Provider [%s] due to: %s", getIdentifier(), e.getMessage()), e);
        }
    }

    /**
     * Using the existing parameters from the ParameterContext and the fetched parameters in the provider, constructs
     * a map from Parameter name to updated Parameter (or null if the parameter has been removed in the fetch).
     * @param parameterContext A ParameterContext
     * @return A map from name to Parameter (or null if parameter should be removed)
     */
    private Map<String, Parameter> getFetchedParameterUpdateMap(final ParameterContext parameterContext, final Set<String> parameterNames) {
        final Set<String> parameterNameFilter = parameterNames == null ? new HashSet<>() : parameterNames;
        final Map<String, Parameter> parameterUpdateMap = new HashMap<>();
        final Map<ParameterDescriptor, Parameter> filteredFetchedParameters = fetchedParameters.entrySet()
                .stream()
                .filter(entry -> parameterNameFilter.contains(entry.getKey().getName()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final boolean isSensitive = isSensitiveParameterProvider();
        final Map<ParameterDescriptor, Parameter> currentParameters = parameterContext.getParameters();
        // Find parameters of the same sensitivity that were removed
        currentParameters.entrySet().stream()
                .filter(entry -> entry.getValue().getDescriptor().isSensitive() == isSensitive)
                .forEach(entry -> {
                    final ParameterDescriptor descriptor = entry.getKey();
                    if (!filteredFetchedParameters.containsKey(descriptor)) {
                        parameterUpdateMap.put(descriptor.getName(), null);
                    }
                });
        // Add all changed and new parameters of the same sensitivity
        for(final Map.Entry<ParameterDescriptor, Parameter> entry : filteredFetchedParameters.entrySet()) {
            final ParameterDescriptor descriptor = entry.getKey();
            final Parameter fetchedParameter = entry.getValue();
            final Parameter currentParameter = currentParameters.get(descriptor);
            if (currentParameter == null) {
                // Add if it's a new parameter
                parameterUpdateMap.put(descriptor.getName(), fetchedParameter);
            } else if (currentParameter.getDescriptor().isSensitive() != isSensitive) {
                throw new IllegalStateException(String.format(
                        "Parameter [%s] is provided by both a Sensitive and a Non-Sensitive Parameter Provider, which is not permitted",
                        descriptor.getName()));
            } else if (!currentParameter.getValue().equals(fetchedParameter.getValue())) {
                // Also add if it's an existing parameter of the same sensitivity that has a changed value
                parameterUpdateMap.put(descriptor.getName(), fetchedParameter);
            }
        }
        return parameterUpdateMap;
    }

    /**
     * Sets provided = true on all parameters in the map
     * @param parameterDescriptorMap A map from descriptor to parameter
     * @return An equivalent map, but with provided = true
     */
    private static Map<ParameterDescriptor, Parameter> toProvidedParameters(final Map<ParameterDescriptor, Parameter> parameterDescriptorMap) {
        return parameterDescriptorMap == null ? Collections.emptyMap() : parameterDescriptorMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new Parameter(entry.getValue().getDescriptor(), entry.getValue().getValue(), null, true)
                ));
    }

    @Override
    public Set<String> getFetchedParameterNames() {
        return fetchedParameters.keySet().stream().map(ParameterDescriptor::getName).collect(Collectors.toSet());
    }

    @Override
    public Map<ParameterContext, Map<String, Parameter>> getFetchedParametersToApply(final Set<String> parameterNames) {
        readLock.lock();
        try {
            return getReferences().stream()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            parameterContext -> getFetchedParameterUpdateMap(parameterContext, parameterNames)
                    ));
        } finally {
            readLock.unlock();
        }
    }
}
