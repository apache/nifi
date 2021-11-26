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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ParameterProviderUsageReference;
import org.apache.nifi.controller.ParametersApplication;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroupKey;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.parameter.ProvidedParameterGroup;
import org.apache.nifi.parameter.ProvidedParameterNameGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardParameterProviderNode extends AbstractComponentNode implements ParameterProviderNode {

    private final AtomicReference<ParameterProviderDetails> parameterProviderRef;
    private final ControllerServiceLookup serviceLookup;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ParameterProviderUsageReference> referencingParameterContexts;

    private final List<ProvidedParameterGroup> fetchedParameterGroups = new ArrayList<>();

    private volatile String comment;

    private final Authorizable parentAuthorizable;

    public StandardParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id, final Authorizable parentAuthorizable,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final ComponentVariableRegistry variableRegistry, final ReloadComponent reloadComponent, final ExtensionManager extensionManager,
                                         final ValidationTrigger validationTrigger) {

        this(parameterProvider, id, parentAuthorizable, controllerServiceProvider, validationContextFactory,
                parameterProvider.getComponent().getClass().getSimpleName(), parameterProvider.getComponent().getClass().getCanonicalName(),
                variableRegistry, reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id, final Authorizable parentAuthorizable,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final String componentType, final String canonicalClassName, final ComponentVariableRegistry variableRegistry,
                                         final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {
        super(id, validationContextFactory, controllerServiceProvider, componentType, canonicalClassName, variableRegistry, reloadComponent,
                extensionManager, validationTrigger, isExtensionMissing);
        this.parameterProviderRef = new AtomicReference<>(new ParameterProviderDetails(parameterProvider));
        this.serviceLookup = controllerServiceProvider;
        this.referencingParameterContexts = new HashSet<>();
        this.parentAuthorizable = parentAuthorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return parentAuthorizable;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ParameterProvider, getIdentifier(), getName());
    }

    @Override
    public boolean isRestricted() {
        return getParameterProvider().getClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public Class<?> getComponentClass() {
        return getParameterProvider().getClass();
    }

    @Override
    public boolean isDeprecated() {
        return getParameterProvider().getClass().isAnnotationPresent(DeprecationNotice.class);
    }

    @Override
    protected ParameterContext getParameterContext() {
        return null;
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
        return true;
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
    public Set<ParameterProviderUsageReference> getReferences() {
        readLock.lock();
        try {
            return Collections.unmodifiableSet(referencingParameterContexts);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addReference(final ParameterProviderUsageReference reference) {
        writeLock.lock();
        try {
            referencingParameterContexts.add(reference);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeReference(final ParameterProviderUsageReference reference) {
        writeLock.lock();
        try {
            referencingParameterContexts.remove(reference);
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
        List<ProvidedParameterGroup> fetchedParameterGroups;
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getExtensionManager(), parameterProvider.getClass(), parameterProvider.getIdentifier())) {
            fetchedParameterGroups = parameterProvider.fetchParameters(configurationContext);
        } catch (final IOException e) {
            throw new RuntimeException(String.format("Error fetching parameters for Parameter Provider [%s]", getName()), e);
        }

        if (fetchedParameterGroups == null || fetchedParameterGroups.isEmpty()) {
            return;
        }


        writeLock.lock();
        try {
            this.fetchedParameterGroups.clear();
            for (final ProvidedParameterGroup group : fetchedParameterGroups) {
                final Collection<Parameter> parameters = group.getItems();

                if (parameters == null) {
                    continue;
                }
                for (final Parameter parameter : parameters) {
                    final ParameterDescriptor descriptor = parameter.getDescriptor();
                    if (descriptor == null) {
                        throw new IllegalStateException("All fetched parameters require a ParameterDescriptor");
                    }
                }
                this.fetchedParameterGroups.add(new ProvidedParameterGroup(group.getGroupKey().getGroupName(), group.getGroupKey().getSensitivity(), toProvidedParameters(parameters)));
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void verifyCanApplyParameters(final Collection<ProvidedParameterNameGroup> parameterNames) {
        if (fetchedParameterGroups.isEmpty()) {
            return;
        }
        readLock.lock();
        try {
            for (final ParameterProviderUsageReference reference : getReferences()) {
                reference.getParameterContext().verifyCanSetParameters(getFetchedParameterUpdateMap(reference, parameterNames));
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDelete() {
        try {
            for (final ParameterProviderUsageReference reference : getReferences()) {
                final ParameterContext parameterContext = reference.getParameterContext();
                if (reference.getSensitivity() == ParameterSensitivity.SENSITIVE) {
                    parameterContext.verifyCanSetSensitiveParameterProvider(null);
                } else {
                    parameterContext.verifyCanSetNonSensitiveParameterProvider(null);
                }
            }
        } catch (final IllegalStateException e) {
            throw new IllegalStateException(String.format("Cannot delete Parameter Provider [%s] due to: %s", getIdentifier(), e.getMessage()), e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verifyConfiguration(final ConfigurationContext context, final ComponentLog logger, final ExtensionManager extensionManager) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final long startNanos = System.nanoTime();
            // Call super's verifyConfig, which will perform component validation
            results.addAll(super.verifyConfig(context.getProperties(), context.getAnnotationData(), null));
            final long validationComplete = System.nanoTime();

            // If any invalid outcomes from validation, we do not want to perform additional verification, because we only run additional verification when the component is valid.
            // This is done in order to make it much simpler to develop these verifications, since the developer doesn't have to worry about whether or not the given values are valid.
            if (!results.isEmpty() && results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED)) {
                return results;
            }

            final ParameterProvider parameterProvider = getParameterProvider();
            if (parameterProvider instanceof VerifiableParameterProvider) {
                logger.debug("{} is a VerifiableParameterProvider. Will perform full verification of configuration.", this);
                final VerifiableParameterProvider verifiable = (VerifiableParameterProvider) parameterProvider;

                // Check if the given configuration requires a different classloader than the current configuration
                final boolean classpathDifferent = isClasspathDifferent(context.getProperties());

                if (classpathDifferent) {
                    // Create a classloader for the given configuration and use that to verify the component's configuration
                    final Bundle bundle = extensionManager.getBundle(getBundleCoordinate());
                    final Set<URL> classpathUrls = getAdditionalClasspathResources(context.getProperties().keySet(), descriptor -> context.getProperty(descriptor).getValue());

                    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                    try (final InstanceClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(getComponentType(), getIdentifier(), bundle, classpathUrls, false)) {
                        Thread.currentThread().setContextClassLoader(detectedClassLoader);
                        results.addAll(verifiable.verify(context, logger));
                    } finally {
                        Thread.currentThread().setContextClassLoader(currentClassLoader);
                    }
                } else {
                    // Verify the configuration, using the component's classloader
                    try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, parameterProvider.getClass(), getIdentifier())) {
                        results.addAll(verifiable.verify(context, logger));
                    }
                }

                final long validationNanos = validationComplete - startNanos;
                final long verificationNanos = System.nanoTime() - validationComplete;
                logger.debug("{} completed full configuration validation in {} plus {} for validation",
                        this, FormatUtils.formatNanos(verificationNanos, false), FormatUtils.formatNanos(validationNanos, false));
            } else {
                logger.debug("{} is not a VerifiableParameterProvider, so will not perform full verification of configuration. Validation took {}", this,
                        FormatUtils.formatNanos(validationComplete - startNanos, false));
            }
        } catch (final Throwable t) {
            logger.error("Failed to perform verification of Parameter Provider's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                    .outcome(Outcome.FAILED)
                    .verificationStepName("Perform Verification")
                    .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                    .build());
        }

        return results;
    }

    /**
     * Using the existing parameters from the ParameterContext and the fetched parameters in the provider, constructs
     * a map from Parameter name to updated Parameter (or null if the parameter has been removed in the fetch).
     * @param reference A ParameterProviderUsageReference
     * @param parameterNameGroups only parameter names in this list are selected for each matching fetched group
     * @return A map from name to Parameter (or null if parameter should be removed)
     */
    private Map<String, Parameter> getFetchedParameterUpdateMap(final ParameterProviderUsageReference reference,
                                                                final Collection<ProvidedParameterNameGroup> parameterNameGroups) {
        final ParameterContext parameterContext = reference.getParameterContext();
        final Map<String, Parameter> parameterUpdateMap = new HashMap<>();

        // Collect all parameter names whose group key matches the reference.  This will be used as a filter on the fetched parameters
        final Set<String> parameterNameFilter = new HashSet<>();
        for (final ProvidedParameterNameGroup parameterNameGroup : parameterNameGroups) {
            final ParameterGroupKey groupKey = parameterNameGroup.getGroupKey();

            if (!reference.matchesParameterGroup(groupKey)) {
                continue;
            }
            parameterNameFilter.addAll(parameterNameGroup.getItems());
        }

        // Collect all parameters whose group key matches the reference and the parameter name filter
        final Map<ParameterDescriptor, Parameter> relevantParameters = new HashMap<>();
        final List<ProvidedParameterGroup> matchingParameterGroups = fetchedParameterGroups.stream()
                .filter(group -> reference.matchesParameterGroup(group.getGroupKey()))
                .collect(Collectors.toList());

        for (final ProvidedParameterGroup parameterGroup : matchingParameterGroups) {
            final ParameterGroupKey groupKey = parameterGroup.getGroupKey();

            for (final Parameter parameter : parameterGroup.getItems()) {
                final ParameterDescriptor descriptor = parameter.getDescriptor();
                final Parameter existingParameter = relevantParameters.get(descriptor);
                if (existingParameter != null) {
                    if (!StringUtils.equals(existingParameter.getValue(), parameter.getValue())) {
                        final String valueComparison = reference.getSensitivity() == ParameterSensitivity.NON_SENSITIVE
                                ? String.format(": %s and %s", existingParameter.getValue(), parameter.getValue()) : "";
                        throw new RuntimeException(String.format("Parameter [%s] was fetched in group [%s] with two different values%s",
                                descriptor.getName(), groupKey, valueComparison));
                    }
                } else if (parameterNameFilter.isEmpty() || parameterNameFilter.contains(descriptor.getName())) {
                    relevantParameters.put(descriptor, parameter);
                }
            }
        }

        if (matchingParameterGroups.isEmpty()) {
            return parameterUpdateMap;
        }

        // Get a filtered list of the parameters with their sensitivity set based on the reference type and with
        // names mapped according to the configuration context
        final List<Parameter> filteredParameters = filterAndMapParameters(relevantParameters.values(), reference.getSensitivity(), parameterNameFilter);

        final Map<ParameterDescriptor, Parameter> fetchedParameterMap = filteredParameters.stream()
                .collect(Collectors.toMap(Parameter::getDescriptor, Function.identity()));

        final boolean isSensitive = reference.getSensitivity() == ParameterSensitivity.SENSITIVE;
        final Map<ParameterDescriptor, Parameter> currentParameters = parameterContext.getParameters();
        // Find parameters of the same sensitivity that were removed
        currentParameters.keySet().forEach(descriptor -> {
                if (descriptor.isSensitive() == isSensitive && !fetchedParameterMap.containsKey(descriptor)) {
                    parameterUpdateMap.put(descriptor.getName(), null);
                }
        });
        // Add all changed and new parameters of the same sensitivity
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : fetchedParameterMap.entrySet()) {
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
     * Filters the list of parameters based on the parameter names, sets the sensitivity accordingly, and maps
     * any parameter names based on the configuration context.
     * @param parameters A list of Parameters
     * @param sensitivity The desired sensitivity of the Parameters
     * @param parameterNamesToInclude A collection of parameter names to include (include all if null or empty)
     * @return A filtered list, with the appropriate sensitivity and mapped names
     */
    private List<Parameter> filterAndMapParameters(final Collection<Parameter> parameters, final ParameterSensitivity sensitivity,
                                                   final Collection<String> parameterNamesToInclude) {
        final Map<String, String> parameterNameMapping = getParameterNameMapping(getConfigurationContext());
        return parameters == null ? Collections.emptyList() : parameters.stream()
                .filter(parameter -> parameterNamesToInclude.contains(parameter.getDescriptor().getName()))
                .map(parameter -> {
                    final String rawParameterName = parameter.getDescriptor().getName();
                    final String mappedParameterName = parameterNameMapping.getOrDefault(rawParameterName, rawParameterName);
                    final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
                            .from(parameter.getDescriptor())
                            .name(mappedParameterName)
                            .sensitive(sensitivity == ParameterSensitivity.SENSITIVE)
                            .build();
                    return new Parameter(parameterDescriptor, parameter.getValue(), parameter.getParameterContextId(), true);
                })
                .collect(Collectors.toList());
    }

    /**
     * Sets provided = true on all parameters in the list
     * @param parameters A list of Parameters
     * @return An equivalent list, but with provided = true
     */
    private static List<Parameter> toProvidedParameters(final Collection<Parameter> parameters) {
        return parameters == null ? Collections.emptyList() : parameters.stream()
                .map(parameter -> new Parameter(parameter.getDescriptor(), parameter.getValue(), null, true))
                .collect(Collectors.toList());
    }

    /**
     * @param context The configuration context
     * @return a map from raw parameter name to mapped parameter name, using the dynamic properties of the context
     */
    private static Map<String, String> getParameterNameMapping(final ConfigurationContext context) {
        return context.getProperties().entrySet().stream()
                .filter(entry -> entry.getKey().isDynamic())
                .collect(Collectors.toMap(
                        entry -> entry.getKey().getName(),
                        Map.Entry::getValue));
    }

    @Override
    public Collection<ProvidedParameterNameGroup> getFetchedParameterNames() {
        return fetchedParameterGroups.stream()
                .map(group -> new ProvidedParameterNameGroup(
                        group.getGroupKey().getGroupName(),
                        group.getGroupKey().getSensitivity(),
                        group.getItems().stream().map(p -> p.getDescriptor().getName()).collect(Collectors.toSet())))
                .collect(Collectors.toList());
    }

    @Override
    public List<ParametersApplication> getFetchedParametersToApply(final Collection<ProvidedParameterNameGroup> parameterNames) {
        readLock.lock();
        try {
            final Map<String, ParametersApplication> parametersApplicationMap = new HashMap<>();
            for (final ParameterProviderUsageReference reference : getReferences()) {
                final ParameterContext parameterContext = reference.getParameterContext();
                final Map<String, Parameter> parameterUpdateMap = getFetchedParameterUpdateMap(reference, parameterNames);
                final ParametersApplication parametersApplication = parametersApplicationMap.get(parameterContext.getName());
                if (parametersApplication == null) {
                    parametersApplicationMap.put(parameterContext.getName(), new ParametersApplication(parameterContext, parameterUpdateMap));
                } else {
                    parametersApplication.getParameterUpdates().putAll(parameterUpdateMap);
                }
            }
            return new ArrayList<>(parametersApplicationMap.values());
        } finally {
            readLock.unlock();
        }
    }
}
