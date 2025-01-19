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

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ParametersApplication;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterGroupConfiguration;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.parameter.VerifiableParameterProvider;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StandardParameterProviderNode extends AbstractComponentNode implements ParameterProviderNode {

    private static final Pattern PARAMETER_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-_. ]+$");

    private final AtomicReference<ParameterProviderDetails> parameterProviderRef;
    private final ControllerServiceLookup serviceLookup;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<ParameterContext> referencingParameterContexts;

    private final List<ParameterGroup> fetchedParameterGroups = new ArrayList<>();

    private volatile String comment;

    private final Authorizable parentAuthorizable;

    public StandardParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id, final Authorizable parentAuthorizable,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(parameterProvider, id, parentAuthorizable, controllerServiceProvider, validationContextFactory,
                parameterProvider.getComponent().getClass().getSimpleName(), parameterProvider.getComponent().getClass().getCanonicalName(),
                reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardParameterProviderNode(final LoggableComponent<ParameterProvider> parameterProvider, final String id, final Authorizable parentAuthorizable,
                                         final ControllerServiceProvider controllerServiceProvider, final ValidationContextFactory validationContextFactory,
                                         final String componentType, final String canonicalClassName, final ReloadComponent reloadComponent,
                                         final ExtensionManager extensionManager, final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {
        super(id, validationContextFactory, controllerServiceProvider, componentType, canonicalClassName, reloadComponent,
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
    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.empty();
    }

    @Override
    public ConfigurationContext getConfigurationContext() {
        return new StandardConfigurationContext(this, serviceLookup, null);
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
    public Set<ParameterContext> getReferences() {
        readLock.lock();
        try {
            return Collections.unmodifiableSet(referencingParameterContexts);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addReference(final ParameterContext reference) {
        writeLock.lock();
        try {
            referencingParameterContexts.add(reference);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeReference(final ParameterContext reference) {
        writeLock.lock();
        try {
            referencingParameterContexts.remove(reference);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected List<ValidationResult> validateConfig() {
        return Collections.emptyList();
    }

    @Override
    public void verifyCanFetchParameters() {
        final ValidationStatus validationStatus = performValidation();
        if (validationStatus != ValidationStatus.VALID) {
            throw new IllegalStateException(String.format("Cannot fetch parameters for %s while validation state is %s", this, validationStatus));
        }
    }

    @Override
    public void fetchParameters() {
        final ParameterProvider parameterProvider = parameterProviderRef.get().getParameterProvider();
        final ConfigurationContext configurationContext = getConfigurationContext();
        List<ParameterGroup> fetchedParameterGroups;
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), parameterProvider.getClass(), parameterProvider.getIdentifier())) {
            fetchedParameterGroups = parameterProvider.fetchParameters(configurationContext);
        } catch (final IOException | RuntimeException e) {
            throw new IllegalStateException(String.format("Error fetching parameters for %s: %s", this, e.getMessage()), e);
        }

        if (fetchedParameterGroups == null || fetchedParameterGroups.isEmpty()) {
            return;
        }

        final Set<String> parameterGroupNames = new HashSet<>();

        writeLock.lock();
        try {
            this.fetchedParameterGroups.clear();
            for (final ParameterGroup group : fetchedParameterGroups) {
                final String groupName = group.getGroupName();
                if (parameterGroupNames.contains(groupName)) {
                    throw new IllegalStateException(String.format("Cannot fetch parameters for %s: Parameter group [%s] is provided twice, which is not allowed", this, groupName));
                }
                final Collection<Parameter> parameters = group.getParameters();

                if (parameters == null) {
                    continue;
                }
                final List<Parameter> validParameters = new ArrayList<>();
                final Set<String> parameterNames = new HashSet<>();
                for (final Parameter parameter : parameters) {
                    final ParameterDescriptor descriptor = parameter.getDescriptor();
                    if (descriptor == null) {
                        throw new IllegalStateException("Cannot fetch parameters for " + this + ": a Parameter is missing a ParameterDescriptor in the fetch response");
                    }
                    final String parameterName = descriptor.getName();
                    if (parameterNames.contains(parameterName)) {
                        throw new IllegalStateException(String.format("Cannot fetch parameters for %s: Parameter [%s] is provided in group [%s] twice, which is not allowed",
                                this, parameterName, groupName));
                    }

                    if (parameter.getValue() == null) {
                        getLogger().warn("Skipping parameter [{}], which is missing a value", parameterName);
                        continue;
                    }

                    if (PARAMETER_NAME_PATTERN.matcher(parameter.getDescriptor().getName()).matches()) {
                        validParameters.add(parameter);
                        parameterNames.add(parameter.getDescriptor().getName());
                    } else {
                        getLogger().warn("Skipping parameter [{}], whose name has invalid characters.  Only alpha-numeric characters (a-z, A-Z, 0-9), hyphens (-), underscores (_), " +
                                "periods (.), and spaces ( ) are accepted.", parameterName);
                    }
                }
                this.fetchedParameterGroups.add(new ParameterGroup(groupName, toProvidedParameters(validParameters)));
                parameterGroupNames.add(groupName);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<ParameterGroup> findFetchedParameterGroup(final String parameterGroupName) {
        Objects.requireNonNull(parameterGroupName, "Parameter Group Name required");

        readLock.lock();
        try {
            return fetchedParameterGroups.stream()
                    .filter(parameterGroup -> parameterGroup.getGroupName().equals(parameterGroupName))
                    .findFirst();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanApplyParameters(final Collection<ParameterGroupConfiguration> parameterGroupConfigurations) {
        if (fetchedParameterGroups.isEmpty()) {
            return;
        }
        readLock.lock();
        try {
            final Map<String, ParameterGroupConfiguration> parameterGroupConfigurationMap = parameterGroupConfigurations.stream()
                    .collect(Collectors.toMap(ParameterGroupConfiguration::getParameterContextName, Function.identity()));
            for (final ParameterContext reference : getReferences()) {
                final ParameterGroupConfiguration groupConfiguration = parameterGroupConfigurationMap.get(reference.getName());
                if (groupConfiguration == null) {
                    continue;
                }
                final Map<String, Parameter> parameterUpdates = getFetchedParameterUpdateMap(reference, groupConfiguration);
                final Collection<String> removedParametersWithReferences = new HashSet<>();
                for (final Map.Entry<String, Parameter> entry : parameterUpdates.entrySet()) {
                    final String parameterName = entry.getKey();
                    if (entry.getValue() == null) {
                        reference.getParameter(parameterName).ifPresent(currentParameter -> {
                            if (reference.hasReferencingComponents(currentParameter)) {
                                removedParametersWithReferences.add(parameterName);
                            }
                        });
                    }
                }
                // Any deletions of parameters with referencing components should be removed from consideration,
                // since we do not remove provided parameters that are deleted until they are no longer referenced
                removedParametersWithReferences.forEach(parameterUpdates::remove);
                reference.verifyCanSetParameters(parameterUpdates);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDelete() {
        if (!getReferences().isEmpty()) {
            final String contextNameList = getReferences().stream()
                    .map(ParameterContext::getName)
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException(String.format("Cannot delete %s while it is referenced by Contexts: [%s]", this, contextNameList));
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

                    final String classloaderIsolationKey = getClassLoaderIsolationKey(context);

                    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                    try (final InstanceClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(getComponentType(), getIdentifier(), bundle, classpathUrls, false,
                            classloaderIsolationKey)) {
                        Thread.currentThread().setContextClassLoader(detectedClassLoader);
                        results.addAll(verifiable.verify(context, logger));
                    } finally {
                        Thread.currentThread().setContextClassLoader(currentClassLoader);
                    }
                } else {
                    // Verify the configuration, using the component's classloader
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, parameterProvider.getClass(), getIdentifier())) {
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
     * @param parameterContext A ParameterContext
     * @param parameterGroupConfiguration The configuration for the fetched parameter group
     * @return A map from name to Parameter (or null if parameter should be removed)
     */
    private Map<String, Parameter> getFetchedParameterUpdateMap(final ParameterContext parameterContext, ParameterGroupConfiguration parameterGroupConfiguration) {
        final Map<String, Parameter> parameterUpdateMap = new HashMap<>();

        final ParameterGroup parameterGroup = fetchedParameterGroups.stream()
                .filter(group -> parameterContext.getParameterProviderConfiguration().getParameterGroupName().equals(group.getGroupName()))
                .findFirst()
                .orElse(null);

        if (parameterGroup == null) {
            return parameterUpdateMap;
        }

        // Get a filtered list of the parameters with their sensitivity set based on group configuration
        final List<Parameter> configuredParameters = configureParameters(parameterGroup.getParameters(), parameterGroupConfiguration);

        final Map<ParameterDescriptor, Parameter> fetchedParameterMap = configuredParameters.stream()
                .collect(Collectors.toMap(Parameter::getDescriptor, Function.identity()));

        final Map<ParameterDescriptor, Parameter> currentParameters = parameterContext.getParameters();
        // Find parameters that were removed
        currentParameters.keySet().forEach(descriptor -> {
                if (!fetchedParameterMap.containsKey(descriptor)) {
                    parameterUpdateMap.put(descriptor.getName(), null);
                }
        });
        // Add all changed and new parameters
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : fetchedParameterMap.entrySet()) {
            final ParameterDescriptor descriptor = entry.getKey();
            final Parameter fetchedParameter = entry.getValue();
            final Parameter currentParameter = currentParameters.get(descriptor);
            if (currentParameter == null) {
                // Add if it's a new parameter
                parameterUpdateMap.put(descriptor.getName(), fetchedParameter);
            } else {
                final boolean isSensitivityChanged = currentParameter.getDescriptor().isSensitive() != fetchedParameter.getDescriptor().isSensitive();
                if (!Objects.equals(currentParameter.getValue(), fetchedParameter.getValue()) || isSensitivityChanged) {
                    // Also add if it's an existing parameter that has a changed value
                    parameterUpdateMap.put(descriptor.getName(), fetchedParameter);

                    if (isSensitivityChanged) {
                        final ParameterSensitivity currentSensitivity = currentParameter.getDescriptor().isSensitive() ? ParameterSensitivity.SENSITIVE : ParameterSensitivity.NON_SENSITIVE;
                        final ParameterSensitivity fetchedSensitivity = fetchedParameter.getDescriptor().isSensitive() ? ParameterSensitivity.SENSITIVE : ParameterSensitivity.NON_SENSITIVE;
                        getLogger().info("Parameter [{}] sensitivity is being changed from {} to {}", descriptor.getName(), currentSensitivity.getName(), fetchedSensitivity.getName());
                    }
                }
            }
        }
        return parameterUpdateMap;
    }

    /**
     * Filters the list of parameters based on the parameter names and sets the sensitivity accordingly
     * @param parameters A list of Parameters
     * @param groupConfiguration The user's configuration of the fetched parameters
     * @return A list of parameters with the configured sensitivities (only those found in the configuration will be included)
     */
    private List<Parameter> configureParameters(final Collection<Parameter> parameters, final ParameterGroupConfiguration groupConfiguration) {
        // The requested parameter names will be used as a filter for the fetched parameters
        final Set<String> parameterNameFilter = new HashSet<>(groupConfiguration.getParameterSensitivities().keySet());

        return parameters == null ? Collections.emptyList() : parameters.stream()
                .filter(parameter -> parameterNameFilter.contains(parameter.getDescriptor().getName()))
                .map(parameter -> {
                    final String parameterName = parameter.getDescriptor().getName();
                    final ParameterSensitivity sensitivity = groupConfiguration.getParameterSensitivities().get(parameterName);
                    if (sensitivity == null) {
                        throw new IllegalArgumentException(String.format("Parameter sensitivity must be specified for parameter [%s] in group [%s]",
                                parameterName, groupConfiguration.getGroupName()));
                    }

                    return new Parameter.Builder()
                        .fromParameter(parameter)
                        .sensitive(sensitivity == ParameterSensitivity.SENSITIVE)
                        .provided(true)
                        .build();
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
                .map(parameter -> new Parameter.Builder().descriptor(parameter.getDescriptor()).value(parameter.getValue()).provided(true).build())
                .collect(Collectors.toList());
    }

    @Override
    public Collection<ParameterGroupConfiguration> getParameterGroupConfigurations() {
        final Map<String, ParameterContext> parameterContextMap = getReferences().stream()
                .collect(Collectors.toMap(context -> context.getParameterProviderConfiguration().getParameterGroupName(), Function.identity()));
        final Collection<ParameterGroupConfiguration> parameterGroupConfigurations = new ArrayList<>();
        fetchedParameterGroups.forEach(parameterGroup -> {
                final ParameterContext parameterContext = parameterContextMap.get(parameterGroup.getGroupName());
                final Set<String> fetchedParameterNames = parameterGroup.getParameters().stream()
                        .map(parameter -> parameter.getDescriptor().getName())
                        .collect(Collectors.toSet());
                final Map<String, ParameterSensitivity> parameterSensitivities = new HashMap<>();
                final ParameterGroupConfiguration groupConfiguration;
                final String parameterContextName;
                final Boolean isSynchronized;
                if (parameterContext != null) {
                    isSynchronized = parameterContext.getParameterProviderConfiguration().isSynchronized();
                    parameterContextName = parameterContext.getName();
                    parameterContext.getParameters().forEach((descriptor, parameter) -> {
                        // Don't add it at all if it was not fetched
                        if (fetchedParameterNames.contains(descriptor.getName())) {
                            final ParameterSensitivity sensitivity = descriptor.isSensitive() ? ParameterSensitivity.SENSITIVE : ParameterSensitivity.NON_SENSITIVE;
                            parameterSensitivities.put(descriptor.getName(), sensitivity);
                        }
                    });
                } else {
                    parameterContextName = parameterGroup.getGroupName();
                    isSynchronized = null;
                }
                parameterGroup.getParameters().forEach(parameter -> {
                    final String parameterName = parameter.getDescriptor().getName();
                    if (!parameterSensitivities.containsKey(parameterName)) {
                        // Null means not configured yet.
                        parameterSensitivities.put(parameterName, null);
                    }
                });

                groupConfiguration = new ParameterGroupConfiguration(parameterGroup.getGroupName(), parameterContextName, parameterSensitivities, isSynchronized);
                parameterGroupConfigurations.add(groupConfiguration);
        });
        return parameterGroupConfigurations;
    }

    @Override
    public List<ParametersApplication> getFetchedParametersToApply(final Collection<ParameterGroupConfiguration> parameterGroupConfigurations) {
        readLock.lock();
        try {
            final Map<String, ParameterGroupConfiguration> parameterGroupConfigurationMap = parameterGroupConfigurations.stream()
                    .collect(Collectors.toMap(ParameterGroupConfiguration::getParameterContextName, Function.identity()));
            final List<ParametersApplication> parametersApplications = new ArrayList<>();
            for (final ParameterContext parameterContext : getReferences()) {
                final ParameterGroupConfiguration groupConfiguration = parameterGroupConfigurationMap.get(parameterContext.getName());
                if (groupConfiguration == null || groupConfiguration.isSynchronized() == null || !groupConfiguration.isSynchronized()) {
                    continue;
                }

                final Map<String, Parameter> parameterUpdateMap = getFetchedParameterUpdateMap(parameterContext, groupConfiguration);
                parametersApplications.add(new ParametersApplication(parameterContext, parameterUpdateMap));
            }
            return parametersApplications;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected String determineClasloaderIsolationKey() {
        final ConfigurableComponent component = getComponent();
        if (!(component instanceof ClassloaderIsolationKeyProvider)) {
            return null;
        }

        final ValidationContext validationContext = getValidationContextFactory().newValidationContext(getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier(),
                getParameterContext(), true);

        return getClassLoaderIsolationKey(validationContext);
    }
}
