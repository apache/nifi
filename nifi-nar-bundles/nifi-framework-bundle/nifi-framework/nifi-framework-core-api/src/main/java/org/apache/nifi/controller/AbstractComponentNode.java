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
package org.apache.nifi.controller;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.ResourceReferenceFactory;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.EnablingServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.service.ControllerServiceDisabledException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.parameter.ParameterUpdate;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractComponentNode implements ComponentNode {
    private static final String PERFORM_VALIDATION_STEP_NAME = "Perform Validation";
    private static final Logger logger = LoggerFactory.getLogger(AbstractComponentNode.class);

    private final String id;
    private final ValidationContextFactory validationContextFactory;
    private final ControllerServiceProvider serviceProvider;
    private final AtomicReference<String> name;
    private final AtomicReference<String> annotationData = new AtomicReference<>();
    private final String componentType;
    private final String componentCanonicalClass;
    private final ComponentVariableRegistry variableRegistry;
    private final ReloadComponent reloadComponent;
    private final ExtensionManager extensionManager;

    private final AtomicBoolean isExtensionMissing;

    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<PropertyDescriptor, PropertyConfiguration> properties = new ConcurrentHashMap<>();
    private final AtomicReference<Set<String>> sensitiveDynamicPropertyNames = new AtomicReference<>(new HashSet<>());
    private volatile String additionalResourcesFingerprint;
    private final AtomicReference<ValidationState> validationState = new AtomicReference<>(new ValidationState(ValidationStatus.VALIDATING, Collections.emptyList()));
    private final ValidationTrigger validationTrigger;
    private volatile boolean triggerValidation = true;
    private final Map<String, Integer> parameterReferenceCounts = new ConcurrentHashMap<>();

    // guaraded by lock
    private ValidationContext validationContext = null;

    public AbstractComponentNode(final String id,
                                 final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                                 final String componentType, final String componentCanonicalClass, final ComponentVariableRegistry variableRegistry,
                                 final ReloadComponent reloadComponent, final ExtensionManager extensionManager, final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {
        this.id = id;
        this.validationContextFactory = validationContextFactory;
        this.serviceProvider = serviceProvider;
        this.name = new AtomicReference<>(componentType);
        this.componentType = componentType;
        this.componentCanonicalClass = componentCanonicalClass;
        this.reloadComponent = reloadComponent;
        this.variableRegistry = variableRegistry;
        this.validationTrigger = validationTrigger;
        this.extensionManager = extensionManager;
        this.isExtensionMissing = new AtomicBoolean(isExtensionMissing);
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public void setExtensionMissing(boolean extensionMissing) {
        this.isExtensionMissing.set(extensionMissing);
    }

    @Override
    public boolean isExtensionMissing() {
        return isExtensionMissing.get();
    }

    @Override
    public String getName() {
        return name.get();
    }

    @Override
    public void setName(final String name) {
        this.name.set(CharacterFilterUtils.filterInvalidXmlCharacters(Objects.requireNonNull(name).intern()));
    }

    @Override
    public String getAnnotationData() {
        return annotationData.get();
    }

    @Override
    public void setAnnotationData(final String data) {
        annotationData.set(CharacterFilterUtils.filterInvalidXmlCharacters(data));
        logger.debug("Resetting Validation State of {} due to setting annotation data", this);
        resetValidationState();
    }

    @Override
    public Set<URL> getAdditionalClasspathResources(final List<PropertyDescriptor> propertyDescriptors) {
        return getAdditionalClasspathResources((Collection<PropertyDescriptor>) propertyDescriptors);
    }

    protected Set<URL> getAdditionalClasspathResources(final Collection<PropertyDescriptor> propertyDescriptors) {
        return getAdditionalClasspathResources(propertyDescriptors, this::getEffectivePropertyValue);
    }

    protected Set<URL> getAdditionalClasspathResources(final Collection<PropertyDescriptor> propertyDescriptors, final Function<PropertyDescriptor, String> effectiveValueLookup) {
        final Set<URL> additionalUrls = new LinkedHashSet<>();
        final ResourceReferenceFactory resourceReferenceFactory = new StandardResourceReferenceFactory();

        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isDynamicClasspathModifier()) {
                final String value = effectiveValueLookup.apply(descriptor);

                if (!StringUtils.isEmpty(value)) {
                    final ResourceContext resourceContext = new StandardResourceContext(resourceReferenceFactory, descriptor);
                    final StandardPropertyValue propertyValue = new StandardPropertyValue(resourceContext, value, null, getParameterLookup(), variableRegistry);
                    final ResourceReferences references = propertyValue.evaluateAttributeExpressions().asResources().flatten();
                    additionalUrls.addAll(references.asURLs());
                }
            }
        }

        return additionalUrls;
    }

    /**
     * Determines if the given set of properties will result in a different classpath than the currently configured set of properties
     * @param properties the properties to analyze
     * @return <code>true</code> if the given set of properties will require a different classpath (and therefore a different classloader) than the currently
     *         configured set of properties
     */
    protected boolean isClasspathDifferent(final Map<PropertyDescriptor, String> properties) {
        // If any property in the given map modifies classpath and is different than the currently configured value,
        // the given properties will require a different classpath.
        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final String value = entry.getValue();
            final String currentlyConfiguredValue = getEffectivePropertyValue(descriptor);

            if (descriptor.isDynamicClasspathModifier() && !Objects.equals(value, currentlyConfiguredValue)) {
                return true;
            }
        }

        // If any property in the currently configured properties modifies classpath and is not in the given set of properties,
        // the given properties will require a different classpath.
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamicClasspathModifier() && !properties.containsKey(descriptor)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Set Properties updates internal Map of Property Descriptors and values along with current definition of Sensitive Dynamic Property Names
     *
     * @param properties Property Names and Values to be updated
     * @param allowRemovalOfRequiredProperties Allow Removal of Required Properties
     * @param updatedSensitiveDynamicPropertyNames Requested Sensitive Dynamic Property Names replaces current configuration
     */
    @Override
    public void setProperties(final Map<String, String> properties, final boolean allowRemovalOfRequiredProperties, final Set<String> updatedSensitiveDynamicPropertyNames) {
        if (properties == null) {
            return;
        }

        lock.lock();
        try {
            Objects.requireNonNull(updatedSensitiveDynamicPropertyNames, "Sensitive Dynamic Property Names required");
            sensitiveDynamicPropertyNames.getAndSet(updatedSensitiveDynamicPropertyNames);

            verifyCanUpdateProperties(properties);

            // Determine the Classloader Isolation Key, if applicable, so we can determine whether or not the key changes by setting properties.
            final String initialIsolationKey = determineClasloaderIsolationKey();

            final PropertyConfigurationMapper configurationMapper = new PropertyConfigurationMapper();
            final Map<String, PropertyConfiguration> configurationMap = configurationMapper.mapRawPropertyValuesToPropertyConfiguration(this, properties);

            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), id)) {
                boolean classpathChanged = false;
                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    final String propertyName = entry.getKey();

                    // Set sensitive status on dynamic properties after getting canonical representation of Property Descriptor
                    final PropertyDescriptor componentDescriptor = getComponent().getPropertyDescriptor(propertyName);
                    final PropertyDescriptor descriptor = componentDescriptor.isDynamic() && updatedSensitiveDynamicPropertyNames.contains(propertyName)
                            ? new PropertyDescriptor.Builder().fromPropertyDescriptor(componentDescriptor).sensitive(true).build()
                            : componentDescriptor;

                    // determine if any of the property changes require resetting the InstanceClassLoader
                    if (descriptor.isDynamicClasspathModifier()) {
                        classpathChanged = true;
                    }

                    final PropertyConfiguration currentConfiguration = this.properties.get(descriptor);
                    if (currentConfiguration != null) {
                        for (final ParameterReference reference : currentConfiguration.getParameterReferences()) {
                            parameterReferenceCounts.merge(reference.getParameterName(), -1, (a, b) -> a == 1 ? null : a + b);
                        }
                    }

                    if (propertyName != null && entry.getValue() == null) {
                        removeProperty(propertyName, allowRemovalOfRequiredProperties);
                    } else if (propertyName != null) {
                        // Use the EL-Agnostic Parameter Parser to gather the list of referenced Parameters. We do this because we want to to keep track of which parameters
                        // are referenced, regardless of whether or not they are referenced from within an EL Expression. However, we also will need to derive a different ParameterTokenList
                        // that we can provide to the PropertyConfiguration, so that when compiling the Expression Language Expressions, we are able to keep the Parameter Reference within
                        // the Expression's text.
                        final PropertyConfiguration propertyConfiguration = configurationMap.get(propertyName);
                        final List<ParameterReference> parameterReferences = propertyConfiguration.getParameterReferences();
                        for (final ParameterReference reference : parameterReferences) {
                            // increment count in map for this parameter
                            parameterReferenceCounts.merge(reference.getParameterName(), 1, (a, b) -> a == -1 ? null : a + b);
                        }

                        setProperty(descriptor, propertyConfiguration, this.properties::get);
                    }
                }

                // Determine the updated Classloader Isolation Key, if applicable.
                final String updatedIsolationKey = determineClasloaderIsolationKey();
                final boolean classloaderIsolationKeyChanged = !Objects.equals(initialIsolationKey, updatedIsolationKey);

                // if at least one property with dynamicallyModifiesClasspath(true) was set, then reload the component with the new urls
                if (classpathChanged || classloaderIsolationKeyChanged) {
                    logger.info("Updating classpath for " + this.componentType + " with the ID " + this.getIdentifier());

                    final Set<URL> additionalUrls = getAdditionalClasspathResources(getComponent().getPropertyDescriptors());
                    try {
                        reload(additionalUrls);
                    } catch (Exception e) {
                        getLogger().error("Error reloading component with id " + id + ": " + e.getMessage(), e);
                    }
                }
            }

            if (isTriggerValidation()) {
                logger.debug("Resetting Validation State of {} due to setting properties", this);
                resetValidationState();
            } else {
                logger.debug("Properties set for {} but not resettingn validation state because validation is paused", this);
            }
        } finally {
            lock.unlock();
        }
    }

    protected String determineClasloaderIsolationKey() {
        final ConfigurableComponent component = getComponent();
        if (!(component instanceof ClassloaderIsolationKeyProvider)) {
            return null;
        }

        final ValidationContext validationContext = getValidationContextFactory().newValidationContext(getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier(),
            getParameterContext(), true);

        return getClassLoaderIsolationKey(validationContext);
    }

    public void verifyCanUpdateProperties(final Map<String, String> properties) {
        verifyModifiable();

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String propertyName = entry.getKey();
            final String value = entry.getValue();

            final ParameterTokenList tokenList = parameterParser.parseTokens(value);
            final List<ParameterReference> referenceList = tokenList.toReferenceList();

            final PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);

            // We don't want to allow a sensitive property to reference a parameter unless the value is solely a parameter reference. I.e.,
            // #{abc} is ok but password#{abc} is not.
            // However, for "ghost" components (isExtensionMissing() == true) we need to allow this, because we consider all properties sensitive.
            // If we don't allow this, we'll fail to even create the ghost component.
            if (descriptor.isSensitive() && !isExtensionMissing()) {
                if (referenceList.size() > 1) {
                    throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' cannot reference more than one Parameter because it is a sensitive property.");
                }

                if (referenceList.size() == 1) {
                    final ParameterReference reference = referenceList.get(0);
                    if (reference.getStartOffset() != 0 || reference.getEndOffset() != value.length() - 1) {
                        throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' is a sensitive property so it can reference a Parameter only if there is no other " +
                            "context around the value. For instance, the value '#{abc}' is allowed but 'password#{abc}' is not allowed.");
                    }
                }
            }
        }
    }

    protected List<ConfigVerificationResult> verifyConfig(final Map<PropertyDescriptor, String> propertyValues, final String annotationData, final ParameterContext parameterContext) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final long startNanos = System.nanoTime();

            final Map<PropertyDescriptor, PropertyConfiguration> descriptorToConfigMap = new LinkedHashMap<>();
            for (final Map.Entry<PropertyDescriptor, String> entry : propertyValues.entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                final String rawValue = entry.getValue();
                final String propertyValue = rawValue == null ? descriptor.getDefaultValue() : rawValue;

                final PropertyConfiguration propertyConfiguration = new PropertyConfiguration(propertyValue, null, Collections.emptyList(), VariableImpact.NEVER_IMPACTED);
                descriptorToConfigMap.put(descriptor, propertyConfiguration);
            }

            final ValidationContext validationContext = getValidationContextFactory().newValidationContext(descriptorToConfigMap, annotationData,
                getProcessGroupIdentifier(), getIdentifier(), parameterContext, false);

            final ValidationState validationState = performValidation(validationContext);
            final ValidationStatus validationStatus = validationState.getStatus();

            if (validationStatus == ValidationStatus.INVALID) {
                for (final ValidationResult result : validationState.getValidationErrors()) {
                    if (result.isValid()) {
                        continue;
                    }

                    results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName(PERFORM_VALIDATION_STEP_NAME)
                        .outcome(Outcome.FAILED)
                        .explanation("Component is invalid: " + result)
                        .build());
                }

                if (results.isEmpty()) {
                    results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName(PERFORM_VALIDATION_STEP_NAME)
                        .outcome(Outcome.FAILED)
                        .explanation("Component is invalid but provided no Validation Results to indicate why")
                        .build());
                }

                logger.debug("{} is not valid with the given configuration. Will not attempt to perform any additional verification of configuration. Validation took {}. Reason not valid: {}",
                    this, results, FormatUtils.formatNanos(System.nanoTime() - startNanos, false));
                return results;
            }

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(PERFORM_VALIDATION_STEP_NAME)
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Component Validation passed")
                .build());
        } catch (final Throwable t) {
            logger.error("Failed to perform verification of component's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName(PERFORM_VALIDATION_STEP_NAME)
                .outcome(Outcome.FAILED)
                .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                .build());
        }

        return results;
    }

    @Override
    public Set<String> getReferencedParameterNames() {
        return Collections.unmodifiableSet(parameterReferenceCounts.keySet());
    }

    @Override
    public boolean isReferencingParameter() {
        return !parameterReferenceCounts.isEmpty();
    }

    @Override
    public Set<String> getReferencedAttributeNames() {
        final Set<String> referencedAttributes = new HashSet<>();

        for (final PropertyDescriptor descriptor : getPropertyDescriptors()) {
            final String effectiveValue = getEffectivePropertyValue(descriptor);
            final Set<String> attributes = Query.prepareWithParametersPreEvaluated(effectiveValue).getExplicitlyReferencedAttributes();
            referencedAttributes.addAll(attributes);
        }

        return referencedAttributes;
    }

    // Keep setProperty/removeProperty private so that all calls go through setProperties
    private void setProperty(final PropertyDescriptor descriptor, final PropertyConfiguration propertyConfiguration, final Function<PropertyDescriptor, PropertyConfiguration> valueToCompareFunction) {
        // Remove current PropertyDescriptor to force updated instance references
        properties.remove(descriptor);

        final PropertyConfiguration propertyModComparisonValue = valueToCompareFunction.apply(descriptor);
        final PropertyConfiguration oldConfiguration = properties.put(descriptor, propertyConfiguration);
        final String effectiveValue = propertyConfiguration.getEffectiveValue(getParameterContext());

        // If the property references a Controller Service, we need to register this component & property descriptor as a reference.
        // If it previously referenced a Controller Service, we need to also remove that reference.
        // It is okay if the new & old values are the same - we just unregister the component/descriptor and re-register it.
        if (descriptor.getControllerServiceDefinition() != null) {
            Optional.ofNullable(oldConfiguration)
                .map(_oldConfiguration -> _oldConfiguration.getEffectiveValue(getParameterContext()))
                .map(oldEffectiveValue -> serviceProvider.getControllerServiceNode(oldEffectiveValue))
                .ifPresent(oldNode -> oldNode.removeReference(this, descriptor));

            Optional.ofNullable(effectiveValue)
                .map(serviceProvider::getControllerServiceNode)
                .ifPresent(newNode -> newNode.addReference(this, descriptor));
        }

        // In the case of a component "reload", we want to call onPropertyModified when the value is changed from the descriptor's default.
        // However, we do not want to update any controller service references because those are tied to the ComponentNode. We only want to
        // allow the newly created component's internal state to be updated.
        if (!propertyConfiguration.equals(propertyModComparisonValue)) {
            try {
                final String oldValue = propertyModComparisonValue == null ? null : propertyModComparisonValue.getEffectiveValue(getParameterContext());
                onPropertyModified(descriptor, oldValue, effectiveValue);
            } catch (final Exception e) {
                // nothing really to do here...
                logger.error("Failed to notify {} that property {} changed", this, descriptor, e);
            }
        }
    }

    /**
     * Removes the property and value for the given property name if a
     * descriptor and value exists for the given name. If the property is
     * optional its value might be reset to default or will be removed entirely
     * if was a dynamic property.
     *
     * @param name the property to remove
     * @param allowRemovalOfRequiredProperties whether or not the property should be removed if it's required
     * @return true if removed; false otherwise
     * @throws java.lang.IllegalArgumentException if the name is null
     */
    private boolean removeProperty(final String name, final boolean allowRemovalOfRequiredProperties) {
        if (null == name) {
            throw new IllegalArgumentException("Name can not be null");
        }

        final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(name);

        final boolean allowRemoval = allowRemovalOfRequiredProperties || !descriptor.isRequired();
        if (!allowRemoval) {
            return false;
        }

        final PropertyConfiguration propertyConfiguration = properties.remove(descriptor);
        if (propertyConfiguration == null || propertyConfiguration.getRawValue() == null) {
            return false;
        }

        final String value = propertyConfiguration.getEffectiveValue(getParameterContext());
        if (descriptor.getControllerServiceDefinition() != null) {
            if (value != null) {
                final ControllerServiceNode oldNode = serviceProvider.getControllerServiceNode(value);
                if (oldNode != null) {
                    oldNode.removeReference(this, descriptor);
                }
            }
        }

        try {
            onPropertyModified(descriptor, value, null);
        } catch (final Exception e) {
            getLogger().error(e.getMessage(), e);
        }

        return true;
    }

    public Map<PropertyDescriptor, PropertyConfiguration> getProperties() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getIdentifier())) {
            final List<PropertyDescriptor> supported = getComponent().getPropertyDescriptors();
            if (supported == null || supported.isEmpty()) {
                return Collections.unmodifiableMap(properties);
            } else {
                final Map<PropertyDescriptor, PropertyConfiguration> props = new LinkedHashMap<>();

                for (final PropertyDescriptor descriptor : supported) {
                    // Get Canonical Property Descriptor
                    props.put(getPropertyDescriptor(descriptor.getName()), null);
                }

                // Get Canonical Property Descriptor for returned Map of properties
                properties.forEach((descriptor, config) -> props.put(getPropertyDescriptor(descriptor.getName()), config));
                return props;
            }
        }
    }

    @Override
    public Map<PropertyDescriptor, String> getRawPropertyValues() {
        return getPropertyValues((descriptor, config) -> config.getRawValue());
    }

    @Override
    public Map<PropertyDescriptor, String> getEffectivePropertyValues() {
        return getPropertyValues((descriptor, config) -> getConfigValue(config, isResolveParameter(descriptor, config)));
    }

    private Map<PropertyDescriptor, String> getPropertyValues(final BiFunction<PropertyDescriptor, PropertyConfiguration, String> valueFunction) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getIdentifier())) {
            final List<PropertyDescriptor> supported = getComponent().getPropertyDescriptors();

            final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
            for (final PropertyDescriptor descriptor : supported) {
                if (descriptor != null) {
                    props.put(descriptor, descriptor.getDefaultValue());
                }
            }

            properties.forEach((descriptor, config) -> props.put(getPropertyDescriptor(descriptor.getName()), valueFunction.apply(descriptor, config)));
            return props;
        }
    }

    @Override
    public PropertyConfiguration getProperty(final PropertyDescriptor property) {
        final PropertyConfiguration configuration = properties.get(property);
        return (configuration == null) ? PropertyConfiguration.EMPTY : configuration;
    }

    @Override
    public String getEffectivePropertyValue(final PropertyDescriptor property) {
        return getProperty(property).getEffectiveValue(getParameterContext());
    }

    @Override
    public String getRawPropertyValue(final PropertyDescriptor property) {
        return getProperty(property).getRawValue();
    }

    @Override
    public void refreshProperties() {
        // use setProperty instead of setProperties so we can bypass the class loading logic.
        // Consider value changed if it is different than the PropertyDescriptor's default value because we need to call the #onPropertiesModified
        // method on the component if the current value is not the default value, since the component itself is being reloaded.
        // Also, create a copy of this.properties instead of iterating directly over this.properties since the call to setProperty can change the
        // underlying map, and the behavior of modifying the map while iterating over its elements is undefined.
        final Map<PropertyDescriptor, PropertyConfiguration> copyOfPropertiesMap = new HashMap<>(this.properties);
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : copyOfPropertiesMap.entrySet()) {
            final PropertyDescriptor propertyDescriptor = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();

            if (propertyDescriptor == null || configuration == null || configuration.getRawValue() == null) {
                continue;
            }

            setProperty(propertyDescriptor, configuration, descriptor -> createPropertyConfiguration(descriptor.getDefaultValue()));
        }
    }

    private PropertyConfiguration createPropertyConfiguration(final String value) {
        final ParameterParser parser = new ExpressionLanguageAwareParameterParser();
        final ParameterTokenList references = parser.parseTokens(value);
        final VariableImpact variableImpact = Query.prepare(value).getVariableImpact();
        return new PropertyConfiguration(value, references, references.toReferenceList(), variableImpact);
    }

    /**
     * Generates fingerprint for the additional urls and compares it with the previous
     * fingerprint value.
     */
    @Override
    public synchronized boolean isReloadAdditionalResourcesNecessary() {
        // Components that don't have any PropertyDescriptors marked `dynamicallyModifiesClasspath`
        // won't have the fingerprint i.e. will be null, in such cases do nothing
        if (additionalResourcesFingerprint == null) {
            return false;
        }

        final Set<PropertyDescriptor> descriptors = this.getProperties().keySet();
        final Set<URL> additionalUrls = this.getAdditionalClasspathResources(descriptors);

        final String newFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        return (!StringUtils.equals(additionalResourcesFingerprint, newFingerprint));
    }

    /**
     * Generates fingerprint for the additional urls and compares it with the previous
     * fingerprint value. If the fingerprint values don't match, the function calls the
     * component's reload() to load the newly found resources.
     */
    @Override
    public synchronized void reloadAdditionalResourcesIfNecessary() {
        // Components that don't have any PropertyDescriptors marked `dynamicallyModifiesClasspath`
        // won't have the fingerprint i.e. will be null, in such cases do nothing
        if (additionalResourcesFingerprint == null) {
            return;
        }

        final Set<PropertyDescriptor> descriptors = this.getProperties().keySet();
        final Set<URL> additionalUrls = this.getAdditionalClasspathResources(descriptors);

        final String newFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
        if(!StringUtils.equals(additionalResourcesFingerprint, newFingerprint)) {
            setAdditionalResourcesFingerprint(newFingerprint);
            try {
                logger.info("Updating classpath for " + this.componentType + " with the ID " + this.getIdentifier());
                reload(additionalUrls);
            } catch (Exception e) {
                logger.error("Error reloading component with id " + id + ": " + e.getMessage(), e);
            }
        }
    }

    @Override
    public int hashCode() {
        return 273171 * id.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ComponentNode)) {
            return false;
        }

        final ComponentNode other = (ComponentNode) obj;
        return id.equals(other.getIdentifier());
    }

    @Override
    public String toString() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().toString();
        }
    }

    @Override
    public ValidationState performValidation(final Map<PropertyDescriptor, PropertyConfiguration> properties, final String annotationData, final ParameterContext parameterContext) {
        final ValidationContext validationContext = validationContextFactory.newValidationContext(properties, annotationData, getProcessGroupIdentifier(), getIdentifier(), parameterContext, true);
        return performValidation(validationContext);
    }

    @Override
    public ValidationState performValidation(final ValidationContext validationContext) {
        final Collection<ValidationResult> results;
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getIdentifier())) {
            results = computeValidationErrors(validationContext);
        }

        final ValidationStatus status = results.isEmpty() ? ValidationStatus.VALID : ValidationStatus.INVALID;
        final ValidationState validationState = new ValidationState(status, results);
        return validationState;
    }

    @Override
    public final ValidationStatus performValidation() {
        while (true) {
            final ValidationState validationState = getValidationState();

            final ValidationContext validationContext = getValidationContext();
            final ValidationState updatedState = performValidation(validationContext);
            final boolean replaced = replaceValidationState(validationState, updatedState);
            if (replaced) {
                return updatedState.getStatus();
            }
        }
    }

    protected Collection<ValidationResult> computeValidationErrors(final ValidationContext validationContext) {
        Throwable failureCause = null;
        try {
            if (!sensitiveDynamicPropertyNames.get().isEmpty() && !isSupportsSensitiveDynamicProperties()) {
                return Collections.singletonList(
                        new ValidationResult.Builder()
                                .subject("Component")
                                .valid(false)
                                .explanation(String.format("Sensitive Dynamic Properties %s configured but not supported", sensitiveDynamicPropertyNames))
                                .build()
                );
            }

            final List<ValidationResult> invalidParameterResults = validateParameterReferences(validationContext);
            if (!invalidParameterResults.isEmpty()) {
                // At this point, we are not able to properly resolve all property values, so we will not attempt to perform
                // any further validation. Doing so would result in values being reported as invalid and containing confusing explanations.
                return invalidParameterResults;
            }

            final List<ValidationResult> validationResults = new ArrayList<>();
            final Collection<ValidationResult> results = getComponent().validate(validationContext);
            validationResults.addAll(results);

            // validate selected controller services implement the API required by the processor
            final Collection<ValidationResult> referencedServiceValidationResults = validateReferencedControllerServices(validationContext);
            validationResults.addAll(referencedServiceValidationResults);

            logger.debug("Computed validation errors with Validation Context {}; results = {}", validationContext, validationResults);

            return validationResults;
        } catch (final ControllerServiceDisabledException e) {
            getLogger().debug("Failed to perform validation due to " + e, e);
            return Collections.singleton(
                new DisabledServiceValidationResult("Component", e.getControllerServiceId(), "performing validation depends on referencing a Controller Service that is currently disabled"));
        } catch (final Exception e) {
            // We don't want to log this as an error because we will return a ValidationResult that is
            // invalid. However, we do want to make the stack trace available if needed, so we log it at
            // a debug level.
            getLogger().debug("Failed to perform validation due to " + e, e);
            failureCause = e;
        } catch (final Error e) {
            getLogger().error("Failed to perform validation due to " + e, e);
            failureCause = e;
        }

        return Collections.singleton(new ValidationResult.Builder()
            .subject("Component")
            .valid(false)
            .explanation("Failed to perform validation due to " + failureCause)
            .build());
    }

    private List<ValidationResult> validateParameterReferences(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final ParameterContext parameterContext = getParameterContext();
        final boolean assignedToProcessGroup = getProcessGroupIdentifier() != null;

        final ConfigurableComponent component = getComponent();
        for (final PropertyDescriptor propertyDescriptor : validationContext.getProperties().keySet()) {
            // If the property descriptor's dependency is not satisfied, the property does not need to be considered, as it's not relevant to the
            // component's functionality.
            final boolean dependencySatisfied = validationContext.isDependencySatisfied(propertyDescriptor, component::getPropertyDescriptor);
            if (!dependencySatisfied) {
                continue;
            }

            final Collection<String> referencedParameters = validationContext.getReferencedParameters(propertyDescriptor.getName());

            if (parameterContext == null && !referencedParameters.isEmpty()) {
                results.add(new ValidationResult.Builder()
                    .subject(propertyDescriptor.getDisplayName())
                    .valid(false)
                    .explanation(assignedToProcessGroup ? "Property references one or more Parameters but no Parameter Context is currently set on the Process Group"
                        : "Property references one or more Parameters, but Parameters may be referenced only by Processors and Controller Services that reside within a Process Group.")
                    .build());

                continue;
            }

            for (final String paramName : referencedParameters) {
                if (!validationContext.isParameterDefined(paramName)) {
                    results.add(new ValidationResult.Builder()
                            .subject(propertyDescriptor.getDisplayName())
                            .valid(false)
                            .explanation("Property references Parameter '" + paramName + "' but the currently selected Parameter Context does not have a Parameter with that name")
                            .build());
                    continue;
                }

                final Optional<Parameter> parameterRef = parameterContext.getParameter(paramName);
                if (parameterRef.isPresent()) {
                    final ParameterDescriptor parameterDescriptor = parameterRef.get().getDescriptor();
                    if (parameterDescriptor.isSensitive() != propertyDescriptor.isSensitive()) {
                        results.add(new ValidationResult.Builder()
                                .subject(propertyDescriptor.getDisplayName())
                                .valid(false)
                                .explanation("The property '" + propertyDescriptor.getDisplayName() + "' cannot reference Parameter '" + parameterDescriptor.getName()
                                        + "' because the Sensitivity of the parameter does not match the Sensitivity of the property.")
                                .build());
                    }
                }
            }
        }

        return results;
    }

    protected final Collection<ValidationResult> validateReferencedControllerServices(final ValidationContext validationContext) {
        final Set<PropertyDescriptor> propertyDescriptors = validationContext.getProperties().keySet();

        final ConfigurableComponent component = getComponent();
        final Collection<ValidationResult> validationResults = new ArrayList<>();
        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.getControllerServiceDefinition() == null) {
                // skip properties that aren't for a controller service
                continue;
            }

            final boolean dependencySatisfied = validationContext.isDependencySatisfied(descriptor, component::getPropertyDescriptor);
            if (!dependencySatisfied) {
                continue;
            }

            final String controllerServiceId = validationContext.getProperty(descriptor).getValue();
            if (controllerServiceId == null) {
                continue;
            }

            final ControllerServiceNode controllerServiceNode = getControllerServiceProvider().getControllerServiceNode(controllerServiceId);
            if (controllerServiceNode == null) {
                final ValidationResult result = createInvalidResult(controllerServiceId, descriptor.getDisplayName(),
                    "Invalid Controller Service: " + controllerServiceId + " is not a valid Controller Service Identifier");

                validationResults.add(result);
                continue;
            }

            final ValidationResult apiResult = validateControllerServiceApi(descriptor, controllerServiceNode);
            if (apiResult != null) {
                validationResults.add(apiResult);
                continue;
            }

            if (!controllerServiceNode.isActive()) {
                validationResults.add(new DisabledServiceValidationResult(descriptor.getDisplayName(), controllerServiceId));
            } else if (ControllerServiceState.ENABLING == controllerServiceNode.getState()) {
                validationResults.add(new EnablingServiceValidationResult(descriptor.getDisplayName(), controllerServiceId));
            }
        }

        return validationResults;
    }


    private ValidationResult validateControllerServiceApi(final PropertyDescriptor descriptor, final ControllerServiceNode controllerServiceNode) {
        final Class<? extends ControllerService> controllerServiceApiClass = descriptor.getControllerServiceDefinition();
        // If a processor accepts any service don't validate it.
        if (controllerServiceApiClass.equals(ControllerService.class)) {
            return null;
        }

        final ClassLoader controllerServiceApiClassLoader = controllerServiceApiClass.getClassLoader();
        final ExtensionManager extensionManager = serviceProvider.getExtensionManager();

        final String serviceId = controllerServiceNode.getIdentifier();
        final String propertyName = descriptor.getDisplayName();

        final Bundle controllerServiceApiBundle = extensionManager.getBundle(controllerServiceApiClassLoader);
        if (controllerServiceApiBundle == null) {
            return createInvalidResult(serviceId, propertyName, "Unable to find bundle for ControllerService API class " + controllerServiceApiClass.getCanonicalName());
        }
        final BundleCoordinate controllerServiceApiCoordinate = controllerServiceApiBundle.getBundleDetails().getCoordinate();

        Bundle controllerServiceBundle = extensionManager.getBundle(controllerServiceNode.getBundleCoordinate());
        final boolean matchesApiByBundleCoordinates;
        if (controllerServiceBundle == null) {
            final List<Bundle> possibleBundles = extensionManager.getBundles(controllerServiceNode.getControllerServiceImplementation().getClass().getName());
            if (possibleBundles.size() != 1) {
                return createInvalidResult(serviceId, propertyName, "Unable to find bundle for coordinate " + controllerServiceNode.getBundleCoordinate());
            }

            controllerServiceBundle = possibleBundles.get(0);
            matchesApiByBundleCoordinates = false;
        } else {
            matchesApiByBundleCoordinates = matchesApiBundleCoordinates(extensionManager, controllerServiceBundle, controllerServiceApiCoordinate);
        }

        final BundleCoordinate controllerServiceCoordinate = controllerServiceBundle.getBundleDetails().getCoordinate();
        if (!matchesApiByBundleCoordinates) {
            final Class<? extends ControllerService> controllerServiceImplClass = controllerServiceNode.getControllerServiceImplementation().getClass();
            logger.debug("Comparing methods from service api '{}' against service implementation '{}'",
                    new Object[]{controllerServiceApiClass.getCanonicalName(), controllerServiceImplClass.getCanonicalName()});

            final ControllerServiceApiMatcher controllerServiceApiMatcher = new ControllerServiceApiMatcher();
            final boolean matchesApi = controllerServiceApiMatcher.matches(controllerServiceApiClass, controllerServiceImplClass);

            if (!matchesApi) {
                final String controllerServiceType = controllerServiceNode.getComponentType();
                final String controllerServiceApiType = controllerServiceApiClass.getSimpleName();

                final String explanation = new StringBuilder()
                        .append(controllerServiceType).append(" - ").append(controllerServiceCoordinate.getVersion())
                        .append(" from ").append(controllerServiceCoordinate.getGroup()).append(" - ").append(controllerServiceCoordinate.getId())
                        .append(" is not compatible with ").append(controllerServiceApiType).append(" - ").append(controllerServiceApiCoordinate.getVersion())
                        .append(" from ").append(controllerServiceApiCoordinate.getGroup()).append(" - ").append(controllerServiceApiCoordinate.getId())
                        .toString();

                return createInvalidResult(serviceId, propertyName, explanation);
            }
        }

        return null;
    }

    private ValidationResult createInvalidResult(final String serviceId, final String propertyName, final String explanation) {
        return new ValidationResult.Builder()
            .input(serviceId)
            .subject(propertyName)
            .valid(false)
            .explanation(explanation)
            .build();
    }

    /**
     * Determines if the given controller service node has the required API as an ancestor.
     *
     * @param controllerServiceImplBundle the bundle of a controller service being referenced by a processor
     * @param requiredApiCoordinate the controller service API required by the processor
     * @return true if the controller service node has the require API as an ancestor, false otherwise
     */
    private boolean matchesApiBundleCoordinates(final ExtensionManager extensionManager, final Bundle controllerServiceImplBundle, final BundleCoordinate requiredApiCoordinate) {
        // start with the coordinate of the controller service for cases where the API and service are in the same bundle
        BundleCoordinate controllerServiceDependencyCoordinate = controllerServiceImplBundle.getBundleDetails().getCoordinate();

        boolean foundApiDependency = false;
        while (controllerServiceDependencyCoordinate != null) {
            // determine if the dependency coordinate matches the required API
            if (requiredApiCoordinate.equals(controllerServiceDependencyCoordinate)) {
                foundApiDependency = true;
                break;
            }

            // move to the next dependency in the chain, or stop if null
            final Bundle controllerServiceDependencyBundle = extensionManager.getBundle(controllerServiceDependencyCoordinate);
            if (controllerServiceDependencyBundle == null) {
                controllerServiceDependencyCoordinate = null;
            } else {
                controllerServiceDependencyCoordinate = controllerServiceDependencyBundle.getBundleDetails().getDependencyCoordinate();
            }
        }

        return foundApiDependency;
    }

    @Override
    public boolean isSensitiveDynamicProperty(final String name) {
        Objects.requireNonNull(name, "Property Name required");
        return sensitiveDynamicPropertyNames.get().contains(name);
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            final PropertyDescriptor propertyDescriptor = getComponent().getPropertyDescriptor(name);
            if (propertyDescriptor.isDynamic() && sensitiveDynamicPropertyNames.get().contains(name)) {
                return new PropertyDescriptor.Builder().fromPropertyDescriptor(propertyDescriptor).sensitive(true).build();
            } else {
                return propertyDescriptor;
            }
        }
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().getPropertyDescriptors();
        }
    }


    protected void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            getComponent().onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public void onParametersModified(final Map<String, ParameterUpdate> updatedParameters) {
        // If the component doesn't reference any parameters, then there's nothing to be done.
        if (!isReferencingParameter()) {
            return;
        }

        final ParameterLookup previousParameterLookup = createParameterLookupForPreviousValues(updatedParameters);

        // For any Property that references an updated Parameter, we need to call onPropertyModified().
        // Additionally, we need to trigger validation to run if this component is affected by the parameter update.
        boolean componentAffected = false;
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : properties.entrySet()) {
            final PropertyDescriptor propertyDescriptor = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();

            // Determine if this property is affected by the Parameter Update
            boolean propertyAffected = false;
            final List<ParameterReference> parameterReferences = configuration.getParameterReferences();
            for (final ParameterReference reference : parameterReferences) {
                final String referencedParamName = reference.getParameterName();
                if (updatedParameters.containsKey(referencedParamName)) {
                    propertyAffected = true;
                    componentAffected = true;
                    break;
                }
            }

            if (propertyAffected) {
                final String previousValue = configuration.getEffectiveValue(previousParameterLookup);
                final String updatedValue = configuration.getEffectiveValue(getParameterContext());

                // Check if the value of the property is truly affected. It's possible that we could have a property configured as something like "#{a}#{b}"
                // Where parameter a = "abc-" and b = "cba". The update could change a to "abc" and b to "-cba". As a result, the property value previously was "abc-cba" and still is.
                // In such a case, we should not call onPropertyModified.
                final boolean propertyUpdated = !Objects.equals(previousValue, updatedValue);
                if (propertyUpdated) {
                    try {
                        logger.debug("Parameter Context updated, resulting in property {} of {} changing. Calling onPropertyModified().", propertyDescriptor, this);
                        onPropertyModified(propertyDescriptor, previousValue, updatedValue);
                    } catch (final Exception e) {
                        // nothing really to do here...
                        logger.error("Failed to notify {} that property {} changed", this, propertyDescriptor, e);
                    }
                } else {
                    logger.debug("Parameter Context updated, and property {} of {} does reference the updated Parameters. However, the overall property value remained unchanged so will not call " +
                        "onPropertyModified().", propertyDescriptor, this);
                }
            }
        }

        // If this component is affected by the Parameter change, we need to re-validate
        if (componentAffected) {
            logger.debug("Configuration of {} changed due to an update to Parameter Context. Resetting validation state", this);
            resetValidationState();
        }
    }

    private ParameterLookup createParameterLookupForPreviousValues(final Map<String, ParameterUpdate> updatedParameters) {
        final ParameterContext currentContext = getParameterContext();
        return new ParameterLookup() {
            @Override
            public Optional<Parameter> getParameter(final String parameterName) {
                final Optional<Parameter> optionalParameter = currentContext == null ? Optional.empty() : currentContext.getParameter(parameterName);

                // Check if there's an update to the parameter. If not, just return the parameter as-is.
                final ParameterUpdate parameterUpdate = updatedParameters.get(parameterName);
                if (parameterUpdate == null) {
                    return optionalParameter;
                }

                // There is an update to the parameter. We want to return the previous value of the Parameter.
                final ParameterDescriptor parameterDescriptor;
                if (optionalParameter.isPresent()) {
                    parameterDescriptor = optionalParameter.get().getDescriptor();
                } else {
                    parameterDescriptor = new ParameterDescriptor.Builder()
                        .name(parameterName)
                        .description("")
                        .sensitive(true)
                        .build();
                }

                final Parameter updatedParameter = new Parameter(parameterDescriptor, parameterUpdate.getPreviousValue());
                return Optional.of(updatedParameter);
            }

            @Override
            public boolean isEmpty() {
                return (currentContext == null || currentContext.isEmpty()) && updatedParameters.isEmpty();
            }

            @Override
            public long getVersion() {
                return 0;
            }
        };
    }

    @Override
    public ValidationStatus getValidationStatus() {
        return validationState.get().getStatus();
    }

    @Override
    public ValidationStatus getValidationStatus(long timeout, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(timeout);
        final long maxTime = System.currentTimeMillis() + millis;

        synchronized (validationState) {
            while (getValidationStatus() == ValidationStatus.VALIDATING) {
                try {
                    final long waitMillis = Math.max(0, maxTime - System.currentTimeMillis());
                    if (waitMillis <= 0) {
                        break;
                    }

                    validationState.wait(waitMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return getValidationStatus();
                }
            }

            return getValidationStatus();
        }
    }

    protected ValidationState getValidationState() {
        return validationState.get();
    }

    private boolean replaceValidationState(final ValidationState expectedState, final ValidationState newState) {
        synchronized (validationState) {
            if (validationState.compareAndSet(expectedState, newState)) {
                validationState.notifyAll();
                return true;
            }

            return false;
        }
    }

    @Override
    public void resetValidationState() {
        lock.lock();
        try {
            validationContext = null;
            validationState.set(new ValidationState(ValidationStatus.VALIDATING, Collections.emptyList()));

            if (isTriggerValidation()) {
                validationTrigger.triggerAsync(this);
            } else {
                logger.debug("Reset validation state of {} but will not trigger async validation because trigger has been paused", this);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pauseValidationTrigger() {
        triggerValidation = false;
    }

    @Override
    public void resumeValidationTrigger() {
        triggerValidation = true;

        logger.debug("Resuming Triggering of Validation State for {}; Resetting validation state", this);
        resetValidationState();
    }

    private boolean isTriggerValidation() {
        return triggerValidation;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return getValidationErrors(Collections.emptySet());
    }

    protected Collection<ValidationResult> getValidationErrors(final Set<ControllerServiceNode> servicesToIgnore) {
        final ValidationState validationState = this.validationState.get();
        if (validationState.getStatus() == ValidationStatus.VALIDATING) {
            return null;
        }

        final Collection<ValidationResult> validationErrors = validationState.getValidationErrors();
        if (servicesToIgnore == null || servicesToIgnore.isEmpty()) {
            return validationErrors;
        }

        final Set<String> ignoredServiceIds = servicesToIgnore.stream()
            .map(ControllerServiceNode::getIdentifier)
            .collect(Collectors.toSet());

        final List<ValidationResult> retainedValidationErrors = new ArrayList<>();
        for (final ValidationResult result : validationErrors) {
            if (!(result instanceof DisabledServiceValidationResult)) {
                retainedValidationErrors.add(result);
                continue;
            }

            final String serviceId = ((DisabledServiceValidationResult) result).getControllerServiceIdentifier();
            if (!ignoredServiceIds.contains(serviceId)) {
                retainedValidationErrors.add(result);
            }
        }

        return retainedValidationErrors;
    }

    public abstract void verifyModifiable() throws IllegalStateException;

    /**
     *
     */
    ControllerServiceProvider getControllerServiceProvider() {
        return this.serviceProvider;
    }

    @Override
    public String getCanonicalClassName() {
        return componentCanonicalClass;
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    protected ValidationContextFactory getValidationContextFactory() {
        return this.validationContextFactory;
    }

    protected ValidationContext getValidationContext() {
        lock.lock();
        try {
            ValidationContext context = this.validationContext;
            if (context != null) {
                return context;
            }

            context = getValidationContextFactory().newValidationContext(getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier(), getParameterContext(), true);

            this.validationContext = context;
            logger.debug("Updating validation context to {}", context);
            return context;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ComponentVariableRegistry getVariableRegistry() {
        return this.variableRegistry;
    }

    protected ReloadComponent getReloadComponent() {
        return this.reloadComponent;
    }

    protected ExtensionManager getExtensionManager() {
        return this.extensionManager;
    }

    @Override
    public void verifyCanUpdateBundle(final BundleCoordinate incomingCoordinate) throws IllegalArgumentException {
        final BundleCoordinate existingCoordinate = getBundleCoordinate();

        // determine if this update is changing the bundle for the processor
        if (!existingCoordinate.equals(incomingCoordinate)) {
            // if it is changing the bundle, only allow it to change to a different version within same group and id
            if (!existingCoordinate.getGroup().equals(incomingCoordinate.getGroup())
                    || !existingCoordinate.getId().equals(incomingCoordinate.getId())) {
                throw new IllegalArgumentException(String.format(
                        "Unable to update component %s from %s to %s because bundle group and id must be the same.",
                        getIdentifier(), existingCoordinate.getCoordinate(), incomingCoordinate.getCoordinate()));
            }
        }
    }

    protected void setAdditionalResourcesFingerprint(String additionalResourcesFingerprint) {
        this.additionalResourcesFingerprint = additionalResourcesFingerprint;
    }

    // Determine whether the property value should be evaluated in terms of the parameter context or not.
    // If the sensitivity of the property does not match the sensitivity of the parameter, the literal value will be returned
    //
    // Examples when SensitiveParam value = 'abc' and MY_PROP is non-sensitive:
    // SensitiveProp            --> 'abc'
    // NonSensitiveProp         --> '#{SensitiveParam}'
    // context.getProperty(MY_PROP).getValue(); '#{SensitiveParam}'
    private boolean isResolveParameter(final PropertyDescriptor descriptor, final PropertyConfiguration config) {
        boolean okToResolve = true;

        final ParameterContext context = getParameterContext();
        if (context == null) {
            return false;
        }
        for (final ParameterReference reference : config.getParameterReferences()) {
            final String parameterName = reference.getParameterName();
            final Optional<Parameter> optionalParameter = context.getParameter(parameterName);
            if (optionalParameter.isPresent()) {
                final boolean paramIsSensitive = optionalParameter.get().getDescriptor().isSensitive();
                if (paramIsSensitive != descriptor.isSensitive()) {
                    okToResolve = false;
                    break;
                }
            }
        }
        return okToResolve;
    }

    // Evaluates the parameter value if it is ok to do so, otherwise return the raw "${param}" literal.
    // This is done to prevent evaluation of a sensitive parameter when setting a non-sensitive property.
    private String getConfigValue(final PropertyConfiguration config, final boolean okToResolve) {
        return okToResolve ? config.getEffectiveValue(getParameterContext()) : config.getRawValue();
    }

    protected abstract ParameterContext getParameterContext();
}
