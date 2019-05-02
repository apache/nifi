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
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.service.ControllerServiceDisabledException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterToken;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractComponentNode implements ComponentNode {
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

    private Set<URL> getAdditionalClasspathResources(final Collection<PropertyDescriptor> propertyDescriptors) {
        final Set<String> modulePaths = new LinkedHashSet<>();
        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isDynamicClasspathModifier()) {
                final PropertyConfiguration propertyConfiguration = getProperty(descriptor);
                final String value = propertyConfiguration == null ? null : propertyConfiguration.getEffectiveValue(getParameterContext());

                if (!StringUtils.isEmpty(value)) {
                    final StandardPropertyValue propertyValue = new StandardPropertyValue(value, null, getParameterLookup(), variableRegistry);
                    modulePaths.add(propertyValue.evaluateAttributeExpressions().getValue());
                }
            }
        }

        final Set<URL> additionalUrls = new LinkedHashSet<>();
        try {
            final URL[] urls = ClassLoaderUtils.getURLsForClasspath(modulePaths, null, true);
            if (urls != null) {
                additionalUrls.addAll(Arrays.asList(urls));
            }
        } catch (MalformedURLException mfe) {
            getLogger().error("Error processing classpath resources for " + id + ": " + mfe.getMessage(), mfe);
        }
        return additionalUrls;
    }


    @Override
    public void setProperties(final Map<String, String> properties, final boolean allowRemovalOfRequiredProperties) {
        if (properties == null) {
            return;
        }

        lock.lock();
        try {
            verifyCanUpdateProperties(properties);

            // Keep track of counts of each parameter reference. This way, when we complete the updates to property values, we can
            // update our counts easily.
            final ParameterParser elAwareParser = new ExpressionLanguageAwareParameterParser();
            final ParameterParser elAgnosticParser = new ExpressionLanguageAgnosticParameterParser();

            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), id)) {
                boolean classpathChanged = false;
                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    // determine if any of the property changes require resetting the InstanceClassLoader
                    final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(entry.getKey());
                    if (descriptor.isDynamicClasspathModifier()) {
                        classpathChanged = true;
                    }

                    final PropertyConfiguration currentConfiguration = this.properties.get(descriptor);
                    if (currentConfiguration != null) {
                        for (final ParameterReference reference : currentConfiguration.getParameterReferences()) {
                            parameterReferenceCounts.merge(reference.getParameterName(), -1, (a, b) -> a.equals(b) ? null : a - b);
                        }
                    }

                    if (entry.getKey() != null && entry.getValue() == null) {
                        removeProperty(entry.getKey(), allowRemovalOfRequiredProperties);
                    } else if (entry.getKey() != null) {
                        final String updatedValue = CharacterFilterUtils.filterInvalidXmlCharacters(entry.getValue());

                        final boolean supportsEl = getPropertyDescriptor(entry.getKey()).isExpressionLanguageSupported();
                        final ParameterParser parser = supportsEl ? elAwareParser : elAgnosticParser;

                        final ParameterTokenList updatedValueReferences = parser.parseTokens(updatedValue);
                        for (final ParameterToken token : updatedValueReferences) {
                            if (token.isParameterReference()) {
                                final ParameterReference reference = (ParameterReference) token;

                                // increment count in map for this parameter
                                parameterReferenceCounts.merge(reference.getParameterName(), 1, (a, b) -> a == -1 ? null : a + b);
                            }
                        }

                        final PropertyConfiguration propertyConfiguration = new PropertyConfiguration(updatedValue, updatedValueReferences);
                        setProperty(entry.getKey(), propertyConfiguration, this.properties::get);
                    }
                }

                // if at least one property with dynamicallyModifiesClasspath(true) was set, then reload the component with the new urls
                if (classpathChanged) {
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

    public void verifyCanUpdateProperties(final Map<String, String> properties) {
        verifyModifiable();

        final ParameterParser elAwareParser = new ExpressionLanguageAwareParameterParser();
        final ParameterParser elAgnosticParser = new ExpressionLanguageAgnosticParameterParser();

        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String propertyName = entry.getKey();
            final String value = entry.getValue();

            final boolean supportsEl = getPropertyDescriptor(entry.getKey()).isExpressionLanguageSupported();
            final ParameterParser parser = supportsEl ? elAwareParser : elAgnosticParser;

            final ParameterTokenList tokenList = parser.parseTokens(value);
            final List<ParameterReference> referenceList = tokenList.toReferenceList();

            final PropertyDescriptor descriptor = getPropertyDescriptor(propertyName);

            if (descriptor.isSensitive()) {
                if (referenceList.size() > 1) {
                    throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' cannot reference more than one Parameter because it is a sensitive property.");
                }

                if (referenceList.size() == 1) {
                    final ParameterReference reference = referenceList.get(0);
                    if (reference.getStartOffset() != 0 || reference.getEndOffset() != value.length() - 1) {
                        throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' is a sensitive property so it can reference a Parameter only if there is no other " +
                            "context around the value. For instance, the value '#{abc}' is allowed but 'password#{abc}' is not allowed.");
                    }

                    final ParameterContext parameterContext = getParameterContext();
                    if (parameterContext != null) {
                        final Optional<Parameter> parameter = parameterContext.getParameter(reference.getParameterName());
                        if (parameter.isPresent() && !parameter.get().getDescriptor().isSensitive()) {
                            throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' is a sensitive property, so it can only reference Parameters that are sensitive.");
                        }
                    }
                }
            } else {
                final ParameterContext parameterContext = getParameterContext();
                if (parameterContext != null) {
                    for (final ParameterReference reference : referenceList) {
                        final Optional<Parameter> parameter = parameterContext.getParameter(reference.getParameterName());
                        if (parameter.isPresent() && parameter.get().getDescriptor().isSensitive()) {
                            throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' cannot reference Parameter '" + parameter.get().getDescriptor().getName()
                                + "' because Sensitive Parameters may only be referenced by Sensitive Properties.");
                        }
                    }
                }
            }

            if (descriptor.getControllerServiceDefinition() != null) {
                final ParameterTokenList allTokensList = elAgnosticParser.parseTokens(value);
                final List<ParameterReference> allParameterReferences = allTokensList.toReferenceList();
                if (!allParameterReferences.isEmpty()) {
                    throw new IllegalArgumentException("The property '" + descriptor.getDisplayName() + "' cannot reference a Parameter because the property is a Controller Service reference. " +
                        "Allowing Controller Service references to make use of Parameters could result in security issues and a poor user experience. As a result, this is not allowed.");
                }
            }
        }
    }

    @Override
    public Set<String> getReferencedParameterNames() {
        return Collections.unmodifiableSet(parameterReferenceCounts.keySet());
    }

    @Override
    public boolean isReferencingParameter() {
        return !parameterReferenceCounts.isEmpty();
    }

    // Keep setProperty/removeProperty private so that all calls go through setProperties
    private void setProperty(final String name, final PropertyConfiguration propertyConfiguration, final Function<PropertyDescriptor, PropertyConfiguration> valueToCompareFunction) {
        if (name == null || propertyConfiguration == null || propertyConfiguration.getRawValue() == null) {
            throw new IllegalArgumentException("Name or Value can not be null");
        }

        final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(name);
        final PropertyConfiguration propertyModComparisonValue = valueToCompareFunction.apply(descriptor);
        final PropertyConfiguration oldConfiguration = properties.put(descriptor, propertyConfiguration);
        final String effectiveValue = propertyConfiguration.getEffectiveValue(getParameterContext());

        if (!propertyConfiguration.equals(oldConfiguration)) {
            if (descriptor.getControllerServiceDefinition() != null) {
                if (oldConfiguration != null) {
                    final ControllerServiceNode oldNode = serviceProvider.getControllerServiceNode(effectiveValue);
                    if (oldNode != null) {
                        oldNode.removeReference(this);
                    }
                }

                final ControllerServiceNode newNode = serviceProvider.getControllerServiceNode(effectiveValue);
                if (newNode != null) {
                    newNode.addReference(this);
                }
            }
        }

        // In the case of a component "reload", we want to call onPropertyModified when the value is changed from the descriptor's default.
        // However, we do not want to update any controller service references because those are tied to the ComponentNode. We only want to
        // allow the newly created component's internal state to be updated.
        if (!propertyConfiguration.equals(propertyModComparisonValue)) {
            try {
                final String oldValue = oldConfiguration == null ? null : oldConfiguration.getEffectiveValue(getParameterContext());
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
        String value = null;

        final boolean allowRemoval = allowRemovalOfRequiredProperties || !descriptor.isRequired();
        if (!allowRemoval) {
            return false;
        }

        final PropertyConfiguration propertyConfiguration = properties.remove(descriptor);
        if (propertyConfiguration == null || propertyConfiguration.getRawValue() == null) {
            return false;
        }

        if (descriptor.getControllerServiceDefinition() != null) {
            if (value != null) {
                final ControllerServiceNode oldNode = serviceProvider.getControllerServiceNode(value);
                if (oldNode != null) {
                    oldNode.removeReference(this);
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
                    props.put(descriptor, null);
                }

                props.putAll(properties);
                return props;
            }
        }
    }

    @Override
    public Map<PropertyDescriptor, String> getRawPropertyValues() {
        return getPropertyValues(PropertyConfiguration::getRawValue);
    }

    @Override
    public Map<PropertyDescriptor, String> getEffectivePropertyValues() {
        return getPropertyValues(config -> config.getEffectiveValue(getParameterContext()));
    }

    private Map<PropertyDescriptor, String> getPropertyValues(final Function<PropertyConfiguration, String> valueFunction) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getIdentifier())) {
            final List<PropertyDescriptor> supported = getComponent().getPropertyDescriptors();

            final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
            for (final PropertyDescriptor descriptor : supported) {
                props.put(descriptor, null);
            }

            properties.forEach((descriptor, config) -> props.put(descriptor, valueFunction.apply(config)));
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
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : this.properties.entrySet()) {
            final PropertyDescriptor propertyDescriptor = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();

            if (propertyDescriptor == null || configuration == null || configuration.getRawValue() == null) {
                continue;
            }

            setProperty(propertyDescriptor.getName(), configuration, descriptor -> createPropertyConfiguration(descriptor.getDefaultValue(), descriptor.isExpressionLanguageSupported()));
        }
    }

    private PropertyConfiguration createPropertyConfiguration(final String value, final boolean supportsEL) {
        final ParameterParser parser = new ExpressionLanguageAwareParameterParser();
        final ParameterTokenList references = parser.parseTokens(value);
        return new PropertyConfiguration(value, references);
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

        final String newFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls);
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
        final ValidationContext validationContext = validationContextFactory.newValidationContext(properties, annotationData, getProcessGroupIdentifier(), getIdentifier(), parameterContext);
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

        for (final PropertyDescriptor propertyDescriptor : validationContext.getProperties().keySet()) {
            final Collection<String> referencedParameters = validationContext.getReferencedParameters(propertyDescriptor.getName());

            if (parameterContext == null && !referencedParameters.isEmpty()) {
                results.add(new ValidationResult.Builder()
                    .subject(propertyDescriptor.getDisplayName())
                    .valid(false)
                    .explanation("Property references one or more Parameters but no Parameter Context is currently set on the Process Group")
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
                }
            }
        }

        return results;
    }

    protected final Collection<ValidationResult> validateReferencedControllerServices(final ValidationContext validationContext) {
        final List<PropertyDescriptor> supportedDescriptors = getComponent().getPropertyDescriptors();
        if (supportedDescriptors == null) {
            return Collections.emptyList();
        }

        final Collection<ValidationResult> validationResults = new ArrayList<>();
        for (final PropertyDescriptor descriptor : supportedDescriptors) {
            if (descriptor.getControllerServiceDefinition() == null) {
                // skip properties that aren't for a controller service
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
            }
        }

        return validationResults;
    }


    private ValidationResult validateControllerServiceApi(final PropertyDescriptor descriptor, final ControllerServiceNode controllerServiceNode) {
        final Class<? extends ControllerService> controllerServiceApiClass = descriptor.getControllerServiceDefinition();
        final ClassLoader controllerServiceApiClassLoader = controllerServiceApiClass.getClassLoader();
        final ExtensionManager extensionManager = serviceProvider.getExtensionManager();

        final String serviceId = controllerServiceNode.getIdentifier();
        final String propertyName = descriptor.getDisplayName();

        final Bundle controllerServiceApiBundle = extensionManager.getBundle(controllerServiceApiClassLoader);
        if (controllerServiceApiBundle == null) {
            return createInvalidResult(serviceId, propertyName, "Unable to find bundle for ControllerService API class " + controllerServiceApiClass.getCanonicalName());
        }
        final BundleCoordinate controllerServiceApiCoordinate = controllerServiceApiBundle.getBundleDetails().getCoordinate();

        final Bundle controllerServiceBundle = extensionManager.getBundle(controllerServiceNode.getBundleCoordinate());
        if (controllerServiceBundle == null) {
            return createInvalidResult(serviceId, propertyName, "Unable to find bundle for coordinate " + controllerServiceNode.getBundleCoordinate());
        }
        final BundleCoordinate controllerServiceCoordinate = controllerServiceBundle.getBundleDetails().getCoordinate();

        final boolean matchesApi = matchesApi(extensionManager, controllerServiceBundle, controllerServiceApiCoordinate);

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
    private boolean matchesApi(final ExtensionManager extensionManager, final Bundle controllerServiceImplBundle, final BundleCoordinate requiredApiCoordinate) {
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
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().getPropertyDescriptor(name);
        }
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().getPropertyDescriptors();
        }
    }


    private void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getComponent().getClass(), getComponent().getIdentifier())) {
            getComponent().onPropertyModified(descriptor, oldValue, newValue);
        }
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

            context = getValidationContextFactory().newValidationContext(getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier(), getParameterContext());

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

    protected abstract ParameterContext getParameterContext();

}
