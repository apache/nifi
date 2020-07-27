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
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractConfiguredComponent implements ConfigurableComponent, ConfiguredComponent {
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfiguredComponent.class);

    private final String id;
    private final ValidationContextFactory validationContextFactory;
    private final ControllerServiceProvider serviceProvider;
    private final AtomicReference<String> name;
    private final AtomicReference<String> annotationData = new AtomicReference<>();
    private final String componentType;
    private final String componentCanonicalClass;
    private final VariableRegistry variableRegistry;
    private final ReloadComponent reloadComponent;

    private final AtomicBoolean isExtensionMissing;

    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<PropertyDescriptor, String> properties = new ConcurrentHashMap<>();

    public AbstractConfiguredComponent(final String id,
                                       final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                                       final String componentType, final String componentCanonicalClass, final VariableRegistry variableRegistry,
                                       final ReloadComponent reloadComponent, final boolean isExtensionMissing) {
        this.id = id;
        this.validationContextFactory = validationContextFactory;
        this.serviceProvider = serviceProvider;
        this.name = new AtomicReference<>(componentType);
        this.componentType = componentType;
        this.componentCanonicalClass = componentCanonicalClass;
        this.variableRegistry = variableRegistry;
        this.isExtensionMissing = new AtomicBoolean(isExtensionMissing);
        this.reloadComponent = reloadComponent;
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
        this.name.set(Objects.requireNonNull(name).intern());
    }

    @Override
    public String getAnnotationData() {
        return annotationData.get();
    }

    @Override
    public void setAnnotationData(final String data) {
        annotationData.set(data);
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }

        lock.lock();
        try {
            verifyModifiable();

            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), id)) {
                boolean classpathChanged = false;
                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    // determine if any of the property changes require resetting the InstanceClassLoader
                    final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(entry.getKey());
                    if (descriptor.isDynamicClasspathModifier()) {
                        classpathChanged = true;
                    }

                    if (entry.getKey() != null && entry.getValue() == null) {
                        removeProperty(entry.getKey());
                    } else if (entry.getKey() != null) {
                        setProperty(entry.getKey(), entry.getValue());
                    }
                }

                // if at least one property with dynamicallyModifiesClasspath(true) was set, then re-calculate the module paths
                // and reset the InstanceClassLoader to the new module paths
                if (classpathChanged) {
                    final Set<String> modulePaths = new LinkedHashSet<>();
                    for (final Map.Entry<PropertyDescriptor, String> entry : this.properties.entrySet()) {
                        final PropertyDescriptor descriptor = entry.getKey();
                        if (descriptor.isDynamicClasspathModifier() && !StringUtils.isEmpty(entry.getValue())) {
                            final StandardPropertyValue propertyValue = new StandardPropertyValue(entry.getValue(), null, variableRegistry);
                            modulePaths.add(propertyValue.evaluateAttributeExpressions().getValue());
                        }
                    }
                    processClasspathModifiers(modulePaths);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // Keep setProperty/removeProperty private so that all calls go through setProperties
    private void setProperty(final String name, final String value) {
        if (null == name || null == value) {
            throw new IllegalArgumentException("Name or Value can not be null");
        }

        final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(name);

        final String oldValue = properties.put(descriptor, value);
        if (!value.equals(oldValue)) {

            if (descriptor.getControllerServiceDefinition() != null) {
                if (oldValue != null) {
                    final ControllerServiceNode oldNode = serviceProvider.getControllerServiceNode(oldValue);
                    if (oldNode != null) {
                        oldNode.removeReference(this);
                    }
                }

                final ControllerServiceNode newNode = serviceProvider.getControllerServiceNode(value);
                if (newNode != null) {
                    newNode.addReference(this);
                }
            }

            try {
                getComponent().onPropertyModified(descriptor, oldValue, value);
            } catch (final Exception e) {
                // nothing really to do here...
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
     * @return true if removed; false otherwise
     * @throws java.lang.IllegalArgumentException if the name is null
     */
    private boolean removeProperty(final String name) {
        if (null == name) {
            throw new IllegalArgumentException("Name can not be null");
        }

        final PropertyDescriptor descriptor = getComponent().getPropertyDescriptor(name);
        String value = null;
        if (!descriptor.isRequired() && (value = properties.remove(descriptor)) != null) {

            if (descriptor.getControllerServiceDefinition() != null) {
                if (value != null) {
                    final ControllerServiceNode oldNode = serviceProvider.getControllerServiceNode(value);
                    if (oldNode != null) {
                        oldNode.removeReference(this);
                    }
                }
            }

            try {
                getComponent().onPropertyModified(descriptor, value, null);
            } catch (final Exception e) {
                getLogger().error(e.getMessage(), e);
            }

            return true;
        }

        return false;
    }

    /**
     * Triggers the reloading of the underlying component using a new InstanceClassLoader that includes the additional URL resources.
     *
     * @param modulePaths a list of module paths where each entry can be a comma-separated list of multiple module paths
     */
    private void processClasspathModifiers(final Set<String> modulePaths) {
        try {
            // compute the URLs from all the modules paths
            final URL[] urls = ClassLoaderUtils.getURLsForClasspath(modulePaths, null, true);

            // convert to a set of URLs
            final Set<URL> additionalUrls = new LinkedHashSet<>();
            if (urls != null) {
                for (final URL url : urls) {
                    additionalUrls.add(url);
                }
            }

            // reload the underlying component with a new InstanceClassLoader that includes the new URLs
            reload(additionalUrls);

        } catch (Exception e) {
            getLogger().warn("Error processing classpath resources for " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            final List<PropertyDescriptor> supported = getComponent().getPropertyDescriptors();
            if (supported == null || supported.isEmpty()) {
                return Collections.unmodifiableMap(properties);
            } else {
                final Map<PropertyDescriptor, String> props = new LinkedHashMap<>();
                for (final PropertyDescriptor descriptor : supported) {
                    props.put(descriptor, null);
                }
                props.putAll(properties);
                return props;
            }
        }
    }

    @Override
    public String getProperty(final PropertyDescriptor property) {
        return properties.get(property);
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

        if (!(obj instanceof ConfiguredComponent)) {
            return false;
        }

        final ConfiguredComponent other = (ConfiguredComponent) obj;
        return id.equals(other.getIdentifier());
    }

    @Override
    public String toString() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().toString();
        }
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            final Collection<ValidationResult> validationResults = getComponent().validate(context);

            // validate selected controller services implement the API required by the processor

            final List<PropertyDescriptor> supportedDescriptors = getComponent().getPropertyDescriptors();
            if (null != supportedDescriptors) {
                for (final PropertyDescriptor descriptor : supportedDescriptors) {
                    if (descriptor.getControllerServiceDefinition() == null) {
                        // skip properties that aren't for a controller service
                        continue;
                    }

                    final String controllerServiceId = context.getProperty(descriptor).getValue();
                    if (controllerServiceId == null) {
                        // if the property value is null we should already have a validation error
                        continue;
                    }

                    final ControllerServiceNode controllerServiceNode = getControllerServiceProvider().getControllerServiceNode(controllerServiceId);
                    if (controllerServiceNode == null) {
                        // if the node was null we should already have a validation error
                        continue;
                    }

                    final Class<? extends ControllerService> controllerServiceApiClass = descriptor.getControllerServiceDefinition();
                    final ClassLoader controllerServiceApiClassLoader = controllerServiceApiClass.getClassLoader();

                    final Bundle controllerServiceApiBundle = ExtensionManager.getBundle(controllerServiceApiClassLoader);
                    final BundleCoordinate controllerServiceApiCoordinate = controllerServiceApiBundle.getBundleDetails().getCoordinate();

                    final Bundle controllerServiceBundle = ExtensionManager.getBundle(controllerServiceNode.getBundleCoordinate());
                    final BundleCoordinate controllerServiceCoordinate = controllerServiceBundle.getBundleDetails().getCoordinate();

                    final boolean matchesApi = matchesApi(controllerServiceBundle, controllerServiceApiCoordinate);

                    if (!matchesApi) {
                        final String controllerServiceType = controllerServiceNode.getComponentType();
                        final String controllerServiceApiType = controllerServiceApiClass.getSimpleName();

                        final String explanation = new StringBuilder()
                                .append(controllerServiceType).append(" - ").append(controllerServiceCoordinate.getVersion())
                                .append(" from ").append(controllerServiceCoordinate.getGroup()).append(" - ").append(controllerServiceCoordinate.getId())
                                .append(" is not compatible with ").append(controllerServiceApiType).append(" - ").append(controllerServiceApiCoordinate.getVersion())
                                .append(" from ").append(controllerServiceApiCoordinate.getGroup()).append(" - ").append(controllerServiceApiCoordinate.getId())
                                .toString();

                        validationResults.add(new ValidationResult.Builder()
                                .input(controllerServiceId)
                                .subject(descriptor.getDisplayName())
                                .valid(false)
                                .explanation(explanation)
                                .build());
                    }

                }
            }

            return validationResults;
        }
    }

    /**
     * Determines if the given controller service node has the required API as an ancestor.
     *
     * @param controllerServiceImplBundle the bundle of a controller service being referenced by a processor
     * @param requiredApiCoordinate the controller service API required by the processor
     * @return true if the controller service node has the require API as an ancestor, false otherwise
     */
    private boolean matchesApi(final Bundle controllerServiceImplBundle, final BundleCoordinate requiredApiCoordinate) {
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
            final Bundle controllerServiceDependencyBundle = ExtensionManager.getBundle(controllerServiceDependencyCoordinate);
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
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().getPropertyDescriptor(name);
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            getComponent().onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
            return getComponent().getPropertyDescriptors();
        }
    }

    @Override
    public boolean isValid() {
        final Collection<ValidationResult> validationResults = validate(validationContextFactory.newValidationContext(
            getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier()));

        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return getValidationErrors(Collections.<String>emptySet());
    }

    public Collection<ValidationResult> getValidationErrors(final Set<String> serviceIdentifiersNotToValidate) {
        final List<ValidationResult> results = new ArrayList<>();
        lock.lock();
        try {
            final ValidationContext validationContext = validationContextFactory.newValidationContext(
                serviceIdentifiersNotToValidate, getProperties(), getAnnotationData(), getProcessGroupIdentifier(), getIdentifier());

            final Collection<ValidationResult> validationResults;
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(getComponent().getClass(), getComponent().getIdentifier())) {
                validationResults = getComponent().validate(validationContext);
            }

            for (final ValidationResult result : validationResults) {
                if (!result.isValid()) {
                    results.add(result);
                }
            }
        } catch (final Throwable t) {
            logger.error("Failed to perform validation of " + this, t);
            results.add(new ValidationResult.Builder().explanation("Failed to run validation due to " + t.toString()).valid(false).build());
        } finally {
            lock.unlock();
        }
        return results;
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

    protected VariableRegistry getVariableRegistry() {
        return this.variableRegistry;
    }

    protected ReloadComponent getReloadComponent() {
        return this.reloadComponent;
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
}
