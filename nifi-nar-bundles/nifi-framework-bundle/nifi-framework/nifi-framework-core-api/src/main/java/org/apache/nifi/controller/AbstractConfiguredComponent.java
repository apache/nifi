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
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.net.MalformedURLException;
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

    private final String id;
    private final ValidationContextFactory validationContextFactory;
    private final ControllerServiceProvider serviceProvider;
    private final AtomicReference<String> name;
    private final AtomicReference<String> annotationData = new AtomicReference<>();
    private final String componentType;
    private final String componentCanonicalClass;
    private final VariableRegistry variableRegistry;

    private final AtomicBoolean isExtensionMissing;

    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<PropertyDescriptor, String> properties = new ConcurrentHashMap<>();

    public AbstractConfiguredComponent(final String id,
                                       final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
                                       final String componentType, final String componentCanonicalClass, final VariableRegistry variableRegistry,
                                       final boolean isExtensionMissing) {
        this.id = id;
        this.validationContextFactory = validationContextFactory;
        this.serviceProvider = serviceProvider;
        this.name = new AtomicReference<>(componentType);
        this.componentType = componentType;
        this.componentCanonicalClass = componentCanonicalClass;
        this.variableRegistry = variableRegistry;
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
     * Adds all of the modules identified by the given module paths to the InstanceClassLoader for this component.
     *
     * @param modulePaths a list of module paths where each entry can be a comma-separated list of multiple module paths
     */
    private void processClasspathModifiers(final Set<String> modulePaths) {
        try {
            final URL[] urls = ClassLoaderUtils.getURLsForClasspath(modulePaths, null, true);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Adding {} resources to the classpath for {}", new Object[] {urls.length, name});
                for (URL url : urls) {
                    getLogger().debug(url.getFile());
                }
            }

            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            if (!(classLoader instanceof InstanceClassLoader)) {
                // Really shouldn't happen, but if we somehow got here and don't have an InstanceClassLoader then log a warning and move on
                final String classLoaderName = classLoader == null ? "null" : classLoader.getClass().getName();
                if (getLogger().isWarnEnabled()) {
                    getLogger().warn(String.format("Unable to modify the classpath for %s, expected InstanceClassLoader, but found %s", name, classLoaderName));
                }
                return;
            }

            final InstanceClassLoader instanceClassLoader = (InstanceClassLoader) classLoader;
            instanceClassLoader.setInstanceResources(urls);
        } catch (MalformedURLException e) {
            // Shouldn't get here since we are suppressing errors
            getLogger().warn("Error processing classpath resources", e);
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
            return getComponent().validate(context);
        }
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

}
