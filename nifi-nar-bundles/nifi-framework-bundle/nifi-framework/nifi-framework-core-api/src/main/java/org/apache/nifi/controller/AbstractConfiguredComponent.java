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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.NarCloseable;

public abstract class AbstractConfiguredComponent implements ConfigurableComponent, ConfiguredComponent {

    private final String id;
    private final ConfigurableComponent component;
    private final ValidationContextFactory validationContextFactory;
    private final ControllerServiceProvider serviceProvider;
    private final AtomicReference<String> name;
    private final AtomicReference<String> annotationData = new AtomicReference<>();
    private final String componentType;
    private final String componentCanonicalClass;

    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<PropertyDescriptor, String> properties = new ConcurrentHashMap<>();

    public AbstractConfiguredComponent(final ConfigurableComponent component, final String id,
        final ValidationContextFactory validationContextFactory, final ControllerServiceProvider serviceProvider,
        final String componentType, final String componentCanonicalClass) {
        this.id = id;
        this.component = component;
        this.validationContextFactory = validationContextFactory;
        this.serviceProvider = serviceProvider;
        this.name = new AtomicReference<>(component.getClass().getSimpleName());
        this.componentType = componentType;
        this.componentCanonicalClass = componentCanonicalClass;
    }

    @Override
    public String getIdentifier() {
        return id;
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
    public void setProperty(final String name, final String value) {
        if (null == name || null == value) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        try {
            verifyModifiable();

            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
                final PropertyDescriptor descriptor = component.getPropertyDescriptor(name);

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
                        component.onPropertyModified(descriptor, oldValue, value);
                    } catch (final Exception e) {
                        // nothing really to do here...
                    }
                }
            }
        } finally {
            lock.unlock();
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
    @Override
    public boolean removeProperty(final String name) {
        if (null == name) {
            throw new IllegalArgumentException();
        }

        lock.lock();
        try {
            verifyModifiable();

            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
                final PropertyDescriptor descriptor = component.getPropertyDescriptor(name);
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
                        component.onPropertyModified(descriptor, value, null);
                    } catch (final Exception e) {
                        // nothing really to do here...
                    }

                    return true;
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            final List<PropertyDescriptor> supported = component.getPropertyDescriptors();
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
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            return component.toString();
        }
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            return component.validate(context);
        }
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            return component.getPropertyDescriptor(name);
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            component.onPropertyModified(descriptor, oldValue, newValue);
        }
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
            return component.getPropertyDescriptors();
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
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(component.getClass())) {
                validationResults = component.validate(validationContext);
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

    protected abstract String getProcessGroupIdentifier();

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
}
