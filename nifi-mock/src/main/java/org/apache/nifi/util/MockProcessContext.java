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
package org.apache.nifi.util;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SchedulingContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.state.MockStateManager;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MockProcessContext extends MockControllerServiceLookup implements SchedulingContext, ControllerServiceLookup, NodeTypeProvider {

    private final ConfigurableComponent component;
    private final String componentName;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final StateManager stateManager;
    private final VariableRegistry variableRegistry;

    private String annotationData = null;
    private boolean yieldCalled = false;
    private boolean enableExpressionValidation = false;
    private boolean allowExpressionValidation = true;
    private volatile boolean incomingConnection = true;
    private volatile boolean nonLoopConnection = true;
    private volatile InputRequirement inputRequirement = null;
    private int maxConcurrentTasks = 1;

    private volatile Set<Relationship> connections = new HashSet<>();
    private volatile Set<Relationship> unavailableRelationships = new HashSet<>();

    private volatile boolean isClustered;
    private volatile boolean isPrimaryNode;

    public MockProcessContext(final ConfigurableComponent component) {
        this(component, null);
    }

    public MockProcessContext(final ConfigurableComponent component, final String componentName) {
        this(component, componentName, new MockStateManager(component), VariableRegistry.EMPTY_REGISTRY);
    }

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component being mocked
     * @param stateManager state manager
     * @param variableRegistry variableRegistry
     */
    public MockProcessContext(final ConfigurableComponent component, final StateManager stateManager, final VariableRegistry variableRegistry) {
        this(component,null,stateManager,variableRegistry);
    }

    /**
     * Creates a new MockProcessContext for the given Processor with given name
     *
     * @param component being mocked
     * @param componentName the name to be given the component;
     * @param stateManager state manager
     * @param variableRegistry variableRegistry
     */
    public MockProcessContext(final ConfigurableComponent component,
                              final String componentName,
                              final StateManager stateManager,
                              final VariableRegistry variableRegistry) {
        this.component = Objects.requireNonNull(component);
        this.componentName = componentName == null ? "" : componentName;
        this.inputRequirement = component.getClass().getAnnotation(InputRequirement.class);
        this.stateManager = stateManager;
        this.variableRegistry = variableRegistry;
    }

    public MockProcessContext(final ControllerService component,
                              final MockProcessContext context,
                              final StateManager stateManager,
                              final VariableRegistry variableRegistry) {
        this(component, null, context, stateManager, variableRegistry);
    }

    public MockProcessContext(final ControllerService component,
                              final String componentName,
                              final MockProcessContext context,
                              final StateManager stateManager,
                              final VariableRegistry variableRegistry) {
        this(component, componentName, stateManager, variableRegistry);

        try {
            annotationData = context.getControllerServiceAnnotationData(component);
            final Map<PropertyDescriptor, String> props = context.getControllerServiceProperties(component);
            properties.putAll(props);

            super.addControllerServices(context);
        } catch (IllegalArgumentException e) {
            // do nothing...the service is being loaded
        }
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return getProperty(descriptor.getName());
    }

    @Override
    public PropertyValue getProperty(final String propertyName) {
        final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = properties.get(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return new MockPropertyValue(propValue, this, variableRegistry, (enableExpressionValidation && allowExpressionValidation) ? descriptor : null);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new MockPropertyValue(rawValue, this, variableRegistry);
    }

    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return setProperty(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
    }

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, the property value is not updated. In
     * either case, the ValidationResult is returned, indicating whether or not
     * the property is valid
     *
     * @param descriptor of property to modify
     * @param value new value
     * @return result
     */
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        requireNonNull(descriptor);
        requireNonNull(value, "Cannot set property to null value; if the intent is to remove the property, call removeProperty instead");
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(descriptor.getName());

        final ValidationResult result = fullyPopulatedDescriptor.validate(value, new MockValidationContext(this, stateManager, variableRegistry));
        String oldValue = properties.put(fullyPopulatedDescriptor, value);
        if (oldValue == null) {
            oldValue = fullyPopulatedDescriptor.getDefaultValue();
        }
        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            component.onPropertyModified(fullyPopulatedDescriptor, oldValue, value);
        }

        return result;
    }

    public boolean removeProperty(final PropertyDescriptor descriptor) {
        Objects.requireNonNull(descriptor);
        return removeProperty(descriptor.getName());
    }

    public boolean removeProperty(final String property) {
        Objects.requireNonNull(property);
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(property);
        String value = null;

        if ((value = properties.remove(fullyPopulatedDescriptor)) != null) {
            if (!value.equals(fullyPopulatedDescriptor.getDefaultValue())) {
                component.onPropertyModified(fullyPopulatedDescriptor, value, null);
            }

            return true;
        }
        return false;
    }

    public void clearProperties() {
        Map<PropertyDescriptor, String> properties = getProperties();
        for (Map.Entry<PropertyDescriptor, String> e : properties.entrySet()) {
            removeProperty(e.getKey());
        }
    }

    @Override
    public void yield() {
        yieldCalled = true;
    }

    public boolean isYieldCalled() {
        return yieldCalled;
    }

    public void addControllerService(final String serviceIdentifier, final ControllerService controllerService, final Map<PropertyDescriptor, String> properties, final String annotationData) {
        requireNonNull(controllerService);
        final ControllerServiceConfiguration config = addControllerService(controllerService);
        config.setProperties(properties);
        config.setAnnotationData(annotationData);
    }

    @Override
    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public void setAnnotationData(final String annotationData) {
        this.annotationData = annotationData;
    }

    @Override
    public String getAnnotationData() {
        return annotationData;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
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

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    /**
     * Validates the current properties, returning ValidationResults for any
     * invalid properties. All processor defined properties will be validated.
     * If they are not included in the in the purposed configuration, the
     * default value will be used.
     *
     * @return Collection of validation result objects for any invalid findings
     * only. If the collection is empty then the processor is valid. Guaranteed
     * non-null
     */
    public Collection<ValidationResult> validate() {
        final List<ValidationResult> results = new ArrayList<>();
        final ValidationContext validationContext = new MockValidationContext(this, stateManager, variableRegistry);
        final Collection<ValidationResult> componentResults = component.validate(validationContext);
        results.addAll(componentResults);

        final Collection<ValidationResult> serviceResults = validateReferencedControllerServices(validationContext);
        results.addAll(serviceResults);

        // verify all controller services are enabled
        for (Map.Entry<String, ControllerServiceConfiguration> service : getControllerServices().entrySet()) {
            if (!service.getValue().isEnabled()) {
                results.add(new ValidationResult.Builder()
                        .explanation("Controller service " + service.getKey() + " for " + this.getName() + " is not enabled")
                        .valid(false)
                        .build());
            }
        }
        return results;
    }

    protected final Collection<ValidationResult> validateReferencedControllerServices(final ValidationContext validationContext) {
        final List<PropertyDescriptor> supportedDescriptors = component.getPropertyDescriptors();
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

            final ControllerService controllerService = getControllerService(controllerServiceId);
            if (controllerService == null) {
                final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(descriptor.getDisplayName())
                    .input(controllerServiceId)
                    .explanation("Invalid Controller Service: " + controllerServiceId + " is not a valid Controller Service Identifier")
                    .build();

                validationResults.add(result);
                continue;
            }

            final Class<? extends ControllerService> requiredServiceClass = descriptor.getControllerServiceDefinition();
            if (!requiredServiceClass.isAssignableFrom(controllerService.getClass())) {
                final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(descriptor.getDisplayName())
                    .input(controllerServiceId)
                    .explanation("Invalid Controller Service: " + controllerServiceId + " does not implement interface " + requiredServiceClass)
                    .build();

                validationResults.add(result);
                continue;
            }

            final boolean enabled = isControllerServiceEnabled(controllerServiceId);
            if (!enabled) {
                validationResults.add(new ValidationResult.Builder()
                    .input(controllerServiceId)
                    .subject(descriptor.getDisplayName())
                    .explanation("Controller Service with ID " + controllerServiceId + " is not enabled")
                    .valid(false)
                    .build());
            }
        }

        return validationResults;
    }

    public boolean isValid() {
        for (final ValidationResult result : validate()) {
            if (!result.isValid()) {
                return false;
            }
        }

        return true;
    }

    public void assertValid() {
        final StringBuilder sb = new StringBuilder();
        int failureCount = 0;

        for (final ValidationResult result : validate()) {
            if (!result.isValid()) {
                sb.append(result.toString()).append("\n");
                failureCount++;
            }
        }

        if (failureCount > 0) {
            Assert.fail("Processor has " + failureCount + " validation failures:\n" + sb.toString());
        }
    }

    @Override
    public String encrypt(final String unencrypted) {
        return "enc{" + unencrypted + "}";
    }

    @Override
    public String decrypt(final String encrypted) {
        if (encrypted.startsWith("enc{") && encrypted.endsWith("}")) {
            return encrypted.substring(4, encrypted.length() - 2);
        }
        return encrypted;
    }

    public void setValidateExpressionUsage(final boolean validate) {
        allowExpressionValidation = validate;
    }

    public void enableExpressionValidation() {
        enableExpressionValidation = true;
    }

    public void disableExpressionValidation() {
        enableExpressionValidation = false;
    }

    Map<PropertyDescriptor, String> getControllerServiceProperties(final ControllerService controllerService) {
        return super.getConfiguration(controllerService.getIdentifier()).getProperties();
    }

    String getControllerServiceAnnotationData(final ControllerService controllerService) {
        return super.getConfiguration(controllerService.getIdentifier()).getAnnotationData();
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public void leaseControllerService(final String identifier) {
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        if (!(component instanceof Processor)) {
            return Collections.emptySet();
        }

        final Set<Relationship> relationships = new HashSet<>(((Processor) component).getRelationships());
        relationships.removeAll(unavailableRelationships);
        return relationships;
    }

    public void setUnavailableRelationships(final Set<Relationship> relationships) {
        this.unavailableRelationships = Collections.unmodifiableSet(new HashSet<>(relationships));
    }

    public Set<Relationship> getUnavailableRelationships() {
        return unavailableRelationships;
    }

    @Override
    public boolean hasIncomingConnection() {
        return incomingConnection;
    }

    public void setIncomingConnection(final boolean hasIncomingConnection) {
        this.incomingConnection = hasIncomingConnection;
    }

    @Override
    public boolean hasConnection(Relationship relationship) {
        return this.connections.contains(relationship);
    }

    public void setNonLoopConnection(final boolean hasNonLoopConnection) {
        this.nonLoopConnection = hasNonLoopConnection;
    }

    @Override
    public boolean hasNonLoopConnection() {
        return nonLoopConnection;
    }

    public void addConnection(final Relationship relationship) {
        this.connections.add(relationship);
    }

    public void removeConnection(final Relationship relationship) {
        this.connections.remove(relationship);
    }

    public void setConnections(final Set<Relationship> connections) {
        if (connections == null) {
            this.connections = Collections.emptySet();
        } else {
            this.connections = Collections.unmodifiableSet(connections);
        }
    }

    @Override
    public boolean isExpressionLanguagePresent(final PropertyDescriptor property) {
        if (property == null || !property.isExpressionLanguageSupported()) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(getProperty(property).getValue());
        return (elRanges != null && !elRanges.isEmpty());
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    @Override
    public String getName() {
        return componentName;
    }

    protected void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Override
    public boolean isClustered() {
        return isClustered;
    }

    @Override
    public boolean isPrimary() {
        return isPrimaryNode;
    }

    public void setClustered(boolean clustered) {
        isClustered = clustered;
    }

    public void setPrimaryNode(boolean primaryNode) {
        if (!isClustered && primaryNode) {
            throw new IllegalArgumentException("Primary node is only available in cluster. Use setClustered(true) first.");
        }
        isPrimaryNode = primaryNode;
    }

    @Override
    public InputRequirement getInputRequirement() {
        return inputRequirement;
    }

}
