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
import org.apache.nifi.components.PropertyDependency;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Assertions;

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
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

public class MockProcessContext extends MockControllerServiceLookup implements ProcessContext, ControllerServiceLookup, NodeTypeProvider {

    private final ConfigurableComponent component;
    private final String componentName;
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final StateManager stateManager;

    private String annotationData = null;
    private boolean yieldCalled = false;
    private boolean allowExpressionValidation = true;
    private volatile boolean incomingConnection = true;
    private volatile boolean nonLoopConnection = true;
    private volatile InputRequirement inputRequirement = null;
    private int maxConcurrentTasks = 1;

    private volatile Set<Relationship> connections = new HashSet<>();
    private volatile Set<Relationship> unavailableRelationships = new HashSet<>();

    private volatile boolean isClustered;
    private volatile boolean isConfiguredForClustering;
    private volatile boolean isPrimaryNode;
    private volatile boolean isConnected = true;

    // This is only for testing purposes as we don't want to set env/sys variables in the tests
    private final Map<String, String> environmentVariables;

    private final ParameterLookup parameterLookup;

    public MockProcessContext(final ConfigurableComponent component) {
        this(component, null, new MockStateManager(component), null);
    }

    public MockProcessContext(final ConfigurableComponent component, final String componentName) {
        this(component, componentName, new MockStateManager(component), null);
    }

    /**
     * Creates a new MockProcessContext for the given Processor
     *
     * @param component    being mocked
     * @param stateManager state manager
     */
    public MockProcessContext(final ConfigurableComponent component, final StateManager stateManager) {
        this(component, null, stateManager, null);
    }

    public MockProcessContext(final ControllerService component,
                              final MockProcessContext context,
                              final StateManager stateManager,
                              final Map<String, String> environmentVariables) {
        this(component, null, context, stateManager, environmentVariables);
    }

    public MockProcessContext(final ControllerService component,
                              final String componentName,
                              final MockProcessContext context,
                              final StateManager stateManager,
                              final Map<String, String> environmentVariables) {
        this(component, componentName, stateManager, environmentVariables);

        try {
            annotationData = context.getControllerServiceAnnotationData(component);

            final Map<PropertyDescriptor, String> props = context.getControllerServiceProperties(component);
            properties.putAll(props);

            super.addControllerServices(context);
        } catch (IllegalArgumentException ignored) {
            // do nothing...the service is being loaded
        }
    }

    /**
     * Creates a new MockProcessContext for the given Processor with given name
     *
     * @param component     being mocked
     * @param componentName the name to be given the component;
     * @param stateManager  state manager
     */
    public MockProcessContext(final ConfigurableComponent component,
                              final String componentName,
                              final StateManager stateManager,
                              final Map<String, String> environmentVariables) {
        this(component, componentName, stateManager, environmentVariables, null);
    }

    /**
     * Creates a new MockProcessContext for the given Processor with given name
     *
     * @param component     being mocked
     * @param componentName the name to be given the component;
     * @param stateManager  state manager
     * @param environmentVariables the environment variables
     * @param contextParameters the context parameters
     */
    public MockProcessContext(final ConfigurableComponent component,
                              final String componentName,
                              final StateManager stateManager,
                              final Map<String, String> environmentVariables,
                              final Map<String, String> contextParameters) {
        this.component = Objects.requireNonNull(component);
        this.componentName = componentName == null ? "" : componentName;
        this.inputRequirement = component.getClass().getAnnotation(InputRequirement.class);
        this.stateManager = stateManager;
        this.environmentVariables = environmentVariables;
        this.parameterLookup = contextParameters != null ? new MockParameterLookup(contextParameters) : ParameterLookup.EMPTY;
    }


    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return getProperty(descriptor.getName());
    }

    public PropertyValue getPropertyWithoutValidatingExpressions(final PropertyDescriptor propertyDescriptor) {
        final PropertyDescriptor canonicalDescriptor = component.getPropertyDescriptor(propertyDescriptor.getName());
        if (canonicalDescriptor == null) {
            return null;
        }

        final String setPropertyValue = properties.get(canonicalDescriptor);
        final String propValue = (setPropertyValue == null) ? canonicalDescriptor.getDefaultValue() : setPropertyValue;

        return new MockPropertyValue(propValue, this, canonicalDescriptor, true, environmentVariables);
    }

    @Override
    public PropertyValue getProperty(final String propertyName) {
        final PropertyDescriptor descriptor = component.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final List<ValidatedPropertyDependency> unsatisfiedDependencies = determineUnsatisfiedDependencies(descriptor);
        if (!unsatisfiedDependencies.isEmpty()) {
            throw new AssertionError("Attempted to use property \"%s\" whose dependencies are not satisfied:\n%s\n\n%s".formatted(
                    descriptor.getName(),
                    unsatisfiedDependencies.stream().map(ValidatedPropertyDependency::toString).collect(Collectors.joining("\n")),
                    """
                        Properties whose dependencies are not satisfied are not shown to the user,
                        their values should be ignored by the implementation if the necessary dependencies are not fulfilled.
                        This precaution is crucial to prevent potentially confusing and unpredictable side effects for the user.
                        See NIFI-14400 (https://issues.apache.org/jira/browse/NIFI-14400) for more background."""
            ));
        }

        final String setPropertyValue = properties.get(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        final boolean alreadyEvaluated = !this.allowExpressionValidation;
        return new MockPropertyValue(propValue, this, descriptor, alreadyEvaluated, environmentVariables, parameterLookup);
    }

    private List<ValidatedPropertyDependency> determineUnsatisfiedDependencies(PropertyDescriptor descriptor) {
        return validatedDependencies(descriptor).stream().filter(not(ValidatedPropertyDependency::isSatisfied)).toList();
    }

    private List<ValidatedPropertyDependency> validatedDependencies(final PropertyDescriptor descriptor) {
        return descriptor.getDependencies().stream().map(dependency -> {
                    final PropertyDescriptor dependencyDescriptor =
                            component.getPropertyDescriptor(dependency.getPropertyName());

                    if (dependencyDescriptor == null) {
                        return new ValidatedPropertyDependency(dependency, null);
                    }
                    component.getPropertyDescriptor(dependency.getPropertyName());

                    if (!determineUnsatisfiedDependencies(dependencyDescriptor).isEmpty()) {
                        return new ValidatedPropertyDependency(dependency, null);
                    }

                    final String dependencyPropertyValue =
                            properties.getOrDefault(dependencyDescriptor, dependencyDescriptor.getDefaultValue());

                    return new ValidatedPropertyDependency(dependency, dependencyPropertyValue);
                }
        ).toList();
    }

    record ValidatedPropertyDependency(PropertyDependency dependency, String value) {
        public boolean isSatisfied() {
            Set<String> dependentValues = dependency.getDependentValues();

            return dependentValues == null ? value != null : dependentValues.contains(value);
        }

        @Override
        public String toString() {
            String result = isSatisfied() ? "satisfied" : "not satisfied";
            Set<String> dependentValues = dependency.getDependentValues();
            String reason = dependentValues == null
                    ? "requires any value" : "requires one of the values %s".formatted(dependentValues);
            return "Dependency \"%s\" is %s with value of %s; %s".formatted(dependency.getPropertyName(), result, value, reason);
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new MockPropertyValue(rawValue, this, environmentVariables, parameterLookup);
    }

    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return setProperty(new PropertyDescriptor.Builder().name(propertyName).build(), propertyValue);
    }

    public PropertyDescriptor getPropertyDescriptor(final String propertyName) {
        return component.getPropertyDescriptor(propertyName);
    }

    /**
     * Updates the value of the property with the given PropertyDescriptor to
     * the specified value IF and ONLY IF the value is valid according to the
     * descriptor's validator. Otherwise, the property value is not updated. In
     * either case, the ValidationResult is returned, indicating whether or not
     * the property is valid
     *
     * @param descriptor of property to modify
     * @param value      new value
     * @return result
     */
    public ValidationResult setProperty(final PropertyDescriptor descriptor, final String value) {
        Objects.requireNonNull(descriptor, "Cannot set property for null descriptor");
        Objects.requireNonNull(value, "Cannot set property " + descriptor.getName() + " to null value; if the intent is to remove the property, call removeProperty instead");
        final PropertyDescriptor fullyPopulatedDescriptor = component.getPropertyDescriptor(descriptor.getName());

        final ValidationResult result = fullyPopulatedDescriptor.validate(value, new MockValidationContext(this, stateManager));
        String oldValue = properties.put(fullyPopulatedDescriptor, value);
        if (oldValue == null) {
            oldValue = fullyPopulatedDescriptor.getDefaultValue();
        }

        if (!Objects.equals(value, oldValue)) {
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
        final Map<PropertyDescriptor, String> props = getProperties();
        for (final Map.Entry<PropertyDescriptor, String> e : props.entrySet()) {
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

    public void addControllerService(final ControllerService controllerService, final Map<PropertyDescriptor, String> properties, final String annotationData) {
        Objects.requireNonNull(controllerService);
        final ControllerServiceConfiguration config = addControllerService(controllerService);
        config.setProperties(properties);
        config.setAnnotationData(annotationData);
    }

    @Override
    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    @Override
    public ExecutionNode getExecutionNode() {
        return ExecutionNode.ALL;
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
        final Map<String, String> propValueMap = new LinkedHashMap<>();
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
        final ValidationContext validationContext = new MockValidationContext(this, stateManager);
        final Collection<ValidationResult> componentResults = component.validate(validationContext);
        final List<ValidationResult> results = new ArrayList<>(componentResults);

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
                sb.append(result).append("\n");
                failureCount++;
            }
        }

        if (failureCount > 0) {
            Assertions.fail("Processor has " + failureCount + " validation failures:\n" + sb);
        }
    }

    public void setValidateExpressionUsage(final boolean validate) {
        allowExpressionValidation = validate;
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

    public Set<Relationship> getAllRelationships() {
        if (!(component instanceof Processor)) {
            return Collections.emptySet();
        }

        return new HashSet<>(((Processor) component).getRelationships());
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        final Set<Relationship> relationships = getAllRelationships();
        relationships.removeAll(unavailableRelationships);
        return relationships;
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        return false;
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

    public void clearConnections() {
        this.connections = new HashSet<>();
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
        if (property == null || property.getExpressionLanguageScope().equals(ExpressionLanguageScope.NONE)) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(getProperty(property).getValue());
        return !elRanges.isEmpty();
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    @Override
    public String getName() {
        return componentName;
    }

    public void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Override
    public boolean isClustered() {
        return isClustered;
    }

    @Override
    public boolean isConfiguredForClustering() {
        return isConfiguredForClustering;
    }

    @Override
    public boolean isPrimary() {
        return isPrimaryNode;
    }

    public void setClustered(boolean clustered) {
        isClustered = clustered;
    }

    public void setIsConfiguredForClustering(final boolean isConfiguredForClustering) {
        this.isConfiguredForClustering = isConfiguredForClustering;
    }

    public void setPrimaryNode(boolean primaryNode) {
        if (!isConfiguredForClustering && primaryNode) {
            throw new IllegalArgumentException("Primary node is only available in cluster. Use setIsConfiguredForClustering(true) first.");
        }
        isPrimaryNode = primaryNode;
    }

    @Override
    public InputRequirement getInputRequirement() {
        return inputRequirement;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    @Override
    public boolean isConnectedToCluster() {
        return isConnected;
    }

    @Override
    public int getRetryCount() {
        return 0;
    }

    @Override
    public boolean isRelationshipRetried(Relationship relationship) {
        return false;
    }

    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }
}
