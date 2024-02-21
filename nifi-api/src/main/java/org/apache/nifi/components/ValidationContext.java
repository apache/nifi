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
package org.apache.nifi.components;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface ValidationContext extends PropertyContext {

    /**
     * @return the {@link ControllerServiceLookup} which can be used to obtain
     * Controller Services
     */
    ControllerServiceLookup getControllerServiceLookup();

    /**
     * @param controllerService to lookup the validation context of
     * @return a ValidationContext that is appropriate for validating the given
     * {@link ControllerService}
     */
    ValidationContext getControllerServiceValidationContext(ControllerService controllerService);

    /**
     * @return a new {@link ExpressionLanguageCompiler} that can be used to
     * compile & evaluate Attribute Expressions
     */
    ExpressionLanguageCompiler newExpressionLanguageCompiler();

    /**
     * @param value to make a PropertyValue object for
     * @return a PropertyValue that represents the given value
     */
    PropertyValue newPropertyValue(String value);

    /**
     * @return a Map of all configured Properties
     */
    Map<PropertyDescriptor, String> getProperties();

    /**
     * @return the currently configured Annotation Data
     */
    String getAnnotationData();

    /**
     * There are times when the framework needs to consider a component valid,
     * even if it references an invalid ControllerService. This method will
     * return <code>false</code> if the component is to be considered valid even
     * if the given Controller Service is referenced and is invalid.
     *
     * @param service to check if validation is required
     * @return <code>false</code> if the component is to be considered valid
     * even if the given Controller Service is referenced and is invalid
     */
    boolean isValidationRequired(ControllerService service);

    /**
     * @param value to test whether expression language is present
     * @return <code>true</code> if the given value contains a NiFi Expression
     * Language expression, <code>false</code> if it does not
     */
    boolean isExpressionLanguagePresent(String value);

    /**
     * @param propertyName to test whether expression language is supported
     * @return <code>true</code> if the property with the given name supports
     * the NiFi Expression Language, <code>false</code> if the property does not
     * support the Expression Language or is not a valid property name
     */
    boolean isExpressionLanguageSupported(String propertyName);

    /**
     * Returns the identifier of the ProcessGroup that the component being validated lives in
     *
     * @return the identifier of the ProcessGroup that the component being validated lives in
     */
    String getProcessGroupIdentifier();

    /**
     * Returns a Collection containing the names of all Parameters that are referenced by the property with the given name
     * @return  a Collection containing the names of all Parameters that are referenced by the property with the given name
     */
    Collection<String> getReferencedParameters(String propertyName);

    /**
     * @param parameterName the name of the Parameter
     * @return <code>true</code> if a Parameter with the given name is defined in the currently selected Parameter Context
     */
    boolean isParameterDefined(String parameterName);

    /**
     * Returns <code>true</code> if a Parameter with the given name is defined and has a non-null value, <code>false</code> if either the Parameter
     * is not defined or the Parameter is defined but has a value of <code>null</code>.
     * @param parameterName the name of the parameter
     * @return <code>true</code> if the Parameter is defined and has a non-null value, false otherwise
     */
    boolean isParameterSet(String parameterName);

    /**
     * Determines whether or not the dependencies of the given Property Descriptor are satisfied.
     * If the given Property Descriptor has no dependency on another property, then the dependency is satisfied.
     * If there is at least one dependency, then all dependencies must be satisfied.
     * In order for a dependency to be considered satisfied, all of the following must be true:
     * <ul>
     *     <li>The property that is depended upon has all of its dependencies satisfied.</li>
     *     <li>If the given Property Descriptor depends on a given AllowableValue, then the property that is depended upon has a value that falls within the given range of Allowable Values for
     *     the dependency.</li>
     * </ul>
     *
     * @param propertyDescriptor the property descriptor
     * @param propertyDescriptorLookup a lookup for converting from a property name to the property descriptor with that name
     * @return <code>true</code> if all dependencies of the given property descriptor are satisfied, <code>false</code> otherwise
     */
    default boolean isDependencySatisfied(PropertyDescriptor propertyDescriptor, Function<String, PropertyDescriptor> propertyDescriptorLookup) {
        return isDependencySatisfied(propertyDescriptor, propertyDescriptorLookup, new HashSet<>());
    }

    private boolean isDependencySatisfied(final PropertyDescriptor propertyDescriptor, final Function<String, PropertyDescriptor> propertyDescriptorLookup, final Set<String> propertiesSeen) {
        final Logger logger = LoggerFactory.getLogger(ValidationContext.class);

        final Set<PropertyDependency> dependencies = propertyDescriptor.getDependencies();
        if (dependencies.isEmpty()) {
            logger.debug("Dependency for {} is satisfied because it has no dependencies", propertyDescriptor);
            return true;
        }

        final boolean added = propertiesSeen.add(propertyDescriptor.getName());
        if (!added) {
            logger.debug("Dependency for {} is not satisifed because its dependency chain contains a loop: {}", propertyDescriptor, propertiesSeen);
            return false;
        }

        try {
            for (final PropertyDependency dependency : dependencies) {
                final String dependencyName = dependency.getPropertyName();

                // Check if the property being depended upon has its dependencies satisfied.
                final PropertyDescriptor dependencyDescriptor = propertyDescriptorLookup.apply(dependencyName);
                if (dependencyDescriptor == null) {
                    logger.debug("Dependency for {} is not satisfied because it has a dependency on {}, which has no property descriptor", propertyDescriptor, dependencyName);
                    return false;
                }

                final PropertyValue propertyValue = getProperty(dependencyDescriptor);
                final String dependencyValue = propertyValue == null ? dependencyDescriptor.getDefaultValue() : propertyValue.getValue();
                if (dependencyValue == null) {
                    logger.debug("Dependency for {} is not satisfied because it has a dependency on {}, which has a null value", propertyDescriptor, dependencyName);
                    return false;
                }

                final boolean transitiveDependencySatisfied = isDependencySatisfied(dependencyDescriptor, propertyDescriptorLookup, propertiesSeen);
                if (!transitiveDependencySatisfied) {
                    logger.debug("Dependency for {} is not satisfied because it has a dependency on {} and {} does not have its dependencies satisfied",
                        propertyDescriptor, dependencyName, dependencyName);
                    return false;
                }

                // Check if the property being depended upon is set to one of the values that satisfies this dependency.
                // If the dependency has no dependent values, then any non-null value satisfies the dependency.
                // The value is already known to be non-null due to the check above.
                final Set<String> dependentValues = dependency.getDependentValues();
                if (dependentValues != null && !dependentValues.contains(dependencyValue)) {
                    logger.debug("Dependency for {} is not satisfied because it depends on {}, which has a value of {}. Dependent values = {}",
                        propertyDescriptor, dependencyName, dependencyValue, dependentValues);
                    return false;
                }
            }

            logger.debug("All dependencies for {} are satisfied", propertyDescriptor);

            return true;
        } finally {
            propertiesSeen.remove(propertyDescriptor.getName());
        }
    }

    /**
     * Determines whether or not incoming and outgoing connections should be validated.
     * If <code>true</code>, then the validation should verify that all Relationships either have one or more connections that include the Relationship,
     * or that the Relationship is auto-terminated.
     * Additionally, if <code>true</code>, then any Processor with an {@link InputRequirement} of {@link InputRequirement.Requirement#INPUT_REQUIRED}
     * should be invalid unless it has an incoming (non-looping) connection, and any Processor with an {@link InputRequirement} of {@link InputRequirement.Requirement#INPUT_FORBIDDEN}
     * should be invalid if it does have any incoming connection.
     *
     * @return <code>true</code> if Connections should be validated, <code>false</code> if Connections should be ignored
     */
    default boolean isValidateConnections() {
        return true;
    }
}
