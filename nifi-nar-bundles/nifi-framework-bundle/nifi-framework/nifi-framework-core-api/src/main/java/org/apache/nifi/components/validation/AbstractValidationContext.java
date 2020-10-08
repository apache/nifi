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

package org.apache.nifi.components.validation;

import org.apache.nifi.components.PropertyDependency;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.parameter.ParameterLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class AbstractValidationContext implements ValidationContext {
    private static final Logger logger = LoggerFactory.getLogger(AbstractValidationContext.class);

    private final ParameterLookup parameterLookup;
    private final Map<PropertyDescriptor, PropertyConfiguration> properties;

    public AbstractValidationContext(final ParameterLookup parameterLookup, final Map<PropertyDescriptor, PropertyConfiguration> properties) {
        this.parameterLookup = parameterLookup;
        this.properties = properties;
    }


    public boolean isDependencySatisfied(final PropertyDescriptor propertyDescriptor, final Function<String, PropertyDescriptor> propertyDescriptorLookup) {
        return isDependencySatisfied(propertyDescriptor, propertyDescriptorLookup, new HashSet<>());
    }

    private boolean isDependencySatisfied(final PropertyDescriptor propertyDescriptor, final Function<String, PropertyDescriptor> propertyDescriptorLookup, final Set<String> propertiesSeen) {
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

                final PropertyConfiguration dependencyConfiguration = properties.get(dependencyDescriptor);
                if (dependencyConfiguration == null) {
                    logger.debug("Dependency for {} is not satisfied because it has a dependency on {}, which does not have a value", propertyDescriptor, dependencyName);
                    return false;
                }

                final String dependencyValue = dependencyConfiguration.getEffectiveValue(parameterLookup);
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
}
