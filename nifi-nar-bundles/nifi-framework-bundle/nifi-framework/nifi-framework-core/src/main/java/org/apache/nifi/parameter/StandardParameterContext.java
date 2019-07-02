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
package org.apache.nifi.parameter;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardParameterContext implements ParameterContext {
    private static final Logger logger = LoggerFactory.getLogger(StandardParameterContext.class);

    private final String id;
    private final ParameterReferenceManager parameterReferenceManager;

    private String name;
    private long version = 0L;
    private final Map<ParameterDescriptor, Parameter> parameters = new LinkedHashMap<>();
    private volatile String description;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public StandardParameterContext(final String id, final String name, final ParameterReferenceManager parameterReferenceManager) {
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.parameterReferenceManager = parameterReferenceManager;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    public String getName() {
        readLock.lock();
        try {
            return name;
        } finally {
            readLock.unlock();
        }
    }

    public void setName(final String name) {
        writeLock.lock();
        try {
            this.version++;
            this.name = name;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setParameters(final Set<Parameter> updatedParameters) {
        writeLock.lock();
        try {
            this.version++;

            verifyCanSetParameters(updatedParameters);

            boolean changeAffectingComponents = false;
            for (final Parameter parameter : updatedParameters) {
                if (parameter.getValue() == null && parameter.getDescriptor().getDescription() == null) {
                    parameters.remove(parameter.getDescriptor());
                    changeAffectingComponents = true;
                } else if (parameter.getValue() == null) {
                    // Value is null but description is not. Just update the description of the existing Parameter.
                    final Parameter existingParameter = parameters.get(parameter.getDescriptor());
                    final ParameterDescriptor existingDescriptor = existingParameter.getDescriptor();
                    final ParameterDescriptor replacementDescriptor = new ParameterDescriptor.Builder()
                        .from(existingDescriptor)
                        .description(parameter.getDescriptor().getDescription())
                        .build();

                    final Parameter replacementParameter = new Parameter(replacementDescriptor, existingParameter.getValue());
                    parameters.put(parameter.getDescriptor(), replacementParameter);
                } else {
                    parameters.put(parameter.getDescriptor(), parameter);
                    changeAffectingComponents = true;
                }
            }

            if (changeAffectingComponents) {
                for (final ProcessGroup processGroup : parameterReferenceManager.getProcessGroupsBound(this)) {
                    try {
                        processGroup.onParameterContextUpdated();
                    } catch (final Exception e) {
                        logger.error("Failed to notify {} that Parameter Context was updated", processGroup, e);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getVersion() {
        readLock.lock();
        try {
            return version;
        } finally {
            readLock.unlock();
        }
    }

    public Optional<Parameter> getParameter(final String parameterName) {
        readLock.lock();
        try {
            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(parameterName).build();
            return getParameter(descriptor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return parameters.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public Optional<Parameter> getParameter(final ParameterDescriptor parameterDescriptor) {
        readLock.lock();
        try {
            return Optional.ofNullable(parameters.get(parameterDescriptor));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getParameters() {
        readLock.lock();
        try {
            return new LinkedHashMap<>(parameters);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return parameterReferenceManager;
    }

    @Override
    public void verifyCanSetParameters(final Set<Parameter> updatedParameters) {
        // Ensure that the updated parameters will not result in changing the sensitivity flag of any parameter.
        for (final Parameter updatedParameter : updatedParameters) {
            validateSensitiveFlag(updatedParameter);

            // Parameters' names and sensitivity flags are immutable. However, the description and value are mutable. If both value and description are
            // set to `null`, this is the indication that the Parameter should be removed. If the value is `null` but the Description is supplied, the user
            // is indicating that only the description is to be changed.
            if (updatedParameter.getValue() == null && updatedParameter.getDescriptor().getDescription() == null) {
                validateReferencingComponents(updatedParameter, "remove");
            } else if (updatedParameter.getValue() != null) {
                validateReferencingComponents(updatedParameter, "update");
            } else {
                // Only parameter is changing. No value is set. This means that the Parameter must already exist.
                final Optional<Parameter> existing = getParameter(updatedParameter.getDescriptor());
                if (!existing.isPresent()) {
                    throw new IllegalStateException("Cannot add Parameter '" + updatedParameter.getDescriptor().getName() + "' without providing a value");
                }
            }
        }
    }

    private void validateSensitiveFlag(final Parameter updatedParameter) {
        final ParameterDescriptor updatedDescriptor = updatedParameter.getDescriptor();
        final Parameter existingParameter = parameters.get(updatedDescriptor);

        if (existingParameter == null) {
            return;
        }

        final ParameterDescriptor existingDescriptor = existingParameter.getDescriptor();
        if (existingDescriptor.isSensitive() != updatedDescriptor.isSensitive() && updatedParameter.getValue() != null) {
            final String existingSensitiveDescription = existingDescriptor.isSensitive() ? "sensitive" : "not sensitive";
            final String updatedSensitiveDescription = updatedDescriptor.isSensitive() ? "sensitive" : "not sensitive";

            throw new IllegalStateException("Cannot update Parameters because doing so would change Parameter '" + existingDescriptor.getName() + "' from " + existingSensitiveDescription
                + " to " + updatedSensitiveDescription);
        }
    }


    private void validateReferencingComponents(final Parameter updatedParameter, final String parameterAction) {
        final String paramName = updatedParameter.getDescriptor().getName();

        for (final ProcessorNode procNode : parameterReferenceManager.getProcessorsReferencing(this, paramName)) {
            if (procNode.isRunning()) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + paramName + "' because it is referenced by " + procNode + ", which is currently running");
            }

            validateParameterSensitivity(updatedParameter, procNode);
        }

        for (final ControllerServiceNode serviceNode : parameterReferenceManager.getControllerServicesReferencing(this, paramName)) {
            final ControllerServiceState serviceState = serviceNode.getState();
            if (serviceState != ControllerServiceState.DISABLED) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + paramName + "' because it is referenced by "
                    + serviceNode + ", which currently has a state of " + serviceState);
            }

            validateParameterSensitivity(updatedParameter, serviceNode);
        }
    }

    private void validateParameterSensitivity(final Parameter parameter, final ComponentNode componentNode) {
        final String paramName = parameter.getDescriptor().getName();

        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry :  componentNode.getProperties().entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                if (parameter.getDescriptor().getName().equals(reference.getParameterName())) {
                    final PropertyDescriptor propertyDescriptor = entry.getKey();
                    if (propertyDescriptor.isSensitive() && !parameter.getDescriptor().isSensitive()) {
                        throw new IllegalStateException("Cannot add Parameter with name '" + paramName + "' unless that Parameter is Sensitive because a Parameter with that name is already " +
                            "referenced from a Sensitive Property");
                    }

                    if (!propertyDescriptor.isSensitive() && parameter.getDescriptor().isSensitive()) {
                        throw new IllegalStateException("Cannot add Parameter with name '" + paramName + "' unless that Parameter is Not Sensitive because a Parameter with that name is already " +
                            "referenced from a Property that is not Sensitive");
                    }
                }
            }
        }
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return new Authorizable() {
            @Override
            public Authorizable getParentAuthorizable() {
                return null;
            }

            @Override
            public Resource getResource() {
                return ResourceFactory.getParameterContextsResource();
            }
        };
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ParameterContext, getIdentifier(), getName());
    }
}
