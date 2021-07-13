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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardParameterContext implements ParameterContext {
    private static final Logger logger = LoggerFactory.getLogger(StandardParameterContext.class);

    private final String id;
    private final ParameterReferenceManager parameterReferenceManager;
    private final Authorizable parentAuthorizable;

    private String name;
    private long version = 0L;
    private final Map<ParameterDescriptor, Parameter> parameters = new LinkedHashMap<>();
    private volatile String description;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public StandardParameterContext(final String id, final String name, final ParameterReferenceManager parameterReferenceManager, final Authorizable parentAuthorizable) {
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.parameterReferenceManager = parameterReferenceManager;
        this.parentAuthorizable = parentAuthorizable;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
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

    public void setParameters(final Map<String, Parameter> updatedParameters) {
        final Map<String, ParameterUpdate> parameterUpdates = new HashMap<>();
        boolean changeAffectingComponents = false;

        writeLock.lock();
        try {
            this.version++;
            verifyCanSetParameters(updatedParameters);

            for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
                final String parameterName = entry.getKey();
                final Parameter parameter = entry.getValue();

                if (parameter == null) {
                    changeAffectingComponents = true;

                    final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(parameterName).build();
                    final Parameter oldParameter = parameters.remove(parameterDescriptor);

                    parameterUpdates.put(parameterName, new StandardParameterUpdate(parameterName, oldParameter.getValue(), null, parameterDescriptor.isSensitive()));
                } else {
                    final Parameter updatedParameter = createFullyPopulatedParameter(parameter);

                    final Parameter oldParameter = parameters.put(updatedParameter.getDescriptor(), updatedParameter);
                    if (oldParameter == null || !Objects.equals(oldParameter.getValue(), updatedParameter.getValue())) {
                        changeAffectingComponents = true;

                        final String previousValue = oldParameter == null ? null : oldParameter.getValue();
                        parameterUpdates.put(parameterName, new StandardParameterUpdate(parameterName, previousValue, updatedParameter.getValue(), updatedParameter.getDescriptor().isSensitive()));
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }

        if (changeAffectingComponents) {
            logger.debug("Parameter Context {} was updated. {} parameters changed ({}). Notifying all affected components.", this, parameterUpdates.size(), parameterUpdates);

            for (final ProcessGroup processGroup : parameterReferenceManager.getProcessGroupsBound(this)) {
                try {
                    processGroup.onParameterContextUpdated(parameterUpdates);
                } catch (final Exception e) {
                    logger.error("Failed to notify {} that Parameter Context was updated", processGroup, e);
                }
            }
        } else {
            logger.debug("Parameter Context {} was updated. {} parameters changed ({}). No existing components are affected.", this, parameterUpdates.size(), parameterUpdates);
        }
    }

    /**
     * When updating a Parameter, the provided 'updated' Parameter may or may not contain a value. This is done because once a Parameter is set,
     * a user may want to change the description of the Parameter but cannot include the value of the Parameter in the request if the Parameter is sensitive (because
     * the value of the Parameter, when retrieved, is masked for sensitive Parameters). As a result, a call may be made to {@link #setParameters(Map)} that includes
     * a Parameter whose value is <code>null</code>. If we encounter this, we do not want to change the value of the Parameter, so we want to insert into our Map
     * a Parameter object whose value is equal to the current value for that Parameter (if any). This method, then, takes a Parameter whose value may or may not be
     * populated and returns a Parameter whose value is populated, if there is an appropriate value to populate it with.
     *
     * @param proposedParameter the proposed parameter
     * @return a Parameter whose descriptor is the same as the given proposed parameter but whose value has been populated based on the existing value for that Parameter (if any)
     * if the given proposed parameter does not have its value populated
     */
    private Parameter createFullyPopulatedParameter(final Parameter proposedParameter) {
        final ParameterDescriptor descriptor = getFullyPopulatedDescriptor(proposedParameter);
        final String value = getFullyPopulatedValue(proposedParameter);
        return new Parameter(descriptor, value);
    }

    private String getFullyPopulatedValue(final Parameter proposedParameter) {
        return proposedParameter.getValue();
    }

    private ParameterDescriptor getFullyPopulatedDescriptor(final Parameter proposedParameter) {
        final ParameterDescriptor descriptor = proposedParameter.getDescriptor();
        if (descriptor.getDescription() != null) {
            return descriptor;
        }

        final Parameter oldParameter = parameters.get(proposedParameter.getDescriptor());

        // We know that the Parameters have the same name, since this is what the Descriptor's hashCode & equality are based on. The only thing that may be different
        // is the description. And since the proposed Parameter does not have a Description, we want to use whatever is currently set.
        return oldParameter == null ? descriptor : oldParameter.getDescriptor();
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
            // When Expression Language is used, the Parameter may require being escaped.
            // This is the case, for instance, if a Parameter name has a space in it.
            // Because of this, we may have a case where we attempt to get a Parameter by name
            // and that Parameter name is enclosed within single tick marks, as a way of escaping
            // the name via the Expression Language. In this case, we want to strip out those
            // escaping tick marks and use just the raw name for looking up the Parameter.
            final ParameterDescriptor unescaped = unescape(parameterDescriptor);
            return Optional.ofNullable(parameters.get(unescaped));
        } finally {
            readLock.unlock();
        }
    }

    private ParameterDescriptor unescape(final ParameterDescriptor descriptor) {
        final String parameterName = descriptor.getName().trim();
        if ((parameterName.startsWith("'") && parameterName.endsWith("'")) || (parameterName.startsWith("\"") && parameterName.endsWith("\""))) {
            final String stripped = parameterName.substring(1, parameterName.length() - 1);
            return new ParameterDescriptor.Builder()
                .from(descriptor)
                .name(stripped)
                .build();
        }

        return descriptor;
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
    public void verifyCanSetParameters(final Map<String, Parameter> updatedParameters) {
        // Ensure that the updated parameters will not result in changing the sensitivity flag of any parameter.
        for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            if (parameter == null) {
                // parameter is being deleted.
                validateReferencingComponents(parameterName, null,"remove");
                continue;
            }

            if (!Objects.equals(parameterName, parameter.getDescriptor().getName())) {
                throw new IllegalArgumentException("Parameter '" + parameterName + "' was specified with the wrong key in the Map");
            }

            validateSensitiveFlag(parameter);
            validateReferencingComponents(parameterName, parameter, "update");
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


    private void validateReferencingComponents(final String parameterName, final Parameter parameter, final String parameterAction) {
        for (final ProcessorNode procNode : parameterReferenceManager.getProcessorsReferencing(this, parameterName)) {
            if (procNode.isRunning()) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + parameterName + "' because it is referenced by " + procNode + ", which is currently running");
            }

            if (parameter != null) {
                validateParameterSensitivity(parameter, procNode);
            }
        }

        for (final ControllerServiceNode serviceNode : parameterReferenceManager.getControllerServicesReferencing(this, parameterName)) {
            final ControllerServiceState serviceState = serviceNode.getState();
            if (serviceState != ControllerServiceState.DISABLED) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + parameterName + "' because it is referenced by "
                    + serviceNode + ", which currently has a state of " + serviceState);
            }

            if (parameter != null) {
                validateParameterSensitivity(parameter, serviceNode);
            }
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
    public String toString() {
        return "StandardParameterContext[name=" + name + "]";
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return new Authorizable() {
            @Override
            public Authorizable getParentAuthorizable() {
                return parentAuthorizable;
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
