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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.parameter.ParameterProviderLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StandardParameterContext implements ParameterContext {
    private static final Logger logger = LoggerFactory.getLogger(StandardParameterContext.class);

    private final String id;
    private final ParameterReferenceManager parameterReferenceManager;
    private final ParameterProviderLookup parameterProviderLookup;
    private final Authorizable parentAuthorizable;

    private String name;
    private AtomicLong version = new AtomicLong(0L);
    private final Map<ParameterDescriptor, Parameter> parameters = new LinkedHashMap<>();
    private final List<ParameterContext> inheritedParameterContexts = new ArrayList<>();
    private ParameterProvider parameterProvider;
    private ParameterProviderConfiguration parameterProviderConfiguration;
    private volatile String description;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    protected StandardParameterContext(final String id, final String name, final ParameterReferenceManager parameterReferenceManager,
                                       final Authorizable parentAuthorizable, final ParameterProviderLookup parameterProviderLookup,
                                       final ParameterProviderConfiguration parameterProviderConfiguration) {
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.parameterReferenceManager = parameterReferenceManager;
        this.parentAuthorizable = parentAuthorizable;
        this.parameterProviderLookup = parameterProviderLookup;
        this.configureParameterProvider(parameterProviderConfiguration);
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public String getName() {
        readLock.lock();
        try {
            return name;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setName(final String name) {
        writeLock.lock();
        try {
            this.version.incrementAndGet();
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

    @Override
    public void setParameters(final Map<String, Parameter> updatedParameters) {
        writeLock.lock();
        final Map<String, ParameterUpdate> parameterUpdates = new HashMap<>();
        try {
            this.version.incrementAndGet();

            final Map<ParameterDescriptor, Parameter> currentEffectiveParameters = getEffectiveParameters();
            final Map<ParameterDescriptor, Parameter> effectiveProposedParameters = getEffectiveParameters(getProposedParameters(updatedParameters));

            final Map<String, Parameter> effectiveParameterUpdates = getEffectiveParameterUpdates(currentEffectiveParameters, effectiveProposedParameters);

            verifyCanSetParameters(effectiveParameterUpdates, true);

            // Update the actual parameters
            updateParameters(parameters, updatedParameters, true);

            // Get a list of all effective updates in order to alert referencing components
            parameterUpdates.putAll(updateParameters(currentEffectiveParameters, effectiveParameterUpdates, false));
        } finally {
            writeLock.unlock();
        }
        alertReferencingComponents(parameterUpdates);
    }

    private Map<ParameterDescriptor, Parameter> getProposedParameters(final Map<String, Parameter> proposedParameterUpdates) {
        final Map<ParameterDescriptor, Parameter> proposedParameters = new HashMap<>(this.parameters);
        for (final Map.Entry<String, Parameter> entry : proposedParameterUpdates.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            if (parameter == null) {
                final Optional<Parameter> existingParameter = getParameter(parameterName);
                existingParameter.ifPresent(value -> proposedParameters.remove(value.getDescriptor()));
            } else {
                // Remove is necessary first in case sensitivity changes
                proposedParameters.remove(parameter.getDescriptor());
                proposedParameters.put(parameter.getDescriptor(), parameter);
            }
        }
        return proposedParameters;
    }

    /**
     * Alerts all referencing components of any relevant updates.
     * @param parameterUpdates A map from parameter name to ParameterUpdate (empty if none are applicable)
     */
    private void alertReferencingComponents(final Map<String, ParameterUpdate> parameterUpdates) {
        if (!parameterUpdates.isEmpty()) {
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
     * Returns a map from parameter name to ParameterUpdate for any actual updates to parameters.
     * @param currentParameters The current parameters
     * @param updatedParameters An updated parameters map
     * @param performUpdate If true, this will actually perform the updates on the currentParameters map.  Otherwise,
     *                      the updates are simply collected and returned.
     * @return A map from parameter name to ParameterUpdate for any actual parameters
     */
    private Map<String, ParameterUpdate> updateParameters(final Map<ParameterDescriptor, Parameter> currentParameters,
                                                          final Map<String, Parameter> updatedParameters, final boolean performUpdate) {
        final Map<String, ParameterUpdate> parameterUpdates = new HashMap<>();

        for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();

            if (parameter == null) {
                final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(parameterName).build();
                final Parameter oldParameter = performUpdate ? currentParameters.remove(parameterDescriptor)
                        : currentParameters.get(parameterDescriptor);

                parameterUpdates.put(parameterName, new StandardParameterUpdate(parameterName, oldParameter.getValue(), null, parameterDescriptor.isSensitive()));
            } else {
                final Parameter updatedParameter = createFullyPopulatedParameter(parameter);
                final Parameter oldParameter;
                if (performUpdate) {
                    // Necessary to remove first, because the sensitivity may change, and ParameterDescriptor#hashCode only relies on `name`
                    oldParameter = currentParameters.remove(updatedParameter.getDescriptor());
                    currentParameters.put(updatedParameter.getDescriptor(), updatedParameter);
                } else {
                    oldParameter = currentParameters.get(updatedParameter.getDescriptor());
                }
                if (oldParameter == null || !Objects.equals(oldParameter.getValue(), updatedParameter.getValue())) {
                    final String previousValue = oldParameter == null ? null : oldParameter.getValue();
                    parameterUpdates.put(parameterName, new StandardParameterUpdate(parameterName, previousValue, updatedParameter.getValue(), updatedParameter.getDescriptor().isSensitive()));
                }
            }
        }
        return parameterUpdates;
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
        return new Parameter.Builder()
            .fromParameter(proposedParameter)
            .descriptor(getFullyPopulatedDescriptor(proposedParameter))
            .build();
    }


    private ParameterDescriptor getFullyPopulatedDescriptor(final Parameter proposedParameter) {
        final ParameterDescriptor descriptor = proposedParameter.getDescriptor();
        if (descriptor.getDescription() != null) {
            return descriptor;
        }

        final Parameter oldParameter = parameters.get(proposedParameter.getDescriptor());

        if (proposedParameter.isProvided()) {
            final String description = oldParameter == null ? null : oldParameter.getDescriptor().getDescription();
            return new ParameterDescriptor.Builder()
                    .from(descriptor)
                    .description(description)
                    .build();
        }

        // We know that the Parameters have the same name, since this is what the Descriptor's hashCode & equality are based on. The only thing that may be different
        // is the description. And since the proposed Parameter does not have a Description, we want to use whatever is currently set.
        return oldParameter == null ? descriptor : oldParameter.getDescriptor();
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    @Override
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
            return getEffectiveParameters().isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
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
            // Short circuit getEffectiveParameters if we know there are no inherited ParameterContexts
            return Optional.ofNullable((inheritedParameterContexts.isEmpty() ? parameters : getEffectiveParameters())
                    .get(unescaped));
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
    public Map<ParameterDescriptor, Parameter> getEffectiveParameters() {
        readLock.lock();
        try {
            return this.getEffectiveParameters(inheritedParameterContexts);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<String, Parameter> getEffectiveParameterUpdates(final Map<String, Parameter> parameterUpdates, final List<ParameterContext> inheritedParameterContexts) {
        Objects.requireNonNull(parameterUpdates, "Parameter Updates must be specified");
        Objects.requireNonNull(inheritedParameterContexts, "Inherited parameter contexts must be specified");

        final Map<ParameterDescriptor, Parameter> currentEffectiveParameters = getEffectiveParameters();
        final Map<ParameterDescriptor, Parameter> effectiveProposedParameters = getEffectiveParameters(inheritedParameterContexts, getProposedParameters(parameterUpdates));

        return getEffectiveParameterUpdates(currentEffectiveParameters, effectiveProposedParameters);
    }

    /**
     * Constructs an effective view of the parameters, including nested parameters, assuming the given map of parameters.
     * This allows an inspection of what parameters would be available if the given parameters were set in this ParameterContext.
     * @param proposedParameters A Map of proposed parameters that should be used in place of the current parameters
     * @return The view of the parameters with all overriding applied
     */
    private Map<ParameterDescriptor, Parameter> getEffectiveParameters(final Map<ParameterDescriptor, Parameter> proposedParameters) {
        return getEffectiveParameters(this.inheritedParameterContexts, proposedParameters);
    }

    /**
     * Constructs an effective view of the parameters, including nested parameters, assuming the given list of ParameterContexts.
     * This allows an inspection of what parameters would be available if the given list were set in this ParameterContext.
     * @param parameterContexts An ordered list of ParameterContexts from which to inherit
     * @return The view of the parameters with all overriding applied
     */
    private Map<ParameterDescriptor, Parameter> getEffectiveParameters(final List<ParameterContext> parameterContexts) {
        return getEffectiveParameters(parameterContexts, this.parameters);
    }

    private Map<ParameterDescriptor, Parameter> getEffectiveParameters(final List<ParameterContext> parameterContexts,
                                                                       final Map<ParameterDescriptor, Parameter> proposedParameters) {
        return getEffectiveParameters(parameterContexts, proposedParameters, new HashMap<>());
    }

    private Map<ParameterDescriptor, Parameter> getEffectiveParameters(final List<ParameterContext> parameterContexts,
                                                                       final Map<ParameterDescriptor, Parameter> proposedParameters,
                                                                       final Map<ParameterDescriptor, List<Parameter>> allOverrides) {
        final Map<ParameterDescriptor, Parameter> effectiveParameters = new LinkedHashMap<>();

        // Loop backwards so that the first ParameterContext in the list will override any parameters later in the list
        for (int i = parameterContexts.size() - 1; i >= 0; i--) {
            ParameterContext parameterContext = parameterContexts.get(i);
            combineOverrides(allOverrides, overrideParameters(effectiveParameters, parameterContext.getEffectiveParameters(), parameterContext));
        }

        // Finally, override all child parameters with our own
        combineOverrides(allOverrides, overrideParameters(effectiveParameters, proposedParameters, this));

        return effectiveParameters;
    }

    private void combineOverrides(final Map<ParameterDescriptor, List<Parameter>> existingOverrides, final Map<ParameterDescriptor, List<Parameter>> newOverrides) {
        for (final Map.Entry<ParameterDescriptor, List<Parameter>> entry : newOverrides.entrySet()) {
            final ParameterDescriptor key = entry.getKey();
            final List<Parameter> existingOverrideList = existingOverrides.computeIfAbsent(key, k -> new ArrayList<>());
            existingOverrideList.addAll(entry.getValue());
        }
    }

    private Map<ParameterDescriptor, List<Parameter>> overrideParameters(final Map<ParameterDescriptor, Parameter> existingParameters,
                                    final Map<ParameterDescriptor, Parameter> overridingParameters,
                                    final ParameterContext overridingContext) {
        final Map<ParameterDescriptor, List<Parameter>> allOverrides = new HashMap<>();
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : existingParameters.entrySet()) {
            final List<Parameter> parameters = new ArrayList<>();
            parameters.add(entry.getValue());
            allOverrides.put(entry.getKey(), parameters);
        }

        for (final Map.Entry<ParameterDescriptor, Parameter> entry : overridingParameters.entrySet()) {
            final ParameterDescriptor overridingParameterDescriptor = entry.getKey();
            Parameter overridingParameter = entry.getValue();

            if (existingParameters.containsKey(overridingParameterDescriptor)) {
                final Parameter existingParameter = existingParameters.get(overridingParameterDescriptor);
                final ParameterDescriptor existingParameterDescriptor = existingParameter.getDescriptor();

                if (existingParameterDescriptor.isSensitive() && !overridingParameterDescriptor.isSensitive()) {
                    throw new IllegalStateException(String.format("Cannot add ParameterContext because Sensitive Parameter [%s] would be overridden by " +
                            "a Non Sensitive Parameter with the same name", existingParameterDescriptor.getName()));
                }

                if (!existingParameterDescriptor.isSensitive() && overridingParameterDescriptor.isSensitive()) {
                    throw new IllegalStateException(String.format("Cannot add ParameterContext because Non Sensitive Parameter [%s] would be overridden by " +
                            "a Sensitive Parameter with the same name", existingParameterDescriptor.getName()));
                }
            }

            if (overridingParameter.getParameterContextId() == null) {
                overridingParameter = new Parameter.Builder().fromParameter(overridingParameter).parameterContextId(overridingContext.getIdentifier()).build();
            }
            allOverrides.computeIfAbsent(overridingParameterDescriptor, p -> new ArrayList<>()).add(overridingParameter);

            existingParameters.put(overridingParameterDescriptor, overridingParameter);
        }
        return allOverrides;
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return parameterReferenceManager;
    }

    @Override
    public ParameterProviderLookup getParameterProviderLookup() {
        return parameterProviderLookup;
    }

    @Override
    public ParameterProvider getParameterProvider() {
        readLock.lock();
        try {
            return parameterProvider;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void configureParameterProvider(final ParameterProviderConfiguration parameterProviderConfiguration) {
        if (parameterProviderConfiguration == null) {
            return;
        }
        this.parameterProviderConfiguration = parameterProviderConfiguration;

        writeLock.lock();
        try {
            final ParameterProviderNode parameterProviderNode = parameterProviderLookup.getParameterProvider(parameterProviderConfiguration.getParameterProviderId());
            if (parameterProviderNode == null) {
                throw new IllegalArgumentException(String.format("Could not configure Parameter Provider %s, which could not be found",
                        parameterProviderConfiguration.getParameterProviderId()));
            }
            final boolean hasUserEnteredParameters = parameters.values().stream().anyMatch(parameter -> !parameter.isProvided());

            if (hasUserEnteredParameters) {
                throw new IllegalArgumentException(String.format("A Parameter Provider [%s] cannot be set since there are already user-entered parameters " +
                        "in Context [%s]", parameterProviderNode.getIdentifier(), name));
            }

            this.parameterProvider = parameterProviderNode.getParameterProvider();
            registerParameterProvider(parameterProvider);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ParameterProviderConfiguration getParameterProviderConfiguration() {
        readLock.lock();
        try {
            return parameterProviderConfiguration;
        } finally {
            readLock.unlock();
        }
    }

    private void registerParameterProvider(final ParameterProvider parameterProvider) {
        final ParameterProviderNode parameterProviderNode = getParameterProviderNode(parameterProvider);
        parameterProviderNode.addReference(this);
    }

    private ParameterProviderNode getParameterProviderNode(final ParameterProvider parameterProvider) {
        final ParameterProviderNode parameterProviderNode = parameterProviderLookup.getParameterProvider(parameterProvider.getIdentifier());
        if (parameterProviderNode == null) {
            throw new IllegalStateException(String.format("Parameter Provider Node is missing for Parameter Provider [%s]", parameterProvider.getIdentifier()));
        }
        return parameterProviderNode;
    }

    /**
     * Verifies that no cycles would exist in the ParameterContext reference graph, if this ParameterContext were
     * to inherit from the given list of ParameterContexts.
     * @param parameterContexts A list of proposed ParameterContexts
     */
    private void verifyNoCycles(final List<ParameterContext> parameterContexts) {
        final Stack<String> traversedIds = new Stack<>();
        traversedIds.push(id);
        verifyNoCycles(traversedIds, parameterContexts);
    }

    /**
     * A helper method for performing a depth first search to verify there are no cycles in the proposed
     * list of ParameterContexts.
     * @param traversedIds A collection of already traversed ids in the graph
     * @param parameterContexts The ParameterContexts for which to check for cycles
     * @throws IllegalStateException If a cycle was detected
     */
    private void verifyNoCycles(final Stack<String> traversedIds, final List<ParameterContext> parameterContexts) {
        for (final ParameterContext parameterContext : parameterContexts) {
            final String id = parameterContext.getIdentifier();
            if (traversedIds.contains(id)) {
                throw new IllegalStateException(String.format("Circular references in Parameter Contexts not allowed. [%s] was detected in a cycle.", parameterContext.getName()));
            }

            traversedIds.push(id);
            verifyNoCycles(traversedIds, parameterContext.getInheritedParameterContexts());
            traversedIds.pop();
        }
    }

    @Override
    public void verifyCanUpdateParameterContext(final Map<String, Parameter> parameterUpdates, final List<ParameterContext> inheritedParameterContexts) {
        verifyCanUpdateParameterContext(parameterUpdates, inheritedParameterContexts, false);
    }

    private void verifyCanUpdateParameterContext(final Map<String, Parameter> parameterUpdates, final List<ParameterContext> inheritedParameterContexts,
                                                 final boolean duringUpdate) {
        verifyProvidedParameters(parameterUpdates);
        if (inheritedParameterContexts == null) {
            return;
        }
        verifyNoCycles(inheritedParameterContexts);

        final Map<ParameterDescriptor, Parameter> currentEffectiveParameters = getEffectiveParameters();
        final Map<String, Parameter> effectiveParameterUpdates = getEffectiveParameterUpdates(parameterUpdates, inheritedParameterContexts);

        try {
            verifyCanSetParameters(currentEffectiveParameters, effectiveParameterUpdates, duringUpdate);
        } catch (final IllegalStateException e) {
            // Wrap with a more accurate message
            throw new IllegalStateException(String.format("Could not update inherited Parameter Contexts for Parameter Context [%s] because: %s",
                    name, e.getMessage()), e);
        }
    }

    private void verifyProvidedParameters(final Map<String, Parameter> parameterUpdates) {
        parameterUpdates.forEach((parameterName, parameter) -> {
            if (parameterProvider != null && parameter != null && !parameter.isProvided()) {
                throw new IllegalArgumentException(String.format("Cannot make user-entered updates to Parameter Context [%s] parameter [%s] because its parameters " +
                        "are provided by Parameter Provider [%s]", name, parameterName, parameterProvider.getIdentifier()));
            }
        });
    }

    @Override
    public void setInheritedParameterContexts(final List<ParameterContext> inheritedParameterContexts) {
        if (inheritedParameterContexts == null || inheritedParameterContexts.equals(this.inheritedParameterContexts)) {
            // No changes
            return;
        }

        verifyCanUpdateParameterContext(Collections.emptyMap(), inheritedParameterContexts, true);

        final Map<String, ParameterUpdate> parameterUpdates = new HashMap<>();

        writeLock.lock();
        try {
            this.version.incrementAndGet();

            final Map<ParameterDescriptor, Parameter> currentEffectiveParameters = getEffectiveParameters();
            final Map<ParameterDescriptor, Parameter> effectiveProposedParameters = getEffectiveParameters(inheritedParameterContexts);
            final Map<String, Parameter> effectiveParameterUpdates = getEffectiveParameterUpdates(currentEffectiveParameters, effectiveProposedParameters);

            this.inheritedParameterContexts.clear();

            this.inheritedParameterContexts.addAll(inheritedParameterContexts);

            parameterUpdates.putAll(updateParameters(currentEffectiveParameters, effectiveParameterUpdates, false));
        } finally {
            writeLock.unlock();
        }

        alertReferencingComponents(parameterUpdates);
    }

    /**
     * Returns a map that can be used to indicate all effective parameters updates, including removed parameters.
     * @param currentEffectiveParameters A current map of effective parameters
     * @param effectiveProposedParameters A map of effective parameters that would result if a proposed update were applied
     * @return a map that can be used to indicate all effective parameters updates, including removed parameters
     */
    private Map<String, Parameter> getEffectiveParameterUpdates(final Map<ParameterDescriptor, Parameter> currentEffectiveParameters,
                                                                       final Map<ParameterDescriptor, Parameter> effectiveProposedParameters) {
        final Map<String, Parameter> effectiveParameterUpdates = new HashMap<>();
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : effectiveProposedParameters.entrySet()) {
            final ParameterDescriptor proposedParameterDescriptor = entry.getKey();
            final Parameter proposedParameter = entry.getValue();
            if (currentEffectiveParameters.containsKey(proposedParameterDescriptor)) {
                final Parameter currentParameter = currentEffectiveParameters.get(proposedParameterDescriptor);
                if (!currentParameter.equals(proposedParameter) || currentParameter.getDescriptor().isSensitive() != proposedParameter.getDescriptor().isSensitive()
                        || !StringUtils.equals(currentParameter.getDescriptor().getDescription(), proposedParameter.getDescriptor().getDescription())) {
                    // The parameter has been updated in some way
                    effectiveParameterUpdates.put(proposedParameterDescriptor.getName(), proposedParameter);
                }
            } else {
                // It's a new parameter
                effectiveParameterUpdates.put(proposedParameterDescriptor.getName(), proposedParameter);
            }
        }
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : currentEffectiveParameters.entrySet()) {
            final ParameterDescriptor currentParameterDescriptor = entry.getKey();
            // If a current parameter is not in the proposed parameters, it was effectively removed
            if (!effectiveProposedParameters.containsKey(currentParameterDescriptor)) {
                final Parameter parameter = entry.getValue();
                final boolean isParameterInherited = !parameter.getParameterContextId().equals(id);
                if (!isParameterInherited && parameter.isProvided() && hasReferencingComponents(parameter)) {
                    logger.info("Provided parameter [{}] was removed from the source, but it is referenced by a component, so the parameter will be preserved",
                            currentParameterDescriptor.getName());
                } else {
                    effectiveParameterUpdates.put(currentParameterDescriptor.getName(), null);
                }
            }
        }
        return effectiveParameterUpdates;
    }

    @Override
    public boolean hasReferencingComponents(final Parameter parameter) {
        if (parameter == null) {
            return false;
        }

        final String parameterName = parameter.getDescriptor().getName();
        return parameterReferenceManager.getProcessorsReferencing(this, parameterName).size() > 0
                || parameterReferenceManager.getControllerServicesReferencing(this, parameterName).size() > 0;
    }

    @Override
    public List<ParameterContext> getInheritedParameterContexts() {
        readLock.lock();
        try {
            return new ArrayList<>(inheritedParameterContexts);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<String> getInheritedParameterContextNames() {
        readLock.lock();
        try {
            return inheritedParameterContexts.stream().map(ParameterContext::getName).collect(Collectors.toList());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean inheritsFrom(final String parameterContextId) {
        readLock.lock();
        try {
            if (!inheritedParameterContexts.isEmpty()) {
                for (final ParameterContext inheritedParameterContext : inheritedParameterContexts) {
                    if (inheritedParameterContext.getIdentifier().equals(parameterContextId)) {
                        return true;
                    }
                    if (inheritedParameterContext.inheritsFrom(parameterContextId)) {
                        return true;
                    }
                }
            }
            return false;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanSetParameters(final Map<String, Parameter> updatedParameters) {
        verifyCanSetParameters(updatedParameters, false);
    }

    /**
     * Ensures that it is legal to update the Parameters for this Parameter Context to match the given set of Parameters
     * @param updatedParameters the updated set of parameters, keyed by Parameter name
     * @param duringUpdate If true, this check will be treated as if a ParameterContext update is imminent, meaning
     *                     referencing components may not be active even for updated values.  If false, a parameter
     *                     value update will be valid even if referencing components are active because it will be
     *                     assumed that these will be stopped prior to the actual update.
     * @throws IllegalStateException if setting the given set of Parameters is not legal
     */
    private void verifyCanSetParameters(final Map<String, Parameter> updatedParameters, final boolean duringUpdate) {
        verifyCanSetParameters(parameters, updatedParameters, duringUpdate);
    }

    public void verifyCanSetParameters(final Map<ParameterDescriptor, Parameter> currentParameters, final Map<String, Parameter> updatedParameters, final boolean duringUpdate) {
        final boolean updatingUserEnteredParameters = updatedParameters.values().stream()
                .anyMatch(parameter -> parameter != null && !parameter.isProvided());
        if (parameterProvider != null && updatingUserEnteredParameters) {
            throw new IllegalArgumentException(String.format("Parameters for Context [%s] cannot be manually updated because they are " +
                    "provided by Parameter Provider [%s]", name, parameterProvider.getIdentifier()));
        }

        // Ensure that the updated parameters will not result in changing the sensitivity flag of any parameter.
        for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            if (parameter == null) {
                // parameter is being deleted.
                validateReferencingComponents(parameterName, null, duringUpdate);
                continue;
            }

            if (!Objects.equals(parameterName, parameter.getDescriptor().getName())) {
                throw new IllegalArgumentException("Parameter '" + parameterName + "' was specified with the wrong key in the Map");
            }

            validateSensitiveFlag(currentParameters, parameter);
            validateReferencingComponents(parameterName, parameter, duringUpdate);
        }
    }

    private void validateSensitiveFlag(final Map<ParameterDescriptor, Parameter> currentParameters, final Parameter updatedParameter) {
        final ParameterDescriptor updatedDescriptor = updatedParameter.getDescriptor();
        final Parameter existingParameter = currentParameters.get(updatedDescriptor);

        if (existingParameter == null) {
            return;
        }

        final ParameterDescriptor existingDescriptor = existingParameter.getDescriptor();
        if (existingDescriptor.isSensitive() != updatedDescriptor.isSensitive() && updatedParameter.getValue() != null) {
            final String existingSensitiveDescription = existingDescriptor.isSensitive() ? "sensitive" : "not sensitive";
            final String updatedSensitiveDescription = updatedDescriptor.isSensitive() ? "sensitive" : "not sensitive";
            if (existingParameter.isProvided() && updatedParameter.isProvided()) {
                return;
            }

            throw new IllegalStateException("Cannot update Parameters because doing so would change Parameter '" + existingDescriptor.getName() + "' from " + existingSensitiveDescription
                + " to " + updatedSensitiveDescription);
        }
    }

    private void validateReferencingComponents(final String parameterName, final Parameter parameter, final boolean duringUpdate) {
        final boolean isDeletion = (parameter == null);
        final String action = isDeletion ? "remove" : "update";
        for (final ProcessorNode procNode : parameterReferenceManager.getProcessorsReferencing(this, parameterName)) {
            if (procNode.isRunning() && (isDeletion || duringUpdate)) {
                throw new IllegalStateException("Cannot " + action + " parameter '" + parameterName + "' because it is referenced by " + procNode + ", which is currently running");
            }

            if (parameter != null) {
                validateParameterSensitivity(parameter, procNode);
            }
        }

        for (final ControllerServiceNode serviceNode : parameterReferenceManager.getControllerServicesReferencing(this, parameterName)) {
            final ControllerServiceState serviceState = serviceNode.getState();
            if (serviceState != ControllerServiceState.DISABLED && (isDeletion || duringUpdate)) {
                throw new IllegalStateException("Cannot " + action + " parameter '" + parameterName + "' because it is referenced by "
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
    public void authorize(final Authorizer authorizer, final RequestAction action, final NiFiUser user) throws AccessDeniedException {
        ParameterContext.super.authorize(authorizer, action, user);

        if (RequestAction.READ == action) {
            for (final ParameterContext parameterContext : inheritedParameterContexts) {
                parameterContext.authorize(authorizer, action, user);
            }
            if (parameterProvider != null) {
                authorizeParameterProviderRead(authorizer, parameterProvider, user);
            }
        }
    }

    @Override
    public boolean isAuthorized(final Authorizer authorizer, final RequestAction action, final NiFiUser user) {
        try {
            authorize(authorizer, action, user);
            return true;
        } catch (final AccessDeniedException e) {
            return false;
        }
    }

    private void authorizeParameterProviderRead(final Authorizer authorizer, final ParameterProvider parameterProvider, final NiFiUser user) {
        final ParameterProviderNode parameterProviderNode = parameterProviderLookup.getParameterProvider(parameterProvider.getIdentifier());
        if (parameterProviderNode != null) {
            parameterProviderNode.authorize(authorizer, RequestAction.READ, user);
        }
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

    /**
     * A ParameterContext's identity is its identifier.
     * @param obj Another object
     * @return Whether this is equal to the object
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj != null && obj.getClass().equals(StandardParameterContext.class)) {
            final StandardParameterContext other = (StandardParameterContext) obj;
            return (getIdentifier().equals(other.getIdentifier()));
        } else {
            return false;
        }
    }

    /**
     * A ParameterContext's identity is its identifier.
     * @return The hash code
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getName()).append(getIdentifier()).toHashCode();
    }

    public static class Builder {
        private String id;
        private String name;
        private ParameterReferenceManager parameterReferenceManager;
        private Authorizable parentAuthorizable;
        private ParameterProviderLookup parameterProviderLookup;
        private ParameterProviderConfiguration parameterProviderConfiguration;

        /**
         * Sets the ParameterContext id -- required
         * @param id The ParameterContext id
         * @return The builder
         */
        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the ParameterContext name -- required
         * @param name The ParameterContext name
         * @return The builder
         */
        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the ParameterReferenceManager
         * @param parameterReferenceManager The ParameterReferenceManager
         * @return The builder
         */
        public Builder parameterReferenceManager(final ParameterReferenceManager parameterReferenceManager) {
            this.parameterReferenceManager = parameterReferenceManager;
            return this;
        }

        /**
         * Sets the parent Authorizable
         * @param parentAuthorizable The parent Authorizable
         * @return The builder
         */
        public Builder parentAuthorizable(final Authorizable parentAuthorizable) {
            this.parentAuthorizable = parentAuthorizable;
            return this;
        }

        /**
         * Sets the ParameterProviderLookup
         * @param parameterProviderLookup The ParameterProviderLookup
         * @return The builder
         */
        public Builder parameterProviderLookup(final ParameterProviderLookup parameterProviderLookup) {
            this.parameterProviderLookup = parameterProviderLookup;
            return this;
        }

        /**
         * Sets the ParameterProvider configuration
         * @param parameterProviderConfiguration The ParameterProvider configuration
         * @return The builder
         */
        public Builder parameterProviderConfiguration(final ParameterProviderConfiguration parameterProviderConfiguration) {
            this.parameterProviderConfiguration = parameterProviderConfiguration;
            return this;
        }

        public StandardParameterContext build() {
            return new StandardParameterContext(id, name, parameterReferenceManager, parentAuthorizable, parameterProviderLookup, parameterProviderConfiguration);
        }
    }
}