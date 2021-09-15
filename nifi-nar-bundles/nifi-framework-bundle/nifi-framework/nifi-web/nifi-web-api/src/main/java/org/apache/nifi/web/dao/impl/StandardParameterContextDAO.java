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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.dao.ParameterContextDAO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StandardParameterContextDAO implements ParameterContextDAO {
    private FlowManager flowManager;
    private Authorizer authorizer;

    @Override
    public boolean hasParameterContext(final String parameterContextId) {
        return flowManager.getParameterContextManager().getParameterContext(parameterContextId) != null;
    }

    @Override
    public void verifyCreate(final ParameterContextDTO parameterContextDto) {
        verifyNoNamingConflict(parameterContextDto.getName());
        verifyInheritedParameterContextRefs(parameterContextDto);
    }

    private void verifyInheritedParameterContextRefs(final ParameterContextDTO parameterContextDto) {
        final List<ParameterContextReferenceEntity> inheritedParameterContexts = parameterContextDto.getInheritedParameterContexts();

        if (inheritedParameterContexts != null) {
            resolveInheritedParameterContexts(parameterContextDto);
            // This will throw an exception if one is not found
            inheritedParameterContexts.forEach(entity -> flowManager.getParameterContextManager()
                    .getParameterContext(entity.getComponent().getId()));
        }
        authorizeReferences(parameterContextDto);


    }

    @Override
    public ParameterContext createParameterContext(final ParameterContextDTO parameterContextDto) {
        final Map<String, Parameter> parameters = getParameters(parameterContextDto, null);

        resolveInheritedParameterContexts(parameterContextDto);

        final AtomicReference<ParameterContext> parameterContextReference = new AtomicReference<>();
        flowManager.withParameterContextResolution(() -> {
            final ParameterContext parameterContext = flowManager.createParameterContext(parameterContextDto.getId(), parameterContextDto.getName(),
                    parameters, parameterContextDto.getInheritedParameterContexts());
            if (parameterContextDto.getDescription() != null) {
                parameterContext.setDescription(parameterContextDto.getDescription());
            }
            parameterContextReference.set(parameterContext);
        });
        return parameterContextReference.get();
    }

    private void authorizeReferences(final ParameterContextDTO parameterContextDto) {
        final NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
        if (parameterContextDto.getInheritedParameterContexts() != null) {
            for (final ParameterContextReferenceEntity ref : parameterContextDto.getInheritedParameterContexts()) {
                final ParameterContext parameterContext = getParameterContext(ref.getComponent().getId());
                parameterContext.authorize(authorizer, RequestAction.READ, nifiUser);
            }
        }
    }

    private void resolveInheritedParameterContexts(final ParameterContextDTO parameterContextDto) {
        final List<ParameterContextReferenceEntity> inheritedParameterContexts = parameterContextDto.getInheritedParameterContexts();
        if (inheritedParameterContexts == null || inheritedParameterContexts.isEmpty()) {
            return;
        }

        final Map<String, ParameterContext> paramContextNameMap = flowManager.getParameterContextManager().getParameterContextNameMapping();
        for (final ParameterContextReferenceEntity ref : inheritedParameterContexts) {
            if (ref.getComponent() == null || (ref.getComponent().getId() == null && ref.getComponent().getName() == null)) {
                throw new IllegalStateException(String.format("Could not resolve inherited parameter context references in Parameter Context [%s]",
                        parameterContextDto.getName()));
            }
            final ParameterContextReferenceDTO refDto = ref.getComponent();
            if (refDto.getId() != null) {
                continue;
            }

            // If resolving by name only, look up the ids
            final ParameterContext resolvedParameterContext = paramContextNameMap.get(refDto.getName());
            if (resolvedParameterContext == null) {
                throw new IllegalStateException(String.format("Parameter Context [%s] references missing inherited Parameter Context [%s]",
                        parameterContextDto.getName(), refDto.getName()));
            }

            ref.setId(resolvedParameterContext.getIdentifier());
            ref.getComponent().setId(resolvedParameterContext.getIdentifier());
        }
    }

    @Override
    public Map<String, Parameter> getParameters(final ParameterContextDTO parameterContextDto, final ParameterContext context) {
        final Set<ParameterEntity> parameterEntities = parameterContextDto.getParameters();
        if (parameterEntities == null) {
            return Collections.emptyMap();
        }

        final Map<String, Parameter> parameterMap = new HashMap<>();
        for (final ParameterEntity parameterEntity : parameterEntities) {
            final ParameterDTO parameterDto = parameterEntity.getParameter();

            // Inherited parameters are only included for referencing components, but we should not save them as direct parameters
            if (parameterDto.getInherited() != null && parameterDto.getInherited()) {
                continue;
            }

            if (parameterDto.getName() == null) {
                throw new IllegalArgumentException("Cannot specify a Parameter without a name");
            }

            final boolean deletion = parameterDto.getDescription() == null && parameterDto.getSensitive() == null && parameterDto.getValue() == null;
            if (deletion) {
                parameterMap.put(parameterDto.getName().trim(), null);
            } else {
                final Parameter parameter = createParameter(parameterDto, context);
                parameterMap.put(parameterDto.getName().trim(), parameter);
            }
        }

        return parameterMap;
    }

    private Parameter createParameter(final ParameterDTO dto, final ParameterContext context) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name(dto.getName())
            .description(dto.getDescription())
            .sensitive(Boolean.TRUE.equals(dto.getSensitive()))
            .build();

        final String value;
        if (dto.getValue() == null && Boolean.TRUE.equals(dto.getValueRemoved())) {
            // Value is being explicitly set to null
            value = null;
        } else if (dto.getValue() == null && context != null) {
            // Value was just never supplied. Use the value from the Parameter Context, if there is one.
            final Optional<Parameter> optionalParameter = context.getParameter(dto.getName());
            value = optionalParameter.map(Parameter::getValue).orElse(dto.getValue());
        } else {
            value = dto.getValue();
        }

        return new Parameter(descriptor, value);
    }

    @Override
    public ParameterContext getParameterContext(final String parameterContextId) {
        final ParameterContext context = flowManager.getParameterContextManager().getParameterContext(parameterContextId);
        if (context == null) {
            throw new ResourceNotFoundException(String.format("Unable to find Parameter Context with id '%s'.", parameterContextId));
        }

        return context;
    }

    @Override
    public Set<ParameterContext> getParameterContexts() {
        return flowManager.getParameterContextManager().getParameterContexts();
    }

    @Override
    public ParameterContext updateParameterContext(final ParameterContextDTO parameterContextDto) {
        verifyUpdate(parameterContextDto, true);

        final ParameterContext context = getParameterContext(parameterContextDto.getId());

        if (parameterContextDto.getName() != null) {
            verifyNoNamingConflict(parameterContextDto.getName(), parameterContextDto.getId());
            context.setName(parameterContextDto.getName());
        }

        if (parameterContextDto.getDescription() != null) {
            context.setDescription(parameterContextDto.getDescription());
        }

        if (parameterContextDto.getParameters() != null) {
            final Map<String, Parameter> parameters = getParameters(parameterContextDto, context);
            context.setParameters(parameters);
        }

        if (parameterContextDto.getInheritedParameterContexts() != null) {
            final List<ParameterContext> inheritedParameterContexts = getInheritedParameterContexts(parameterContextDto);
            context.setInheritedParameterContexts(inheritedParameterContexts);
        }
        return context;
    }

    @Override
    public List<ParameterContext> getInheritedParameterContexts(final ParameterContextDTO parameterContextDto) {
        resolveInheritedParameterContexts(parameterContextDto);

        final List<ParameterContext> inheritedParameterContexts = new ArrayList<>();
        inheritedParameterContexts.addAll(parameterContextDto.getInheritedParameterContexts().stream()
                .map(entity -> flowManager.getParameterContextManager().getParameterContext(entity.getComponent().getId()))
                .collect(Collectors.toList()));

        return inheritedParameterContexts;
    }

    @Override
    public void verifyUpdate(final ParameterContextDTO parameterContextDto, final boolean verifyComponentStates) {
        verifyNoNamingConflict(parameterContextDto.getName(), parameterContextDto.getId());
        verifyInheritedParameterContextRefs(parameterContextDto);

        final ParameterContext currentContext = getParameterContext(parameterContextDto.getId());

        final List<ParameterContext> inheritedParameterContexts = getInheritedParameterContexts(parameterContextDto);
        currentContext.verifyCanSetInheritedParameterContexts(inheritedParameterContexts);

        for (final ParameterEntity parameterEntity : parameterContextDto.getParameters()) {
            final ParameterDTO parameterDto = parameterEntity.getParameter();
            final String parameterName = parameterDto.getName();
            final ParameterReferenceManager referenceManager = currentContext.getParameterReferenceManager();

            for (final ProcessorNode processor : referenceManager.getProcessorsReferencing(currentContext, parameterName)) {
                verifyParameterUpdate(parameterDto, processor, currentContext.getName(), verifyComponentStates, processor.isRunning(), "Processor that is running");
            }

            final Set<ControllerServiceNode> referencingServices = referenceManager.getControllerServicesReferencing(currentContext, parameterName);
            for (final ControllerServiceNode serviceNode : referencingServices) {
                final ControllerServiceState serviceState = serviceNode.getState();
                final boolean serviceActive = serviceState != ControllerServiceState.DISABLED;

                verifyParameterUpdate(parameterDto, serviceNode, currentContext.getName(), verifyComponentStates, serviceActive,
                    "Controller Service [id=" + serviceNode.getIdentifier() + "] with a state of " + serviceState + " (state expected to be DISABLED)");
            }
        }
    }

    private void verifyParameterUpdate(final ParameterDTO parameterDto, final ComponentNode component, final String contextName,
                                            final boolean verifyComponentStates, final boolean active, final String activeExplanation) {

        final String parameterName = parameterDto.getName();
        final Boolean parameterSensitive = parameterDto.getSensitive();
        final boolean parameterDeletion = parameterDto.getDescription() == null && parameterDto.getSensitive() == null && parameterDto.getValue() == null;

        // For any parameter that is added or modified, we need to ensure that the new configuration will not result in a Sensitive Parameter being referenced by a non-Sensitive Property
        // or a Non-Sensitive Parameter being referenced by a Sensitive Property.
        // Additionally, if 'verifyComponentStates' or parameter is being deleted, we must ensure that any component that references a value that is to be updated
        // is stopped (if a processor) or disabled (if a controller service).
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : component.getProperties().entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                final String referencedParameterName = reference.getParameterName();
                if (referencedParameterName.equals(parameterName)) {
                    if (entry.getKey().isSensitive() && !parameterDeletion && !Boolean.TRUE.equals(parameterSensitive)) {
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because the update would add a Non-Sensitive Parameter " +
                            "named '" + parameterName + "' but this Parameter already is referenced by a Sensitive Property.");
                    }

                    if (!entry.getKey().isSensitive() && !parameterDeletion && Boolean.TRUE.equals(parameterSensitive)) {
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because the update would add a Sensitive Parameter named " +
                            "'" + parameterName + "' but this Parameter already is referenced by a Non-Sensitive Property.");
                    }

                    if (active && (verifyComponentStates || parameterDeletion)) {
                        if (parameterDeletion) {
                            // First check if the actual parameter context is now missing the parameter: it may not be,
                            // if the parameter is inherited from another context
                            final ProcessGroup processGroup = flowManager.getGroup(component.getProcessGroupIdentifier());
                            final ParameterContext parameterContext = processGroup.getParameterContext();
                            final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
                                    .name(parameterName).build();
                            if (!parameterContext.hasEffectiveValueIfRemoved(parameterDescriptor)) {
                                throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because the " + parameterName + " Parameter is being referenced by a " +
                                        activeExplanation + ".");
                            }
                        } else {
                            throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because it has Parameters that are being referenced by a " +
                                    activeExplanation + ".");
                        }
                    }
                }
            }
        }
    }

    /**
     * Ensures that no Parameter Context exists with the given name. If any does already exist with this name, an IllegalStateException will be thrown.
     * @param contextName the name of the Parameter Context
     * @throws IllegalStateException if any Parameter Context already exists with the given name
     */
    private void verifyNoNamingConflict(final String contextName) {
        verifyNoNamingConflict(contextName, null);
    }

    /**
     * Ensures that no Parameter Context exists with the given name, unless that Parameter Context also has the given identifier. If any does already exist with this name, and its identifier does
     * not match the given identifier, an IllegalStateException will be thrown. Otherwise, this method will return gracefully.
     * @param contextName the name of the Parameter Context
     * @param contextId the Identifier of the Parameter Context whose name should not be compared against the given name
     * @throws IllegalStateException if any Parameter Context already exists with the given name and a non-matching identifier
     */
    private void verifyNoNamingConflict(final String contextName, final String contextId) {
        if (contextName == null) {
            return;
        }

        for (final ParameterContext parameterContext : flowManager.getParameterContextManager().getParameterContexts()) {
            if (parameterContext.getName().equals(contextName)) {
                if (contextId == null || contextId.equals(parameterContext.getIdentifier())) {
                    continue;
                }

                throw new IllegalStateException("Cannot update Parameter Context name because another Parameter Context already exists with the name '" + contextName + "'");
            }
        }
    }


    @Override
    public void verifyDelete(final String parameterContextId) {
        // Find all Process Groups that are bound to the Parameter Context
        final List<ProcessGroup> groupsReferencingParameterContext = getBoundProcessGroups(parameterContextId);

        // If any component is referencing a Parameter and is running/enabled then fail
        for (final ProcessGroup group : groupsReferencingParameterContext) {
            for (final ProcessorNode processor : group.getProcessors()) {
                if (processor.isReferencingParameter() && processor.isRunning()) {
                    throw new IllegalStateException("Cannot delete Parameter Context with ID " + parameterContextId + " because it is in use by at least one Processor that is running");
                }
            }

            for (final ControllerServiceNode service : group.getControllerServices(false)) {
                if (service.isReferencingParameter() && service.getState() != ControllerServiceState.DISABLED) {
                    throw new IllegalStateException("Cannot delete Parameter Context with ID " + parameterContextId + " because it is in use by at least one Controller Service that is enabled");
                }
            }
        }

        for (final ParameterContext parameterContext : flowManager.getParameterContextManager().getParameterContexts()) {
            if (parameterContext.getInheritedParameterContexts().stream().anyMatch(pc -> pc.getIdentifier().equals(parameterContextId))) {
                throw new IllegalStateException(String.format("Cannot delete Parameter Context with ID [%s] because it is referenced by at least one Parameter Context [%s]",
                        parameterContextId, parameterContext.getName()));
            }
        }
    }

    @Override
    public void deleteParameterContext(final String parameterContextId) {
        verifyDelete(parameterContextId);

        // Remove the Parameter Context from the manager
        flowManager.getParameterContextManager().removeParameterContext(parameterContextId);

        // Update all Process Groups that currently are bound to the Parameter Context so that they are no longer bound to any Parameter Context
        getBoundProcessGroups(parameterContextId).forEach(group -> group.setParameterContext(null));
    }

    public void setFlowController(final FlowController flowController) {
        this.flowManager = flowController.getFlowManager();
    }

    private List<ProcessGroup> getBoundProcessGroups(final String parameterContextId) {
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        return rootGroup.findAllProcessGroups(group -> group.getParameterContext() != null && group.getParameterContext().getIdentifier().equals(parameterContextId));
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
