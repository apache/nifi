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
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.dao.ParameterContextDAO;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StandardParameterContextDAO implements ParameterContextDAO {
    private FlowManager flowManager;

    @Override
    public boolean hasParameterContext(final String parameterContextId) {
        return flowManager.getParameterContextManager().getParameterContext(parameterContextId) != null;
    }

    @Override
    public void verifyCreate(final ParameterContextDTO parameterContextDto) {
        verifyNoNamingConflict(parameterContextDto.getName());
    }

    @Override
    public ParameterContext createParameterContext(final ParameterContextDTO parameterContextDto) {
        final Map<String, Parameter> parameters = getParameters(parameterContextDto, null);
        final ParameterContext parameterContext = flowManager.createParameterContext(parameterContextDto.getId(), parameterContextDto.getName(), parameters);
        if (parameterContextDto.getDescription() != null) {
            parameterContext.setDescription(parameterContextDto.getDescription());
        }
        return parameterContext;
    }

    private Map<String, Parameter> getParameters(final ParameterContextDTO parameterContextDto, final ParameterContext context) {
        final Set<ParameterEntity> parameterEntities = parameterContextDto.getParameters();
        if (parameterEntities == null) {
            return Collections.emptyMap();
        }

        final Map<String, Parameter> parameterMap = new HashMap<>();
        for (final ParameterEntity parameterEntity : parameterEntities) {
            final ParameterDTO parameterDto = parameterEntity.getParameter();

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

        return context;
    }

    @Override
    public void verifyUpdate(final ParameterContextDTO parameterContextDto, final boolean verifyComponentStates) {
        verifyNoNamingConflict(parameterContextDto.getName(), parameterContextDto.getId());

        final ParameterContext currentContext = getParameterContext(parameterContextDto.getId());
        for (final ParameterEntity parameterEntity : parameterContextDto.getParameters()) {
            final ParameterDTO parameterDto = parameterEntity.getParameter();
            final String parameterName = parameterDto.getName();
            final ParameterReferenceManager referenceManager = currentContext.getParameterReferenceManager();

            for (final ProcessorNode processor : referenceManager.getProcessorsReferencing(currentContext, parameterName)) {
                verifyParameterUpdate(parameterDto, processor, currentContext.getName(), verifyComponentStates, processor.isRunning(), "Processor that is running");
            }

            for (final ControllerServiceNode serviceNode : referenceManager.getControllerServicesReferencing(currentContext, parameterName)) {
                verifyParameterUpdate(parameterDto, serviceNode, currentContext.getName(), verifyComponentStates,
                    serviceNode.getState() != ControllerServiceState.DISABLED, "Controller Service that is enabled");
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
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because it has Parameters that are being referenced by a " +
                            activeExplanation + ".");
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
}
