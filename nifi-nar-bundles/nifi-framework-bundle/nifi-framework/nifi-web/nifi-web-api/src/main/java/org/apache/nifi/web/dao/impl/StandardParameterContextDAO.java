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
import org.apache.nifi.web.dao.ParameterContextDAO;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        final Set<Parameter> parameters = getParameters(parameterContextDto);
        return flowManager.createParameterContext(parameterContextDto.getId(), parameterContextDto.getName(), parameters);
    }

    private Set<Parameter> getParameters(final ParameterContextDTO parameterContextDto) {
        final Set<ParameterDTO> parameterDtos = parameterContextDto.getParameters();
        if (parameterDtos == null) {
            return Collections.emptySet();
        }

        return parameterContextDto.getParameters().stream()
            .map(this::createParameter)
            .collect(Collectors.toSet());
    }

    private Parameter createParameter(final ParameterDTO dto) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name(dto.getName())
            .description(dto.getDescription())
            .sensitive(Boolean.TRUE.equals(dto.getSensitive()))
            .build();

        return new Parameter(descriptor, dto.getValue());
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
            final boolean conflict = flowManager.getParameterContextManager().getParameterContexts().stream()
                .anyMatch(paramContext -> paramContext.getName().equals(parameterContextDto.getName()) && !paramContext.getIdentifier().equals(parameterContextDto.getId()));

            if (conflict) {
                throw new IllegalStateException("Cannot update Parameter Context name because another Parameter Context already exists with the name '" + parameterContextDto.getName() + "'");
            }

            context.setName(parameterContextDto.getName());
        }

        if (parameterContextDto.getParameters() != null) {
            final Set<Parameter> parameters = getParameters(parameterContextDto);
            context.setParameters(parameters);
        }

        return context;
    }

    @Override
    public void verifyUpdate(final ParameterContextDTO parameterContextDto, final boolean verifyComponentStates) {
        verifyNoNamingConflict(parameterContextDto.getName());

        final ParameterContext currentContext = getParameterContext(parameterContextDto.getId());
        for (final ParameterDTO parameterDto : parameterContextDto.getParameters()) {
            final String parameterName = parameterDto.getName();
            final ParameterReferenceManager referenceManager = currentContext.getParameterReferenceManager();

            for (final ProcessorNode processor : referenceManager.getProcessorsReferencing(currentContext, parameterName)) {
                verifyParameterUpdate(parameterName, processor, parameterDto.getSensitive(), currentContext.getName(), verifyComponentStates, processor.isRunning(), "Processor that is running");
            }

            for (final ControllerServiceNode serviceNode : referenceManager.getControllerServicesReferencing(currentContext, parameterName)) {
                verifyParameterUpdate(parameterName, serviceNode, parameterDto.getSensitive(), currentContext.getName(), verifyComponentStates,
                    serviceNode.getState() != ControllerServiceState.DISABLED, "Controller Service that is enabled");
            }
        }
    }

    private void verifyParameterUpdate(final String parameterName, final ComponentNode component, final Boolean parameterSensitive, final String contextName,
                                            final boolean verifyComponentStates, final boolean active, final String activeExplanation) {
        // For any parameter that is added or modified, we need to ensure that the new configuration will not result in a Sensitive Parameter being referenced by a non-Sensitive Property
        // or a Non-Sensitive Parameter being referenced by a Sensitive Property.
        // Additionally, if 'verifyComponentStates', we must ensure that any component that references a value that is to be updated is stopped (if a processor) or disabled (if a controller service)
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : component.getProperties().entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                final String referencedParameterName = reference.getParameterName();
                if (referencedParameterName.equals(parameterName)) {
                    if (entry.getKey().isSensitive() && !Boolean.TRUE.equals(parameterSensitive)) {
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because the update would add a Non-Sensitive Parameter " +
                            "named '" + parameterName + "' but this Parameter already is referenced by a Sensitive Property.");
                    }

                    if (!entry.getKey().isSensitive() && Boolean.TRUE.equals(parameterSensitive)) {
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because the update would add a Sensitive Parameter named " +
                            "'" + parameterName + "' but this Parameter already is referenced by a Non-Sensitive Property.");
                    }

                    if (verifyComponentStates && active) {
                        throw new IllegalStateException("Cannot update Parameter Context " + contextName + " because it has Parameters that are being referenced by a " +
                            activeExplanation + ".");
                    }
                }
            }
        }
    }

    private void verifyNoNamingConflict(final String contextName) {
        if (contextName == null) {
            return;
        }

        final boolean conflict = flowManager.getParameterContextManager().getParameterContexts().stream()
            .anyMatch(paramContext -> paramContext.getName().equals(contextName));

        if (conflict) {
            throw new IllegalStateException("Cannot update Parameter Context name because another Parameter Context already exists with the name '" + contextName + "'");
        }
    }


    @Override
    public void verifyDelete(final String parameterContextId) {
        // Find all Process Groups that are bound to the Parameter Context
        final List<ProcessGroup> groupsReferencingParameterContext = getBoundProcessGroups(parameterContextId);

        // If any component is referencing a Parameter and is running/enabled then fail
        for (final ProcessGroup group : groupsReferencingParameterContext) {
            for (final ProcessorNode processor : group.getProcessors()) {
                if (!processor.getReferencedParameterNames().isEmpty() && processor.isRunning()) {
                    throw new IllegalStateException("Cannot delete Parameter Context with ID " + parameterContextId + " because it is in use by at least one Processor that is running");
                }
            }

            for (final ControllerServiceNode service : group.getControllerServices(false)) {
                if (!service.getReferencedParameterNames().isEmpty() && service.getState() != ControllerServiceState.DISABLED) {
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
