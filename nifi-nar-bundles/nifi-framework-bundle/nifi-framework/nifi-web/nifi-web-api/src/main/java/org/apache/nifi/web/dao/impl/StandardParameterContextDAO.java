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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.NonSensitiveParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.SensitiveParameterProvider;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.dao.ParameterContextDAO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        final String sensitiveParameterProviderId = getParameterProviderId(parameterContextDto.getSensitiveParameterProviderRef());
        final String nonSensitiveParameterProviderId = getParameterProviderId(parameterContextDto.getNonSensitiveParameterProviderRef());

        verifyParameterSourceConflicts(getParameterProvider(parameterContextDto.getSensitiveParameterProviderRef(), true), parameters);
        verifyParameterSourceConflicts(getParameterProvider(parameterContextDto.getNonSensitiveParameterProviderRef(), false), parameters);

        final AtomicReference<ParameterContext> parameterContextReference = new AtomicReference<>();
        flowManager.withParameterContextResolution(() -> {
            final List<String> referencedIds = parameterContextDto.getInheritedParameterContexts() == null
                    ? new ArrayList<>(0)
                    : parameterContextDto.getInheritedParameterContexts().stream().map(ParameterContextReferenceEntity::getId).collect(Collectors.toList());

            final ParameterContext parameterContext = flowManager.createParameterContext(parameterContextDto.getId(), parameterContextDto.getName(),
                    parameters, referencedIds, sensitiveParameterProviderId, nonSensitiveParameterProviderId);
            if (parameterContextDto.getDescription() != null) {
                parameterContext.setDescription(parameterContextDto.getDescription());
            }

            parameterContextReference.set(parameterContext);
        });
        return parameterContextReference.get();
    }

    private String getParameterProviderId(final ComponentReferenceEntity parameterProviderReference) {
        final String parameterProviderId = ParameterContextDTO.getReferenceId(parameterProviderReference);
        final ParameterProviderNode parameterProvider = flowManager.getParameterProvider(parameterProviderId);
        if (parameterProviderId != null && parameterProvider == null) {
            throw new IllegalArgumentException("Unable to locate Parameter Provider with id '" +  parameterProviderId + "'");
        }
        return parameterProviderId;
    }

    private void authorizeReferences(final ParameterContextDTO parameterContextDto) {
        final NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
        if (parameterContextDto.getInheritedParameterContexts() != null) {
            for (final ParameterContextReferenceEntity ref : parameterContextDto.getInheritedParameterContexts()) {
                final ParameterContext parameterContext = getParameterContext(ref.getComponent().getId());
                parameterContext.authorize(authorizer, RequestAction.READ, nifiUser);
            }
        }
        final ParameterProviderNode sensitiveParameterProvider = getParameterProviderNode(parameterContextDto.getSensitiveParameterProviderRef(), true);
        if (sensitiveParameterProvider != null) {
            sensitiveParameterProvider.authorize(authorizer, RequestAction.READ, nifiUser);
        }
        final ParameterProviderNode nonSensitiveParameterProvider = getParameterProviderNode(parameterContextDto.getNonSensitiveParameterProviderRef(), false);
        if (nonSensitiveParameterProvider != null) {
            nonSensitiveParameterProvider.authorize(authorizer, RequestAction.READ, nifiUser);
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
                throw new IllegalArgumentException(String.format("Could not resolve inherited parameter context references in Parameter Context [%s]",
                        parameterContextDto.getName()));
            }
            final ParameterContextReferenceDTO refDto = ref.getComponent();
            if (refDto.getId() != null) {
                continue;
            }

            // If resolving by name only, look up the ids
            final ParameterContext resolvedParameterContext = paramContextNameMap.get(refDto.getName());
            if (resolvedParameterContext == null) {
                throw new IllegalArgumentException(String.format("Parameter Context [%s] references missing inherited Parameter Context [%s]",
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

        final String parameterContextId = dto.getParameterContext() == null ? null : dto.getParameterContext().getId();

        return new Parameter(descriptor, value, parameterContextId, dto.getProvided());
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

        final SensitiveParameterProvider sensitiveParameterProvider = (SensitiveParameterProvider) getParameterProvider(parameterContextDto
                .getSensitiveParameterProviderRef(), true);
        context.setSensitiveParameterProvider(sensitiveParameterProvider);
        final NonSensitiveParameterProvider nonSensitiveParameterProvider = (NonSensitiveParameterProvider) getParameterProvider(parameterContextDto
                .getNonSensitiveParameterProviderRef(), false);
        context.setNonSensitiveParameterProvider(nonSensitiveParameterProvider);
        return context;
    }

    @Override
    public List<ParameterContext> getInheritedParameterContexts(final ParameterContextDTO parameterContextDto) {
        resolveInheritedParameterContexts(parameterContextDto);

        final List<ParameterContext> inheritedParameterContexts = new ArrayList<>();
        if (parameterContextDto.getInheritedParameterContexts() != null) {
            inheritedParameterContexts.addAll(parameterContextDto.getInheritedParameterContexts().stream()
                    .map(entity -> flowManager.getParameterContextManager().getParameterContext(entity.getComponent().getId()))
                    .collect(Collectors.toList()));
        }

        return inheritedParameterContexts;
    }

    @Override
    public void verifyUpdate(final ParameterContextDTO parameterContextDto, final boolean verifyComponentStates) {
        verifyNoNamingConflict(parameterContextDto.getName(), parameterContextDto.getId());
        verifyInheritedParameterContextRefs(parameterContextDto);

        final ParameterContext currentContext = getParameterContext(parameterContextDto.getId());

        final List<ParameterContext> inheritedParameterContexts = getInheritedParameterContexts(parameterContextDto);
        final Map<String, Parameter> parameters = parameterContextDto.getParameters() == null ? Collections.emptyMap() : getParameters(parameterContextDto, currentContext);
        currentContext.verifyCanUpdateParameterContext(parameters, inheritedParameterContexts);

        final SensitiveParameterProvider sensitiveParameterProvider = getSensitiveParameterProvider(parameterContextDto);
        final NonSensitiveParameterProvider nonSensitiveParameterProvider = getNonSensitiveParameterProvider(parameterContextDto);

        final Map<String, Parameter> proposedParameters = new HashMap<>();
        if (parameterContextDto.getParameters() != null) {
            proposedParameters.putAll(getParameters(parameterContextDto, currentContext));
        }

        verifyParameterSourceConflicts(sensitiveParameterProvider, proposedParameters);
        verifyParameterSourceConflicts(nonSensitiveParameterProvider, proposedParameters);
    }

    @Override
    public SensitiveParameterProvider getSensitiveParameterProvider(final ParameterContextDTO parameterContextDTO) {
        return (SensitiveParameterProvider) getParameterProvider(parameterContextDTO.getSensitiveParameterProviderRef(), true);
    }

    @Override
    public NonSensitiveParameterProvider getNonSensitiveParameterProvider(final ParameterContextDTO parameterContextDTO) {
        return (NonSensitiveParameterProvider) getParameterProvider(parameterContextDTO.getNonSensitiveParameterProviderRef(), false);
    }

    private void verifyParameterSourceConflicts(final ParameterProvider parameterProvider, final Map<String, Parameter> parameterUpdates) {
        if (parameterProvider != null) {
            final boolean sensitive = parameterProvider instanceof SensitiveParameterProvider;
            final String sensitivity = sensitive ? "Sensitive" : "Non-Sensitive";
            final boolean hasUserEnteredParameters = parameterUpdates.values().stream()
                    .filter(Objects::nonNull)
                    .filter(parameter -> !parameter.isProvided())
                    .anyMatch(parameter -> parameter.getDescriptor().isSensitive() == sensitive);
            if (hasUserEnteredParameters) {
                throw new IllegalArgumentException(String.format("User-entered %1$s Parameters may not be entered if a %1$s Parameter Provider is selected", sensitivity));
            }
        }
    }

    private ParameterProvider getParameterProvider(final ComponentReferenceEntity parameterProviderReference, boolean isSensitiveProvider) {
        final ParameterProviderNode parameterProviderNode = getParameterProviderNode(parameterProviderReference, isSensitiveProvider);
        return parameterProviderNode == null ? null : parameterProviderNode.getParameterProvider();
    }

    private ParameterProviderNode getParameterProviderNode(final ComponentReferenceEntity parameterProviderReference, boolean isSensitiveProvider) {
        ParameterProviderNode parameterProviderNode = null;
        final String parameterProviderId = ParameterContextDTO.getReferenceId(parameterProviderReference);
        if (parameterProviderId != null) {
            parameterProviderNode = flowManager.getParameterProvider(parameterProviderId);
            if (parameterProviderNode != null) {
                if (isSensitiveProvider && !parameterProviderNode.isSensitiveParameterProvider()) {
                    throw new IllegalStateException(String.format("Cannot set Non-Sensitive Parameter Provider [%s] as a Sensitive Parameter Provider", parameterProviderNode.getName()));
                }
                if (!isSensitiveProvider && parameterProviderNode.isSensitiveParameterProvider()) {
                    throw new IllegalStateException(String.format("Cannot set Sensitive Parameter Provider [%s] as a Non-Sensitive Parameter Provider", parameterProviderNode.getName()));
                }
            } else {
                throw new IllegalArgumentException("Unable to locate Parameter Provider with id '" +  parameterProviderId + "'");
            }
        }
        return parameterProviderNode;
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

        flowManager.getAllParameterProviders().forEach(provider -> {
            for (final ParameterContext referencedContext : provider.getReferences()) {
                if (referencedContext.getIdentifier().equals(parameterContextId)) {
                    provider.removeReference(referencedContext);
                }
            }
        });
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
