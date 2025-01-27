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
package org.apache.nifi.web.util;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.AbstractParameterResource;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ParameterUpdateManager {
    private static final Logger logger = LoggerFactory.getLogger(ParameterUpdateManager.class);

    private final NiFiServiceFacade serviceFacade;
    private final DtoFactory dtoFactory;
    private final Authorizer authorizer;
    private final AbstractParameterResource parameterResource;

    public ParameterUpdateManager(final NiFiServiceFacade serviceFacade, final DtoFactory dtoFactory, final Authorizer authorizer, final AbstractParameterResource parameterResource) {
        this.serviceFacade = serviceFacade;
        this.dtoFactory = dtoFactory;
        this.authorizer = authorizer;
        this.parameterResource = parameterResource;
    }

    public void authorizeAffectedComponent(final AffectedComponentEntity entity, final AuthorizableLookup lookup, final NiFiUser user, final boolean requireRead, final boolean requireWrite) {
        final AffectedComponentDTO dto = entity.getComponent();
        if (dto == null) {
            // If the DTO is null, it is an indication that the user does not have permissions.
            // However, we don't want to just throw an AccessDeniedException because we would rather
            // ensure that all of the appropriate actions are taken by the pluggable Authorizer. As a result,
            // we attempt to find the component as a Processor and fall back to finding it as a Controller Service.
            // We then go ahead and attempt the authorization, expecting it to fail.
            Authorizable authorizable;
            try {
                authorizable = lookup.getProcessor(entity.getId()).getAuthorizable();
            } catch (final ResourceNotFoundException rnfe) {
                authorizable = lookup.getControllerService(entity.getId()).getAuthorizable();
            }

            if (requireRead) {
                authorizable.authorize(authorizer, RequestAction.READ, user);
            }
            if (requireWrite) {
                authorizable.authorize(authorizer, RequestAction.WRITE, user);
            }
        } else if (AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(dto.getReferenceType())) {
            final Authorizable processor = lookup.getProcessor(dto.getId()).getAuthorizable();

            if (requireRead) {
                processor.authorize(authorizer, RequestAction.READ, user);
            }
            if (requireWrite) {
                processor.authorize(authorizer, RequestAction.WRITE, user);
            }
        } else if (AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getReferenceType())) {
            final Authorizable service = lookup.getControllerService(dto.getId()).getAuthorizable();

            if (requireRead) {
                service.authorize(authorizer, RequestAction.READ, user);
            }
            if (requireWrite) {
                service.authorize(authorizer, RequestAction.WRITE, user);
            }
        } else if (AffectedComponentDTO.COMPONENT_TYPE_STATELESS_GROUP.equals(dto.getReferenceType())) {
            final Authorizable group = lookup.getProcessGroup(dto.getId()).getAuthorizable();

            if (requireRead) {
                group.authorize(authorizer, RequestAction.READ, user);
            }
            if (requireWrite) {
                group.authorize(authorizer, RequestAction.WRITE, user);
            }
        }
    }

    public List<ParameterContextEntity> updateParameterContexts(final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest,
                                                                final ComponentLifecycle componentLifecycle, final URI uri, final Set<AffectedComponentEntity> affectedComponents,
                                                                final boolean replicateRequest, final Revision revision, final List<ParameterContextEntity> updatedContextEntities)
            throws LifecycleManagementException, ResumeFlowException {

        final Set<AffectedComponentEntity> runningStatelessGroups = affectedComponents.stream()
                .filter(entity -> entity.getComponent() != null)
                .filter(entity -> AffectedComponentDTO.COMPONENT_TYPE_STATELESS_GROUP.equals(entity.getComponent().getReferenceType()))
                .filter(component -> "Running".equalsIgnoreCase(component.getComponent().getState()))
                .collect(Collectors.toSet());

        final Set<AffectedComponentEntity> runningProcessors = affectedComponents.stream()
                .filter(entity -> entity.getComponent() != null)
                .filter(entity -> AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(entity.getComponent().getReferenceType()))
                .filter(component -> "Running".equalsIgnoreCase(component.getComponent().getState()))
                .collect(Collectors.toSet());

        final Set<AffectedComponentEntity> servicesRequiringDisabledState = affectedComponents.stream()
                .filter(entity -> entity.getComponent() != null)
                .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
                .filter(dto -> {
                    final String state = dto.getComponent().getState();
                    return "Enabling".equalsIgnoreCase(state) || "Enabled".equalsIgnoreCase(state) || "Disabling".equalsIgnoreCase(state);
                })
                .collect(Collectors.toSet());

        stopComponents(runningStatelessGroups, "stateless process group", asyncRequest, componentLifecycle, uri);
        stopComponents(runningProcessors, "processor", asyncRequest, componentLifecycle, uri);

        if (asyncRequest.isCancelled()) {
            return null;
        }

        // We want to disable only those Controller Services that are currently enabled or enabling, but we need to wait for
        // services that are currently Disabling to become disabled before we are able to consider this step complete.
        final Set<AffectedComponentEntity> enabledControllerServices = servicesRequiringDisabledState.stream()
                .filter(dto -> {
                    final String state = dto.getComponent().getState();
                    return "Enabling".equalsIgnoreCase(state) || "Enabled".equalsIgnoreCase(state);
                })
                .collect(Collectors.toSet());

        disableControllerServices(enabledControllerServices, servicesRequiringDisabledState, asyncRequest, componentLifecycle, uri);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        asyncRequest.markStepComplete();
        final List<ParameterContextEntity> updatedEntities = new ArrayList<>();
        try {
            for (final ParameterContextEntity updatedContextEntity : updatedContextEntities) {
                logger.info("Updating Parameter Context with ID {}", updatedContextEntity.getId());

                updatedEntities.add(performParameterContextUpdate(asyncRequest, uri, replicateRequest, revision, updatedContextEntity));
                logger.info("Successfully updated Parameter Context with ID {}", updatedContextEntity.getId());
            }
            asyncRequest.markStepComplete();
        } finally {
            // TODO: can almost certainly be refactored so that the same code is shared between VersionsResource and ParameterContextResource.
            if (!asyncRequest.isCancelled()) {
                enableControllerServices(enabledControllerServices, enabledControllerServices, asyncRequest, componentLifecycle, uri);
            }

            if (!asyncRequest.isCancelled()) {
                restartComponents(runningProcessors, "processor", asyncRequest, componentLifecycle, uri);
                restartComponents(runningStatelessGroups, "stateless process group", asyncRequest, componentLifecycle, uri);

                asyncRequest.markStepComplete();
            }
        }

        asyncRequest.setCancelCallback(null);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        return updatedEntities;
    }

    private ParameterContextEntity performParameterContextUpdate(final AsynchronousWebRequest<?, ?> asyncRequest, final URI exampleUri, final boolean replicateRequest, final Revision revision,
                                                                 final ParameterContextEntity updatedContext) throws LifecycleManagementException {

        if (replicateRequest) {
            final URI updateUri;
            try {
                updateUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                        exampleUri.getPort(), "/nifi-api/parameter-contexts/" + updatedContext.getId(), null, exampleUri.getFragment());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }

            final Map<String, String> headers = new HashMap<>();
            headers.put("content-type", MediaType.APPLICATION_JSON);

            final NiFiUser user = asyncRequest.getUser();
            final NodeResponse clusterResponse = parameterResource.updateParameterContext(updatedContext, updateUri, headers, user);

            final int updateFlowStatus = clusterResponse.getStatus();
            if (updateFlowStatus != Response.Status.OK.getStatusCode()) {
                final String explanation = getResponseEntity(clusterResponse, String.class);
                logger.error("Failed to update flow across cluster when replicating PUT request to {} for user {}. Received {} response with explanation: {}",
                        updateUri, user, updateFlowStatus, explanation);
                throw new LifecycleManagementException("Failed to update Flow on all nodes in cluster due to " + explanation);
            }

            return serviceFacade.getParameterContext(updatedContext.getId(), false, user);
        } else {
            serviceFacade.verifyUpdateParameterContext(updatedContext.getComponent(), true);
            return serviceFacade.updateParameterContext(revision, updatedContext.getComponent());
        }
    }

    /**
     * Extracts the response entity from the specified node response.
     *
     * @param nodeResponse node response
     * @param clazz class
     * @param <T> type of class
     * @return the response entity
     */
    @SuppressWarnings("unchecked")
    public static <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            if (nodeResponse.getClientResponse() != null) {
                entity = nodeResponse.getClientResponse().readEntity(clazz);
            } else {
                entity = (T) nodeResponse.getThrowable().toString();
            }
        }
        return entity;
    }

    private void stopComponents(final Set<AffectedComponentEntity> components, final String componentType, final AsynchronousWebRequest<?, ?> asyncRequest,
                                final ComponentLifecycle componentLifecycle, final URI uri)
            throws LifecycleManagementException {

        logger.info("Stopping {} {}s in order to update Parameter Context", components.size(), componentType);
        final CancellableTimedPause stopComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(stopComponentsPause::cancel);
        componentLifecycle.scheduleComponents(uri, "root", components, ScheduledState.STOPPED, stopComponentsPause, InvalidComponentAction.SKIP);
    }

    private void restartComponents(final Set<AffectedComponentEntity> components, final String componentType, final AsynchronousWebRequest<?, ?> asyncRequest,
                                   final ComponentLifecycle componentLifecycle, final URI uri)
            throws ResumeFlowException, LifecycleManagementException {

        if (logger.isDebugEnabled()) {
            logger.debug("Restarting {} {}s after having updated Parameter Context: {}", components.size(), componentType, components);
        } else {
            logger.info("Restarting {} {}s after having updated Parameter Context", components.size(), componentType);
        }

        // Step 14. Restart all components
        final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(components);

        final CancellableTimedPause startComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(startComponentsPause::cancel);

        try {
            componentLifecycle.scheduleComponents(uri, "root", componentsToStart, ScheduledState.RUNNING, startComponentsPause, InvalidComponentAction.SKIP);
        } catch (final IllegalStateException ise) {
            // Component Lifecycle will restart the Processors only if they are valid. If IllegalStateException gets thrown, we need to provide
            // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
            throw new ResumeFlowException("Failed to restart components because " + ise.getMessage(), ise);
        }
    }

    private void disableControllerServices(final Set<AffectedComponentEntity> enabledControllerServices, final Set<AffectedComponentEntity> controllerServicesRequiringDisabledState,
                                           final AsynchronousWebRequest<?, ?> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri) throws LifecycleManagementException {

        asyncRequest.markStepComplete();
        logger.info("Disabling {} Controller Services in order to update Parameter Context", enabledControllerServices.size());
        final CancellableTimedPause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(disableServicesPause::cancel);
        componentLifecycle.activateControllerServices(uri, "root", enabledControllerServices, controllerServicesRequiringDisabledState, ControllerServiceState.DISABLED, disableServicesPause,
                InvalidComponentAction.WAIT);
    }

    private void enableControllerServices(final Set<AffectedComponentEntity> controllerServices, final Set<AffectedComponentEntity> controllerServicesRequiringDisabledState,
                                          final AsynchronousWebRequest<?, ?> asyncRequest, final ComponentLifecycle componentLifecycle,
                                          final URI uri) throws LifecycleManagementException, ResumeFlowException {
        if (logger.isDebugEnabled()) {
            logger.debug("Re-Enabling {} Controller Services: {}", controllerServices.size(), controllerServices);
        } else {
            logger.info("Re-Enabling {} Controller Services after having updated Parameter Context", controllerServices.size());
        }

        // Step 13. Re-enable all disabled controller services
        final CancellableTimedPause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(enableServicesPause::cancel);
        final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(controllerServices);

        try {
            componentLifecycle.activateControllerServices(uri, "root", servicesToEnable, controllerServicesRequiringDisabledState,
                    ControllerServiceState.ENABLED, enableServicesPause, InvalidComponentAction.SKIP);
            asyncRequest.markStepComplete();
        } catch (final IllegalStateException ise) {
            // Component Lifecycle will re-enable the Controller Services only if they are valid. If IllegalStateException gets thrown, we need to provide
            // a more intelligent error message as to exactly what happened, rather than indicate that the Parameter Context could not be updated.
            throw new ResumeFlowException("Failed to re-enable Controller Services because " + ise.getMessage(), ise);
        }
    }

    private Set<AffectedComponentEntity> getUpdatedEntities(final Set<AffectedComponentEntity> originalEntities) {
        final Set<AffectedComponentEntity> entities = new LinkedHashSet<>();

        for (final AffectedComponentEntity original : originalEntities) {
            try {
                final AffectedComponentEntity updatedEntity = AffectedComponentUtils.updateEntity(original, serviceFacade, dtoFactory);
                if (updatedEntity != null) {
                    entities.add(updatedEntity);
                }
            } catch (final ResourceNotFoundException ignored) {
                // Component was removed. Just continue on without adding anything to the entities.
                // We do this because the intent is to get updated versions of the entities with current
                // Revisions so that we can change the states of the components. If the component was removed,
                // then we can just drop the entity, since there is no need to change its state.
            }
        }

        return entities;
    }
}
