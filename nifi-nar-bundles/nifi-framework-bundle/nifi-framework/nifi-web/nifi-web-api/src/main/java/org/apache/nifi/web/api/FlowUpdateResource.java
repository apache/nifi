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
package org.apache.nifi.web.api;

import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeParameterReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.registry.flow.FlowRegistryUtils;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.FlowUpdateRequestDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupDescriptorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.util.AffectedComponentUtils;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.InvalidComponentAction;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Parameterized abstract resource for use in updating flows.
 *
 * @param <T>   Entity to use for describing a process group for update purposes
 * @param <U>   Entity to capture the status and result of an update request
 */
public abstract class FlowUpdateResource<T extends ProcessGroupDescriptorEntity, U extends FlowUpdateRequestEntity> extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(FlowUpdateResource.class);

    protected NiFiServiceFacade serviceFacade;
    protected Authorizer authorizer;

    protected DtoFactory dtoFactory;
    protected ComponentLifecycle clusterComponentLifecycle;
    protected ComponentLifecycle localComponentLifecycle;

    protected RequestManager<T, T> requestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L),
                    "Process Group Update Thread");

    /**
     * Perform actual flow update
     */
    protected abstract ProcessGroupEntity performUpdateFlow(final String groupId, final Revision revision, final T requestEntity,
                                                            final VersionedFlowSnapshot flowSnapshot, final String idGenerationSeed,
                                                            final boolean verifyNotModified, final boolean updateDescendantVersionedFlows);

    /**
     * Create the entity that is passed for update flow replication
     */
    protected abstract Entity createReplicateUpdateFlowEntity(final Revision revision, final T requestEntity,
                                                              final VersionedFlowSnapshot flowSnapshot);

    /**
     * Create the entity that captures the status and result of an update request
     */
    protected abstract U createUpdateRequestEntity();

    /**
     * Perform additional logic to finalize an update request entity
     */
    protected abstract void finalizeCompletedUpdateRequest(U updateRequestEntity);

    /**
     * Initiate a flow update. Return a response containing an entity that reflects the status of the async request.
     *
     * This is used by both import-based flow updates and registry-based flow updates.
     *
     * @param groupId                      the id of the process group to update
     * @param requestEntity                the entity containing the request, either versioning info or the flow contents
     * @param allowDirtyFlowUpdate         allow updating a flow with versioned changes present
     * @param requestType                  the type of request ("replace-requests" or "update-requests")
     * @param replicateUriPath             the uri path to use for replicating the request (differs from initial request uri)
     * @param flowSnapshotSupplier         provides access to the flow snapshot to be used for replacement
     * @return response containing status of the async request
     */
    protected Response initiateFlowUpdate(final String groupId, final T requestEntity, final boolean allowDirtyFlowUpdate,
                                          final String requestType, final String replicateUriPath,
                                          final Supplier<VersionedFlowSnapshot> flowSnapshotSupplier) {
        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // We will perform the updating of the flow in a background thread because it can be a long-running process.
        // In order to do this, we will need some parameters that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these parameters up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Workflow for this process:
        // 0. Obtain the versioned flow snapshot to use for the update
        //    a. Retrieve flow snapshot from request entity (import) or from registry (version change)
        // 1. Determine which components would be affected (and are enabled/running)
        //    a. Component itself is modified in some way, other than position changing.
        //    b. Source and Destination of any Connection that is modified.
        //    c. Any Processor or Controller Service that references a Controller Service that is modified.
        // 2. Verify READ and WRITE permissions for user, for every component.
        // 3. Verify that all components in the snapshot exist on all nodes (i.e., the NAR exists)?
        // 4: Verify that Process Group can be updated. Only versioned flows care about the verifyNotDirty flag.
        // 5. Stop all Processors, Funnels, Ports that are affected.
        // 6. Wait for all of the components to finish stopping.
        // 7. Disable all Controller Services that are affected.
        // 8. Wait for all Controller Services to finish disabling.
        // 9. Ensure that if any connection was deleted, that it has no data in it. Ensure that no Input Port
        //    was removed, unless it currently has no incoming connections. Ensure that no Output Port was removed,
        //    unless it currently has no outgoing connections. Checking ports & connections could be done before
        //    stopping everything, but removal of Connections cannot.
        // 10. Update variable registry to include new variables
        //    (only new variables so don't have to worry about affected components? Or do we need to in case a processor
        //    is already referencing the variable? In which case we need to include the affected components above in the
        //    set of affected components before stopping/disabling.).
        // 11. Update components in the Process Group; update Version Control Information (registry version change only).
        // 12. Re-Enable all affected Controller Services that were not removed.
        // 13. Re-Start all Processors, Funnels, Ports that are affected and not removed.

        // Step 0: Obtain the versioned flow snapshot to use for the update
        final VersionedFlowSnapshot flowSnapshot = flowSnapshotSupplier.get();

        // The new flow may not contain the same versions of components in existing flow. As a result, we need to update
        // the flow snapshot to contain compatible bundles.
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());

        // If there are any Controller Services referenced that are inherited from the parent group, resolve those to point to the appropriate Controller Service, if we are able to.
        serviceFacade.resolveInheritedControllerServices(flowSnapshot, groupId, user);

        // Step 1: Determine which components will be affected by updating the flow
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByFlowUpdate(groupId, flowSnapshot);

        // build a request wrapper
        final InitiateUpdateFlowRequestWrapper requestWrapper =
                new InitiateUpdateFlowRequestWrapper(requestEntity, componentLifecycle, requestType, getAbsolutePath(), replicateUriPath,
                                                     affectedComponents, replicateRequest, flowSnapshot);

        final Revision requestRevision = getRevision(revisionDto, groupId);
        return withWriteLock(
                serviceFacade,
                requestWrapper,
                requestRevision,
                lookup -> authorizeFlowUpdate(lookup, user, groupId, flowSnapshot),
                () -> {
                    // Step 3: Verify that all components in the snapshot exist on all nodes
                    // Step 4: Verify that Process Group can be updated. Only versioned flows care about the verifyNotDirty flag
                    serviceFacade.verifyCanUpdate(groupId, flowSnapshot, false, !allowDirtyFlowUpdate);
                },
                (revision, wrapper) -> submitFlowUpdateRequest(user, groupId, revision, wrapper, allowDirtyFlowUpdate)
        );
    }

    /**
     * Authorize read/write permissions for the given user on every component of the given flow in support of flow update.
     *
     * @param lookup        A lookup instance to use for retrieving components for authorization purposes
     * @param user          the user to authorize
     * @param groupId       the id of the process group being evaluated
     * @param flowSnapshot  the new flow contents to examine for restricted components
     */
    protected void authorizeFlowUpdate(final AuthorizableLookup lookup, final NiFiUser user, final String groupId,
                                       final VersionedFlowSnapshot flowSnapshot) {
        // Step 2: Verify READ and WRITE permissions for user, for every component.
        final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
        authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true,
                false, true, true, true);
        authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.WRITE, true,
                false, true, true, false);

        final VersionedProcessGroup groupContents = flowSnapshot.getFlowContents();
        final Set<ConfigurableComponent> restrictedComponents = FlowRegistryUtils.getRestrictedComponents(groupContents, serviceFacade);
        restrictedComponents.forEach(restrictedComponent -> {
            final ComponentAuthorizable restrictedComponentAuthorizable = lookup.getConfigurableComponent(restrictedComponent);
            authorizeRestrictions(authorizer, restrictedComponentAuthorizable);
        });

        final Map<String, VersionedParameterContext> parameterContexts = flowSnapshot.getParameterContexts();
        if (parameterContexts != null) {
            parameterContexts.values().forEach(
                    context -> AuthorizeParameterReference.authorizeParameterContextAddition(context, serviceFacade, authorizer, lookup, user)
            );
        }
    }

    /**
     * Create and submit the flow update request. Return response containing an entity reflecting the status of the async request.
     *
     * This is used by import-based flow replacements, registry-based flow updates and registry-based flow reverts
     *
     * @param user                        the user that submitted the update request
     * @param groupId                     the id of the process group to update
     * @param revision                    a revision object representing a unique request to update a specific process group
     * @param wrapper                     wrapper object containing many variables needed for performing the flow update
     * @param allowDirtyFlowUpdate        allow updating a flow with versioned changes present
     * @return response containing status of the update flow request
     */
    protected Response submitFlowUpdateRequest(final NiFiUser user, final String groupId, final Revision revision,
                                               final InitiateUpdateFlowRequestWrapper wrapper, final boolean allowDirtyFlowUpdate) {
        final String requestType = wrapper.getRequestType();
        final String idGenerationSeed = getIdGenerationSeed().orElse(null);

        // Steps 5+ occur asynchronously
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = UUID.randomUUID().toString();
        final AsynchronousWebRequest<T, T> request =
                new StandardAsynchronousWebRequest<>(requestId, wrapper.getRequestEntity(), groupId, user, getUpdateFlowSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<T, T>> updateTask =
                vcur -> {
                    try {
                        updateFlow(groupId, wrapper.getComponentLifecycle(), wrapper.getRequestUri(),
                                wrapper.getAffectedComponents(), wrapper.isReplicateRequest(), wrapper.getReplicateUriPath(),
                                revision, wrapper.getRequestEntity(), wrapper.getFlowSnapshot(), request,
                                idGenerationSeed, allowDirtyFlowUpdate);

                        // no need to store any result of above flow update because it's not used
                        vcur.markStepComplete();
                    } catch (final ResumeFlowException rfe) {
                        // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                        // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                        logger.warn(rfe.getMessage(), rfe);
                        vcur.fail(rfe.getMessage());
                    } catch (final Exception e) {
                        logger.error("Failed to perform update flow request ", e);
                        vcur.fail("Failed to perform update flow request due to " + e.getMessage());
                    }
                };

        requestManager.submitRequest(requestType, requestId, request, updateTask);

        return createUpdateRequestResponse(requestType, requestId, request, false);
    }

    /**
     * Perform the specified flow update
     */
    private void updateFlow(final String groupId, final ComponentLifecycle componentLifecycle, final URI requestUri,
                            final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                            final String replicateUriPath, final Revision revision, final T requestEntity,
                            final VersionedFlowSnapshot flowSnapshot, final AsynchronousWebRequest<T, T> asyncRequest,
                            final String idGenerationSeed, final boolean allowDirtyFlowUpdate)
            throws LifecycleManagementException, ResumeFlowException {

        // Steps 5-6: Determine which components must be stopped and stop them.
        final Set<String> stoppableReferenceTypes = new HashSet<>();
        stoppableReferenceTypes.add(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        stoppableReferenceTypes.add(AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT);
        stoppableReferenceTypes.add(AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT);
        stoppableReferenceTypes.add(AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT);
        stoppableReferenceTypes.add(AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT);

        final Set<AffectedComponentEntity> runningComponents = affectedComponents.stream()
                .filter(dto -> stoppableReferenceTypes.contains(dto.getComponent().getReferenceType()))
                .filter(dto -> "Running".equalsIgnoreCase(dto.getComponent().getState()))
                .collect(Collectors.toSet());

        logger.info("Stopping {} Processors", runningComponents.size());
        final CancellableTimedPause stopComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(stopComponentsPause::cancel);
        componentLifecycle.scheduleComponents(requestUri, groupId, runningComponents, ScheduledState.STOPPED, stopComponentsPause, InvalidComponentAction.SKIP);

        if (asyncRequest.isCancelled()) {
            return;
        }
        asyncRequest.markStepComplete();

        // Steps 7-8. Disable enabled controller services that are affected
        final Set<AffectedComponentEntity> enabledServices = affectedComponents.stream()
                .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
                .filter(dto -> "Enabled".equalsIgnoreCase(dto.getComponent().getState()))
                .collect(Collectors.toSet());

        logger.info("Disabling {} Controller Services", enabledServices.size());
        final CancellableTimedPause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(disableServicesPause::cancel);
        componentLifecycle.activateControllerServices(requestUri, groupId, enabledServices, ControllerServiceState.DISABLED, disableServicesPause, InvalidComponentAction.SKIP);

        if (asyncRequest.isCancelled()) {
            return;
        }
        asyncRequest.markStepComplete();

        try {
            if (replicateRequest) {
                // If replicating request, steps 9-11 are performed on each node individually
                final NiFiUser user = NiFiUserUtils.getNiFiUser();

                URI replicateUri = null;
                try {
                    replicateUri = new URI(requestUri.getScheme(), requestUri.getUserInfo(), requestUri.getHost(), requestUri.getPort(),
                            replicateUriPath, null, requestUri.getFragment());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }

                final Map<String, String> headers = new HashMap<>();
                headers.put("content-type", MediaType.APPLICATION_JSON);

                // each concrete class creates its own type of entity for replication
                final Entity replicateEntity = createReplicateUpdateFlowEntity(revision, requestEntity, flowSnapshot);

                final NodeResponse clusterResponse;
                try {
                    logger.debug("Replicating PUT request to {} for user {}", replicateUri, user);

                    if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                        clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, replicateUri, replicateEntity, headers).awaitMergedResponse();
                    } else {
                        clusterResponse = getRequestReplicator().forwardToCoordinator(
                                getClusterCoordinatorNode(), user, HttpMethod.PUT, replicateUri, replicateEntity, headers).awaitMergedResponse();
                    }
                } catch (final InterruptedException ie) {
                    logger.warn("Interrupted while replicating PUT request to {} for user {}", replicateUri, user);
                    Thread.currentThread().interrupt();
                    throw new LifecycleManagementException("Interrupted while updating flows across cluster", ie);
                }

                final int updateFlowStatus = clusterResponse.getStatus();
                if (updateFlowStatus != Status.OK.getStatusCode()) {
                    final String explanation = getResponseEntity(clusterResponse, String.class);
                    logger.error("Failed to update flow across cluster when replicating PUT request to {} for user {}. Received {} response with explanation: {}",
                            replicateUri, user, updateFlowStatus, explanation);
                    throw new LifecycleManagementException("Failed to update Flow on all nodes in cluster due to " + explanation);
                }
            } else {
                // Step 9: Ensure that if any connection exists in the flow and does not exist in the proposed snapshot,
                // that it has no data in it. Ensure that no Input Port was removed, unless it currently has no incoming connections.
                // Ensure that no Output Port was removed, unless it currently has no outgoing connections.
                serviceFacade.verifyCanUpdate(groupId, flowSnapshot, true, !allowDirtyFlowUpdate);

                // Step 10-11. Update Process Group to the new flow and update variable registry with any Variables that were added or removed.
                // Each concrete class defines its own update flow functionality
                performUpdateFlow(groupId, revision, requestEntity, flowSnapshot, idGenerationSeed, !allowDirtyFlowUpdate, true);
            }
        } finally {
            if (!asyncRequest.isCancelled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Re-Enabling {} Controller Services: {}", enabledServices.size(), enabledServices);
                }

                asyncRequest.markStepComplete();

                // Step 12. Re-enable all disabled controller services
                final CancellableTimedPause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                asyncRequest.setCancelCallback(enableServicesPause::cancel);
                final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(enabledServices);
                logger.info("Successfully updated flow; re-enabling {} Controller Services", servicesToEnable.size());

                try {
                    componentLifecycle.activateControllerServices(requestUri, groupId, servicesToEnable, ControllerServiceState.ENABLED, enableServicesPause, InvalidComponentAction.SKIP);
                } catch (final IllegalStateException ise) {
                    // Component Lifecycle will re-enable the Controller Services only if they are valid. If IllegalStateException gets thrown, we need to provide
                    // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
                    throw new ResumeFlowException("Successfully updated flow but could not re-enable all Controller Services because " + ise.getMessage(), ise);
                }
            }

            if (!asyncRequest.isCancelled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Restart {} Processors: {}", runningComponents.size(), runningComponents);
                }

                asyncRequest.markStepComplete();

                // Step 13. Restart all components
                final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(runningComponents);

                // If there are any Remote Group Ports that are supposed to be started and have no connections, we want to remove those from our Set.
                // This will happen if the Remote Group Port is transmitting when the flow change happens but the new flow does not have a connection
                // to the port. In such a case, the Port still is included in the Updated Entities because we do not remove them when updating the flow
                // (they are removed in the background).
                final Set<AffectedComponentEntity> avoidStarting = new HashSet<>();
                for (final AffectedComponentEntity componentEntity : componentsToStart) {
                    final AffectedComponentDTO componentDto = componentEntity.getComponent();
                    final String referenceType = componentDto.getReferenceType();
                    if (!AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT.equals(referenceType)
                            && !AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT.equals(referenceType)) {
                        continue;
                    }

                    boolean startComponent;
                    try {
                        startComponent = serviceFacade.isRemoteGroupPortConnected(componentDto.getProcessGroupId(), componentDto.getId());
                    } catch (final ResourceNotFoundException rnfe) {
                        // Could occur if RPG is refreshed at just the right time.
                        startComponent = false;
                    }

                    // We must add the components to avoid starting to a separate Set and then remove them below,
                    // rather than removing the component here, because doing so would result in a ConcurrentModificationException.
                    if (!startComponent) {
                        avoidStarting.add(componentEntity);
                    }
                }
                componentsToStart.removeAll(avoidStarting);

                final CancellableTimedPause startComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                asyncRequest.setCancelCallback(startComponentsPause::cancel);
                logger.info("Restarting {} Processors", componentsToStart.size());

                try {
                    componentLifecycle.scheduleComponents(requestUri, groupId, componentsToStart, ScheduledState.RUNNING, startComponentsPause, InvalidComponentAction.SKIP);
                } catch (final IllegalStateException ise) {
                    // Component Lifecycle will restart the Processors only if they are valid. If IllegalStateException gets thrown, we need to provide
                    // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
                    throw new ResumeFlowException("Successfully updated flow but could not restart all Processors because " + ise.getMessage(), ise);
                }
            }
        }

        asyncRequest.setCancelCallback(null);
    }

    /**
     * Get a list of steps to perform for upload flow
     */
    private static List<UpdateStep> getUpdateFlowSteps() {
        final List<UpdateStep> updateSteps = new ArrayList<>();
        updateSteps.add(new StandardUpdateStep("Stopping Affected Processors"));
        updateSteps.add(new StandardUpdateStep("Disabling Affected Controller Services"));
        updateSteps.add(new StandardUpdateStep("Updating Flow"));
        updateSteps.add(new StandardUpdateStep("Re-Enabling Controller Services"));
        updateSteps.add(new StandardUpdateStep("Restarting Affected Processors"));
        return updateSteps;
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
    protected <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            entity = nodeResponse.getClientResponse().readEntity(clazz);
        }
        return entity;
    }

    protected Set<AffectedComponentEntity> getUpdatedEntities(final Set<AffectedComponentEntity> originalEntities) {
        final Set<AffectedComponentEntity> entities = new LinkedHashSet<>();

        for (final AffectedComponentEntity original : originalEntities) {
            try {
                final AffectedComponentEntity updatedEntity = AffectedComponentUtils.updateEntity(original, serviceFacade, dtoFactory);
                if (updatedEntity != null) {
                    entities.add(updatedEntity);
                }
            } catch (final ResourceNotFoundException rnfe) {
                // Component was removed. Just continue on without adding anything to the entities.
                // We do this because the intent is to get updated versions of the entities with current
                // Revisions so that we can change the states of the components. If the component was removed,
                // then we can just drop the entity, since there is no need to change its state.
            }
        }

        return entities;
    }

    /**
     * Process a request to retrieve an existing flow update request.
     *
     * This is used by import-based flow replacements, registry-based flow updates and registry-based flow reverts
     *
     * @param requestType                  the type of request ("replace-requests", "update-requests" or "revert-requests")
     * @param requestId                    the unique identifier for the update request
     * @return response containing the requested flow update request
     */
    protected Response retrieveFlowUpdateRequest(final String requestType, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<T, T> asyncRequest = requestManager.getRequest(requestType, requestId, user);

        return createUpdateRequestResponse(requestType, requestId, asyncRequest, true);
    }

    /**
     * Process a request to cancel/delete an existing flow update request.
     *
     * This is used by import-based flow replacements, registry-based flow updates and registry-based flow reverts
     *
     * @param requestType                  the type of request ("replace-requests", "update-requests" or "revert-requests")
     * @param requestId                    the unique identifier for the update request
     * @param disconnectedNodeAcknowledged acknowledges that this node is disconnected to allow for mutable requests to proceed
     * @return response containing the deleted flow update request
     */
    protected Response deleteFlowUpdateRequest(final String requestType, final String requestId,
                                               final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<T, T> asyncRequest = requestManager.removeRequest(requestType, requestId, user);

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        return createUpdateRequestResponse(requestType, requestId, asyncRequest, true);
    }

    /**
     * Create response containing entity that reflects the status of the update request
     *
     * @param requestType               the type of request ("replace-requests", "update-requests" or "revert-requests")
     * @param requestId                 the unique identifier for the update request
     * @param asyncRequest              async request object
     * @param finalizeCompletedRequest  if true, perform additional custom operations to finalize the update request
     * @return response containing entity that reflects the status of the update request
     */
    protected Response createUpdateRequestResponse(final String requestType, final String requestId,
                                                   final AsynchronousWebRequest<T,T> asyncRequest,
                                                   final boolean finalizeCompletedRequest) {
        final String groupId = asyncRequest.getComponentId();

        final U updateRequestEntity = createUpdateRequestEntity();
        final RevisionDTO groupRevision = serviceFacade.getProcessGroup(groupId).getRevision();
        updateRequestEntity.setProcessGroupRevision(groupRevision);

        final FlowUpdateRequestDTO updateRequestDto = updateRequestEntity.getRequest();
        updateRequestDto.setComplete(asyncRequest.isComplete());
        updateRequestDto.setFailureReason(asyncRequest.getFailureReason());
        updateRequestDto.setLastUpdated(asyncRequest.getLastUpdated());
        updateRequestDto.setProcessGroupId(groupId);
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri(getRequestPathFirstSegment(), requestType, requestId));
        updateRequestDto.setPercentCompleted(asyncRequest.getPercentComplete());
        updateRequestDto.setState(asyncRequest.getState());

        if (finalizeCompletedRequest) {
            // perform additional custom operations to finalize the update request
            finalizeCompletedUpdateRequest(updateRequestEntity);
        }

        return generateOkResponse(updateRequestEntity).build();
    }

    /**
     * Access the current request URI's first path segment (i.e., "versions" or "process-groups").
     *
     * This avoids having to hardcode the value as an argument to an update flow request.
     */
    protected String getRequestPathFirstSegment() {
        return uriInfo.getPathSegments().get(0).getPath();
    }

    protected class InitiateUpdateFlowRequestWrapper extends Entity {
        private final T requestEntity;
        private final ComponentLifecycle componentLifecycle;
        private final String requestType;
        private final URI requestUri;
        private final String replicateUriPath;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final VersionedFlowSnapshot flowSnapshot;

        public InitiateUpdateFlowRequestWrapper(final T requestEntity, final ComponentLifecycle componentLifecycle,
                                                final String requestType, final URI requestUri, final String replicateUriPath,
                                                final Set<AffectedComponentEntity> affectedComponents,
                                                final boolean replicateRequest, final VersionedFlowSnapshot flowSnapshot) {
            this.requestEntity = requestEntity;
            this.componentLifecycle = componentLifecycle;
            this.requestType = requestType;
            this.requestUri = requestUri;
            this.replicateUriPath = replicateUriPath;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.flowSnapshot = flowSnapshot;
        }

        public T getRequestEntity() {
            return requestEntity;
        }

        public ComponentLifecycle getComponentLifecycle() {
            return componentLifecycle;
        }

        public String getRequestType() {
            return requestType;
        }

        public URI getRequestUri() {
            return requestUri;
        }

        public String getReplicateUriPath() {
            return replicateUriPath;
        }

        public Set<AffectedComponentEntity> getAffectedComponents() {
            return affectedComponents;
        }

        public boolean isReplicateRequest() {
            return replicateRequest;
        }

        public VersionedFlowSnapshot getFlowSnapshot() {
            return flowSnapshot;
        }
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setDtoFactory(DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setClusterComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.clusterComponentLifecycle = componentLifecycle;
    }

    public void setLocalComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.localComponentLifecycle = componentLifecycle;
    }

}
