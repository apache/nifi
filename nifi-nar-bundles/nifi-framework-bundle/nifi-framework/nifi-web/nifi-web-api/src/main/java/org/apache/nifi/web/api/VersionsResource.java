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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.FlowRegistryUtils;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.dto.VersionedFlowUpdateRequestDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.CreateActiveRequestEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.AffectedComponentUtils;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Path("/versions")
@Api(value = "/versions", description = "Endpoint for managing version control for a flow")
public class VersionsResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(VersionsResource.class);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private ComponentLifecycle clusterComponentLifecycle;
    private ComponentLifecycle localComponentLifecycle;
    private DtoFactory dtoFactory;

    private RequestManager<VersionControlInformationEntity> requestManager = new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Version Control Update Thread");

    // We need to ensure that only a single Version Control Request can occur throughout the flow.
    // Otherwise, User 1 could log into Node 1 and choose to Version Control Group A.
    // At the same time, User 2 could log into Node 2 and choose to Version Control Group B, which is a child of Group A.
    // As a result, only one of the requests would succeed (the other would be rejected due to the Revision being wrong).
    // However, the request that was rejected may well have already caused the flow to be added to the Flow Registry,
    // so we would have created a flow in the Flow Registry that will never be referenced and will essentially be "orphaned."
    private ActiveRequest activeRequest = null;
    private final Object activeRequestMonitor = new Object();

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(value = "Gets the Version Control information for a process group",
        response = VersionControlInformationEntity.class,
        notes = NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getVersionInformation(@ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the version control information for this process group
        VersionControlInformationEntity entity = serviceFacade.getVersionControlInformation(groupId);
        if (entity == null) {
            final ProcessGroupEntity processGroup = serviceFacade.getProcessGroup(groupId);
            entity = new VersionControlInformationEntity();
            entity.setProcessGroupRevision(processGroup.getRevision());
        }

        return generateOkResponse(entity).build();
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("active-requests")
    @ApiOperation(
        value = "Create a version control request",
            response = String.class,
        notes = "Creates a request so that a Process Group can be placed under Version Control or have its Version Control configuration changed. Creating this request will "
            + "prevent any other threads from simultaneously saving local changes to Version Control. It will not, however, actually save the local flow to the Flow Registry. A "
            + "POST to /versions/process-groups/{id} should be used to initiate saving of the local flow to the Flow Registry. "
            + NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Write - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response createVersionControlRequest(
            @ApiParam(value = "The versioned flow details.", required = true) final CreateActiveRequestEntity requestEntity) {

        if (requestEntity.getProcessGroupId() == null) {
            throw new IllegalArgumentException("The id of the process group that will be updated must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
            serviceFacade,
            requestEntity,
            lookup -> {
                final Authorizable processGroup = lookup.getProcessGroup(requestEntity.getProcessGroupId()).getAuthorizable();
                processGroup.authorize(authorizer, RequestAction.WRITE, user);
            },
            /* verifier */ null,
            entity -> {
                final String requestId = generateUuid();

                // We need to ensure that only a single Version Control Request can occur throughout the flow.
                // Otherwise, User 1 could log into Node 1 and choose to Version Control Group A.
                // At the same time, User 2 could log into Node 2 and choose to Version Control Group B, which is a child of Group A.
                // As a result, may could end up in a situation where we are creating flows in the registry that are never referenced.
                synchronized (activeRequestMonitor) {
                    if (activeRequest == null || activeRequest.isExpired()) {
                        activeRequest = new ActiveRequest(requestId, user, entity.getProcessGroupId());
                    } else {
                        throw new IllegalStateException("A request is already underway to place a Process Group in this NiFi instance under Version Control. "
                            + "Only a single such request is allowed to occurred at a time. Please try the request again momentarily.");
                    }
                }

                return generateOkResponse(requestId).build();
            });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("active-requests/{id}")
    @ApiOperation(
            value = "Updates the request with the given ID",
            response = VersionControlInformationEntity.class,
            notes = NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can update it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateVersionControlRequest(@ApiParam("The request ID.") @PathParam("id") final String requestId,
        @ApiParam(value = "The version control component mapping.", required = true) final VersionControlComponentMappingEntity requestEntity) {

        // Verify request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO versionControlInfo = requestEntity.getVersionControlInformation();
        if (versionControlInfo == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied");
        }
        if (versionControlInfo.getGroupId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Process Group ID");
        }
        if (versionControlInfo.getBucketId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Bucket ID");
        }
        if (versionControlInfo.getFlowId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Flow ID");
        }
        if (versionControlInfo.getRegistryId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Registry ID");
        }
        if (versionControlInfo.getVersion() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Version");
        }

        final Map<String, String> mapping = requestEntity.getVersionControlComponentMapping();
        if (mapping == null) {
            throw new IllegalArgumentException("Version Control Component Mapping must be supplied");
        }

        // Replicate if necessary
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // Perform the update
        synchronized (activeRequestMonitor) {
            if (activeRequest == null) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (!requestId.equals(activeRequest.getRequestId())) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (activeRequest.isExpired()) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " has already expired");
            }

            if (activeRequest.isUpdatePerformed()) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " has already been performed");
            }

            final String groupId = requestEntity.getVersionControlInformation().getGroupId();

            if (!activeRequest.getProcessGroupId().equals(groupId)) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " was created for a different process group id");
            }

            final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
            return withWriteLock(
                serviceFacade,
                requestEntity,
                groupRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();
                    if (user == null) {
                        throw new AccessDeniedException("Unknown user.");
                    }

                    if (!user.equals(activeRequest.getUser())) {
                        throw new AccessDeniedException("Only the user that creates the Version Control Request can use it.");
                    }
                },
                null,
                (rev, mappingEntity) -> {
                    // set the version control information
                    final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                        mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());

                    // indicate that the active request has performed the update
                    activeRequest.updatePerformed();

                    return generateOkResponse(responseEntity).build();
                });
        }
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("active-requests/{id}")
    @ApiOperation(
            value = "Deletes the version control request with the given ID",
            notes = "Deletes the Version Control Request with the given ID. This will allow other threads to save flows to the Flow Registry. See also the documentation "
                + "for POSTing to /versions/active-requests for information regarding why this is done. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can remove it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteVersionControlRequest(
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam("The request ID.") @PathParam("id") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        synchronized (activeRequestMonitor) {
            if (activeRequest == null) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (!requestId.equals(activeRequest.getRequestId())) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            return withWriteLock(
                serviceFacade,
                null,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();
                    if (user == null) {
                        throw new AccessDeniedException("Unknown user.");
                    }

                    if (!user.equals(activeRequest.getUser())) {
                        throw new AccessDeniedException("Only the user that creates the Version Control Request can use it.");
                    }
                },
                null,
                requestEntity -> {
                    // clear the active request
                    activeRequest = null;

                    return generateOkResponse().build();
                });
        }
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(
            value = "Save the Process Group with the given ID",
            response = VersionControlInformationEntity.class,
            notes = "Begins version controlling the Process Group with the given ID or commits changes to the Versioned Flow, "
                + "depending on if the provided VersionControlInformation includes a flowId. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Read - /process-groups/{uuid}"),
                @Authorization(value = "Write - /process-groups/{uuid}"),
                @Authorization(value = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                @Authorization(value = "Read - any referenced Controller Services by any encapsulated components - /controller-services/{uuid}")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response saveToFlowRegistry(
        @ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The versioned flow details.", required = true) final StartVersionControlRequestEntity requestEntity) {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionedFlowDTO versionedFlowDto = requestEntity.getVersionedFlow();
        if (versionedFlowDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getBucketId())) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getFlowName()) && StringUtils.isEmpty(versionedFlowDto.getFlowId())) {
            throw new IllegalArgumentException("The Flow Name or Flow ID must be supplied.");
        }
        if (versionedFlowDto.getFlowName() != null && versionedFlowDto.getFlowName().length() > 1000) {
            throw new IllegalArgumentException("The Flow Name cannot exceed 1,000 characters");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getRegistryId())) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (versionedFlowDto.getDescription() != null && versionedFlowDto.getDescription().length() > 65535) {
            throw new IllegalArgumentException("Flow Description cannot exceed 65,535 characters");
        }
        if (versionedFlowDto.getComments() != null && versionedFlowDto.getComments().length() > 65535) {
            throw new IllegalArgumentException("Comments cannot exceed 65,535 characters");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // ensure we're not attempting to version the root group
        final ProcessGroupEntity root = serviceFacade.getProcessGroup(FlowManager.ROOT_GROUP_ID_ALIAS);
        if (root.getId().equals(groupId)) {
            throw new IllegalArgumentException("The Root Process Group cannot be versioned.");
        }

        if (isReplicateRequest()) {
            // We first have to obtain a "lock" on all nodes in the cluster so that multiple Version Control requests
            // are not being made simultaneously. We do this by making a POST to /nifi-api/versions/active-requests.
            // The Response gives us back the Request ID.
            final URI requestUri;
            try {
                final URI originalUri = getAbsolutePath();
                final String requestId = lockVersionControl(originalUri, groupId);

                requestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                    originalUri.getPort(), "/nifi-api/versions/active-requests/" + requestId, null, originalUri.getFragment());
            } catch (final URISyntaxException e) {
                throw new RuntimeException(e);
            }


            // Now that we have the Request, we know that no other thread is updating the Flow Registry. So we can now
            // create the Flow in the Flow Registry and push the Process Group as the first version of the Flow. Once we've
            // succeeded with that, we need to update all nodes' Process Group to contain the new Version Control Information.
            // Finally, we can delete the Request.
            try {
                final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, requestEntity);
                replicateVersionControlMapping(mappingEntity, requestEntity, requestUri, groupId);

                final VersionControlInformationEntity responseEntity = serviceFacade.getVersionControlInformation(groupId);
                return generateOkResponse(responseEntity).build();
            } finally {
                unlockVersionControl(requestUri, groupId);
            }
        }

        // Perform local task. If running in a cluster environment, we will never get to this point. This is because
        // in the above block, we check if (isReplicate()) and if true, we implement the 'cluster logic', but this
        // does not involve replicating the actual request, because we only want a single node to handle the logic of
        // creating the flow in the Registry.
        final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
        return withWriteLock(
            serviceFacade,
            requestEntity,
            groupRevision,
            lookup -> {
                final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                final Authorizable processGroup = groupAuthorizable.getAuthorizable();

                // require write to this group
                processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                // require read to this group and all descendants
                authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true, false, true, true);
            },
            () -> {
                final VersionedFlowDTO versionedFlow = requestEntity.getVersionedFlow();
                final String registryId = versionedFlow.getRegistryId();
                final String bucketId = versionedFlow.getBucketId();
                final String flowId = versionedFlow.getFlowId();
                serviceFacade.verifyCanSaveToFlowRegistry(groupId, registryId, bucketId, flowId);
            },
            (rev, flowEntity) -> {
                // Register the current flow with the Flow Registry.
                final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, flowEntity);

                // Update the Process Group's Version Control Information
                final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                    mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());
                return generateOkResponse(responseEntity).build();
            });
    }

    private void unlockVersionControl(final URI requestUri, final String groupId) {
        final NodeResponse clusterResponse;
        try {
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.DELETE, requestUri, new MultivaluedHashMap<>(), Collections.emptyMap()).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.DELETE, requestUri, new MultivaluedHashMap<>(), Collections.emptyMap()).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("After starting Version Control on Process Group with ID " + groupId + ", interrupted while waiting for deletion of Version Control Request. "
                + "Users may be unable to Version Control other Process Groups until the request lock times out.", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            logger.error("After starting Version Control on Process Group with ID " + groupId + ", failed to delete Version Control Request. "
                + "Users may be unable to Version Control other Process Groups until the request lock times out. Response status code was " + clusterResponse.getStatus());
        }
    }

    private String lockVersionControl(final URI originalUri, final String groupId) throws URISyntaxException {
        final URI createRequestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
            originalUri.getPort(), "/nifi-api/versions/active-requests", null, originalUri.getFragment());

        final NodeResponse clusterResponse;
        try {
            // create an active request entity to indicate the group id
            final CreateActiveRequestEntity activeRequestEntity = new CreateActiveRequestEntity();
            activeRequestEntity.setProcessGroupId(groupId);

            final Map<String, String> headers = new HashMap<>();
            headers.put("content-type", MediaType.APPLICATION_JSON);

            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.POST, createRequestUri, activeRequestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.POST, createRequestUri, activeRequestEntity, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating Version Control Information for Process Group with ID " + groupId + ".", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            final String errorResponse = getResponseEntity(clusterResponse, String.class);
            throw new IllegalStateException(
                "Failed to create a Version Control Request across all nodes in the cluster. Received response code " + clusterResponse.getStatus() + " with content: " + errorResponse);
        }

        final String requestId = getResponseEntity(clusterResponse, String.class);
        return requestId;
    }

    private void replicateVersionControlMapping(final VersionControlComponentMappingEntity mappingEntity, final StartVersionControlRequestEntity requestEntity,
                                                final URI requestUri, final String groupId) {

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        final NodeResponse clusterResponse;
        try {
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.PUT, requestUri, mappingEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.PUT, requestUri, mappingEntity, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();

            if (requestEntity.getVersionedFlow().getFlowId() == null) {
                // We had to create the flow for this snapshot. Since we failed to replicate the Version Control Info, remove the
                // flow from the Flow Registry (use best effort; if we can't remove it, just log and move on).
                final VersionControlInformationDTO vci = mappingEntity.getVersionControlInformation();
                try {
                    serviceFacade.deleteVersionedFlow(vci.getRegistryId(), vci.getBucketId(), vci.getFlowId());
                } catch (final Exception e) {
                    logger.error("Created Versioned Flow with ID {} in bucket with ID {} but failed to replicate the Version Control Information to cluster. "
                        + "Attempted to delete the newly created (empty) flow from the Flow Registry but failed", vci.getFlowId(), vci.getBucketId(), e);
                }
            }

            throw new RuntimeException("Interrupted while updating Version Control Information for Process Group with ID " + groupId + ".", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            if (requestEntity.getVersionedFlow().getFlowId() == null) {
                // We had to create the flow for this snapshot. Since we failed to replicate the Version Control Info, remove the
                // flow from the Flow Registry (use best effort; if we can't remove it, just log and move on).
                final VersionControlInformationDTO vci = mappingEntity.getVersionControlInformation();
                try {
                    serviceFacade.deleteVersionedFlow(vci.getRegistryId(), vci.getBucketId(), vci.getFlowId());
                } catch (final Exception e) {
                    logger.error("Created Versioned Flow with ID {} in bucket with ID {} but failed to replicate the Version Control Information to cluster. "
                        + "Attempted to delete the newly created (empty) flow from the Flow Registry but failed", vci.getFlowId(), vci.getBucketId(), e);
                }
            }

            final String message = "Failed to update Version Control Information for Process Group with ID " + groupId + ".";
            final Throwable cause = clusterResponse.getThrowable();
            if (cause == null) {
                throw new IllegalStateException(message);
            } else {
                throw new IllegalStateException(message, cause);
            }
        }
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(
            value = "Stops version controlling the Process Group with the given ID",
            response = VersionControlInformationEntity.class,
            notes = "Stops version controlling the Process Group with the given ID. The Process Group will no longer track to any Versioned Flow. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Read - /process-groups/{uuid}"),
                @Authorization(value = "Write - /process-groups/{uuid}"),
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response stopVersionControl(
        @ApiParam(
                value = "The version is used to verify the client is working with the latest version of the flow.",
                required = false)
        @QueryParam(VERSION) final LongParameter version,
        @ApiParam(
                value = "If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false)
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
        @ApiParam(
                value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                required = false
        )
        @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
        @ApiParam("The process group id.") @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), groupId);
        return withWriteLock(
            serviceFacade,
            null,
            requestRevision,
            lookup -> {
                final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                final VersionControlInformationEntity currentVersionControlInfo = serviceFacade.getVersionControlInformation(groupId);
                if (currentVersionControlInfo == null) {
                    throw new IllegalStateException("Process Group with ID " + groupId + " is not currently under Version Control");
                }
            },
            (revision, groupEntity) -> {
                // disconnect from version control
                final VersionControlInformationEntity entity = serviceFacade.deleteVersionControl(revision, groupId);

                // generate the response
                return generateOkResponse(entity).build();
            });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(
            value = "Update the version of a Process Group with the given ID",
            response = VersionControlInformationEntity.class,
            notes = "For a Process Group that is already under Version Control, this will update the version of the flow to a different version. This endpoint expects "
                + "that the given snapshot will not modify any Processor that is currently running or any Controller Service that is enabled. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Read - /process-groups/{uuid}"),
                @Authorization(value = "Write - /process-groups/{uuid}")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateFlowVersion(@ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionedFlowSnapshotEntity requestEntity) {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified.");
        }

        final VersionedFlowSnapshot requestFlowSnapshot = requestEntity.getVersionedFlowSnapshot();
        if (requestFlowSnapshot == null) {
            throw new IllegalArgumentException("Versioned Flow Snapshot must be supplied.");
        }

        final VersionedFlowSnapshotMetadata requestSnapshotMetadata = requestFlowSnapshot.getSnapshotMetadata();
        if (requestSnapshotMetadata == null) {
            throw new IllegalArgumentException("Snapshot Metadata must be supplied.");
        }
        if (requestSnapshotMetadata.getBucketIdentifier() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestSnapshotMetadata.getFlowIdentifier() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
            serviceFacade,
            requestEntity,
            requestRevision,
            lookup -> {
                final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                // We do not enforce that the Process Group is 'not dirty' because at this point,
                // the client has explicitly indicated the dataflow that the Process Group should
                // provide and provided the Revision to ensure that they have the most up-to-date
                // view of the Process Group.
                serviceFacade.verifyCanUpdate(groupId, requestFlowSnapshot, true, false);
            },
            (rev, entity) -> {
                final VersionedFlowSnapshot flowSnapshot = entity.getVersionedFlowSnapshot();
                final VersionedFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();

                final Bucket bucket = flowSnapshot.getBucket();
                final VersionedFlow flow = flowSnapshot.getFlow();

                // Update the Process Group to match the proposed flow snapshot
                final VersionControlInformationDTO versionControlInfoDto = new VersionControlInformationDTO();
                versionControlInfoDto.setBucketId(snapshotMetadata.getBucketIdentifier());
                versionControlInfoDto.setBucketName(bucket.getName());
                versionControlInfoDto.setFlowId(snapshotMetadata.getFlowIdentifier());
                versionControlInfoDto.setFlowName(flow.getName());
                versionControlInfoDto.setFlowDescription(flow.getDescription());
                versionControlInfoDto.setGroupId(groupId);
                versionControlInfoDto.setVersion(snapshotMetadata.getVersion());
                versionControlInfoDto.setRegistryId(entity.getRegistryId());
                versionControlInfoDto.setRegistryName(serviceFacade.getFlowRegistryName(entity.getRegistryId()));

                final VersionedFlowState flowState = snapshotMetadata.getVersion() == flow.getVersionCount() ? VersionedFlowState.UP_TO_DATE : VersionedFlowState.STALE;
                versionControlInfoDto.setState(flowState.name());

                final ProcessGroupEntity updatedGroup = serviceFacade.updateProcessGroupContents(rev, groupId, versionControlInfoDto, flowSnapshot, getIdGenerationSeed().orElse(null), false,
                    true, entity.getUpdateDescendantVersionedFlows());
                final VersionControlInformationDTO updatedVci = updatedGroup.getComponent().getVersionControlInformation();

                final VersionControlInformationEntity responseEntity = new VersionControlInformationEntity();
                responseEntity.setProcessGroupRevision(updatedGroup.getRevision());
                responseEntity.setVersionControlInformation(updatedVci);

                return generateOkResponse(responseEntity).build();
            });
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @ApiOperation(
            value = "Returns the Update Request with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "Returns the Update Request with the given ID. Once an Update Request has been created by performing a POST to /versions/update-requests/process-groups/{id}, "
                + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                + "current state of the request, and any failures. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can get it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getUpdateRequest(@ApiParam("The ID of the Update Request") @PathParam("id") final String updateRequestId) {
        return retrieveRequest("update-requests", updateRequestId);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/{id}")
    @ApiOperation(
            value = "Returns the Revert Request with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "Returns the Revert Request with the given ID. Once a Revert Request has been created by performing a POST to /versions/revert-requests/process-groups/{id}, "
                + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                + "current state of the request, and any failures. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can get it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getRevertRequest(@ApiParam("The ID of the Revert Request") @PathParam("id") final String revertRequestId) {
        return retrieveRequest("revert-requests", revertRequestId);
    }

    private Response retrieveRequest(final String requestType, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VersionControlInformationEntity> asyncRequest = requestManager.getRequest(requestType, requestId, user);

        final VersionedFlowUpdateRequestDTO updateRequestDto = new VersionedFlowUpdateRequestDTO();
        updateRequestDto.setComplete(asyncRequest.isComplete());
        updateRequestDto.setFailureReason(asyncRequest.getFailureReason());
        updateRequestDto.setLastUpdated(asyncRequest.getLastUpdated());
        updateRequestDto.setProcessGroupId(asyncRequest.getProcessGroupId());
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri("versions", requestType, requestId));
        updateRequestDto.setState(asyncRequest.getState());
        updateRequestDto.setPercentCompleted(asyncRequest.getPercentComplete());

        if (updateRequestDto.isComplete()) {
            final VersionControlInformationEntity vciEntity = serviceFacade.getVersionControlInformation(asyncRequest.getProcessGroupId());
            updateRequestDto.setVersionControlInformation(vciEntity == null ? null : vciEntity.getVersionControlInformation());
        }

        final RevisionDTO groupRevision = serviceFacade.getProcessGroup(asyncRequest.getProcessGroupId()).getRevision();

        final VersionedFlowUpdateRequestEntity updateRequestEntity = new VersionedFlowUpdateRequestEntity();
        updateRequestEntity.setProcessGroupRevision(groupRevision);
        updateRequestEntity.setRequest(updateRequestDto);

        return generateOkResponse(updateRequestEntity).build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @ApiOperation(
            value = "Deletes the Update Request with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "Deletes the Update Request with the given ID. After a request is created via a POST to /versions/update-requests/process-groups/{id}, it is expected "
                + "that the client will properly clean up the request by DELETE'ing it, once the Update process has completed. If the request is deleted before the request "
                + "completes, then the Update request will finish the step that it is currently performing and then will cancel any subsequent steps. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can remove it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteUpdateRequest(
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam("The ID of the Update Request") @PathParam("id") final String updateRequestId) {

        return deleteRequest("update-requests", updateRequestId, disconnectedNodeAcknowledged.booleanValue());
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/{id}")
    @ApiOperation(
            value = "Deletes the Revert Request with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "Deletes the Revert Request with the given ID. After a request is created via a POST to /versions/revert-requests/process-groups/{id}, it is expected "
                + "that the client will properly clean up the request by DELETE'ing it, once the Revert process has completed. If the request is deleted before the request "
                + "completes, then the Revert request will finish the step that it is currently performing and then will cancel any subsequent steps. "
                + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Only the user that submitted the request can remove it")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteRevertRequest(
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam("The ID of the Revert Request") @PathParam("id") final String revertRequestId) {

        return deleteRequest("revert-requests", revertRequestId, disconnectedNodeAcknowledged.booleanValue());
    }


    private Response deleteRequest(final String requestType, final String requestId, final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VersionControlInformationEntity> asyncRequest = requestManager.removeRequest(requestType, requestId, user);
        if (asyncRequest == null) {
            throw new ResourceNotFoundException("Could not find request of type " + requestType + " with ID " + requestId);
        }

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        final VersionedFlowUpdateRequestDTO updateRequestDto = new VersionedFlowUpdateRequestDTO();
        updateRequestDto.setComplete(asyncRequest.isComplete());
        updateRequestDto.setFailureReason(asyncRequest.getFailureReason());
        updateRequestDto.setLastUpdated(asyncRequest.getLastUpdated());
        updateRequestDto.setProcessGroupId(asyncRequest.getProcessGroupId());
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri("versions", requestType, requestId));
        updateRequestDto.setPercentCompleted(asyncRequest.getPercentComplete());
        updateRequestDto.setState(asyncRequest.getState());

        if (updateRequestDto.isComplete()) {
            final VersionControlInformationEntity vciEntity = serviceFacade.getVersionControlInformation(asyncRequest.getProcessGroupId());
            updateRequestDto.setVersionControlInformation(vciEntity == null ? null : vciEntity.getVersionControlInformation());
        }

        final RevisionDTO groupRevision = serviceFacade.getProcessGroup(asyncRequest.getProcessGroupId()).getRevision();

        final VersionedFlowUpdateRequestEntity updateRequestEntity = new VersionedFlowUpdateRequestEntity();
        updateRequestEntity.setProcessGroupRevision(groupRevision);
        updateRequestEntity.setRequest(updateRequestDto);

        return generateOkResponse(updateRequestEntity).build();
    }



    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/process-groups/{id}")
    @ApiOperation(
            value = "Initiate the Update Request of a Process Group with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "For a Process Group that is already under Version Control, this will initiate the action of changing "
                + "from a specific version of the flow in the Flow Registry to a different version of the flow. This can be a lengthy "
                + "process, as it will stop any Processors and disable any Controller Services necessary to perform the action and then restart them. As a result, "
                + "the endpoint will immediately return a VersionedFlowUpdateRequestEntity, and the process of updating the flow will occur "
                + "asynchronously in the background. The client may then periodically poll the status of the request by issuing a GET request to "
                + "/versions/update-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to "
                + "/versions/update-requests/{requestId}. " + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Read - /process-groups/{uuid}"),
                @Authorization(value = "Write - /process-groups/{uuid}"),
                @Authorization(value = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                @Authorization(value = "Write - /{component-type}/{uuid} - For all encapsulated components"),
                @Authorization(value = "Write - if the template contains any restricted components - /restricted-components")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response initiateVersionControlUpdate(
        @ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionControlInformationEntity requestEntity) {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO requestVersionControlInfoDto = requestEntity.getVersionControlInformation();
        if (requestVersionControlInfoDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (requestVersionControlInfoDto.getGroupId() == null) {
            throw new IllegalArgumentException("The Process Group ID must be supplied.");
        }
        if (!requestVersionControlInfoDto.getGroupId().equals(groupId)) {
            throw new IllegalArgumentException("The Process Group ID in the request body does not match the Process Group ID of the requested resource.");
        }
        if (requestVersionControlInfoDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getFlowId() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getVersion() == null) {
            throw new IllegalArgumentException("The Version of the flow must be supplied.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // We will perform the updating of the Versioned Flow in a background thread because it can be a long-running process.
        // In order to do this, we will need some parameters that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these parameters up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();


        // Workflow for this process:
        // 0. Obtain the versioned flow snapshot to use for the update
        //    a. Contact registry to download the desired version.
        //    b. Get Variable Registry of this Process Group and all ancestor groups
        //    c. Perform diff to find any new variables
        //    d. Get Variable Registry of any child Process Group in the versioned flow
        //    e. Perform diff to find any new variables
        //    f. Prompt user to fill in values for all new variables
        // 1. Determine which components would be affected (and are enabled/running)
        //    a. Component itself is modified in some way, other than position changing.
        //    b. Source and Destination of any Connection that is modified.
        //    c. Any Processor or Controller Service that references a Controller Service that is modified.
        // 2. Verify READ and WRITE permissions for user, for every component.
        // 3. Verify that all components in the snapshot exist on all nodes (i.e., the NAR exists)?
        // 4. Verify that Process Group is already under version control. If not, must start Version Control instead of updateFlow
        // 5. Verify that Process Group is not 'dirty'.
        // 6. Stop all Processors, Funnels, Ports that are affected.
        // 7. Wait for all of the components to finish stopping.
        // 8. Disable all Controller Services that are affected.
        // 9. Wait for all Controller Services to finish disabling.
        // 10. Ensure that if any connection was deleted, that it has no data in it. Ensure that no Input Port
        //    was removed, unless it currently has no incoming connections. Ensure that no Output Port was removed,
        //    unless it currently has no outgoing connections. Checking ports & connections could be done before
        //    stopping everything, but removal of Connections cannot.
        // 11. Update variable registry to include new variables
        //    (only new variables so don't have to worry about affected components? Or do we need to in case a processor
        //    is already referencing the variable? In which case we need to include the affected components above in the
        //    Set of affected components before stopping/disabling.).
        // 12. Update components in the Process Group; update Version Control Information.
        // 13. Re-Enable all affected Controller Services that were not removed.
        // 14. Re-Start all Processors, Funnels, Ports that are affected and not removed.

        // Step 0: Get the Versioned Flow Snapshot from the Flow Registry
        final VersionedFlowSnapshot flowSnapshot = serviceFacade.getVersionedFlowSnapshot(requestEntity.getVersionControlInformation(), true);

        // The flow in the registry may not contain the same versions of components that we have in our flow. As a result, we need to update
        // the flow snapshot to contain compatible bundles.
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());

        // Step 1: Determine which components will be affected by updating the version
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByVersionChange(groupId, flowSnapshot);

        // build a request wrapper
        final InitiateChangeFlowVersionRequestWrapper requestWrapper = new InitiateChangeFlowVersionRequestWrapper(requestEntity, componentLifecycle, getAbsolutePath(), affectedComponents,
                replicateRequest, flowSnapshot);

        final Revision requestRevision = getRevision(requestEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
            serviceFacade,
            requestWrapper,
            requestRevision,
            lookup -> {
                // Step 2: Verify READ and WRITE permissions for user, for every component.
                final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true, false, true, true);
                authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.WRITE, true, false, true, true);

                final VersionedProcessGroup groupContents = flowSnapshot.getFlowContents();
                final Set<ConfigurableComponent> restrictedComponents = FlowRegistryUtils.getRestrictedComponents(groupContents, serviceFacade);
                restrictedComponents.forEach(restrictedComponent -> {
                    final ComponentAuthorizable restrictedComponentAuthorizable = lookup.getConfigurableComponent(restrictedComponent);
                    authorizeRestrictions(authorizer, restrictedComponentAuthorizable);
                });
            },
            () -> {
                // Step 3: Verify that all components in the snapshot exist on all nodes
                // Step 4: Verify that Process Group is already under version control. If not, must start Version Control instead of updating flow
                // Step 5: Verify that Process Group is not 'dirty'
                serviceFacade.verifyCanUpdate(groupId, flowSnapshot, false, true);
            },
            (revision, wrapper) -> {
                final String idGenerationSeed = getIdGenerationSeed().orElse(null);

                // Create an asynchronous request that will occur in the background, because this request may
                // result in stopping components, which can take an indeterminate amount of time.
                final String requestId = UUID.randomUUID().toString();
                final AsynchronousWebRequest<VersionControlInformationEntity> request = new StandardAsynchronousWebRequest<>(requestId, groupId, user, "Stopping Affected Processors");

                // Submit the request to be performed in the background
                final Consumer<AsynchronousWebRequest<VersionControlInformationEntity>> updateTask = vcur -> {
                    try {
                        final VersionControlInformationEntity updatedVersionControlEntity = updateFlowVersion(groupId, wrapper.getComponentLifecycle(), wrapper.getExampleUri(),
                            wrapper.getAffectedComponents(), wrapper.isReplicateRequest(), revision, wrapper.getVersionControlInformationEntity(), wrapper.getFlowSnapshot(), request,
                            idGenerationSeed, true, true);

                        vcur.markComplete(updatedVersionControlEntity);
                    } catch (final ResumeFlowException rfe) {
                        // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                        // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                        logger.error(rfe.getMessage(), rfe);
                        vcur.setFailureReason(rfe.getMessage());
                    } catch (final Exception e) {
                        logger.error("Failed to update flow to new version", e);
                        vcur.setFailureReason("Failed to update flow to new version due to " + e);
                    }
                };

                requestManager.submitRequest("update-requests", requestId, request, updateTask);

                // Generate the response.
                final VersionedFlowUpdateRequestDTO updateRequestDto = new VersionedFlowUpdateRequestDTO();
                updateRequestDto.setComplete(request.isComplete());
                updateRequestDto.setFailureReason(request.getFailureReason());
                updateRequestDto.setLastUpdated(request.getLastUpdated());
                updateRequestDto.setProcessGroupId(groupId);
                updateRequestDto.setRequestId(requestId);
                updateRequestDto.setUri(generateResourceUri("versions", "update-requests", requestId));
                updateRequestDto.setPercentCompleted(request.getPercentComplete());
                updateRequestDto.setState(request.getState());

                final VersionedFlowUpdateRequestEntity updateRequestEntity = new VersionedFlowUpdateRequestEntity();
                final RevisionDTO groupRevision = serviceFacade.getProcessGroup(groupId).getRevision();
                updateRequestEntity.setProcessGroupRevision(groupRevision);
                updateRequestEntity.setRequest(updateRequestDto);

                return generateOkResponse(updateRequestEntity).build();
            });
    }



    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/process-groups/{id}")
    @ApiOperation(
            value = "Initiate the Revert Request of a Process Group with the given ID",
            response = VersionedFlowUpdateRequestEntity.class,
            notes = "For a Process Group that is already under Version Control, this will initiate the action of reverting "
                + "any local changes that have been made to the Process Group since it was last synchronized with the Flow Registry. This will result in the "
                + "flow matching the Versioned Flow that exists in the Flow Registry. This can be a lengthy "
                + "process, as it will stop any Processors and disable any Controller Services necessary to perform the action and then restart them. As a result, "
                + "the endpoint will immediately return a VersionedFlowUpdateRequestEntity, and the process of updating the flow will occur "
                + "asynchronously in the background. The client may then periodically poll the status of the request by issuing a GET request to "
                + "/versions/revert-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to "
                + "/versions/revert-requests/{requestId}. " + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                @Authorization(value = "Read - /process-groups/{uuid}"),
                @Authorization(value = "Write - /process-groups/{uuid}"),
                @Authorization(value = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                @Authorization(value = "Write - /{component-type}/{uuid} - For all encapsulated components"),
                @Authorization(value = "Write - if the template contains any restricted components - /restricted-components")
            })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response initiateRevertFlowVersion(@ApiParam("The process group id.") @PathParam("id") final String groupId,
        @ApiParam(value = "The controller service configuration details.", required = true) final VersionControlInformationEntity requestEntity) throws IOException {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO requestVersionControlInfoDto = requestEntity.getVersionControlInformation();
        if (requestVersionControlInfoDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (requestVersionControlInfoDto.getGroupId() == null) {
            throw new IllegalArgumentException("The Process Group ID must be supplied.");
        }
        if (!requestVersionControlInfoDto.getGroupId().equals(groupId)) {
            throw new IllegalArgumentException("The Process Group ID in the request body does not match the Process Group ID of the requested resource.");
        }
        if (requestVersionControlInfoDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getFlowId() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getVersion() == null) {
            throw new IllegalArgumentException("The Version of the flow must be supplied.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // We will perform the updating of the Versioned Flow in a background thread because it can be a long-running process.
        // In order to do this, we will need some parameters that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these parameters up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Step 0: Get the Versioned Flow Snapshot from the Flow Registry
        final VersionedFlowSnapshot flowSnapshot = serviceFacade.getVersionedFlowSnapshot(requestEntity.getVersionControlInformation(), true);

        // The flow in the registry may not contain the same versions of components that we have in our flow. As a result, we need to update
        // the flow snapshot to contain compatible bundles.
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());

        // Step 1: Determine which components will be affected by updating the version
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByVersionChange(groupId, flowSnapshot);

        // build a request wrapper
        final InitiateChangeFlowVersionRequestWrapper requestWrapper = new InitiateChangeFlowVersionRequestWrapper(requestEntity, componentLifecycle, getAbsolutePath(), affectedComponents,
                replicateRequest, flowSnapshot);

        final Revision requestRevision = getRevision(requestEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
            serviceFacade,
            requestWrapper,
            requestRevision,
            lookup -> {
                // Step 2: Verify READ and WRITE permissions for user, for every component.
                final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true, false, true, true);
                authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.WRITE, true, false, true, true);

                final VersionedProcessGroup groupContents = flowSnapshot.getFlowContents();
                final Set<ConfigurableComponent> restrictedComponents = FlowRegistryUtils.getRestrictedComponents(groupContents, serviceFacade);
                restrictedComponents.forEach(restrictedComponent -> {
                    final ComponentAuthorizable restrictedComponentAuthorizable = lookup.getConfigurableComponent(restrictedComponent);
                    authorizeRestrictions(authorizer, restrictedComponentAuthorizable);
                });
            },
            () -> {
                // Step 3: Verify that all components in the snapshot exist on all nodes
                // Step 4: Verify that Process Group is already under version control. If not, must start Version Control instead of updating flow
                serviceFacade.verifyCanRevertLocalModifications(groupId, flowSnapshot);
            },
            (revision, wrapper) -> {
                final VersionControlInformationEntity versionControlInformationEntity = wrapper.getVersionControlInformationEntity();
                final VersionControlInformationDTO versionControlInformationDTO = versionControlInformationEntity.getVersionControlInformation();

                // Ensure that the information passed in is correct
                final VersionControlInformationEntity currentVersionEntity = serviceFacade.getVersionControlInformation(groupId);
                if (currentVersionEntity == null) {
                    throw new IllegalStateException("Process Group cannot be reverted to the previous version of the flow because Process Group is not under Version Control.");
                }

                final VersionControlInformationDTO currentVersion = currentVersionEntity.getVersionControlInformation();
                if (!currentVersion.getBucketId().equals(versionControlInformationDTO.getBucketId())) {
                    throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                }
                if (!currentVersion.getFlowId().equals(versionControlInformationDTO.getFlowId())) {
                    throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                }
                if (!currentVersion.getRegistryId().equals(versionControlInformationDTO.getRegistryId())) {
                    throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                }
                if (!currentVersion.getVersion().equals(versionControlInformationDTO.getVersion())) {
                    throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                }

                final String idGenerationSeed = getIdGenerationSeed().orElse(null);

                // Create an asynchronous request that will occur in the background, because this request may
                // result in stopping components, which can take an indeterminate amount of time.
                final String requestId = UUID.randomUUID().toString();
                final AsynchronousWebRequest<VersionControlInformationEntity> request = new StandardAsynchronousWebRequest<>(requestId, groupId, user, "Stopping Affected Processors");

                // Submit the request to be performed in the background
                final Consumer<AsynchronousWebRequest<VersionControlInformationEntity>> updateTask = vcur -> {
                    try {
                        final VersionControlInformationEntity updatedVersionControlEntity = updateFlowVersion(groupId, wrapper.getComponentLifecycle(), wrapper.getExampleUri(),
                            wrapper.getAffectedComponents(), wrapper.isReplicateRequest(), revision, versionControlInformationEntity, wrapper.getFlowSnapshot(), request,
                            idGenerationSeed, false, true);

                        vcur.markComplete(updatedVersionControlEntity);
                    } catch (final ResumeFlowException rfe) {
                        // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                        // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                        logger.error(rfe.getMessage(), rfe);
                        vcur.setFailureReason(rfe.getMessage());
                    } catch (final Exception e) {
                        logger.error("Failed to update flow to new version", e);
                        vcur.setFailureReason("Failed to update flow to new version due to " + e.getMessage());
                    }
                };

                requestManager.submitRequest("revert-requests", requestId, request, updateTask);

                // Generate the response.
                final VersionedFlowUpdateRequestDTO updateRequestDto = new VersionedFlowUpdateRequestDTO();
                updateRequestDto.setComplete(request.isComplete());
                updateRequestDto.setFailureReason(request.getFailureReason());
                updateRequestDto.setLastUpdated(request.getLastUpdated());
                updateRequestDto.setProcessGroupId(groupId);
                updateRequestDto.setRequestId(requestId);
                updateRequestDto.setState(request.getState());
                updateRequestDto.setPercentCompleted(request.getPercentComplete());
                updateRequestDto.setUri(generateResourceUri("versions", "revert-requests", requestId));


                final VersionedFlowUpdateRequestEntity updateRequestEntity = new VersionedFlowUpdateRequestEntity();
                final RevisionDTO groupRevision = serviceFacade.getProcessGroup(groupId).getRevision();
                updateRequestEntity.setProcessGroupRevision(groupRevision);
                updateRequestEntity.setRequest(updateRequestDto);

                return generateOkResponse(updateRequestEntity).build();
            });
    }

    private VersionControlInformationEntity updateFlowVersion(final String groupId, final ComponentLifecycle componentLifecycle, final URI exampleUri,
        final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest, final Revision revision, final VersionControlInformationEntity requestEntity,
        final VersionedFlowSnapshot flowSnapshot, final AsynchronousWebRequest<VersionControlInformationEntity> asyncRequest, final String idGenerationSeed,
        final boolean verifyNotModified, final boolean updateDescendantVersionedFlows) throws LifecycleManagementException, ResumeFlowException {

        // Steps 6-7: Determine which components must be stopped and stop them.
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
        componentLifecycle.scheduleComponents(exampleUri, groupId, runningComponents, ScheduledState.STOPPED, stopComponentsPause);

        if (asyncRequest.isCancelled()) {
            return null;
        }
        asyncRequest.update(new Date(), "Disabling Affected Controller Services", 20);

        // Steps 8-9. Disable enabled controller services that are affected
        final Set<AffectedComponentEntity> enabledServices = affectedComponents.stream()
            .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
            .filter(dto -> "Enabled".equalsIgnoreCase(dto.getComponent().getState()))
            .collect(Collectors.toSet());

        logger.info("Disabling {} Controller Services", enabledServices.size());
        final CancellableTimedPause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(disableServicesPause::cancel);
        componentLifecycle.activateControllerServices(exampleUri, groupId, enabledServices, ControllerServiceState.DISABLED, disableServicesPause);

        if (asyncRequest.isCancelled()) {
            return null;
        }
        asyncRequest.update(new Date(), "Updating Flow", 40);

        logger.info("Updating Process Group with ID {} to version {} of the Versioned Flow", groupId, flowSnapshot.getSnapshotMetadata().getVersion());

        // If replicating request, steps 10-12 are performed on each node individually, and this is accomplished
        // by replicating a PUT to /nifi-api/versions/process-groups/{groupId}
        try {
            if (replicateRequest) {
                final NiFiUser user = NiFiUserUtils.getNiFiUser();

                final URI updateUri;
                try {
                    updateUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                        exampleUri.getPort(), "/nifi-api/versions/process-groups/" + groupId, null, exampleUri.getFragment());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }

                final Map<String, String> headers = new HashMap<>();
                headers.put("content-type", MediaType.APPLICATION_JSON);

                final VersionedFlowSnapshotEntity snapshotEntity = new VersionedFlowSnapshotEntity();
                snapshotEntity.setProcessGroupRevision(dtoFactory.createRevisionDTO(revision));
                snapshotEntity.setRegistryId(requestEntity.getVersionControlInformation().getRegistryId());
                snapshotEntity.setVersionedFlow(flowSnapshot);
                snapshotEntity.setUpdateDescendantVersionedFlows(updateDescendantVersionedFlows);

                final NodeResponse clusterResponse;
                try {
                    logger.debug("Replicating PUT request to {} for user {}", updateUri, user);

                    if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                        clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, updateUri, snapshotEntity, headers).awaitMergedResponse();
                    } else {
                        clusterResponse = getRequestReplicator().forwardToCoordinator(
                            getClusterCoordinatorNode(), user, HttpMethod.PUT, updateUri, snapshotEntity, headers).awaitMergedResponse();
                    }
                } catch (final InterruptedException ie) {
                    logger.warn("Interrupted while replicating PUT request to {} for user {}", updateUri, user);
                    Thread.currentThread().interrupt();
                    throw new LifecycleManagementException("Interrupted while updating flows across cluster", ie);
                }

                final int updateFlowStatus = clusterResponse.getStatus();
                if (updateFlowStatus != Status.OK.getStatusCode()) {
                    final String explanation = getResponseEntity(clusterResponse, String.class);
                    logger.error("Failed to update flow across cluster when replicating PUT request to {} for user {}. Received {} response with explanation: {}",
                        updateUri, user, updateFlowStatus, explanation);
                    throw new LifecycleManagementException("Failed to update Flow on all nodes in cluster due to " + explanation);
                }

            } else {
                // Step 10: Ensure that if any connection exists in the flow and does not exist in the proposed snapshot,
                // that it has no data in it. Ensure that no Input Port was removed, unless it currently has no incoming connections.
                // Ensure that no Output Port was removed, unless it currently has no outgoing connections.
                serviceFacade.verifyCanUpdate(groupId, flowSnapshot, true, verifyNotModified);

                // Step 11-12. Update Process Group to the new flow and update variable registry with any Variables that were added or removed
                final VersionControlInformationDTO requestVci = requestEntity.getVersionControlInformation();

                final Bucket bucket = flowSnapshot.getBucket();
                final VersionedFlow flow = flowSnapshot.getFlow();

                final VersionedFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
                final VersionControlInformationDTO vci = new VersionControlInformationDTO();
                vci.setBucketId(metadata.getBucketIdentifier());
                vci.setBucketName(bucket.getName());
                vci.setFlowDescription(flow.getDescription());
                vci.setFlowId(flow.getIdentifier());
                vci.setFlowName(flow.getName());
                vci.setGroupId(groupId);
                vci.setRegistryId(requestVci.getRegistryId());
                vci.setRegistryName(serviceFacade.getFlowRegistryName(requestVci.getRegistryId()));
                vci.setVersion(metadata.getVersion());
                vci.setState(flowSnapshot.isLatest() ? VersionedFlowState.UP_TO_DATE.name() : VersionedFlowState.STALE.name());

                serviceFacade.updateProcessGroupContents(revision, groupId, vci, flowSnapshot, idGenerationSeed, verifyNotModified, false, updateDescendantVersionedFlows);
            }
        } finally {
            if (!asyncRequest.isCancelled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Re-Enabling {} Controller Services: {}", enabledServices.size(), enabledServices);
                }

                asyncRequest.update(new Date(), "Re-Enabling Controller Services", 60);

                // Step 13. Re-enable all disabled controller services
                final CancellableTimedPause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                asyncRequest.setCancelCallback(enableServicesPause::cancel);
                final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(enabledServices);
                logger.info("Successfully updated flow; re-enabling {} Controller Services", servicesToEnable.size());

                try {
                    componentLifecycle.activateControllerServices(exampleUri, groupId, servicesToEnable, ControllerServiceState.ENABLED, enableServicesPause);
                } catch (final IllegalStateException ise) {
                    // Component Lifecycle will re-enable the Controller Services only if they are valid. If IllegalStateException gets thrown, we need to provide
                    // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
                    throw new ResumeFlowException("Failed to re-enable Controller Services because " + ise.getMessage(), ise);
                }
            }

            if (!asyncRequest.isCancelled()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Restart {} Processors: {}", runningComponents.size(), runningComponents);
                }

                asyncRequest.update(new Date(), "Restarting Processors", 80);

                // Step 14. Restart all components
                final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(runningComponents);

                // If there are any Remote Group Ports that are supposed to be started and have no connections, we want to remove those from our Set.
                // This will happen if the Remote Group Port is transmitting when the version change happens but the new flow version does not have
                // a connection to the port. In such a case, the Port still is included in the Updated Entities because we do not remove them
                // when updating the flow (they are removed in the background).
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
                    componentLifecycle.scheduleComponents(exampleUri, groupId, componentsToStart, ScheduledState.RUNNING, startComponentsPause);
                } catch (final IllegalStateException ise) {
                    // Component Lifecycle will restart the Processors only if they are valid. If IllegalStateException gets thrown, we need to provide
                    // a more intelligent error message as to exactly what happened, rather than indicate that the flow could not be updated.
                    throw new ResumeFlowException("Failed to restart components because " + ise.getMessage(), ise);
                }
            }
        }

        asyncRequest.setCancelCallback(null);
        if (asyncRequest.isCancelled()) {
            return null;
        }
        asyncRequest.update(new Date(), "Complete", 100);

        return serviceFacade.getVersionControlInformation(groupId);
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
    private <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            entity = nodeResponse.getClientResponse().readEntity(clazz);
        }
        return entity;
    }


    private Set<AffectedComponentEntity> getUpdatedEntities(final Set<AffectedComponentEntity> originalEntities) {
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


    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setClusterComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.clusterComponentLifecycle = componentLifecycle;
    }

    public void setLocalComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.localComponentLifecycle = componentLifecycle;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }


    private static class ActiveRequest {
        private static final long MAX_REQUEST_LOCK_NANOS = TimeUnit.MINUTES.toNanos(1L);

        private final String requestId;
        private final NiFiUser user;
        private final String processGroupId;
        private final long creationNanos = System.nanoTime();
        private final long expirationTime = creationNanos + MAX_REQUEST_LOCK_NANOS;

        private boolean updatePerformed = false;

        private ActiveRequest(final String requestId, final NiFiUser user, final String processGroupId) {
            this.requestId = requestId;
            this.user = user;
            this.processGroupId = processGroupId;
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTime;
        }

        public String getRequestId() {
            return requestId;
        }

        public NiFiUser getUser() {
            return user;
        }

        public String getProcessGroupId() {
            return processGroupId;
        }

        public void updatePerformed() {
            updatePerformed = true;
        }

        public boolean isUpdatePerformed() {
            return updatePerformed;
        }
    }


    private static class InitiateChangeFlowVersionRequestWrapper extends Entity {
        private final VersionControlInformationEntity versionControlInformationEntity;
        private final ComponentLifecycle componentLifecycle;
        private final URI exampleUri;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final VersionedFlowSnapshot flowSnapshot;

        public InitiateChangeFlowVersionRequestWrapper(final VersionControlInformationEntity versionControlInformationEntity, final ComponentLifecycle componentLifecycle,
                                                       final URI exampleUri, final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                                                       final VersionedFlowSnapshot flowSnapshot) {

            this.versionControlInformationEntity = versionControlInformationEntity;
            this.componentLifecycle = componentLifecycle;
            this.exampleUri = exampleUri;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.flowSnapshot = flowSnapshot;
        }

        public VersionControlInformationEntity getVersionControlInformationEntity() {
            return versionControlInformationEntity;
        }

        public ComponentLifecycle getComponentLifecycle() {
            return componentLifecycle;
        }

        public URI getExampleUri() {
            return exampleUri;
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
}
