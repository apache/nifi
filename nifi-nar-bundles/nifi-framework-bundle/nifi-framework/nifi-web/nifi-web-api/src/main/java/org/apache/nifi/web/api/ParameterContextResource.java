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
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
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
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateRequestDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateStepDTO;
import org.apache.nifi.web.api.dto.ParameterContextValidationRequestDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultsEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextValidationRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.AffectedComponentUtils;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.InvalidComponentAction;
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
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@Path("/parameter-contexts")
@Api(value = "/parameter-contexts", description = "Endpoint for managing version control for a flow")
public class ParameterContextResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(ParameterContextResource.class);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private DtoFactory dtoFactory;
    private ComponentLifecycle clusterComponentLifecycle;
    private ComponentLifecycle localComponentLifecycle;

    private RequestManager<ParameterContextEntity> updateRequestManager = new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Parameter Context Update Thread");
    private RequestManager<ComponentValidationResultsEntity> validationRequestManager = new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L),
        "Parameter Context Validation Thread");



    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        value = "Gets all Parameter Contexts",
        response = ParameterContextsEntity.class,
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{id} for each Parameter Context")
        }
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
        }
    )
    public Response getParameterContexts() {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final Set<ParameterContextEntity> parameterContexts = serviceFacade.getParameterContexts();
        final ParameterContextsEntity entity = new ParameterContextsEntity();
        entity.setParameterContexts(parameterContexts);

        // generate the response
        return generateOkResponse(entity).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
        value = "Returns the Parameter Context with the given ID",
        response = ParameterContextEntity.class,
        notes = "Returns the Parameter Context with the given ID.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{id}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getParameterContext(@ApiParam("The ID of the Parameter Context") @PathParam("id") final String parameterContextId) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterContext = lookup.getParameterContext(parameterContextId);
            parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specified parameter context
        final ParameterContextEntity entity = serviceFacade.getParameterContext(parameterContextId, NiFiUserUtils.getNiFiUser());
        entity.setUri(generateResourceUri("parameter-contexts", entity.getId()));

        // generate the response
        return generateOkResponse(entity).build();
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
        value = "Create a Parameter Context",
        response = ParameterContextEntity.class,
        authorizations = {
            @Authorization(value = "Write - /parameter-contexts")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response createParameterContext(
        @ApiParam(value = "The Parameter Context.", required = true) final ParameterContextEntity requestEntity) {

        if (requestEntity == null || requestEntity.getComponent() == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (requestEntity.getRevision() == null || (requestEntity.getRevision().getVersion() == null || requestEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Parameter Context.");
        }

        final ParameterContextDTO context = requestEntity.getComponent();
        if (context.getName() == null) {
            throw new IllegalArgumentException("Parameter Context's Name must be specified");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
            serviceFacade,
            requestEntity,
            lookup -> {
                final Authorizable parameterContexts = lookup.getParameterContexts();
                parameterContexts.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> serviceFacade.verifyCreateParameterContext(requestEntity.getComponent()),
            entity -> {
                final String contextId = generateUuid();
                entity.getComponent().setId(contextId);

                final Revision revision = getRevision(entity.getRevision(), contextId);
                final ParameterContextEntity contextEntity = serviceFacade.createParameterContext(revision, entity.getComponent());

                // generate a 201 created response
                final String uri = generateResourceUri("parameter-contexts", contextEntity.getId());
                contextEntity.setUri(uri);
                return generateCreatedResponse(URI.create(uri), contextEntity).build();
            });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
        value = "Modifies a Parameter Context",
        response = ParameterContextEntity.class,
        notes = "This endpoint will update a Parameter Context to match the provided entity. However, this request will fail if any component is running and is referencing a Parameter in the " +
            "Parameter Context. Generally, this endpoint is not called directly. Instead, an update request should be submitted by making a POST to the " +
            "/parameter-contexts/update-requests endpoint. That endpoint will, in turn, call this endpoint.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{id}"),
            @Authorization(value = "Write - /parameter-contexts/{id}")
        }
    )
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateParameterContext(
        @PathParam("id") String contextId,
        @ApiParam(value = "The updated Parameter Context", required = true) ParameterContextEntity requestEntity) {

        // Validate request
        if (requestEntity.getId() == null) {
            throw new IllegalArgumentException("The ID of the Parameter Context must be specified");
        }
        if (!requestEntity.getId().equals(contextId)) {
            throw new IllegalArgumentException("The ID of the Parameter Context must match the ID specified in the URL's path");
        }

        final ParameterContextDTO updateDto = requestEntity.getComponent();
        if (updateDto == null) {
            throw new IllegalArgumentException("The Parameter Context must be supplied");
        }

        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("The Revision of the Parameter Context must be specified.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestEntity.getRevision(), updateDto.getId());
        return withWriteLock(
            serviceFacade,
            requestEntity,
            requestRevision,
            lookup -> {
                final Authorizable parameterContext = lookup.getParameterContext(contextId);
                parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                parameterContext.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> serviceFacade.verifyUpdateParameterContext(updateDto, true),
            (rev, entity) -> {
                final ParameterContextEntity updatedEntity = serviceFacade.updateParameterContext(rev, entity.getComponent());

                updatedEntity.setUri(generateResourceUri("parameter-contexts", entity.getId()));
                return generateOkResponse(updatedEntity).build();
            }
        );
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests")
    @ApiOperation(
        value = "Initiate the Update Request of a Parameter Context",
        response = ParameterContextUpdateRequestEntity.class,
        notes = "This will initiate the process of updating a Parameter Context. Changing the value of a Parameter may require that one or more components be stopped and " +
            "restarted, so this acttion may take significantly more time than many other REST API actions. As a result, this endpoint will immediately return a ParameterContextUpdateRequestEntity, " +
            "and the process of updating the necessary components will occur asynchronously in the background. The client may then periodically poll the status of the request by " +
            "issuing a GET request to /parameter-contexts/update-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
            "/parameter-contexts/update-requests/{requestId}.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{parameterContextId}"),
            @Authorization(value = "Write - /parameter-contexts/{parameterContextId}"),
            @Authorization(value = "Read - for every component that is affected by the update"),
            @Authorization(value = "Write - for every component that is affected by the update")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response submitParameterContextUpdate(
        @ApiParam(value = "The updated version of the parameter context.", required = true) final ParameterContextEntity requestEntity) {

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Parameter Context Revision must be specified");
        }

        final ParameterContextDTO contextDto = requestEntity.getComponent();
        if (contextDto == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (contextDto.getId() == null) {
            throw new IllegalArgumentException("Parameter Context's ID must be specified");
        }

        // We will perform the updating of the Parameter Context in a background thread because it can be a long-running process.
        // In order to do this, we will need some objects that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these objects up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Workflow for this process:
        // 1. Determine which components will be affected and are enabled/running
        // 2. Verify READ and WRITE permissions for user, for every component that is affected
        // 3. Verify READ and WRITE permissions for user, for Parameter Context
        // 4. Stop all Processors that are affected.
        // 5. Wait for all of the Processors to finish stopping.
        // 6. Disable all Controller Services that are affected.
        // 7. Wait for all Controller Services to finish disabling.
        // 8. Update Parameter Context
        // 9. Re-Enable all affected Controller Services
        // 10. Re-Start all Processors

        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByParameterContextUpdate(contextDto);
        logger.debug("Received Update Request for Parameter Context: {}; the following {} components will be affected: {}", requestEntity, affectedComponents.size(), affectedComponents);

        final InitiateChangeParameterContextRequestWrapper requestWrapper = new InitiateChangeParameterContextRequestWrapper(requestEntity, componentLifecycle, getAbsolutePath(),
            affectedComponents, replicateRequest, user);

        final Revision requestRevision = getRevision(requestEntity.getRevision(), contextDto.getId());
        return withWriteLock(
            serviceFacade,
            requestWrapper,
            requestRevision,
            lookup -> {
                // Verify READ and WRITE permissions for user, for the Parameter Context itself
                final Authorizable parameterContext = lookup.getParameterContext(requestEntity.getId());
                parameterContext.authorize(authorizer, RequestAction.READ, user);
                parameterContext.authorize(authorizer, RequestAction.WRITE, user);

                // Verify READ and WRITE permissions for user, for every component that is affected
                for (final AffectedComponentEntity entity : affectedComponents) {
                    final AffectedComponentDTO dto = entity.getComponent();
                    if (AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(dto.getReferenceType())) {
                        final Authorizable processor = lookup.getProcessor(dto.getId()).getAuthorizable();
                        processor.authorize(authorizer, RequestAction.READ, user);
                        processor.authorize(authorizer, RequestAction.WRITE, user);
                    }
                }
            },
            () -> {
                // Verify Request
                serviceFacade.verifyUpdateParameterContext(contextDto, false);
            },
            this::submitUpdateRequest
        );
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @ApiOperation(
        value = "Returns the Update Request with the given ID",
        response = ParameterContextUpdateRequestEntity.class,
        notes = "Returns the Update Request with the given ID. Once an Update Request has been created by performing a POST to /nifi-api/parameter-contexts, "
            + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
            + "current state of the request, and any failures. ",
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
    public Response getParameterContextUpdate(@ApiParam("The ID of the Update Request") @PathParam("id") final String updateRequestId) {
        return retrieveUpdateRequest("update-requests", updateRequestId);
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @ApiOperation(
        value = "Deletes the Update Request with the given ID",
        response = ParameterContextUpdateRequestEntity.class,
        notes = "Deletes the Update Request with the given ID. After a request is created via a POST to /nifi-api/parameter-contexts, it is expected "
            + "that the client will properly clean up the request by DELETE'ing it, once the Update process has completed. If the request is deleted before the request "
            + "completes, then the Update request will finish the step that it is currently performing and then will cancel any subsequent steps.",
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

        return deleteUpdateRequest("update-requests", updateRequestId, disconnectedNodeAcknowledged.booleanValue());
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
        value = "Deletes the Parameter Context with the given ID",
        response = ParameterContextEntity.class,
        notes = "Deletes the Parameter Context with the given ID.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{uuid}"),
            @Authorization(value = "Write - /parameter-contexts/{uuid}"),
            @Authorization(value = "Read - /process-groups/{uuid}, for any Process Group that is currently bound to the Parameter Context"),
            @Authorization(value = "Write - /process-groups/{uuid}, for any Process Group that is currently bound to the Parameter Context")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteParameterContext(
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
        @ApiParam("The Parameter Context ID.") @PathParam("id") final String parameterContextId) {


        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), parameterContextId);
        return withWriteLock(
            serviceFacade,
            null,
            requestRevision,
            lookup -> {
                final Authorizable parameterContext = lookup.getParameterContext(parameterContextId);
                parameterContext.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            () -> serviceFacade.verifyDeleteParameterContext(parameterContextId),
            (revision, groupEntity) -> {
                // disconnect from version control
                final ParameterContextEntity entity = serviceFacade.deleteParameterContext(revision, parameterContextId);

                // generate the response
                return generateOkResponse(entity).build();
            });

    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("validation-requests")
    @ApiOperation(
        value = "Initiate a Validation Request to determine how the validity of components will change if a Paramter Context were to be updated",
        response = ParameterContextValidationRequestEntity.class,
        notes = "This will initiate the process of validating all components whose Process Group is bound to the specified Parameter Context. Performing validation against " +
            "an arbitrary number of components may be expect and take significantly more time than many other REST API actions. As a result, this endpoint will immediately return " +
            "a ParameterContextValidationRequestEntity, " +
            "and the process of validating the necessary components will occur asynchronously in the background. The client may then periodically poll the status of the request by " +
            "issuing a GET request to /parameter-contexts/validation-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
            "/parameter-contexts/validation-requests/{requestId}.",
        authorizations = {
            @Authorization(value = "Read - /parameter-contexts/{parameterContextId}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response submitValidationRequest(@ApiParam(value = "The validation request", required=true) final ParameterContextValidationRequestEntity requestEntity) {
        final ParameterContextValidationRequestDTO requestDto = requestEntity.getRequest();
        if (requestDto == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (requestDto.getParameterContext() == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }
        if (requestDto.getParameterContext().getId() == null) {
            throw new IllegalArgumentException("Parameter Context's ID must be specified");
        }

        if (isReplicateRequest()) {
            return replicate("POST", requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
            serviceFacade,
            requestEntity,
            lookup -> {
                final Authorizable parameterContext = lookup.getParameterContext(requestEntity.getRequest().getParameterContext().getId());
                parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            },
            () -> {},
            entity -> performAsyncValidation(entity, NiFiUserUtils.getNiFiUser())
        );
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("validation-requests/{id}")
    @ApiOperation(
        value = "Returns the Validation Request with the given ID",
        response = ParameterContextValidationRequestEntity.class,
        notes = "Returns the Validation Request with the given ID. Once a Validation Request has been created by performing a POST to /nifi-api/validation-contexts, "
            + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
            + "current state of the request, and any failures. ",
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
    public Response getValidationRequest(@ApiParam("The ID of the Validation Request") @PathParam("id") final String validationRequestId) {
        if (isReplicateRequest()) {
            return replicate("GET");
        }

        return retrieveValidationRequest("validation-requests", validationRequestId);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("validation-requests/{id}")
    @ApiOperation(
        value = "Deletes the Validation Request with the given ID",
        response = ParameterContextValidationRequestEntity.class,
        notes = "Deletes the Validation Request with the given ID. After a request is created via a POST to /nifi-api/validation-contexts, it is expected "
            + "that the client will properly clean up the request by DELETE'ing it, once the validation process has completed. If the request is deleted before the request "
            + "completes, then the Validation request will finish the step that it is currently performing and then will cancel any subsequent steps.",
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
    public Response deleteValidationRequest(
        @ApiParam(
            value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
            required = false
        )
        @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
        @ApiParam("The ID of the Update Request") @PathParam("id") final String validationRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        return deleteValidationRequest("validation-requests", validationRequestId, disconnectedNodeAcknowledged.booleanValue());
    }



    private Response performAsyncValidation(final ParameterContextValidationRequestEntity requestEntity, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = generateUuid();
        final AsynchronousWebRequest<ComponentValidationResultsEntity> request = new StandardAsynchronousWebRequest<>(requestId, null, user, getValidationSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<ComponentValidationResultsEntity>> validationTask = asyncRequest -> {
            try {
                final ComponentValidationResultsEntity resultEntity = validateComponents(requestEntity, user);
                asyncRequest.markStepComplete(resultEntity);
            } catch (final Exception e) {
                logger.error("Failed to validate components", e);
                asyncRequest.fail("Failed to validation components due to " + e);
            }
        };

        validationRequestManager.submitRequest("validation-requests", requestId, request, validationTask);

        // Generate the response.
        final ParameterContextValidationRequestDTO validationRequestDto = new ParameterContextValidationRequestDTO();
        validationRequestDto.setComplete(request.isComplete());
        validationRequestDto.setFailureReason(request.getFailureReason());
        validationRequestDto.setLastUpdated(request.getLastUpdated());
        validationRequestDto.setRequestId(requestId);
        validationRequestDto.setUri(generateResourceUri("parameter-contexts", "validation-requests", requestId));
        validationRequestDto.setPercentCompleted(request.getPercentComplete());
        validationRequestDto.setState(request.getState());
        validationRequestDto.setComponentValidationResults(request.getResults());

        final ParameterContextValidationRequestEntity validationRequestEntity = new ParameterContextValidationRequestEntity();
        validationRequestEntity.setRequest(validationRequestDto);

        return generateOkResponse(validationRequestEntity).build();
    }

    private List<UpdateStep> getValidationSteps() {
        return Collections.singletonList(new StandardUpdateStep("Validating Components"));
    }

    private ComponentValidationResultsEntity validateComponents(final ParameterContextValidationRequestEntity requestEntity, final NiFiUser user) {
        final List<ComponentValidationResultEntity> resultEntities = serviceFacade.validateComponents(requestEntity.getRequest().getParameterContext(), user);
        final ComponentValidationResultsEntity resultsEntity = new ComponentValidationResultsEntity();
        resultsEntity.setValidationResults(resultEntities);
        return resultsEntity;
    }


    private Response submitUpdateRequest(final Revision requestRevision, final InitiateChangeParameterContextRequestWrapper requestWrapper) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = UUID.randomUUID().toString();
        final AsynchronousWebRequest<ParameterContextEntity> request = new StandardAsynchronousWebRequest<>(requestId, requestWrapper.getParameterContextEntity().getId(),
            requestWrapper.getUser(), getUpdateSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<ParameterContextEntity>> updateTask = asyncRequest -> {
            try {
                final ParameterContextEntity updatedParameterContextEntity = updateParameterContext(asyncRequest, requestWrapper.getComponentLifecycle(), requestWrapper.getExampleUri(),
                    requestWrapper.getAffectedComponents(), requestWrapper.isReplicateRequest(), requestRevision, requestWrapper.getParameterContextEntity());

                asyncRequest.markStepComplete(updatedParameterContextEntity);
            } catch (final ResumeFlowException rfe) {
                // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                logger.error(rfe.getMessage(), rfe);
                asyncRequest.fail(rfe.getMessage());
            } catch (final Exception e) {
                logger.error("Failed to update Parameter Context", e);
                asyncRequest.fail("Failed to update Parameter Context due to " + e);
            }
        };

        updateRequestManager.submitRequest("update-requests", requestId, request, updateTask);

        // Generate the response.
        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(request, "update-requests", requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private List<UpdateStep> getUpdateSteps() {
        return Arrays.asList(new StandardUpdateStep("Stopping Affected Processors"),
            new StandardUpdateStep("Disabling Affected Controller Services"),
            new StandardUpdateStep("Updating Parameter Context"),
            new StandardUpdateStep("Re-Enabling Affected Controller Services"),
            new StandardUpdateStep("Restarting Affected Processors"));
    }


    private ParameterContextEntity updateParameterContext(final AsynchronousWebRequest<ParameterContextEntity> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri,
                                                          final Set<AffectedComponentEntity> affectedComponents,
                                                          final boolean replicateRequest, final Revision revision, final ParameterContextEntity updatedContextEntity)
        throws LifecycleManagementException, ResumeFlowException {

        final Set<AffectedComponentEntity> runningProcessors = affectedComponents.stream()
            .filter(component -> AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(component.getComponent().getReferenceType()))
            .filter(component -> "Running".equalsIgnoreCase(component.getComponent().getState()))
            .collect(Collectors.toSet());

        final Set<AffectedComponentEntity> enabledControllerServices = affectedComponents.stream()
            .filter(dto -> AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(dto.getComponent().getReferenceType()))
            .filter(dto -> "Enabled".equalsIgnoreCase(dto.getComponent().getState()))
            .collect(Collectors.toSet());

        stopProcessors(runningProcessors, asyncRequest, componentLifecycle, uri);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        disableControllerServices(enabledControllerServices, asyncRequest, componentLifecycle, uri);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        asyncRequest.markStepComplete();
        logger.info("Updating Parameter Context with ID {}", updatedContextEntity.getId());

        final ParameterContextEntity updatedEntity;
        try {
            updatedEntity = performParameterContextUpdate(asyncRequest, uri, replicateRequest, revision, updatedContextEntity);
            logger.info("Successfully updated Parameter Context with ID {}", updatedContextEntity.getId());
        } finally {
            // TODO: can almost certainly be refactored so that the same code is shared between VersionsResource and ParameterContextResource.
            if (!asyncRequest.isCancelled()) {
                enableControllerServices(enabledControllerServices, asyncRequest, componentLifecycle, uri);
            }

            if (!asyncRequest.isCancelled()) {
                restartProcessors(runningProcessors, asyncRequest, componentLifecycle, uri);
            }
        }

        asyncRequest.setCancelCallback(null);
        if (asyncRequest.isCancelled()) {
            return null;
        }

        return updatedEntity;
    }

    private ParameterContextEntity performParameterContextUpdate(final AsynchronousWebRequest<?> asyncRequest, final URI exampleUri, final boolean replicateRequest, final Revision revision,
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
            final NodeResponse clusterResponse;
            try {
                logger.debug("Replicating PUT request to {} for user {}", updateUri, user);

                if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                    clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, updateUri, updatedContext, headers).awaitMergedResponse();
                } else {
                    clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), user, HttpMethod.PUT, updateUri, updatedContext, headers).awaitMergedResponse();
                }
            } catch (final InterruptedException ie) {
                logger.warn("Interrupted while replicating PUT request to {} for user {}", updateUri, user);
                Thread.currentThread().interrupt();
                throw new LifecycleManagementException("Interrupted while updating flows across cluster", ie);
            }

            final int updateFlowStatus = clusterResponse.getStatus();
            if (updateFlowStatus != Response.Status.OK.getStatusCode()) {
                final String explanation = getResponseEntity(clusterResponse, String.class);
                logger.error("Failed to update flow across cluster when replicating PUT request to {} for user {}. Received {} response with explanation: {}",
                    updateUri, user, updateFlowStatus, explanation);
                throw new LifecycleManagementException("Failed to update Flow on all nodes in cluster due to " + explanation);
            }

            return serviceFacade.getParameterContext(updatedContext.getId(), user);
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
    private <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            entity = nodeResponse.getClientResponse().readEntity(clazz);
        }
        return entity;
    }


    private void stopProcessors(final Set<AffectedComponentEntity> processors, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri)
        throws LifecycleManagementException {

        logger.info("Stopping {} Processors in order to update Parameter Context", processors.size());
        final CancellableTimedPause stopComponentsPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(stopComponentsPause::cancel);
        componentLifecycle.scheduleComponents(uri, "root", processors, ScheduledState.STOPPED, stopComponentsPause, InvalidComponentAction.SKIP);
    }

    private void restartProcessors(final Set<AffectedComponentEntity> processors, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle, final URI uri)
        throws ResumeFlowException, LifecycleManagementException {

        if (logger.isDebugEnabled()) {
            logger.debug("Restarting {} Processors after having updated Parameter Context: {}", processors.size(), processors);
        } else {
            logger.info("Restarting {} Processors after having updated Parameter Context", processors.size());
        }

        asyncRequest.markStepComplete();

        // Step 14. Restart all components
        final Set<AffectedComponentEntity> componentsToStart = getUpdatedEntities(processors);

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

    private void disableControllerServices(final Set<AffectedComponentEntity> controllerServices, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle,
                                           final URI uri) throws LifecycleManagementException {

        asyncRequest.markStepComplete();
        logger.info("Disabling {} Controller Services in order to update Parameter Context", controllerServices.size());
        final CancellableTimedPause disableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(disableServicesPause::cancel);
        componentLifecycle.activateControllerServices(uri, "root", controllerServices, ControllerServiceState.DISABLED, disableServicesPause, InvalidComponentAction.SKIP);
    }

    private void enableControllerServices(final Set<AffectedComponentEntity> controllerServices, final AsynchronousWebRequest<?> asyncRequest, final ComponentLifecycle componentLifecycle,
                                          final URI uri) throws LifecycleManagementException, ResumeFlowException {
        if (logger.isDebugEnabled()) {
            logger.debug("Re-Enabling {} Controller Services: {}", controllerServices.size(), controllerServices);
        } else {
            logger.info("Re-Enabling {} Controller Services after having updated Parameter Context", controllerServices.size());
        }

        asyncRequest.markStepComplete();

        // Step 13. Re-enable all disabled controller services
        final CancellableTimedPause enableServicesPause = new CancellableTimedPause(250, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        asyncRequest.setCancelCallback(enableServicesPause::cancel);
        final Set<AffectedComponentEntity> servicesToEnable = getUpdatedEntities(controllerServices);

        try {
            componentLifecycle.activateControllerServices(uri, "root", servicesToEnable, ControllerServiceState.ENABLED, enableServicesPause, InvalidComponentAction.SKIP);
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
            } catch (final ResourceNotFoundException rnfe) {
                // Component was removed. Just continue on without adding anything to the entities.
                // We do this because the intent is to get updated versions of the entities with current
                // Revisions so that we can change the states of the components. If the component was removed,
                // then we can just drop the entity, since there is no need to change its state.
            }
        }

        return entities;
    }


    private Response retrieveValidationRequest(final String requestType, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final AsynchronousWebRequest<ComponentValidationResultsEntity> asyncRequest = validationRequestManager.getRequest(requestType, requestId, user);
        final ParameterContextValidationRequestEntity requestEntity = createValidationRequestEntity(asyncRequest, requestType, requestId);
        return generateOkResponse(requestEntity).build();
    }

    private Response deleteValidationRequest(final String requestType, final String requestId, final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<ComponentValidationResultsEntity> asyncRequest = validationRequestManager.removeRequest(requestType, requestId, user);
        if (asyncRequest == null) {
            throw new ResourceNotFoundException("Could not find request of type " + requestType + " with ID " + requestId);
        }

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        final ParameterContextValidationRequestEntity requestEntity = createValidationRequestEntity(asyncRequest, requestType, requestId);
        return generateOkResponse(requestEntity).build();
    }

    private ParameterContextValidationRequestEntity createValidationRequestEntity(final AsynchronousWebRequest<ComponentValidationResultsEntity> asyncRequest, final String requestType,
                                                                                  final String requestId) {
        final ParameterContextValidationRequestDTO requestDto = new ParameterContextValidationRequestDTO();

        requestDto.setComplete(asyncRequest.isComplete());
        requestDto.setFailureReason(asyncRequest.getFailureReason());
        requestDto.setLastUpdated(asyncRequest.getLastUpdated());
        requestDto.setRequestId(requestId);
        requestDto.setUri(generateResourceUri("parameter-contexts", requestType, requestId));
        requestDto.setState(asyncRequest.getState());
        requestDto.setPercentCompleted(asyncRequest.getPercentComplete());
        requestDto.setComponentValidationResults(asyncRequest.getResults());

        final ParameterContextValidationRequestEntity entity = new ParameterContextValidationRequestEntity();
        entity.setRequest(requestDto);
        return entity;
    }

    private Response retrieveUpdateRequest(final String requestType, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<ParameterContextEntity> asyncRequest = updateRequestManager.getRequest(requestType, requestId, user);
        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(asyncRequest, requestType, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private Response deleteUpdateRequest(final String requestType, final String requestId, final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<ParameterContextEntity> asyncRequest = updateRequestManager.removeRequest(requestType, requestId, user);
        if (asyncRequest == null) {
            throw new ResourceNotFoundException("Could not find request of type " + requestType + " with ID " + requestId);
        }

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(asyncRequest, requestType, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private ParameterContextUpdateRequestEntity createUpdateRequestEntity(final AsynchronousWebRequest<ParameterContextEntity> asyncRequest, final String requestType, final String requestId) {
        final ParameterContextUpdateRequestDTO updateRequestDto = new ParameterContextUpdateRequestDTO();
        updateRequestDto.setComplete(asyncRequest.isComplete());
        updateRequestDto.setFailureReason(asyncRequest.getFailureReason());
        updateRequestDto.setLastUpdated(asyncRequest.getLastUpdated());
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri("parameter-contexts", requestType, requestId));
        updateRequestDto.setState(asyncRequest.getState());
        updateRequestDto.setPercentCompleted(asyncRequest.getPercentComplete());

        final List<ParameterContextUpdateStepDTO> updateSteps = new ArrayList<>();
        for (final UpdateStep updateStep : asyncRequest.getUpdateSteps()) {
            final ParameterContextUpdateStepDTO stepDto = new ParameterContextUpdateStepDTO();
            stepDto.setDescription(updateStep.getDescription());
            stepDto.setComplete(updateStep.isComplete());
            stepDto.setFailureReason(updateStep.getFailureReason());
            updateSteps.add(stepDto);
        }
        updateRequestDto.setUpdateSteps(updateSteps);

        final ParameterContextUpdateRequestEntity updateRequestEntity = new ParameterContextUpdateRequestEntity();

        if (updateRequestDto.isComplete()) {
            final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(asyncRequest.getComponentId(), NiFiUserUtils.getNiFiUser());
            updateRequestDto.setParameterContext(contextEntity == null ? null : contextEntity.getComponent());
            updateRequestEntity.setParameterContextRevision(contextEntity == null ? null : contextEntity.getRevision());
        }

        updateRequestEntity.setRequest(updateRequestDto);
        return updateRequestEntity;
    }


    private static class InitiateChangeParameterContextRequestWrapper extends Entity {
        private final ParameterContextEntity parameterContextEntity;
        private final ComponentLifecycle componentLifecycle;
        private final URI exampleUri;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final NiFiUser nifiUser;

        public InitiateChangeParameterContextRequestWrapper(final ParameterContextEntity parameterContextEntity, final ComponentLifecycle componentLifecycle,
                                                            final URI exampleUri, final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                                                            final NiFiUser nifiUser) {

            this.parameterContextEntity = parameterContextEntity;
            this.componentLifecycle = componentLifecycle;
            this.exampleUri = exampleUri;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.nifiUser = nifiUser;
        }

        public ParameterContextEntity getParameterContextEntity() {
            return parameterContextEntity;
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

        public NiFiUser getUser() {
            return nifiUser;
        }
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

}
