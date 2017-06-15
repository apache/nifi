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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Set;

/**
 * RESTful endpoint for managing a Remote group.
 */
@Path("/remote-process-groups")
@Api(
        value = "/remote-process-groups",
        description = "Endpoint for managing a Remote Process Group."
)
public class RemoteProcessGroupResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the remaining content for each remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroupEntities groups
     * @return dtos
     */
    public Set<RemoteProcessGroupEntity> populateRemainingRemoteProcessGroupEntitiesContent(Set<RemoteProcessGroupEntity> remoteProcessGroupEntities) {
        for (RemoteProcessGroupEntity remoteProcessEntities : remoteProcessGroupEntities) {
            populateRemainingRemoteProcessGroupEntityContent(remoteProcessEntities);
        }
        return remoteProcessGroupEntities;
    }

    /**
     * Populates the remaining content for each remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroupEntity groups
     * @return dtos
     */
    public RemoteProcessGroupEntity populateRemainingRemoteProcessGroupEntityContent(RemoteProcessGroupEntity remoteProcessGroupEntity) {
        remoteProcessGroupEntity.setUri(generateResourceUri("remote-process-groups", remoteProcessGroupEntity.getId()));
        return remoteProcessGroupEntity;
    }

    /**
     * Retrieves the specified remote process group.
     *
     * @param id The id of the remote process group to retrieve
     * @return A remoteProcessGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a remote process group",
            response = RemoteProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /remote-process-groups/{uuid}", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getRemoteProcessGroup(
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
            remoteProcessGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the remote process group
        final RemoteProcessGroupEntity entity = serviceFacade.getRemoteProcessGroup(id);
        populateRemainingRemoteProcessGroupEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Removes the specified remote process group.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id                 The id of the remote process group to be removed.
     * @return A remoteProcessGroupEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a remote process group",
            response = RemoteProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /remote-process-groups/{uuid}", type = ""),
                    @Authorization(value = "Write - Parent Process Group - /process-groups/{uuid}", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response removeRemoteProcessGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @QueryParam(VERSION) final LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final RemoteProcessGroupEntity requestRemoteProcessGroupEntity = new RemoteProcessGroupEntity();
        requestRemoteProcessGroupEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);

                    // ensure write permission to the remote process group
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    remoteProcessGroup.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteRemoteProcessGroup(id),
                (revision, remoteProcessGroupEntity) -> {
                    final RemoteProcessGroupEntity entity = serviceFacade.deleteRemoteProcessGroup(revision, remoteProcessGroupEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group input port.
     *
     * @param httpServletRequest           request
     * @param id                           The id of the remote process group to update.
     * @param portId                       The id of the input port to update.
     * @param requestRemoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports/{port-id}")
    @ApiOperation(
            value = "Updates a remote port",
            notes = NON_GUARANTEED_ENDPOINT,
            response = RemoteProcessGroupPortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /remote-process-groups/{uuid}", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response updateRemoteProcessGroupInputPort(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") final String portId,
            @ApiParam(
                    value = "The remote process group port.",
                    required = true
            ) final RemoteProcessGroupPortEntity requestRemoteProcessGroupPortEntity) {

        if (requestRemoteProcessGroupPortEntity == null || requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (requestRemoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // ensure the group ids are the same
        if (!id.equals(requestRemoteProcessGroupPort.getGroupId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroupPort.getGroupId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupPortEntity);
        }

        final Revision requestRevision = getRevision(requestRemoteProcessGroupPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupInputPort(id, requestRemoteProcessGroupPort),
                (revision, remoteProcessGroupPortEntity) -> {
                    final RemoteProcessGroupPortDTO remoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();

                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupInputPort(revision, id, remoteProcessGroupPort);

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group output port.
     *
     * @param httpServletRequest           request
     * @param id                           The id of the remote process group to update.
     * @param portId                       The id of the output port to update.
     * @param requestRemoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports/{port-id}")
    @ApiOperation(
            value = "Updates a remote port",
            notes = NON_GUARANTEED_ENDPOINT,
            response = RemoteProcessGroupPortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /remote-process-groups/{uuid}", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response updateRemoteProcessGroupOutputPort(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") String portId,
            @ApiParam(
                    value = "The remote process group port.",
                    required = true
            ) RemoteProcessGroupPortEntity requestRemoteProcessGroupPortEntity) {

        if (requestRemoteProcessGroupPortEntity == null || requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (requestRemoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // ensure the group ids are the same
        if (!id.equals(requestRemoteProcessGroupPort.getGroupId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroupPort.getGroupId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupPortEntity);
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemoteProcessGroupPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupOutputPort(id, requestRemoteProcessGroupPort),
                (revision, remoteProcessGroupPortEntity) -> {
                    final RemoteProcessGroupPortDTO remoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();

                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupOutputPort(revision, id, remoteProcessGroupPort);

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group.
     *
     * @param httpServletRequest       request
     * @param id                       The id of the remote process group to update.
     * @param requestRemoteProcessGroupEntity A remoteProcessGroupEntity.
     * @return A remoteProcessGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates a remote process group",
            response = RemoteProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /remote-process-groups/{uuid}", type = "")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response updateRemoteProcessGroup(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The remote process group.",
                    required = true
            ) final RemoteProcessGroupEntity requestRemoteProcessGroupEntity) {

        if (requestRemoteProcessGroupEntity == null || requestRemoteProcessGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("Remote process group details must be specified.");
        }

        if (requestRemoteProcessGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupDTO requestRemoteProcessGroup = requestRemoteProcessGroupEntity.getComponent();
        if (!id.equals(requestRemoteProcessGroup.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroup.getId(), id));
        }

        final PositionDTO proposedPosition = requestRemoteProcessGroup.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupEntity);
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemoteProcessGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getRemoteProcessGroup(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroup(requestRemoteProcessGroup),
                (revision, remoteProcessGroupEntity) -> {
                    final RemoteProcessGroupDTO remoteProcessGroup = remoteProcessGroupEntity.getComponent();

                    // if the target uri is set we have to verify it here - we don't support updating the target uri on
                    // an existing remote process group, however if the remote process group is being created with an id
                    // as is the case in clustered mode we need to verify the remote process group. treat this request as
                    // though its a new remote process group.
                    if (remoteProcessGroup.getTargetUri() != null) {
                        // parse the uri
                        final URI uri;
                        try {
                            uri = URI.create(remoteProcessGroup.getTargetUri());
                        } catch (final IllegalArgumentException e) {
                            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + remoteProcessGroup.getTargetUri());
                        }

                        // validate each part of the uri
                        if (uri.getScheme() == null || uri.getHost() == null) {
                            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + remoteProcessGroup.getTargetUri());
                        }

                        if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
                            throw new IllegalArgumentException("The specified remote process group URL is invalid because it is not http or https: " + remoteProcessGroup.getTargetUri());
                        }

                        // normalize the uri to the other controller
                        String controllerUri = uri.toString();
                        if (controllerUri.endsWith("/")) {
                            controllerUri = StringUtils.substringBeforeLast(controllerUri, "/");
                        }

                        // update with the normalized uri
                        remoteProcessGroup.setTargetUri(controllerUri);
                    }

                    // update the specified remote process group
                    final RemoteProcessGroupEntity entity = serviceFacade.updateRemoteProcessGroup(revision, remoteProcessGroup);
                    populateRemainingRemoteProcessGroupEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
