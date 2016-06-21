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
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UpdateResult;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

@Path("/users")
@Api(
        value = "/users",
        description = "Endpoint for managing users."
)
public class UsersResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public UsersResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, NiFiProperties properties, RequestReplicator requestReplicator, ClusterCoordinator clusterCoordinator) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
    }

    /**
     * Populates the uri for the specified user.
     *
     * @param userEntity userEntity
     * @return userEntity
     */
    public UserEntity populateRemainingUserEntityContent(UserEntity userEntity) {
        if (userEntity.getComponent() != null) {
            populateRemainingUserContent(userEntity.getComponent());
        }
        return userEntity;
    }

    /**
     * Populates the uri for the specified user.
     */
    public UserDTO populateRemainingUserContent(UserDTO user) {
        // populate the user href
        user.setUri(generateResourceUri("users", user.getId()));
        return user;
    }

    /**
     * Creates a new user.
     *
     * @param httpServletRequest request
     * @param userEntity         An userEntity.
     * @return An userEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a user",
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response createUser(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user configuration details.",
                    required = true
            ) final UserEntity userEntity) {

        if (userEntity == null || userEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (userEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User ID cannot be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, userEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable users = lookup.getUsersAuthorizable();
                users.authorize(authorizer, RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the user id as appropriate
        userEntity.getComponent().setId(generateUuid());

        // get revision from the config
        final RevisionDTO revisionDTO = userEntity.getRevision();
        Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userEntity.getComponent().getId());

        // create the user and generate the json
        final UserEntity entity = serviceFacade.createUser(revision, userEntity.getComponent());
        populateRemainingUserEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
    }

    /**
     * Retrieves the specified user.
     *
     * @param id The id of the user to retrieve
     * @return An userEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a user",
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
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
    public Response getUser(
            @ApiParam(
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable users = lookup.getUsersAuthorizable();
            users.authorize(authorizer, RequestAction.READ);
        });

        // get the user
        final UserEntity entity = serviceFacade.getUser(id, true);
        populateRemainingUserEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates a user.
     *
     * @param httpServletRequest request
     * @param id                 The id of the user to update.
     * @param userEntity         An userEntity.
     * @return An userEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a user",
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response updateUser(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The user configuration details.",
                    required = true
            ) final UserEntity userEntity) {

        if (userEntity == null || userEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (userEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserDTO userDTO = userEntity.getComponent();
        if (!id.equals(userDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user id (%s) in the request body does not equal the "
                    + "user id of the requested resource (%s).", userDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, userEntity);
        }

        // Extract the revision
        final Revision revision = getRevision(userEntity, id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    final Authorizable users = lookup.getUsersAuthorizable();
                    users.authorize(authorizer, RequestAction.WRITE);
                },
                null,
                () -> {
                    // update the user
                    final UpdateResult<UserEntity> updateResult = serviceFacade.updateUser(revision, userDTO);

                    // get the results
                    final UserEntity entity = updateResult.getResult();
                    populateRemainingUserEntityContent(entity);

                    if (updateResult.isNew()) {
                        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
                    } else {
                        return clusterContext(generateOkResponse(entity)).build();
                    }
                }
        );
    }

    /**
     * Removes the specified user.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the user to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a user",
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
    public Response removeUser(
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
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    final Authorizable users = lookup.getUsersAuthorizable();
                    users.authorize(authorizer, RequestAction.READ);
                },
                () -> {
                },
                () -> {
                    // delete the specified user
                    final UserEntity entity = serviceFacade.deleteUser(revision, id);
                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }
}
