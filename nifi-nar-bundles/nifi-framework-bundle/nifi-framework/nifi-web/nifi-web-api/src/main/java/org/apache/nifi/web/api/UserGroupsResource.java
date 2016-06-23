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
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.UserGroupEntity;
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

@Path("/user-groups")
@Api(
        value = "/user-groups",
        description = "Endpoint for managing user groups."
)
public class UserGroupsResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public UserGroupsResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, NiFiProperties properties, RequestReplicator requestReplicator, ClusterCoordinator clusterCoordinator) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
    }

    /**
     * Populates the uri for the specified user group.
     *
     * @param userGroupEntity userGroupEntity
     * @return userGroupEntity
     */
    public UserGroupEntity populateRemainingUserGroupEntityContent(UserGroupEntity userGroupEntity) {
        if (userGroupEntity.getComponent() != null) {
            populateRemainingUserGroupContent(userGroupEntity.getComponent());
        }
        return userGroupEntity;
    }

    /**
     * Populates the uri for the specified userGroup.
     */
    public UserGroupDTO populateRemainingUserGroupContent(UserGroupDTO userGroup) {
        // populate the user group href
        userGroup.setUri(generateResourceUri("user-groups", userGroup.getId()));
        return userGroup;
    }

    /**
     * Creates a new user group.
     *
     * @param httpServletRequest request
     * @param userGroupEntity    An userGroupEntity.
     * @return An userGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a user group",
            response = UserGroupEntity.class,
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
    public Response createUserGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user group configuration details.",
                    required = true
            ) final UserGroupEntity userGroupEntity) {

        if (userGroupEntity == null || userGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (userGroupEntity.getRevision() == null || (userGroupEntity.getRevision().getVersion() == null || userGroupEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Processor.");
        }

        if (userGroupEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User group ID cannot be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, userGroupEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable userGroups = lookup.getUserGroupsAuthorizable();
                userGroups.authorize(authorizer, RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the user group id as appropriate
        userGroupEntity.getComponent().setId(generateUuid());

        // get revision from the config
        final RevisionDTO revisionDTO = userGroupEntity.getRevision();
        Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userGroupEntity.getComponent().getId());

        // create the user group and generate the json
        final UserGroupEntity entity = serviceFacade.createUserGroup(revision, userGroupEntity.getComponent());
        populateRemainingUserGroupEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
    }

    /**
     * Retrieves the specified user group.
     *
     * @param id The id of the user group to retrieve
     * @return An userGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a user group",
            response = UserGroupEntity.class,
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
    public Response getUserGroup(
            @ApiParam(
                    value = "The user group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable userGroups = lookup.getUserGroupsAuthorizable();
            userGroups.authorize(authorizer, RequestAction.READ);
        });

        // get the user group
        final UserGroupEntity entity = serviceFacade.getUserGroup(id, true);
        populateRemainingUserGroupEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates a user group.
     *
     * @param httpServletRequest request
     * @param id                 The id of the user group to update.
     * @param userGroupEntity    An userGroupEntity.
     * @return An userGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a user group",
            response = UserGroupEntity.class,
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
    public Response updateUserGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The user group configuration details.",
                    required = true
            ) final UserGroupEntity userGroupEntity) {

        if (userGroupEntity == null || userGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (userGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserGroupDTO userGroupDTO = userGroupEntity.getComponent();
        if (!id.equals(userGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user group id (%s) in the request body does not equal the "
                    + "user group id of the requested resource (%s).", userGroupDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, userGroupEntity);
        }

        // Extract the revision
        final Revision revision = getRevision(userGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    final Authorizable userGroups = lookup.getUserGroupsAuthorizable();
                    userGroups.authorize(authorizer, RequestAction.WRITE);
                },
                null,
                () -> {
                    // update the user group
                    final UserGroupEntity entity = serviceFacade.updateUserGroup(revision, userGroupDTO);
                    populateRemainingUserGroupEntityContent(entity);

                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }

    /**
     * Removes the specified user group.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the user group to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a user group",
            response = UserGroupEntity.class,
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
    public Response removeUserGroup(
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
                    value = "The user group id.",
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
                    final Authorizable userGroups = lookup.getUserGroupsAuthorizable();
                    userGroups.authorize(authorizer, RequestAction.READ);
                },
                () -> {
                },
                () -> {
                    // delete the specified user group
                    final UserGroupEntity entity = serviceFacade.deleteUserGroup(revision, id);
                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }
}
