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
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.TenantDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.TenantsEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.dao.AccessPolicyDAO;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Path("tenants")
@Api(
        value = "tenants",
        description = "Endpoint for managing users and user groups."
)
public class TenantsResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public TenantsResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, NiFiProperties properties, RequestReplicator requestReplicator,
        ClusterCoordinator clusterCoordinator, FlowController flowController) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
        setFlowController(flowController);
    }

    /**
     * Populates the uri for the specified users.
     *
     * @param userEntities users
     * @return user entities
     */
    public Set<UserEntity> populateRemainingUserEntitiesContent(Set<UserEntity> userEntities) {
        for (UserEntity userEntity : userEntities) {
            populateRemainingUserEntityContent(userEntity);
        }
        return userEntities;
    }

    /**
     * Populates the uri for the specified user.
     *
     * @param userEntity userEntity
     * @return userEntity
     */
    public UserEntity populateRemainingUserEntityContent(UserEntity userEntity) {
        userEntity.setUri(generateResourceUri("tenants", "users", userEntity.getId()));
        return userEntity;
    }

    /**
     * Creates a new user.
     *
     * @param httpServletRequest request
     * @param requestUserEntity         An userEntity.
     * @return An userEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @ApiOperation(
            value = "Creates a user",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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
            ) final UserEntity requestUserEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserEntity == null || requestUserEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (requestUserEntity.getRevision() == null || (requestUserEntity.getRevision().getVersion() == null || requestUserEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new User.");
        }

        if (requestUserEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestUserEntity.getComponent().getIdentity())) {
            throw new IllegalArgumentException("User identity must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestUserEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                userEntity -> {
                    // set the user id as appropriate
                    userEntity.getComponent().setId(generateUuid());

                    // get revision from the config
                    final RevisionDTO revisionDTO = userEntity.getRevision();
                    Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userEntity.getComponent().getId());

                    // create the user and generate the json
                    final UserEntity entity = serviceFacade.createUser(revision, userEntity.getComponent());
                    populateRemainingUserEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
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
    @Path("users/{id}")
    @ApiOperation(
            value = "Gets a user",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /tenants", type = "")
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

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the user
        final UserEntity entity = serviceFacade.getUser(id);
        populateRemainingUserEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves all the of users in this NiFi.
     *
     * @return A UsersEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @ApiOperation(
            value = "Gets all users",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UsersEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /tenants", type = "")
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
    public Response getUsers() {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the users
        final Set<UserEntity> users = serviceFacade.getUsers();

        // create the response entity
        final UsersEntity entity = new UsersEntity();
        entity.setGenerated(new Date());
        entity.setUsers(populateRemainingUserEntitiesContent(users));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates a user.
     *
     * @param httpServletRequest request
     * @param id                 The id of the user to update.
     * @param requestUserEntity         An userEntity.
     * @return An userEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @ApiOperation(
            value = "Updates a user",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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
            ) final UserEntity requestUserEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserEntity == null || requestUserEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (requestUserEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserDTO requestUserDTO = requestUserEntity.getComponent();
        if (!id.equals(requestUserDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user id (%s) in the request body does not equal the "
                    + "user id of the requested resource (%s).", requestUserDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUserEntity);
        }

        // Extract the revision
        final Revision requestRevision = getRevision(requestUserEntity, id);
        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userEntity) -> {
                    // update the user
                    final UserEntity entity = serviceFacade.updateUser(revision, userEntity.getComponent());
                    populateRemainingUserEntityContent(entity);

                    return generateOkResponse(entity).build();
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
    @Path("users/{id}")
    @ApiOperation(
            value = "Deletes a user",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final UserEntity requestUserEntity = new UserEntity();
        requestUserEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userEntity) -> {
                    // delete the specified user
                    final UserEntity entity = serviceFacade.deleteUser(revision, userEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Populates the uri for the specified user groups.
     *
     * @param userGroupEntities user groups
     * @return user group entities
     */
    public Set<UserGroupEntity> populateRemainingUserGroupEntitiesContent(Set<UserGroupEntity> userGroupEntities) {
        for (UserGroupEntity userGroupEntity : userGroupEntities) {
            populateRemainingUserGroupEntityContent(userGroupEntity);
        }
        return userGroupEntities;
    }

    /**
     * Populates the uri for the specified user group.
     *
     * @param userGroupEntity userGroupEntity
     * @return userGroupEntity
     */
    public UserGroupEntity populateRemainingUserGroupEntityContent(UserGroupEntity userGroupEntity) {
        userGroupEntity.setUri(generateResourceUri("tenants", "user-groups", userGroupEntity.getId()));
        return userGroupEntity;
    }

    /**
     * Creates a new user group.
     *
     * @param httpServletRequest request
     * @param requestUserGroupEntity    An userGroupEntity.
     * @return An userGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @ApiOperation(
            value = "Creates a user group",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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
            ) final UserGroupEntity requestUserGroupEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserGroupEntity == null || requestUserGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (requestUserGroupEntity.getRevision() == null || (requestUserGroupEntity.getRevision().getVersion() == null || requestUserGroupEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new User Group.");
        }

        if (requestUserGroupEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User group ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestUserGroupEntity.getComponent().getIdentity())) {
            throw new IllegalArgumentException("User group identity must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestUserGroupEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                userGroupEntity -> {
                    // set the user group id as appropriate
                    userGroupEntity.getComponent().setId(generateUuid());

                    // get revision from the config
                    final RevisionDTO revisionDTO = userGroupEntity.getRevision();
                    Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userGroupEntity.getComponent().getId());

                    // create the user group and generate the json
                    final UserGroupEntity entity = serviceFacade.createUserGroup(revision, userGroupEntity.getComponent());
                    populateRemainingUserGroupEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
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
    @Path("user-groups/{id}")
    @ApiOperation(
            value = "Gets a user group",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /tenants", type = "")
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

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the user group
        final UserGroupEntity entity = serviceFacade.getUserGroup(id);
        populateRemainingUserGroupEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves all the of user groups in this NiFi.
     *
     * @return A UserGroupsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @ApiOperation(
            value = "Gets all user groups",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserGroupsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /tenants", type = "")
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
    public Response getUserGroups() {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the user groups
        final Set<UserGroupEntity> users = serviceFacade.getUserGroups();

        // create the response entity
        final UserGroupsEntity entity = new UserGroupsEntity();
        entity.setUserGroups(populateRemainingUserGroupEntitiesContent(users));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates a user group.
     *
     * @param httpServletRequest request
     * @param id                 The id of the user group to update.
     * @param requestUserGroupEntity    An userGroupEntity.
     * @return An userGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @ApiOperation(
            value = "Updates a user group",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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
            ) final UserGroupEntity requestUserGroupEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserGroupEntity == null || requestUserGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (requestUserGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserGroupDTO requestUserGroupDTO = requestUserGroupEntity.getComponent();
        if (!id.equals(requestUserGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user group id (%s) in the request body does not equal the "
                    + "user group id of the requested resource (%s).", requestUserGroupDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUserGroupEntity);
        }

        // Extract the revision
        final Revision requestRevision = getRevision(requestUserGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userGroupEntity) -> {
                    // update the user group
                    final UserGroupEntity entity = serviceFacade.updateUserGroup(revision, userGroupEntity.getComponent());
                    populateRemainingUserGroupEntityContent(entity);

                    return generateOkResponse(entity).build();
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
    @Path("user-groups/{id}")
    @ApiOperation(
            value = "Deletes a user group",
            notes = NON_GUARANTEED_ENDPOINT,
            response = UserGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /tenants", type = "")
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

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final UserGroupEntity requestUserGroupEntity = new UserGroupEntity();
        requestUserGroupEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userGroupEntity) -> {
                    // delete the specified user group
                    final UserGroupEntity entity = serviceFacade.deleteUserGroup(revision, userGroupEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    // ------------
    // search users
    // ------------

    /**
     * Searches for a tenant with a given identity.
     *
     * @param value Search value that will be matched against a user/group identity
     * @return Tenants match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-results")
    @ApiOperation(
            value = "Searches for a tenant with the specified identity",
            notes = NON_GUARANTEED_ENDPOINT,
            response = TenantsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /tenants", type = "")
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
    public Response searchTenants(
            @ApiParam(
                    value = "Identity to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        final List<TenantEntity> userMatches = new ArrayList<>();
        final List<TenantEntity> userGroupMatches = new ArrayList<>();

        // get the users
        for (final UserEntity userEntity : serviceFacade.getUsers()) {
            final UserDTO user = userEntity.getComponent();
            if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(user.getIdentity(), value)) {
                final TenantDTO tenant = new TenantDTO();
                tenant.setId(user.getId());
                tenant.setIdentity(user.getIdentity());
                tenant.setConfigurable(user.getConfigurable());

                final TenantEntity entity = new TenantEntity();
                entity.setPermissions(userEntity.getPermissions());
                entity.setRevision(userEntity.getRevision());
                entity.setId(userEntity.getId());
                entity.setComponent(tenant);

                userMatches.add(entity);
            }
        }

        // get the user groups
        for (final UserGroupEntity userGroupEntity : serviceFacade.getUserGroups()) {
            final UserGroupDTO userGroup = userGroupEntity.getComponent();
            if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(userGroup.getIdentity(), value)) {
                final TenantDTO tenant = new TenantDTO();
                tenant.setId(userGroup.getId());
                tenant.setIdentity(userGroup.getIdentity());
                tenant.setConfigurable(userGroup.getConfigurable());

                final TenantEntity entity = new TenantEntity();
                entity.setPermissions(userGroupEntity.getPermissions());
                entity.setRevision(userGroupEntity.getRevision());
                entity.setId(userGroupEntity.getId());
                entity.setComponent(tenant);

                userGroupMatches.add(entity);
            }
        }

        // build the response
        final TenantsEntity results = new TenantsEntity();
        results.setUsers(userMatches);
        results.setUserGroups(userGroupMatches);

        // generate an 200 - OK response
        return noCache(Response.ok(results)).build();
    }
}
