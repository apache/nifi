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

import com.sun.jersey.api.Responses;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.search.UserGroupSearchResultDTO;
import org.apache.nifi.web.api.dto.search.UserSearchResultDTO;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserSearchResultsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.NiFiServiceFacade;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing this Controller's users.
 */
@Api(hidden = true)
public class UserResource extends ApplicationResource {

    /*
     * Developer Note: Clustering assumes a centralized security provider. The
     * cluster manager will manage user accounts when in clustered mode and
     * interface with the authorization provider. However, when nodes perform
     * Site-to-Site, the authorization details of the remote NiFi will be cached
     * locally. These details need to be invalidated when certain actions are
     * performed (revoking/deleting accounts, changing user authorities, user
     * group, etc).
     */
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private NiFiServiceFacade serviceFacade;

    /**
     * Creates a new user account request.
     *
     * @return A string
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("") // necessary due to a bug in swagger
    @ApiOperation(
            value = "Creates a user",
            response = String.class
    )
    public Response createUser() {
        if (!properties.getSupportNewAccountRequests()) {
            return Responses.notFound().entity("This NiFi does not support new account requests.").build();
        }

        final NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
        if (nifiUser != null) {
            throw new IllegalArgumentException("User account already created " + nifiUser.getIdentity());
        }

        // create an account request for the current user
        final UserDTO user = serviceFacade.createUser();

        final String uri = generateResourceUri("controller", "users", user.getId());
        return generateCreatedResponse(URI.create(uri), "Not authorized. User account created. Authorization pending.").build();
    }

    /**
     * Gets all users that are registered within this Controller.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param grouped Whether to return the users in their groups.
     * @return A usersEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to a bug in swagger
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all users",
            response = UsersEntity.class,
            authorizations = {
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
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
    public Response getUsers(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether to return the users in their respective groups.",
                    required = false
            )
            @QueryParam("grouped") @DefaultValue("false") Boolean grouped) {

        // get the users
        final Collection<UserDTO> users = serviceFacade.getUsers(grouped);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final UsersEntity usersEntity = new UsersEntity();
        usersEntity.setRevision(revision);
        usersEntity.setUsers(users);
        usersEntity.setGenerated(new Date());

        // build the response
        return generateOkResponse(usersEntity).build();
    }

    /**
     * Gets the details for the specified user.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The user id.
     * @return A userEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @Path("/{id}")
    @ApiOperation(
            value = "Gets a user",
            response = UserEntity.class,
            authorizations = {
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
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") String id) {

        // get the specified user
        final UserDTO userDTO = serviceFacade.getUser(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final UserEntity userEntity = new UserEntity();
        userEntity.setRevision(revision);
        userEntity.setUser(userDTO);

        // build the response
        return generateOkResponse(userEntity).build();
    }

    /**
     * Searches for users with match the specified query.
     *
     * @param value Search value that will be matched against users
     * @return A userSearchResultsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/search-results")
    @PreAuthorize("hasAnyRole('ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Searches for users",
            response = UserSearchResultsEntity.class,
            authorizations = {
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
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
    public Response searchUsers(
            @ApiParam(
                    value = "The search terms.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        final List<UserSearchResultDTO> userMatches = new ArrayList<>();
        final List<UserGroupSearchResultDTO> userGroupMatches = new ArrayList<>();

        // get the users
        final Collection<UserDTO> users = serviceFacade.getUsers(Boolean.FALSE);
        final Collection<String> matchedGroups = new HashSet<>();

        // check each to see if it matches the search term
        for (UserDTO user : users) {
            // count the user if there is no search or it matches the address
            if (StringUtils.isBlank(value)) {
                // record the group match if there is one and it hasn't already been encountered
                if (user.getUserGroup() != null && !matchedGroups.contains(user.getUserGroup())) {
                    // add the matched group
                    matchedGroups.add(user.getUserGroup());

                    // record the group match
                    final UserGroupSearchResultDTO userGroupMatch = new UserGroupSearchResultDTO();
                    userGroupMatch.setGroup(user.getUserGroup());
                    userGroupMatches.add(userGroupMatch);
                }

                // record the user match
                final UserSearchResultDTO userMatch = new UserSearchResultDTO();
                userMatch.setUserDn(user.getDn());
                userMatch.setUserName(user.getUserName());
                userMatches.add(userMatch);
            } else {
                // look for a user match
                if (StringUtils.containsIgnoreCase(user.getDn(), value) || StringUtils.containsIgnoreCase(user.getUserName(), value)) {
                    // record the user match
                    final UserSearchResultDTO userMatch = new UserSearchResultDTO();
                    userMatch.setUserDn(user.getDn());
                    userMatch.setUserName(user.getUserName());
                    userMatches.add(userMatch);
                }

                // look for a dn match
                if (StringUtils.containsIgnoreCase(user.getUserGroup(), value)) {
                    // record the group match if it hasn't already been encountered
                    if (!matchedGroups.contains(user.getUserGroup())) {
                        // add the matched group
                        matchedGroups.add(user.getUserGroup());

                        // record the group match
                        final UserGroupSearchResultDTO userGroupMatch = new UserGroupSearchResultDTO();
                        userGroupMatch.setGroup(user.getUserGroup());
                        userGroupMatches.add(userGroupMatch);
                    }
                }
            }
        }

        // sort the user matches
        Collections.sort(userMatches, new Comparator<UserSearchResultDTO>() {
            @Override
            public int compare(UserSearchResultDTO user1, UserSearchResultDTO user2) {
                return user1.getUserName().compareTo(user2.getUserName());
            }
        });

        // sort the user group matches
        Collections.sort(userGroupMatches, new Comparator<UserGroupSearchResultDTO>() {
            @Override
            public int compare(UserGroupSearchResultDTO userGroup1, UserGroupSearchResultDTO userGroup2) {
                return userGroup1.getGroup().compareTo(userGroup2.getGroup());
            }
        });

        // build the response
        final UserSearchResultsEntity results = new UserSearchResultsEntity();
        results.setUserResults(userMatches);
        results.setUserGroupResults(userGroupMatches);

        // generate an 200 - OK response
        return noCache(Response.ok(results)).build();
    }

    /**
     * Updates the specified user.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the user to update.
     * @param rawAuthorities Array of authorities to assign to the specified user.
     * @param status The status of the specified users account.
     * @param formParams form params
     * @return A userEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @Path("/{id}")
    public Response updateUser(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @FormParam("authorities[]") Set<String> rawAuthorities,
            @FormParam("status") String status,
            MultivaluedMap<String, String> formParams) {

        // create the user
        final UserDTO userDTO = new UserDTO();
        userDTO.setId(id);
        userDTO.setStatus(status);

        // get the collection of specified authorities
        final Set<String> authorities = new HashSet<>();
        for (String authority : rawAuthorities) {
            if (StringUtils.isNotBlank(authority)) {
                authorities.add(authority);
            }
        }

        // set the authorities
        if (!authorities.isEmpty() || formParams.containsKey("authorities")) {
            userDTO.setAuthorities(authorities);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the user entity
        UserEntity userEntity = new UserEntity();
        userEntity.setRevision(revision);
        userEntity.setUser(userDTO);

        // update the user
        return updateUser(httpServletRequest, id, userEntity);
    }

    /**
     * Updates the specified user.
     *
     * @param httpServletRequest request
     * @param id The id of the user to update.
     * @param userEntity A userEntity
     * @return A userEntity
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @Path("/{id}")
    @ApiOperation(
            value = "Updates a user",
            response = UserEntity.class,
            authorizations = {
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
    public Response updateUser(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The user configuration details.",
                    required = true
            ) UserEntity userEntity) {

        if (userEntity == null || userEntity.getUser() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        // ensure the same user id is being used
        final UserDTO userDTO = userEntity.getUser();
        if (!id.equals(userDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user id (%s) in the request body does "
                    + "not equal the user id of the requested resource (%s).", userDTO.getId(), id));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        if (userEntity.getRevision() == null) {
            revision.setClientId(new ClientIdParameter().getClientId());
        } else {
            revision.setClientId(userEntity.getRevision().getClientId());
        }

        // this user is being modified, replicate to the nodes to invalidate this account
        // so that it will be re-authorized during the next attempted access - if this wasn't
        // done the account would remain stale for up to the configured cache duration. this
        // is acceptable sometimes but when updating a users authorities or groups via the UI
        // they shouldn't have to wait for the changes to take effect`
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // identify yourself as the NCM attempting to invalidate the user
            final Map<String, String> headers = getHeaders(headersToOverride);
            headers.put(WebClusterManager.CLUSTER_INVALIDATE_USER_HEADER, Boolean.TRUE.toString());

            final RevisionDTO invalidateUserRevision = new RevisionDTO();
            revision.setClientId(revision.getClientId());

            final UserDTO invalidateUser = new UserDTO();
            invalidateUser.setId(userDTO.getId());

            final UserEntity invalidateUserEntity = new UserEntity();
            invalidateUserEntity.setRevision(invalidateUserRevision);
            invalidateUserEntity.setUser(userDTO);

            // replicate the invalidate request to each node - if this request is not successful return that fact,
            // otherwise continue with the desired user modification
            final NodeResponse response = clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), invalidateUserEntity, headers);
            if (!response.is2xx()) {
                return response.getResponse();
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // handle an invalidate request from the NCM
        final String invalidateRequest = httpServletRequest.getHeader(WebClusterManager.CLUSTER_INVALIDATE_USER_HEADER);
        if (invalidateRequest != null) {
            serviceFacade.invalidateUser(id);
            return generateOkResponse().build();
        }

        // update the user
        final UserDTO reponseUserDTO = serviceFacade.updateUser(userDTO);

        // create the response entity
        UserEntity responseUserEntity = new UserEntity();
        responseUserEntity.setRevision(revision);
        responseUserEntity.setUser(reponseUserDTO);

        // build the response
        return generateOkResponse(responseUserEntity).build();
    }

    /**
     * Deletes the specified user.
     *
     * @param httpServletRequest request
     * @param id The user id
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A userEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Deletes a user",
            response = UserEntity.class,
            authorizations = {
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
    public Response deleteUser(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The user id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // this user is being modified, replicate to the nodes to invalidate this account
        // so that it will be re-authorized during the next attempted access - if this wasn't
        // done the account would remain stale for up to the configured cache duration. this
        // is acceptable sometimes but when removing a user via the UI they shouldn't have to
        // wait for the changes to take effect
        if (properties.isClusterManager()) {
            // identify yourself as the NCM attempting to invalidate the user
            final Map<String, String> headers = getHeaders();
            headers.put(WebClusterManager.CLUSTER_INVALIDATE_USER_HEADER, Boolean.TRUE.toString());

            // replicate the invalidate request to each node - if this request is not successful return that fact,
            // otherwise continue with the desired user modification
            final NodeResponse response = clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), headers);
            if (!response.is2xx()) {
                return response.getResponse();
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // handle an invalidate request from the NCM
        final String invalidateRequest = httpServletRequest.getHeader(WebClusterManager.CLUSTER_INVALIDATE_USER_HEADER);
        if (invalidateRequest != null) {
            serviceFacade.invalidateUser(id);
            return generateOkResponse().build();
        }

        // ungroup the specified user
        serviceFacade.deleteUser(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final UserEntity entity = new UserEntity();
        entity.setRevision(revision);

        // generate ok response
        return generateOkResponse(entity).build();
    }

    /* setters */
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }
}
