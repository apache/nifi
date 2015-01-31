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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.HttpMethod;
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
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing this Controller's user groups.
 */
public class UserGroupResource extends ApplicationResource {

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
     * Updates a new user group.
     *
     * @param httpServletRequest
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param userIds A collection of user ids to include in this group. If a
     * user already belongs to another group, they will be placed in this group
     * instead. Existing users in this group will remain in this group.
     * @param group The name of the group.
     * @param rawAuthorities Array of authorities to assign to the specified
     * user.
     * @param status The status of the specified users account.
     * @param formParams
     * @return A userGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{group}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @TypeHint(UserGroupEntity.class)
    public Response updateUserGroup(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("group") String group,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("userIds[]") Set<String> userIds,
            @FormParam("authorities[]") Set<String> rawAuthorities,
            @FormParam("status") String status,
            MultivaluedMap<String, String> formParams) {

        // get the collection of specified authorities
        final Set<String> authorities = new HashSet<>();
        for (String authority : rawAuthorities) {
            if (StringUtils.isNotBlank(authority)) {
                authorities.add(authority);
            }
        }

        // create the user group dto
        final UserGroupDTO userGroup = new UserGroupDTO();
        userGroup.setGroup(group);
        userGroup.setUserIds(userIds);
        userGroup.setStatus(status);

        // set the authorities
        if (!authorities.isEmpty() || formParams.containsKey("authorities")) {
            userGroup.setAuthorities(authorities);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the user group entity
        final UserGroupEntity entity = new UserGroupEntity();
        entity.setRevision(revision);
        entity.setUserGroup(userGroup);

        // create the user group
        return updateUserGroup(httpServletRequest, group, entity);
    }

    /**
     * Creates a new user group with the specified users.
     *
     * @param httpServletRequest
     * @param group The user group.
     * @param userGroupEntity A userGroupEntity.
     * @return A userGroupEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{group}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @TypeHint(UserGroupEntity.class)
    public Response updateUserGroup(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("group") String group,
            UserGroupEntity userGroupEntity) {

        if (userGroupEntity == null || userGroupEntity.getUserGroup() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        // get the user group
        UserGroupDTO userGroup = userGroupEntity.getUserGroup();

        // ensure the same id is being used
        if (!group.equals(userGroup.getGroup())) {
            throw new IllegalArgumentException(String.format("The user group (%s) in the request body does "
                    + "not equal the user group of the requested resource (%s).", userGroup.getGroup(), group));
        }

        // the user group must be specified and cannot be blank
        if (StringUtils.isBlank(userGroup.getGroup())) {
            throw new IllegalArgumentException("User group must be specified and cannot be blank.");
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        if (userGroupEntity.getRevision() == null) {
            revision.setClientId(new ClientIdParameter().getClientId());
        } else {
            revision.setClientId(userGroupEntity.getRevision().getClientId());
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
            headers.put(WebClusterManager.CLUSTER_INVALIDATE_USER_GROUP_HEADER, Boolean.TRUE.toString());

            final RevisionDTO invalidateUserRevision = new RevisionDTO();
            revision.setClientId(revision.getClientId());

            final UserGroupDTO invalidateUserGroup = new UserGroupDTO();
            invalidateUserGroup.setGroup(group);
            invalidateUserGroup.setUserIds(userGroup.getUserIds());

            final UserGroupEntity invalidateUserGroupEntity = new UserGroupEntity();
            invalidateUserGroupEntity.setRevision(invalidateUserRevision);
            invalidateUserGroupEntity.setUserGroup(invalidateUserGroup);

            // replicate the invalidate request to each node - if this request is not successful return that fact,
            // otherwise continue with the desired user modification
            final NodeResponse response = clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), invalidateUserGroupEntity, headers);
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
        final String invalidateRequest = httpServletRequest.getHeader(WebClusterManager.CLUSTER_INVALIDATE_USER_GROUP_HEADER);
        if (invalidateRequest != null) {
            serviceFacade.invalidateUserGroup(userGroup.getGroup(), userGroup.getUserIds());
            return generateOkResponse().build();
        }

        // create the user group
        userGroup = serviceFacade.updateUserGroup(userGroup);

        // create the response entity
        final UserGroupEntity entity = new UserGroupEntity();
        entity.setRevision(revision);
        entity.setUserGroup(userGroup);

        // generate the URI for this group and return
        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the user from the specified group. The user will not be removed,
     * just the fact that they were in this group.
     *
     * @param httpServletRequest
     * @param group The user group.
     * @param userId The user id to remove.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A userGroupEntity.
     */
    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{group}/users/{userId}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @TypeHint(UserGroupEntity.class)
    public Response removeUserFromGroup(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("group") String group,
            @PathParam("userId") String userId,
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
            serviceFacade.invalidateUser(userId);
            return generateOkResponse().build();
        }

        // ungroup the specified user
        serviceFacade.removeUserFromGroup(userId);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final UserGroupEntity entity = new UserGroupEntity();
        entity.setRevision(revision);

        // generate ok response
        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the user group. The users will not be removed, just the fact that
     * they were grouped.
     *
     * @param httpServletRequest
     * @param group The user group.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A userGroupEntity.
     */
    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{group}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @TypeHint(UserGroupEntity.class)
    public Response ungroup(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("group") String group,
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // this user is being modified, replicate to the nodes to invalidate this account
        // so that it will be re-authorized during the next attempted access - if this wasn't
        // done the account would remain stale for up to the configured cache duration. this
        // is acceptable sometimes but when removing a user via the UI they shouldn't have to 
        // wait for the changes to take effect
        if (properties.isClusterManager()) {
            // identify yourself as the NCM attempting to invalidate the user
            final Map<String, String> headers = getHeaders();
            headers.put(WebClusterManager.CLUSTER_INVALIDATE_USER_GROUP_HEADER, Boolean.TRUE.toString());

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
        final String invalidateRequest = httpServletRequest.getHeader(WebClusterManager.CLUSTER_INVALIDATE_USER_GROUP_HEADER);
        if (invalidateRequest != null) {
            serviceFacade.invalidateUserGroup(group, null);
            return generateOkResponse().build();
        }

        // delete the user group
        serviceFacade.removeUserGroup(group);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final UserGroupEntity entity = new UserGroupEntity();
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
