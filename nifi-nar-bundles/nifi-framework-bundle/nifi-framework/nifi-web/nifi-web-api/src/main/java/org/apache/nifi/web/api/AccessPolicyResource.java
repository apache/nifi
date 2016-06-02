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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
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

/**
 * RESTful endpoint for managing access policies.
 */
@Path("/policies")
@Api(
        value = "/policies",
        description = "Endpoint for managing access policies."
)
public class AccessPolicyResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public AccessPolicyResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, NiFiProperties properties, RequestReplicator requestReplicator, ClusterCoordinator clusterCoordinator) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
    }

    /**
     * Populates the uri for the specified access policy.
     *
     * @param accessPolicyEntity accessPolicyEntity
     * @return accessPolicyEntity
     */
    public AccessPolicyEntity populateRemainingAccessPolicyEntityContent(AccessPolicyEntity accessPolicyEntity) {
        if (accessPolicyEntity.getComponent() != null) {
            populateRemainingAccessPolicyContent(accessPolicyEntity.getComponent());
        }
        return accessPolicyEntity;
    }

    /**
     * Populates the uri for the specified accessPolicy.
     */
    public AccessPolicyDTO populateRemainingAccessPolicyContent(AccessPolicyDTO accessPolicy) {
        // populate the access policy href
        accessPolicy.setUri(generateResourceUri("policies", accessPolicy.getId()));
        return accessPolicy;
    }

    /**
     * Creates a new access policy.
     *
     * @param httpServletRequest request
     * @param accessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates an access policy",
            notes = "    Available resources:\n" +
                    "        /flow                       - READ - allows user/entity to load the UI and see the flow structure\n" +
                    "                                    - WRITE - NA\n" +
                    "        /resource                   - READ - allows user/entity to retrieve the available resources\n" +
                    "                                    - WRITE - NA\n" +
                    "        /system                     - READ - allows user/entity to retrieve system level diagnostics (CPU load, disk utilization, etc)\n" +
                    "                                    - WRITE - NA\n" +
                    "        /controller                 - READ - allows user/entity to retrieve configuration details for the controller (controller bulletins, thread pool, reporting tasks, etc)\n" +
                    "                                    - WRITE - allows user/entity to modify configuration details for the controller\n" +
                    "        /provenance                 - READ - allows user/entity to perform provenance requests. results will be filtered based on access to provenance data per component\n" +
                    "                                    - WRITE - NA\n" +
                    "        /token                      - READ - NA\n" +
                    "                                    - WRITE - allows user/entity to create a token for access the REST API\n" +
                    "        /site-to-site               - READ - allows user/entity to retrieve configuration details for performing site to site data transfers with this NiFi\n" +
                    "                                    - WRITE - NA\n" +
                    "        /proxy                      - READ - NA\n" +
                    "                                    - WRITE - allows user/entity to create a proxy request on behalf of another user\n" +
                    "        /process-groups/{id}        - READ - allows user/entity to retrieve configuration details for the process group and all descendant components without explicit " +
                    "access policies\n" +
                    "                                    - WRITE - allows user/entity to create/update/delete configuration details for the process group and all descendant components without " +
                    "explicit access policies\n" +
                    "        /processors/{id}            - READ - allows user/entity to retrieve configuration details for the processor overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the processor overriding any inherited authorizations from an ancestor process group\n" +
                    "        /input-ports/{id}           - READ - allows user/entity to retrieve configuration details for the input port overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the input port overriding any inherited authorizations from an ancestor process group\n" +
                    "        /output-ports/{id}          - READ - allows user/entity to retrieve configuration details for the output port overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the output port overriding any inherited authorizations from an ancestor process group\n" +
                    "        /labels/{id}                - READ - allows user/entity to retrieve configuration details for the label overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the label overriding any inherited authorizations from an ancestor process group\n" +
                    "        /connections/{id}           - READ - allows user/entity to retrieve configuration details for the connection overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the label overriding any inherited authorizations from an ancestor process group\n" +
                    "        /remote-process-groups/{id} - READ - allows user/entity to retrieve configuration details for the remote process group overriding any inherited authorizations from an " +
                    "ancestor process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the remote process group overriding any inherited authorizations from an ancestor process " +
                    "group\n" +
                    "        /templates/{id}             - READ - allows user/entity to retrieve configuration details for the template overriding any inherited authorizations from an ancestor " +
                    "process group\n" +
                    "                                    - WRITE - allows user/entity to create/update/delete the template overriding any inherited authorizations from an ancestor process group\n" +
                    "        /controller-services/{id}   - READ - allows user/entity to retrieve configuration details for the controller service overriding any inherited authorizations from an " +
                    "ancestor process group\n" +
                    "                                    - WRITE - allows user/entity to update/delete the controller service overriding any inherited authorizations from an ancestor process " +
                    "group\n" +
                    "        /reporting-tasks/{id}       - READ - allows user/entity to retrieve configuration details for the reporting tasks overriding any inherited authorizations from the " +
                    "controller\n" +
                    "                                    - WRITE - allows user/entity to create/update/delete the reporting tasks overriding any inherited authorizations from the controller\n" +
                    "        /{type}/{id}/provenance     - READ - allows user/entity to view provenance data from the underlying component\n" +
                    "                                    - WRITE - NA\n",
            response = AccessPolicyEntity.class,
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
    public Response createAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity accessPolicyEntity) {

        if (accessPolicyEntity == null || accessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (accessPolicyEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Access policy ID cannot be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, accessPolicyEntity);
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable accessPolicies = lookup.getAccessPoliciesAuthorizable();
                accessPolicies.authorize(authorizer, RequestAction.WRITE);
            });
        }
        if (validationPhase) {
            return generateContinueResponse().build();
        }

        // set the access policy id as appropriate
        accessPolicyEntity.getComponent().setId(generateUuid());

        // get revision from the config
        final RevisionDTO revisionDTO = accessPolicyEntity.getRevision();
        Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), accessPolicyEntity.getComponent().getId());

        // create the access policy and generate the json
        final AccessPolicyEntity entity = serviceFacade.createAccessPolicy(revision, accessPolicyEntity.getComponent());
        populateRemainingAccessPolicyEntityContent(entity);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
    }

    /**
     * Retrieves the specified access policy.
     *
     * @param id The id of the access policy to retrieve
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets an access policy",
            response = AccessPolicyEntity.class,
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
    public Response getAccessPolicy(
            @ApiParam(
                    value = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getAccessPolicyAuthorizable(id);
            accessPolicy.authorize(authorizer, RequestAction.READ);
        });

        // get the access policy
        final AccessPolicyEntity entity = serviceFacade.getAccessPolicy(id);
        populateRemainingAccessPolicyEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates an access policy.
     *
     * @param httpServletRequest request
     * @param id                 The id of the access policy to update.
     * @param accessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a access policy",
            response = AccessPolicyEntity.class,
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
    public Response updateAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity accessPolicyEntity) {

        if (accessPolicyEntity == null || accessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (accessPolicyEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final AccessPolicyDTO accessPolicyDTO = accessPolicyEntity.getComponent();
        if (!id.equals(accessPolicyDTO.getId())) {
            throw new IllegalArgumentException(String.format("The access policy id (%s) in the request body does not equal the "
                    + "access policy id of the requested resource (%s).", accessPolicyDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, accessPolicyEntity);
        }

        // Extract the revision
        final Revision revision = getRevision(accessPolicyEntity, id);
        return withWriteLock(
                serviceFacade,
                revision,
                lookup -> {
                    final Authorizable accessPolicy = lookup.getAccessPolicyAuthorizable(id);
                    accessPolicy.authorize(authorizer, RequestAction.WRITE);
                },
                null,
                () -> {
                    // update the access policy
                    final UpdateResult<AccessPolicyEntity> updateResult = serviceFacade.updateAccessPolicy(revision, accessPolicyDTO);

                    // get the results
                    final AccessPolicyEntity entity = updateResult.getResult();
                    populateRemainingAccessPolicyEntityContent(entity);

                    if (updateResult.isNew()) {
                        return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
                    } else {
                        return clusterContext(generateOkResponse(entity)).build();
                    }
                }
        );
    }

    /**
     * Removes the specified access policy.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the access policy to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes an access policy",
            response = AccessPolicyEntity.class,
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
    public Response removeAccessPolicy(
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
                    value = "The access policy id.",
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
                    final Authorizable accessPolicy = lookup.getAccessPolicyAuthorizable(id);
                    accessPolicy.authorize(authorizer, RequestAction.READ);
                },
                () -> {
                },
                () -> {
                    // delete the specified access policy
                    final AccessPolicyEntity entity = serviceFacade.deleteAccessPolicy(revision, id);
                    return clusterContext(generateOkResponse(entity)).build();
                }
        );
    }
}
