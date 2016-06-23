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
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.entity.FunnelEntity;
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
import java.util.Set;

/**
 * RESTful endpoint for managing a Funnel.
 */
@Path("/funnels")
@Api(
    value = "/funnel",
    description = "Endpoint for managing a Funnel."
)
public class FunnelResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified funnels.
     *
     * @param funnelEntities funnels
     * @return funnels
     */
    public Set<FunnelEntity> populateRemainingFunnelEntitiesContent(Set<FunnelEntity> funnelEntities) {
        for (FunnelEntity funnelEntity : funnelEntities) {
            populateRemainingFunnelEntityContent(funnelEntity);
        }
        return funnelEntities;
    }

    /**
     * Populates the uri for the specified funnel.
     *
     * @param funnelEntity funnel
     * @return funnel
     */
    public FunnelEntity populateRemainingFunnelEntityContent(FunnelEntity funnelEntity) {
        if (funnelEntity.getComponent() != null) {
            populateRemainingFunnelContent(funnelEntity.getComponent());
        }
        return funnelEntity;
    }

    /**
     * Populates the uri for the specified funnels.
     *
     * @param funnels funnels
     * @return funnels
     */
    public Set<FunnelDTO> populateRemainingFunnelsContent(Set<FunnelDTO> funnels) {
        for (FunnelDTO funnel : funnels) {
            populateRemainingFunnelContent(funnel);
        }
        return funnels;
    }

    /**
     * Populates the uri for the specified funnel.
     */
    public FunnelDTO populateRemainingFunnelContent(FunnelDTO funnel) {
        // populate the funnel href
        funnel.setUri(generateResourceUri("funnels", funnel.getId()));
        return funnel;
    }

    /**
     * Retrieves the specified funnel.
     *
     * @param id The id of the funnel to retrieve
     * @return A funnelEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a funnel",
            response = FunnelEntity.class,
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
    public Response getFunnel(
            @ApiParam(
                    value = "The funnel id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable funnel = lookup.getFunnel(id);
            funnel.authorize(authorizer, RequestAction.READ);
        });

        // get the funnel
        final FunnelEntity entity = serviceFacade.getFunnel(id);
        populateRemainingFunnelEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new Funnel.
     *
     * @param httpServletRequest request
     * @param id The id of the funnel to update.
     * @param funnelEntity A funnelEntity.
     * @return A funnelEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a funnel",
            response = FunnelEntity.class,
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
    public Response updateFunnel(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The funnel id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The funnel configuration details.",
                    required = true
            ) final FunnelEntity funnelEntity) {

        if (funnelEntity == null || funnelEntity.getComponent() == null) {
            throw new IllegalArgumentException("Funnel details must be specified.");
        }

        if (funnelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final FunnelDTO requestFunnelDTO = funnelEntity.getComponent();
        if (!id.equals(requestFunnelDTO.getId())) {
            throw new IllegalArgumentException(String.format("The funnel id (%s) in the request body does not equal the "
                    + "funnel id of the requested resource (%s).", requestFunnelDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, funnelEntity);
        }

        // Extract the revision
        final Revision revision = getRevision(funnelEntity, id);
        return withWriteLock(
            serviceFacade,
            revision,
            lookup -> {
                Authorizable authorizable = lookup.getFunnel(id);
                authorizable.authorize(authorizer, RequestAction.WRITE);
            },
            null,
            () -> {
                // update the funnel
                final FunnelEntity entity = serviceFacade.updateFunnel(revision, requestFunnelDTO);
                populateRemainingFunnelEntityContent(entity);

                return clusterContext(generateOkResponse(entity)).build();
            }
        );
    }

    /**
     * Removes the specified funnel.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the funnel to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a funnel",
            response = FunnelEntity.class,
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
    public Response removeFunnel(
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
                    value = "The funnel id.",
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
                final Authorizable funnel = lookup.getFunnel(id);
                funnel.authorize(authorizer, RequestAction.WRITE);
            },
            () -> serviceFacade.verifyDeleteFunnel(id),
            () -> {
                // delete the specified funnel
                final FunnelEntity entity = serviceFacade.deleteFunnel(revision, id);
                return clusterContext(generateOkResponse(entity)).build();
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
