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
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.entity.LabelEntity;
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
 * RESTful endpoint for managing a Label.
 */
@Path("/labels")
@Api(
    value = "/labels",
    description = "Endpoint for managing a Label."
)
public class LabelResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified labels.
     *
     * @param labelEntities labels
     * @return entites
     */
    public Set<LabelEntity> populateRemainingLabelEntitiesContent(Set<LabelEntity> labelEntities) {
        for (LabelEntity labelEntity : labelEntities) {
            populateRemainingLabelEntityContent(labelEntity);
        }
        return labelEntities;
    }

    /**
     * Populates the uri for the specified labels.
     *
     * @param labelEntity label
     * @return entities
     */
    public LabelEntity populateRemainingLabelEntityContent(LabelEntity labelEntity) {
        if (labelEntity.getComponent() != null) {
            populateRemainingLabelContent(labelEntity.getComponent());
        }
        return labelEntity;
    }

    /**
     * Populates the uri for the specified labels.
     *
     * @param labels labels
     * @return dtos
     */
    public Set<LabelDTO> populateRemainingLabelsContent(Set<LabelDTO> labels) {
        for (LabelDTO label : labels) {
            populateRemainingLabelContent(label);
        }
        return labels;
    }

    /**
     * Populates the uri for the specified label.
     */
    public LabelDTO populateRemainingLabelContent(LabelDTO label) {
        // populate the label href
        label.setUri(generateResourceUri("labels", label.getId()));
        return label;
    }

    /**
     * Retrieves the specified label.
     *
     * @param id The id of the label to retrieve
     * @return A labelEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a label",
            response = LabelEntity.class,
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
    public Response getLabel(
            @ApiParam(
                    value = "The label id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable label = lookup.getLabel(id);
            label.authorize(authorizer, RequestAction.READ);
        });

        // get the label
        final LabelEntity entity = serviceFacade.getLabel(id);
        populateRemainingLabelEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified label.
     *
     * @param httpServletRequest request
     * @param id The id of the label to update.
     * @param labelEntity A labelEntity.
     * @return A labelEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a label",
            response = LabelEntity.class,
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
    public Response updateLabel(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The label id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The label configuraiton details.",
                    required = true
            ) final LabelEntity labelEntity) {

        if (labelEntity == null || labelEntity.getComponent() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (labelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final LabelDTO requestLabelDTO = labelEntity.getComponent();
        if (!id.equals(requestLabelDTO.getId())) {
            throw new IllegalArgumentException(String.format("The label id (%s) in the request body does not equal the "
                    + "label id of the requested resource (%s).", requestLabelDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, labelEntity);
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = getRevision(labelEntity, id);
        return withWriteLock(
            serviceFacade,
            revision,
            lookup -> {
                Authorizable authorizable  = lookup.getLabel(id);
                authorizable.authorize(authorizer, RequestAction.WRITE);
            },
            null,
            () -> {
                // update the label
                final LabelEntity entity = serviceFacade.updateLabel(revision, requestLabelDTO);
                populateRemainingLabelEntityContent(entity);

                return clusterContext(generateOkResponse(entity)).build();
            }
        );
    }

    /**
     * Removes the specified label.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the label to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a label",
            response = LabelEntity.class,
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
    public Response removeLabel(
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
                    value = "The label id.",
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
                final Authorizable label = lookup.getLabel(id);
                label.authorize(authorizer, RequestAction.WRITE);
            },
            null,
            () -> {
                // delete the specified label
                final LabelEntity entity = serviceFacade.deleteLabel(revision, id);
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
