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
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.PortEntity;
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
 * RESTful endpoint for managing an Output Port.
 */
@Path("/output-ports")
@Api(
        value = "/output-ports",
        description = "Endpoint for managing an Output Port."
)
public class OutputPortResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified output ports.
     *
     * @param outputPortEntities ports
     * @return dtos
     */
    public Set<PortEntity> populateRemainingOutputPortEntitiesContent(Set<PortEntity> outputPortEntities) {
        for (PortEntity outputPortEntity : outputPortEntities) {
            populateRemainingOutputPortEntityContent(outputPortEntity);
        }
        return outputPortEntities;
    }

    /**
     * Populates the uri for the specified output ports.
     *
     * @param outputPortEntity ports
     * @return dtos
     */
    public PortEntity populateRemainingOutputPortEntityContent(PortEntity outputPortEntity) {
        outputPortEntity.setUri(generateResourceUri("output-ports", outputPortEntity.getId()));
        return outputPortEntity;
    }

    /**
     * Retrieves the specified output port.
     *
     * @param id The id of the output port to retrieve
     * @return A outputPortEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets an output port",
            response = PortEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /output-ports/{uuid}", type = "")
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
    public Response getOutputPort(
            @ApiParam(
                    value = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable outputPort = lookup.getOutputPort(id);
            outputPort.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the port
        final PortEntity entity = serviceFacade.getOutputPort(id);
        populateRemainingOutputPortEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified output port.
     *
     * @param httpServletRequest request
     * @param id                 The id of the output port to update.
     * @param requestPortEntity         A outputPortEntity.
     * @return A outputPortEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates an output port",
            response = PortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /output-ports/{uuid}", type = "")
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
    public Response updateOutputPort(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The output port configuration details.",
                    required = true
            ) final PortEntity requestPortEntity) {

        if (requestPortEntity == null || requestPortEntity.getComponent() == null) {
            throw new IllegalArgumentException("Output port details must be specified.");
        }

        if (requestPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        PortDTO requestPortDTO = requestPortEntity.getComponent();
        if (!id.equals(requestPortDTO.getId())) {
            throw new IllegalArgumentException(String.format("The output port id (%s) in the request body does not equal the "
                    + "output port id of the requested resource (%s).", requestPortDTO.getId(), id));
        }

        final PositionDTO proposedPosition = requestPortDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestPortEntity);
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getOutputPort(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateOutputPort(requestPortDTO),
                (revision, portEntity) -> {
                    final PortDTO portDTO = portEntity.getComponent();

                    // update the output port
                    final PortEntity entity = serviceFacade.updateOutputPort(revision, portDTO);
                    populateRemainingOutputPortEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified output port.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id                 The id of the output port to remove.
     * @return A outputPortEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes an output port",
            response = PortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /output-ports/{uuid}", type = ""),
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
    public Response removeOutputPort(
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
                    value = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final PortEntity requestPortEntity = new PortEntity();
        requestPortEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable outputPort = lookup.getOutputPort(id);

                    // ensure write permission to the output port
                    outputPort.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    outputPort.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteOutputPort(id),
                (revision, portEntity) -> {
                    // delete the specified output port
                    final PortEntity entity = serviceFacade.deleteOutputPort(revision, portEntity.getId());
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
