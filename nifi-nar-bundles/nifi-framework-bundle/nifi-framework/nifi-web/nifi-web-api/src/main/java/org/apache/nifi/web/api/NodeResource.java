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
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.springframework.security.access.prepost.PreAuthorize;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * RESTful endpoint for managing a cluster connection.
 */
@Api(hidden = true)
public class NodeResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private NiFiProperties properties;

    /**
     * Gets the contents of the specified node in this NiFi cluster.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The node id.
     * @return A nodeEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a node in the cluster",
            response = NodeEntity.class,
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
    public Response getNode(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (properties.isClusterManager()) {

            // get the specified relationship
            final NodeDTO dto = serviceFacade.getNode(id);

            // create the revision
            final RevisionDTO revision = new RevisionDTO();
            revision.setClientId(clientId.getClientId());

            // create the response entity
            final NodeEntity entity = new NodeEntity();
            entity.setRevision(revision);
            entity.setNode(dto);

            // generate the response
            return generateOkResponse(entity).build();

        }

        throw new IllegalClusterResourceRequestException("Only a cluster manager can process the request.");
    }


    /**
     * Updates the contents of the specified node in this NiFi cluster.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the node.
     * @param status The status of the node.
     * @param primary Whether the node should be make primary.
     * @return A nodeEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ADMIN')")
    public Response updateNode(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @FormParam("status") String status,
            @FormParam("primary") Boolean primary) {

        // create the node dto
        final NodeDTO nodeDTO = new NodeDTO();
        nodeDTO.setNodeId(id);
        nodeDTO.setStatus(status);
        nodeDTO.setPrimary(primary);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the node entity
        final NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setRevision(revision);
        nodeEntity.setNode(nodeDTO);

        // update the node
        return updateNode(id, nodeEntity);
    }

    /**
     * Updates the contents of the specified node in this NiFi cluster.
     *
     * @param id The id of the node
     * @param nodeEntity A nodeEntity
     * @return A nodeEntity
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Updates a node in the cluster",
            response = NodeEntity.class,
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
    public Response updateNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The node configuration. The only configuration that will be honored at this endpoint is the status or primary flag.",
                    required = true
            )
            NodeEntity nodeEntity) {

        if (properties.isClusterManager()) {

            if (nodeEntity == null || nodeEntity.getNode() == null) {
                throw new IllegalArgumentException("Node details must be specified.");
            }

            // get the request node
            final NodeDTO requestNodeDTO = nodeEntity.getNode();
            if (!id.equals(requestNodeDTO.getNodeId())) {
                throw new IllegalArgumentException(String.format("The node id (%s) in the request body does "
                        + "not equal the node id of the requested resource (%s).", requestNodeDTO.getNodeId(), id));
            }

            // update the node
            final NodeDTO node = serviceFacade.updateNode(requestNodeDTO);

            // create the revision
            final RevisionDTO revision = new RevisionDTO();
            if (nodeEntity.getRevision() == null) {
                revision.setClientId(new ClientIdParameter().getClientId());
            } else {
                revision.setClientId(nodeEntity.getRevision().getClientId());
            }

            // create the response entity
            NodeEntity entity = new NodeEntity();
            entity.setRevision(revision);
            entity.setNode(node);

            // generate the response
            return generateOkResponse(entity).build();
        }

        throw new IllegalClusterResourceRequestException("Only a cluster manager can process the request.");
    }

    /**
     * Removes the specified from this NiFi cluster.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the node
     * @return A nodeEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Removes a node from the cluster",
            response = NodeEntity.class,
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
    public Response deleteNode(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (properties.isClusterManager()) {

            serviceFacade.deleteNode(id);

            // create the revision
            final RevisionDTO revision = new RevisionDTO();
            revision.setClientId(clientId.getClientId());

            // create the response entity
            final NodeEntity entity = new NodeEntity();
            entity.setRevision(revision);

            // generate the response
            return generateOkResponse(entity).build();
        }

        throw new IllegalClusterResourceRequestException("Only a cluster manager can process the request.");

    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
