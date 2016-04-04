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
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.entity.InputPortEntity;
import org.apache.nifi.web.api.entity.InputPortsEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.springframework.security.access.prepost.PreAuthorize;

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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for managing an Input Port.
 */
@Api(hidden = true)
public class InputPortResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private String groupId;

    /**
     * Populates the uri for the specified input ports.
     *
     * @param inputPorts ports
     * @return ports
     */
    public Set<PortDTO> populateRemainingInputPortsContent(Set<PortDTO> inputPorts) {
        for (PortDTO inputPort : inputPorts) {
            populateRemainingInputPortContent(inputPort);
        }
        return inputPorts;
    }

    /**
     * Populates the uri for the specified input ports.
     */
    private PortDTO populateRemainingInputPortContent(PortDTO inputPort) {
        // populate the input port uri
        inputPort.setUri(generateResourceUri("controller", "process-groups", inputPort.getParentGroupId(), "input-ports", inputPort.getId()));
        return inputPort;
    }

    /**
     * Retrieves all the of input ports in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A inputPortsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all input ports",
            response = InputPortsEntity.class,
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
    public Response getInputPorts(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the input ports
        final Set<PortDTO> inputPorts = populateRemainingInputPortsContent(serviceFacade.getInputPorts(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final InputPortsEntity entity = new InputPortsEntity();
        entity.setRevision(revision);
        entity.setInputPorts(inputPorts);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new input port.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @param name The input ports name.
     * @return A inputPortEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response createInputPort(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y,
            @FormParam("name") String name) {

        // ensure the position has been specified
        if (x == null || y == null) {
            throw new IllegalArgumentException("The position (x, y) must be specified");
        }

        // create the input port DTO
        final PortDTO inputPortDTO = new PortDTO();
        inputPortDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        inputPortDTO.setName(name);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the input port entity entity
        final InputPortEntity portEntity = new InputPortEntity();
        portEntity.setRevision(revision);
        portEntity.setInputPort(inputPortDTO);

        // create the input port
        return createInputPort(httpServletRequest, portEntity);
    }

    /**
     * Creates a new input port.
     *
     * @param httpServletRequest request
     * @param portEntity A inputPortEntity.
     * @return A inputPortEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates an input port",
            response = InputPortEntity.class,
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
    public Response createInputPort(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The input port configuration details.",
                    required = true
            ) InputPortEntity portEntity) {

        if (portEntity == null || portEntity.getInputPort() == null) {
            throw new IllegalArgumentException("Port details must be specified.");
        }

        if (portEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (portEntity.getInputPort().getId() != null) {
            throw new IllegalArgumentException("Input port ID cannot be specified.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            portEntity.getInputPort().setId(id);

            // convert POST request to PUT request to force entity ID to be the same across nodes
            URI putUri = null;
            try {
                putUri = new URI(getAbsolutePath().toString() + "/" + id);
            } catch (final URISyntaxException e) {
                throw new WebApplicationException(e);
            }

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate put request
            return clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(portEntity), getHeaders(headersToOverride)).getResponse();

        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the input port and generate the json
        final RevisionDTO revision = portEntity.getRevision();
        final ConfigurationSnapshot<PortDTO> controllerResponse = serviceFacade.createInputPort(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, portEntity.getInputPort());
        final PortDTO port = controllerResponse.getConfiguration();
        populateRemainingInputPortContent(port);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final InputPortEntity entity = new InputPortEntity();
        entity.setRevision(updatedRevision);
        entity.setInputPort(port);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(port.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified input port.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the input port to retrieve
     * @return A inputPortEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets an input port",
            response = InputPortEntity.class,
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
    public Response getInputPort(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the port
        final PortDTO port = serviceFacade.getInputPort(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final InputPortEntity entity = new InputPortEntity();
        entity.setRevision(revision);
        entity.setInputPort(populateRemainingInputPortContent(port));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified input port status.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor history to retrieve.
     * @return A portStatusEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}/status")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for an input port",
        response = PortStatusEntity.class,
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
    public Response getInputPortStatus(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The input port id.",
            required = true
        )
        @PathParam("id") String id) {

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
            }
        }

        // get the specified input port status
        final PortStatusDTO portStatus = serviceFacade.getInputPortStatus(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final PortStatusEntity entity = new PortStatusEntity();
        entity.setRevision(revision);
        entity.setPortStatus(portStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified input port.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the input port to update.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @param groupAccessControl The allowed groups for this input port.
     * @param userAccessControl The allowed users for this input port.
     * @param comments Any comments about this input port.
     * @param name The input ports name.
     * @param state The state of this port.
     * @param concurrentlySchedulableTaskCount The number of concurrently schedulable tasks.
     * @param formParams form params
     * @return A inputPortEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateInputPort(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y,
            @FormParam("comments") String comments,
            @FormParam("groupAccessControl[]") Set<String> groupAccessControl,
            @FormParam("userAccessControl[]") Set<String> userAccessControl,
            @FormParam("name") String name,
            @FormParam("state") String state,
            @FormParam("concurrentlySchedulableTaskCount") IntegerParameter concurrentlySchedulableTaskCount,
            MultivaluedMap<String, String> formParams) {

        // create the input port DTO
        final PortDTO portDTO = new PortDTO();
        portDTO.setId(id);
        portDTO.setComments(comments);
        portDTO.setName(name);
        portDTO.setState(state);

        if (concurrentlySchedulableTaskCount != null) {
            portDTO.setConcurrentlySchedulableTaskCount(concurrentlySchedulableTaskCount.getInteger());
        }

        // require both coordinates to be specified
        if (x != null && y != null) {
            portDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        }

        // only set the group access control when applicable
        if (!groupAccessControl.isEmpty() || formParams.containsKey("groupAccessControl[]")) {
            portDTO.setGroupAccessControl(groupAccessControl);
        }

        // only set the user access control when applicable
        if (!userAccessControl.isEmpty() || formParams.containsKey("userAccessControl[]")) {
            portDTO.setUserAccessControl(userAccessControl);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the input port entity
        final InputPortEntity portEntity = new InputPortEntity();
        portEntity.setRevision(revision);
        portEntity.setInputPort(portDTO);

        // update the port
        return updateInputPort(httpServletRequest, id, portEntity);
    }

    /**
     * Updates the specified input port.
     *
     * @param httpServletRequest request
     * @param id The id of the input port to update.
     * @param portEntity A inputPortEntity.
     * @return A inputPortEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates an input port",
            response = InputPortEntity.class,
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
    public Response updateInputPort(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The input port configuration details.",
                    required = true
            ) InputPortEntity portEntity) {

        if (portEntity == null || portEntity.getInputPort() == null) {
            throw new IllegalArgumentException("Input port details must be specified.");
        }

        if (portEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final PortDTO requestPortDTO = portEntity.getInputPort();
        if (!id.equals(requestPortDTO.getId())) {
            throw new IllegalArgumentException(String.format("The input port id (%s) in the request body does not equal the "
                    + "input port id of the requested resource (%s).", requestPortDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(portEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateInputPort(groupId, requestPortDTO);
            return generateContinueResponse().build();
        }

        // update the input port
        final RevisionDTO revision = portEntity.getRevision();
        final ConfigurationSnapshot<PortDTO> controllerResponse = serviceFacade.updateInputPort(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, requestPortDTO);

        // get the results
        final PortDTO responsePortDTO = controllerResponse.getConfiguration();
        populateRemainingInputPortContent(responsePortDTO);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final InputPortEntity entity = new InputPortEntity();
        entity.setRevision(updatedRevision);
        entity.setInputPort(responsePortDTO);

        if (controllerResponse.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(responsePortDTO.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified input port.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the input port to remove.
     * @return A inputPortEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes an input port",
            response = InputPortEntity.class,
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
    public Response removeInputPort(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @QueryParam(VERSION) LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyDeleteInputPort(groupId, id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified input port
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteInputPort(new Revision(clientVersion, clientId.getClientId()), groupId, id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final InputPortEntity entity = new InputPortEntity();
        entity.setRevision(revision);

        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
