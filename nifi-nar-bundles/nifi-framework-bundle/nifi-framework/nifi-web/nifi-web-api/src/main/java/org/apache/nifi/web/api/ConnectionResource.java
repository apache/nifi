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

import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for managing a Connection.
 */
@Path("connections")
public class ConnectionResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    /**
     * Populate the URIs for the specified connections.
     *
     * @param connections connections
     * @return dtos
     */
    public Set<ConnectionDTO> populateRemainingConnectionsContent(Set<ConnectionDTO> connections) {
        for (ConnectionDTO connection : connections) {
            populateRemainingConnectionContent(connection);
        }
        return connections;
    }

    /**
     * Populate the URIs for the specified connection.
     *
     * @param connection connection
     * @return dto
     */
    public ConnectionDTO populateRemainingConnectionContent(ConnectionDTO connection) {
        // populate the remaining properties
        connection.setUri(generateResourceUri("connections", connection.getId()));
        return connection;
    }

    /**
     * Populate the URIs for the specified flowfile listing.
     *
     * @param connectionId connection
     * @param flowFileListing flowfile listing
     * @return dto
     */
    public ListingRequestDTO populateRemainingFlowFileListingContent(final String connectionId, final ListingRequestDTO flowFileListing) {
        // uri of the listing
        flowFileListing.setUri(generateResourceUri("connections", connectionId, "listing-requests", flowFileListing.getId()));

        // uri of each flowfile
        if (flowFileListing.getFlowFileSummaries() != null) {
            for (FlowFileSummaryDTO flowFile : flowFileListing.getFlowFileSummaries()) {
                populateRemainingFlowFileContent(connectionId, flowFile);
            }
        }
        return flowFileListing;
    }

    /**
     * Populate the URIs for the specified flowfile.
     *
     * @param connectionId the connection id
     * @param flowFile the flowfile
     * @return the dto
     */
    public FlowFileSummaryDTO populateRemainingFlowFileContent(final String connectionId, final FlowFileSummaryDTO flowFile) {
        flowFile.setUri(generateResourceUri("connections", connectionId, "flowfiles", flowFile.getUuid()));
        return flowFile;
    }

    /**
     * Retrieves the specified connection.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection.
     * @return A connectionEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a connection",
            response = ConnectionEntity.class,
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
    public Response getConnection(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the specified relationship
        ConnectionDTO connection = serviceFacade.getConnection(id);

        // create the revision
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        entity.setConnection(populateRemainingConnectionContent(connection));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified connection status.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection history to retrieve.
     * @return A connectionStatusEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for a connection",
        response = ConnectionStatusEntity.class,
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
    public Response getConnectionStatus(
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
            value = "The connection id.",
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
                final ConnectionStatusEntity entity = (ConnectionStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getConnectionStatus().setNodeSnapshots(null);
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

        // get the specified connection status
        final ConnectionStatusDTO connectionStatus = serviceFacade.getConnectionStatus(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final ConnectionStatusEntity entity = new ConnectionStatusEntity();
        entity.setRevision(revision);
        entity.setConnectionStatus(connectionStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified connection status history.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection to retrieve.
     * @return A statusHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the status history for a connection",
            response = StatusHistoryEntity.class,
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
    public Response getConnectionStatusHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the specified processor status history
        final StatusHistoryDTO connectionStatusHistory = serviceFacade.getConnectionStatusHistory(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setRevision(revision);
        entity.setStatusHistory(connectionStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified connection.
     *
     * @param httpServletRequest request
     * @param id The id of the connection.
     * @param connectionEntity A connectionEntity.
     * @return A connectionEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a connection",
            response = ConnectionEntity.class,
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
    public Response updateConnection(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The connection configuration details.",
                    required = true
            ) ConnectionEntity connectionEntity) {

        if (connectionEntity == null || connectionEntity.getConnection() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (connectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ConnectionDTO connection = connectionEntity.getConnection();
        if (!id.equals(connection.getId())) {
            throw new IllegalArgumentException(String.format("The connection id "
                    + "(%s) in the request body does not equal the connection id of the "
                    + "requested resource (%s).", connection.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(connectionEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateConnection(connection);
            return generateContinueResponse().build();
        }

        // update the relationship target
        final RevisionDTO revision = connectionEntity.getRevision();
        final ConfigurationSnapshot<ConnectionDTO> controllerResponse = serviceFacade.updateConnection(
                new Revision(revision.getVersion(), revision.getClientId()), connection);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // marshall the target and add the source processor
        final ConnectionDTO connectionDTO = controllerResponse.getConfiguration();
        populateRemainingConnectionContent(connectionDTO);

        // create the response entity
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(updatedRevision);
        entity.setConnection(connectionDTO);

        // generate the response
        if (controllerResponse.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(connectionDTO.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified connection.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection.
     * @return An Entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a connection",
            response = ConnectionEntity.class,
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
    public Response deleteConnection(
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
                    value = "The connection id.",
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
            serviceFacade.verifyDeleteConnection(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the connection
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteConnection(new Revision(clientVersion, clientId.getClientId()), id);

        // create the revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(clientId.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(updatedRevision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the specified flowfile from the specified connection.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The connection id
     * @param flowFileUuid The flowfile uuid
     * @param clusterNodeId The cluster node id where the flowfile resides
     * @return a flowFileDTO
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/flowfiles/{flowfile-uuid}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Gets a FlowFile from a Connection.",
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
    public Response getFlowFile(
            @ApiParam(
                value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "The connection id.",
                required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                value = "The flowfile uuid.",
                required = true
            )
            @PathParam("flowfile-uuid") String flowFileUuid,
            @ApiParam(
                value = "The id of the node where the content exists if clustered.",
                required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
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

        // get the flowfile
        final FlowFileDTO flowfileDto = serviceFacade.getFlowFile(connectionId, flowFileUuid);
        populateRemainingFlowFileContent(connectionId, flowfileDto);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final FlowFileEntity entity = new FlowFileEntity();
        entity.setRevision(revision);
        entity.setFlowFile(flowfileDto);

        return generateOkResponse(entity).build();
    }

    /**
     * Gets the content for the specified flowfile in the specified connection.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The connection id
     * @param flowFileUuid The flowfile uuid
     * @param clusterNodeId The cluster node id
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/{connection-id}/flowfiles/{flowfile-uuid}/content")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Gets the content for a FlowFile in a Connection.",
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
    public Response downloadFlowFileContent(
            @ApiParam(
                value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "The connection id.",
                required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                value = "The flowfile uuid.",
                required = true
            )
            @PathParam("flowfile-uuid") String flowFileUuid,
            @ApiParam(
                value = "The id of the node where the content exists if clustered.",
                required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
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

        // get the uri of the request
        final String uri = generateResourceUri("connections", connectionId, "flowfiles", flowFileUuid, "content");

        // get an input stream to the content
        final DownloadableContent content = serviceFacade.getContent(connectionId, flowFileUuid, uri);

        // generate a streaming response
        final StreamingOutput response = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try (InputStream is = content.getContent()) {
                    // stream the content to the response
                    StreamUtils.copy(is, output);

                    // flush the response
                    output.flush();
                }
            }
        };

        // use the appropriate content type
        String contentType = content.getType();
        if (contentType == null) {
            contentType = MediaType.APPLICATION_OCTET_STREAM;
        }

        return generateOkResponse(response).type(contentType).header("Content-Disposition", String.format("attachment; filename=\"%s\"", content.getFilename())).build();
    }

    /**
     * Creates a request to list the flowfiles in the queue of the specified connection.
     *
     * @param httpServletRequest request
     * @param id The id of the connection
     * @return A listRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/listing-requests")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Lists the contents of the queue in this connection.",
        response = ListingRequestEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
        }
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 202, message = "The request has been accepted. A HTTP response header will contain the URI where the response can be polled."),
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
        }
    )
    public Response createFlowFileListing(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The connection id.",
                required = true
            )
            @PathParam("connection-id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyListQueue(id);
            return generateContinueResponse().build();
        }

        // ensure the id is the same across the cluster
        final String listingRequestId;
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            listingRequestId = UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            listingRequestId = UUID.randomUUID().toString();
        }

        // submit the listing request
        final ListingRequestDTO listingRequest = serviceFacade.createFlowFileListingRequest(id, listingRequestId);
        populateRemainingFlowFileListingContent(id, listingRequest);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();

        // create the response entity
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setRevision(revision);
        entity.setListingRequest(listingRequest);

        // generate the URI where the response will be
        final URI location = URI.create(listingRequest.getUri());
        return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
    }

    /**
     * Checks the status of an outstanding listing request.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The id of the connection
     * @param listingRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/listing-requests/{listing-request-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Gets the current status of a listing request for the specified connection.",
        response = ListingRequestEntity.class,
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
    public Response getListingRequest(
            @ApiParam(
                value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "The connection id.",
                required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                value = "The listing request id.",
                required = true
            )
            @PathParam("listing-request-id") String listingRequestId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the listing request
        final ListingRequestDTO listingRequest = serviceFacade.getFlowFileListingRequest(connectionId, listingRequestId);
        populateRemainingFlowFileListingContent(connectionId, listingRequest);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setRevision(revision);
        entity.setListingRequest(listingRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified listing request.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The connection id
     * @param listingRequestId The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/listing-requests/{listing-request-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Cancels and/or removes a request to list the contents of this connection.",
        response = DropRequestEntity.class,
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
    public Response deleteListingRequest(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "The connection id.",
                required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                value = "The listing request id.",
                required = true
            )
            @PathParam("listing-request-id") String listingRequestId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the listing request
        final ListingRequestDTO listingRequest = serviceFacade.deleteFlowFileListingRequest(connectionId, listingRequestId);

        // prune the results as they were already received when the listing completed
        listingRequest.setFlowFileSummaries(null);

        // populate remaining content
        populateRemainingFlowFileListingContent(connectionId, listingRequest);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setRevision(revision);
        entity.setListingRequest(listingRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Creates a request to delete the flowfiles in the queue of the specified connection.
     *
     * @param httpServletRequest request
     * @param id The id of the connection
     * @return A dropRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/drop-requests")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a request to drop the contents of the queue in this connection.",
        response = DropRequestEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
        }
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 202, message = "The request has been accepted. A HTTP response header will contain the URI where the response can be polled."),
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
        }
    )
    public Response createDropRequest(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The connection id.",
            required = true
        )
        @PathParam("connection-id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // ensure the id is the same across the cluster
        final String dropRequestId;
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            dropRequestId = UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            dropRequestId = UUID.randomUUID().toString();
        }

        // submit the drop request
        final DropRequestDTO dropRequest = serviceFacade.createFlowFileDropRequest(id, dropRequestId);
        dropRequest.setUri(generateResourceUri("connections", id, "drop-requests", dropRequest.getId()));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setRevision(revision);
        entity.setDropRequest(dropRequest);

        // generate the URI where the response will be
        final URI location = URI.create(dropRequest.getUri());
        return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
    }

    /**
     * Checks the status of an outstanding drop request.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The id of the connection
     * @param dropRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/drop-requests/{drop-request-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Gets the current status of a drop request for the specified connection.",
            response = DropRequestEntity.class,
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
    public Response getDropRequest(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                    value = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") String dropRequestId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the drop request
        final DropRequestDTO dropRequest = serviceFacade.getFlowFileDropRequest(connectionId, dropRequestId);
        dropRequest.setUri(generateResourceUri("connections", connectionId, "drop-requests", dropRequestId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setRevision(revision);
        entity.setDropRequest(dropRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified drop request.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The connection id
     * @param dropRequestId The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{connection-id}/drop-requests/{drop-request-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Cancels and/or removes a request to drop the contents of this connection.",
            response = DropRequestEntity.class,
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
    public Response removeDropRequest(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("connection-id") String connectionId,
            @ApiParam(
                    value = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") String dropRequestId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the drop request
        final DropRequestDTO dropRequest = serviceFacade.deleteFlowFileDropRequest(connectionId, dropRequestId);
        dropRequest.setUri(generateResourceUri("connections", connectionId, "drop-requests", dropRequestId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setRevision(revision);
        entity.setDropRequest(dropRequest);

        return generateOkResponse(entity).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
