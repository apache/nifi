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
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
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
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * RESTful endpoint for managing a Remote group.
 */
@Path("remote-process-groups")
public class RemoteProcessGroupResource extends ApplicationResource {

    private static final String VERBOSE_DEFAULT_VALUE = "false";

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    /**
     * Populates the remaining content for each remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroups groups
     * @return dtos
     */
    public Set<RemoteProcessGroupDTO> populateRemainingRemoteProcessGroupsContent(Set<RemoteProcessGroupDTO> remoteProcessGroups) {
        for (RemoteProcessGroupDTO remoteProcessGroup : remoteProcessGroups) {
            populateRemainingRemoteProcessGroupContent(remoteProcessGroup);
        }
        return remoteProcessGroups;
    }

    /**
     * Populates the remaining content for the specified remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroup group
     * @return dto
     */
    public RemoteProcessGroupDTO populateRemainingRemoteProcessGroupContent(RemoteProcessGroupDTO remoteProcessGroup) {
        // populate the remaining content
        remoteProcessGroup.setUri(generateResourceUri("remote-process-groups", remoteProcessGroup.getId()));

        return remoteProcessGroup;
    }

    /**
     * Retrieves the specified remote process group.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true remote group contents (ports) will be included.
     * @param id The id of the remote process group to retrieve
     * @return A remoteProcessGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a remote process group",
            response = RemoteProcessGroupEntity.class,
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
    public Response getRemoteProcessGroup(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether to include any encapulated ports or just details about the remote process group.",
                    required = false
            )
            @QueryParam("verbose") @DefaultValue(VERBOSE_DEFAULT_VALUE) Boolean verbose,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the label
        final RemoteProcessGroupDTO remoteProcessGroup = serviceFacade.getRemoteProcessGroup(id);

        // prune the response as necessary
        if (!verbose) {
            remoteProcessGroup.setContents(null);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(revision);
        entity.setRemoteProcessGroup(populateRemainingRemoteProcessGroupContent(remoteProcessGroup));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified remote process group status.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor history to retrieve.
     * @return A remoteProcessGroupStatusEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for a remote process group",
        response = ProcessorStatusEntity.class,
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
    public Response getRemoteProcessGroupStatus(
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
            value = "The remote process group id.",
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
                final RemoteProcessGroupStatusEntity entity = (RemoteProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getRemoteProcessGroupStatus().setNodeSnapshots(null);
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

        // get the specified remote process group status
        final RemoteProcessGroupStatusDTO remoteProcessGroupStatus = serviceFacade.getRemoteProcessGroupStatus(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final RemoteProcessGroupStatusEntity entity = new RemoteProcessGroupStatusEntity();
        entity.setRevision(revision);
        entity.setRemoteProcessGroupStatus(remoteProcessGroupStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the remote process group to retrieve the status fow.
     * @return A statusHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the status history",
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
    public Response getRemoteProcessGroupStatusHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the specified processor status history
        final StatusHistoryDTO remoteProcessGroupStatusHistory = serviceFacade.getRemoteProcessGroupStatusHistory(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setRevision(revision);
        entity.setStatusHistory(remoteProcessGroupStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Removes the specified remote process group.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the remote process group to be removed.
     * @return A remoteProcessGroupEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a remote process group",
            response = RemoteProcessGroupEntity.class,
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
    public Response removeRemoteProcessGroup(
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
                    value = "The remote process group id.",
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
            serviceFacade.verifyDeleteRemoteProcessGroup(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteRemoteProcessGroup(new Revision(clientVersion, clientId.getClientId()), id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(revision);

        // create the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified remote process group input port.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the remote process group to update.
     * @param portId The id of the input port to update.
     * @param isTransmitting Whether or not this port is transmitting.
     * @param isCompressed Whether or not this port should compress.
     * @param concurrentlySchedulableTaskCount The number of concurrent tasks that should be supported
     *
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/input-ports/{port-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateRemoteProcessGroupInputPort(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @PathParam("port-id") String portId,
            @FormParam("transmitting") Boolean isTransmitting,
            @FormParam("compressed") Boolean isCompressed,
            @FormParam("concurrentlySchedulableTaskCount") IntegerParameter concurrentlySchedulableTaskCount) {

        // create the remote group port dto
        final RemoteProcessGroupPortDTO remotePort = new RemoteProcessGroupPortDTO();
        remotePort.setId(portId);
        remotePort.setUseCompression(isCompressed);
        remotePort.setTransmitting(isTransmitting);

        if (concurrentlySchedulableTaskCount != null) {
            remotePort.setConcurrentlySchedulableTaskCount(concurrentlySchedulableTaskCount.getInteger());
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the remote group port entity
        final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
        entity.setRevision(revision);
        entity.setRemoteProcessGroupPort(remotePort);

        return updateRemoteProcessGroupInputPort(httpServletRequest, id, portId, entity);
    }

    /**
     * Updates the specified remote process group input port.
     *
     * @param httpServletRequest request
     * @param id The id of the remote process group to update.
     * @param portId The id of the input port to update.
     * @param remoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     *
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/input-ports/{port-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a remote port",
            response = RemoteProcessGroupPortEntity.class,
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
    public Response updateRemoteProcessGroupInputPort(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("id") String id,
            @PathParam("port-id") String portId,
            RemoteProcessGroupPortEntity remoteProcessGroupPortEntity) {

        if (remoteProcessGroupPortEntity == null || remoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (remoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(remoteProcessGroupPortEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            // verify the update at this time
            serviceFacade.verifyUpdateRemoteProcessGroupInputPort(id, requestRemoteProcessGroupPort);
            return generateContinueResponse().build();
        }

        // update the specified remote process group
        final RevisionDTO revision = remoteProcessGroupPortEntity.getRevision();
        final ConfigurationSnapshot<RemoteProcessGroupPortDTO> controllerResponse
                = serviceFacade.updateRemoteProcessGroupInputPort(new Revision(revision.getVersion(),
                                revision.getClientId()), id, requestRemoteProcessGroupPort);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
        entity.setRevision(updatedRevision);
        entity.setRemoteProcessGroupPort(controllerResponse.getConfiguration());

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified remote process group output port.
     *
     * @param httpServletRequest request
     * @param id The id of the remote process group to update.
     * @param portId The id of the output port to update.
     * @param remoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     *
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/output-ports/{port-id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a remote port",
            response = RemoteProcessGroupPortEntity.class,
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
    public Response updateRemoteProcessGroupOutputPort(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("id") String id,
            @PathParam("port-id") String portId,
            RemoteProcessGroupPortEntity remoteProcessGroupPortEntity) {

        if (remoteProcessGroupPortEntity == null || remoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (remoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(remoteProcessGroupPortEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            // verify the update at this time
            serviceFacade.verifyUpdateRemoteProcessGroupOutputPort(id, requestRemoteProcessGroupPort);
            return generateContinueResponse().build();
        }

        // update the specified remote process group
        final RevisionDTO revision = remoteProcessGroupPortEntity.getRevision();
        final ConfigurationSnapshot<RemoteProcessGroupPortDTO> controllerResponse
                = serviceFacade.updateRemoteProcessGroupOutputPort(new Revision(revision.getVersion(),
                                revision.getClientId()), id, requestRemoteProcessGroupPort);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
        entity.setRevision(updatedRevision);
        entity.setRemoteProcessGroupPort(controllerResponse.getConfiguration());

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified remote process group.
     *
     * @param httpServletRequest request
     * @param id The id of the remote process group to update.
     * @param remoteProcessGroupEntity A remoteProcessGroupEntity.
     * @return A remoteProcessGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a remote process group",
            response = RemoteProcessGroupEntity.class,
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
    public Response updateRemoteProcessGroup(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("id") String id,
            RemoteProcessGroupEntity remoteProcessGroupEntity) {

        if (remoteProcessGroupEntity == null || remoteProcessGroupEntity.getRemoteProcessGroup() == null) {
            throw new IllegalArgumentException("Remote process group details must be specified.");
        }

        if (remoteProcessGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupDTO requestRemoteProcessGroup = remoteProcessGroupEntity.getRemoteProcessGroup();
        if (!id.equals(requestRemoteProcessGroup.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroup.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(remoteProcessGroupEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            // verify the update at this time
            serviceFacade.verifyUpdateRemoteProcessGroup(requestRemoteProcessGroup);
            return generateContinueResponse().build();
        }

        // if the target uri is set we have to verify it here - we don't support updating the target uri on
        // an existing remote process group, however if the remote process group is being created with an id
        // as is the case in clustered mode we need to verify the remote process group. treat this request as
        // though its a new remote process group.
        if (requestRemoteProcessGroup.getTargetUri() != null) {
            // parse the uri
            final URI uri;
            try {
                uri = URI.create(requestRemoteProcessGroup.getTargetUri());
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("The specified remote process group URL is malformed: " + requestRemoteProcessGroup.getTargetUri());
            }

            // validate each part of the uri
            if (uri.getScheme() == null || uri.getHost() == null) {
                throw new IllegalArgumentException("The specified remote process group URL is malformed: " + requestRemoteProcessGroup.getTargetUri());
            }

            if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
                throw new IllegalArgumentException("The specified remote process group URL is invalid because it is not http or https: " + requestRemoteProcessGroup.getTargetUri());
            }

            // normalize the uri to the other controller
            String controllerUri = uri.toString();
            if (controllerUri.endsWith("/")) {
                controllerUri = StringUtils.substringBeforeLast(controllerUri, "/");
            }

            // update with the normalized uri
            requestRemoteProcessGroup.setTargetUri(controllerUri);
        }

        // update the specified remote process group
        final RevisionDTO revision = remoteProcessGroupEntity.getRevision();
        final ConfigurationSnapshot<RemoteProcessGroupDTO> controllerResponse
                = serviceFacade.updateRemoteProcessGroup(new Revision(revision.getVersion(), revision.getClientId()), requestRemoteProcessGroup);

        final RemoteProcessGroupDTO responseRemoteProcessGroup = controllerResponse.getConfiguration();
        populateRemainingRemoteProcessGroupContent(responseRemoteProcessGroup);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setRemoteProcessGroup(responseRemoteProcessGroup);

        if (controllerResponse.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(responseRemoteProcessGroup.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
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
