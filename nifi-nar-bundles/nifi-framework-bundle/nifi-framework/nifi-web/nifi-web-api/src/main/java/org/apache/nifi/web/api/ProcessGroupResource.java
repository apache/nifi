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

import com.sun.jersey.api.core.ResourceContext;
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
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionsEntity;
import org.apache.nifi.web.api.entity.FlowSnippetEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.FunnelsEntity;
import org.apache.nifi.web.api.entity.InputPortEntity;
import org.apache.nifi.web.api.entity.InputPortsEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;
import org.apache.nifi.web.api.entity.OutputPortEntity;
import org.apache.nifi.web.api.entity.OutputPortsEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
import org.apache.nifi.web.api.request.LongParameter;

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
import javax.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for managing a Group.
 */
@Path("process-groups")
public class ProcessGroupResource extends ApplicationResource {

    private static final String VERBOSE = "false";
    private static final String RECURSIVE = "false";

    @Context
    private ResourceContext resourceContext;

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    private ProcessorResource processorResource;
    private InputPortResource inputPortResource;
    private OutputPortResource outputPortResource;
    private FunnelResource funnelResource;
    private LabelResource labelResource;
    private RemoteProcessGroupResource remoteProcessGroupResource;
    private ConnectionResource connectionResource;

    /**
     * Populates the remaining fields in the specified process groups.
     *
     * @param processGroups groups
     * @return group dto
     */
    public Set<ProcessGroupDTO> populateRemainingProcessGroupsContent(Set<ProcessGroupDTO> processGroups) {
        for (ProcessGroupDTO processGroup : processGroups) {
            populateRemainingProcessGroupContent(processGroup);
        }
        return processGroups;
    }

    /**
     * Populates the remaining fields in the specified process group.
     *
     * @param processGroup group
     * @return group dto
     */
    private ProcessGroupDTO populateRemainingProcessGroupContent(ProcessGroupDTO processGroup) {
        FlowSnippetDTO flowSnippet = processGroup.getContents();

        // populate the remaining fields for the processors, connections, process group refs, remote process groups, and labels if appropriate
        if (flowSnippet != null) {
            populateRemainingSnippetContent(flowSnippet);
        }

        // set the process group uri
        processGroup.setUri(generateResourceUri("process-groups",  processGroup.getId()));

        return processGroup;
    }

    /**
     * Populates the remaining content of the specified snippet.
     */
    private FlowSnippetDTO populateRemainingSnippetContent(FlowSnippetDTO snippet) {
        processorResource.populateRemainingProcessorsContent(snippet.getProcessors());
        connectionResource.populateRemainingConnectionsContent(snippet.getConnections());
        inputPortResource.populateRemainingInputPortsContent(snippet.getInputPorts());
        outputPortResource.populateRemainingOutputPortsContent(snippet.getOutputPorts());
        remoteProcessGroupResource.populateRemainingRemoteProcessGroupsContent(snippet.getRemoteProcessGroups());
        funnelResource.populateRemainingFunnelsContent(snippet.getFunnels());
        labelResource.populateRemainingLabelsContent(snippet.getLabels());

        // go through each process group child and populate its uri
        if (snippet.getProcessGroups() != null) {
            populateRemainingProcessGroupsContent(snippet.getProcessGroups());
        }

        return snippet;
    }

    /**
     * Copies the specified snippet within this ProcessGroup. The snippet instance that is instantiated cannot be referenced at a later time, therefore there is no
     * corresponding URI. Instead the request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param snippetId The id of the snippet to copy.
     * @param originX The x coordinate of the origin of the bounding box.
     * @param originY The y coordinate of the origin of the bounding box.
     * @return A flowSnippetEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/snippet-instance")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Copies a snippet",
            response = FlowSnippetEntity.class,
            authorizations = {
                @Authorization(value = "ROLE_DFM", type = "ROLE_DFM")
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
    public Response copySnippet(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The process group id.",
                required = true
            )
            @PathParam("id") String groupId,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @FormParam(VERSION) LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The snippet id.",
                    required = true
            )
            @FormParam("snippetId") String snippetId,
            @ApiParam(
                    value = "The x coordinate of the origin of the bounding box where the new components will be placed.",
                    required = true
            )
            @FormParam("originX") DoubleParameter originX,
            @ApiParam(
                    value = "The y coordinate of the origin of the bounding box where the new components will be placed.",
                    required = true
            )
            @FormParam("originY") DoubleParameter originY) {

        // ensure the position has been specified
        if (originX == null || originY == null) {
            throw new IllegalArgumentException("The  origin position (x, y) must be specified");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // copy the specified snippet
        final ConfigurationSnapshot<FlowSnippetDTO> controllerResponse = serviceFacade.copySnippet(
                new Revision(clientVersion, clientId.getClientId()),
                groupId, snippetId, originX.getDouble(), originY.getDouble());

        // get the snippet
        final FlowSnippetDTO flowSnippet = controllerResponse.getConfiguration();

        // prune response as necessary
        for (ProcessGroupDTO group : flowSnippet.getProcessGroups()) {
            if (group.getContents() != null) {
                group.setContents(null);
            }
        }

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final FlowSnippetEntity entity = new FlowSnippetEntity();
        entity.setRevision(revision);
        entity.setContents(populateRemainingSnippetContent(flowSnippet));

        // generate the response
        return clusterContext(generateCreatedResponse(getAbsolutePath(), entity)).build();
    }

    /**
     * Instantiates the specified template within this ProcessGroup. The template instance that is instantiated cannot be referenced at a later time, therefore there is no
     * corresponding URI. Instead the request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param templateId The id of the template to instantiate.
     * @param originX The x coordinate of the origin of the bounding box.
     * @param originY The y coordinate of the origin of the bounding box.
     * @return A flowSnippetEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/template-instance")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Instantiates a template",
            response = FlowSnippetEntity.class,
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
    public Response instantiateTemplate(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The process group id.",
                required = true
            )
            @PathParam("id") String groupId,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @FormParam(VERSION) LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the template",
                    required = false
            )
            @FormParam("templateId") String templateId,
            @ApiParam(
                    value = "The x coordinate of the origin of the bounding box where the new components will be placed.",
                    required = true
            )
            @FormParam("originX") DoubleParameter originX,
            @ApiParam(
                    value = "The y coordinate of the origin of the bounding box where the new components will be placed.",
                    required = true
            )
            @FormParam("originY") DoubleParameter originY) {

        // ensure the position has been specified
        if (originX == null || originY == null) {
            throw new IllegalArgumentException("The  origin position (x, y) must be specified");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // create the template and generate the json
        final ConfigurationSnapshot<FlowSnippetDTO> response = serviceFacade.createTemplateInstance(
                new Revision(clientVersion, clientId.getClientId()), groupId, originX.getDouble(), originY.getDouble(), templateId);
        final FlowSnippetDTO flowSnippet = response.getConfiguration();

        // prune response as necessary
        for (ProcessGroupDTO group : flowSnippet.getProcessGroups()) {
            if (group.getContents() != null) {
                group.setContents(null);
            }
        }

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(response.getVersion());

        // create the response entity
        final FlowSnippetEntity entity = new FlowSnippetEntity();
        entity.setRevision(revision);
        entity.setContents(populateRemainingSnippetContent(flowSnippet));

        // generate the response
        return clusterContext(generateCreatedResponse(getAbsolutePath(), entity)).build();
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendent groups and their content will be included if the verbose flag is also set to true.
     * @param groupId The id of the process group.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true processor configuration and property details will be included in the response.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a process group",
            response = ProcessGroupEntity.class,
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
    public Response getProcessGroup(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The process group id.",
                    required = false
            )
            @PathParam("id") String groupId,
            @ApiParam(
                    value = "Whether the response should contain all encapsulated components or just the immediate children.",
                    required = false
            )
            @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive,
            @ApiParam(
                    value = "Whether to include any encapulated components or just details about the process group.",
                    required = false
            )
            @QueryParam("verbose") @DefaultValue(VERBOSE) Boolean verbose) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // only recurse if the request is verbose and recursive
        final boolean recurse = verbose && recursive;

        // get this process group contents
        final ConfigurationSnapshot<ProcessGroupDTO> controllerResponse = serviceFacade.getProcessGroup(groupId, recurse);
        final ProcessGroupDTO processGroup = controllerResponse.getConfiguration();

        // prune the response if necessary
        if (!verbose) {
            processGroup.setContents(null);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();
        processGroupEntity.setRevision(revision);
        processGroupEntity.setProcessGroup(populateRemainingProcessGroupContent(processGroup));

        return clusterContext(generateOkResponse(processGroupEntity)).build();
    }

    /**
     * Updates the specified process group.
     *
     * @param httpServletRequest request
     * @param id The id of the process group.
     * @param processGroupEntity A processGroupEntity.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a process group",
            response = ProcessGroupEntity.class,
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
    public Response updateProcessGroupReference(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The process group configuration details.",
                    required = true
            )
            ProcessGroupEntity processGroupEntity) {

        if (processGroupEntity == null || processGroupEntity.getProcessGroup() == null) {
            throw new IllegalArgumentException("Process group details must be specified.");
        }

        if (processGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the same id is being used
        final ProcessGroupDTO requestProcessGroupDTO = processGroupEntity.getProcessGroup();
        if (!id.equals(requestProcessGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestProcessGroupDTO.getId(), id));
        }

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(processGroupEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateProcessGroup(requestProcessGroupDTO);
            return generateContinueResponse().build();
        }

        // update the process group
        final RevisionDTO revision = processGroupEntity.getRevision();
        final ConfigurationSnapshot<ProcessGroupDTO> response = serviceFacade.updateProcessGroup(
                new Revision(revision.getVersion(), revision.getClientId()), requestProcessGroupDTO);
        final ProcessGroupDTO processGroup = response.getConfiguration();

        // create the revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(response.getVersion());

        // create the response entity
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessGroup(populateRemainingProcessGroupContent(processGroup));

        if (response.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(processGroup.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified process group reference.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the process group to be removed.
     * @return A processGroupEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a process group",
            response = ProcessGroupEntity.class,
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
    public Response removeProcessGroupReference(
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
                    value = "The process group id.",
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
            serviceFacade.verifyDeleteProcessGroup(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the process group
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteProcessGroup(new Revision(clientVersion, clientId.getClientId()), id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);

        // create the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the status report for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendant groups and the status of their content will be included.
     * @param groupId The group id
     * @return A processGroupStatusEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @ApiOperation(
            value = "Gets the status for a process group",
            notes = "The status for a process group includes status for all descendent components. When invoked on the root group with "
            + "recursive set to true, it will return the current status of every component in the flow.",
            response = ProcessGroupStatusEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN"),
                @Authorization(value = "NiFi", type = "ROLE_NIFI")
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
    public Response getProcessGroupStatus(
            @ApiParam(
                value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "Whether all descendant groups and the status of their content will be included. Optional, defaults to false",
                required = false
            )
            @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive,
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
                value = "The process group id.",
                required = true
            )
            @PathParam("id") String groupId) {

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final ProcessGroupStatusEntity entity = (ProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessGroupStatus().setNodeSnapshots(null);
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

        // get the status
        final ProcessGroupStatusDTO statusReport = serviceFacade.getProcessGroupStatus(groupId);

        // prune the response as necessary
        if (!recursive) {
            pruneChildGroups(statusReport.getAggregateSnapshot());
            if (statusReport.getNodeSnapshots() != null) {
                for (final NodeProcessGroupStatusSnapshotDTO nodeSnapshot : statusReport.getNodeSnapshots()) {
                    pruneChildGroups(nodeSnapshot.getStatusSnapshot());
                }
            }
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessGroupStatusEntity entity = new ProcessGroupStatusEntity();
        entity.setRevision(revision);
        entity.setProcessGroupStatus(statusReport);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    private void pruneChildGroups(final ProcessGroupStatusSnapshotDTO snapshot) {
        for (final ProcessGroupStatusSnapshotDTO childProcessGroupStatus : snapshot.getProcessGroupStatusSnapshots()) {
            childProcessGroupStatus.setConnectionStatusSnapshots(null);
            childProcessGroupStatus.setProcessGroupStatusSnapshots(null);
            childProcessGroupStatus.setInputPortStatusSnapshots(null);
            childProcessGroupStatus.setOutputPortStatusSnapshots(null);
            childProcessGroupStatus.setProcessorStatusSnapshots(null);
            childProcessGroupStatus.setRemoteProcessGroupStatusSnapshots(null);
        }
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param groupId The group id
     * @return A processorEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets status history for a remote process group",
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
    public Response getProcessGroupStatusHistory(
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the specified processor status history
        final StatusHistoryDTO processGroupStatusHistory = serviceFacade.getProcessGroupStatusHistory(groupId);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setRevision(revision);
        entity.setStatusHistory(processGroupStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Adds the specified process group.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param processGroupEntity A processGroupEntity
     * @return A processGroupEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a process group",
        response = ProcessGroupEntity.class,
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
    public Response createProcessGroup(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = false
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The process group configuration details.",
            required = true
        )
            ProcessGroupEntity processGroupEntity) {

        if (processGroupEntity == null || processGroupEntity.getProcessGroup() == null) {
            throw new IllegalArgumentException("Process group details must be specified.");
        }

        if (processGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (processGroupEntity.getProcessGroup().getId() != null) {
            throw new IllegalArgumentException("Process group ID cannot be specified.");
        }

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(processGroupEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            processGroupEntity.getProcessGroup().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            processGroupEntity.getProcessGroup().setId(UUID.randomUUID().toString());
        }

        // create the process group contents
        final RevisionDTO revision = processGroupEntity.getRevision();
        final ConfigurationSnapshot<ProcessGroupDTO> controllerResponse = serviceFacade.createProcessGroup(groupId,
            new Revision(revision.getVersion(), revision.getClientId()), processGroupEntity.getProcessGroup());
        final ProcessGroupDTO processGroup = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessGroup(populateRemainingProcessGroupContent(processGroup));

        // generate a 201 created response
        String uri = processGroup.getUri();
        return clusterContext(generateCreatedResponse(URI.create(uri), entity)).build();
    }

    /**
     * Retrieves all the processors in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A processorsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all process groups",
        response = ProcessorsEntity.class,
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
    public Response getProcessGroups(
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the process groups
        final Set<ProcessGroupDTO> processGroupDTOs = serviceFacade.getProcessGroups(groupId);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessGroupsEntity entity = new ProcessGroupsEntity();
        entity.setRevision(revision);
        entity.setProcessGroups(populateRemainingProcessGroupsContent(processGroupDTOs));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new processor.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param processorEntity A processorEntity.
     * @return A processorEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/processors")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a new processor",
        response = ProcessorEntity.class,
        authorizations = {
            @Authorization(value = "ROLE_DFM", type = "ROLE_DFM")
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
    public Response createProcessor(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The process group id.",
                required = true
            )
            @PathParam("id") String groupId,
            @ApiParam(
                value = "The processor configuration details.",
                required = true
            )
            ProcessorEntity processorEntity) {

        if (processorEntity == null || processorEntity.getProcessor() == null) {
            throw new IllegalArgumentException("Processor details must be specified.");
        }

        if (processorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (processorEntity.getProcessor().getId() != null) {
            throw new IllegalArgumentException("Processor ID cannot be specified.");
        }

        if (StringUtils.isBlank(processorEntity.getProcessor().getType())) {
            throw new IllegalArgumentException("The type of processor to create must be specified.");
        }

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(processorEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            processorEntity.getProcessor().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            processorEntity.getProcessor().setId(UUID.randomUUID().toString());
        }

        // create the new processor
        final RevisionDTO revision = processorEntity.getRevision();
        final ConfigurationSnapshot<ProcessorDTO> controllerResponse = serviceFacade.createProcessor(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, processorEntity.getProcessor());
        final ProcessorDTO processor = controllerResponse.getConfiguration();
        processorResource.populateRemainingProcessorContent(processor);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // generate the response entity
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessor(processor);

        // generate a 201 created response
        String uri = processor.getUri();
        return clusterContext(generateCreatedResponse(URI.create(uri), entity)).build();
    }

    /**
     * Retrieves all the processors in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A processorsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/processors")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all processors",
        response = ProcessorsEntity.class,
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
    public Response getProcessors(
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the processors
        final Set<ProcessorDTO> processorDTOs = serviceFacade.getProcessors(groupId);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessorsEntity entity = new ProcessorsEntity();
        entity.setRevision(revision);
        entity.setProcessors(processorResource.populateRemainingProcessorsContent(processorDTOs));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new input port.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param portEntity A inputPortEntity.
     * @return A inputPortEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
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
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
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

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(portEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            portEntity.getInputPort().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            portEntity.getInputPort().setId(UUID.randomUUID().toString());
        }

        // create the input port and generate the json
        final RevisionDTO revision = portEntity.getRevision();
        final ConfigurationSnapshot<PortDTO> controllerResponse = serviceFacade.createInputPort(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, portEntity.getInputPort());
        final PortDTO port = controllerResponse.getConfiguration();
        inputPortResource.populateRemainingInputPortContent(port);

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
     * Retrieves all the of input ports in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A inputPortsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the input ports
        final Set<PortDTO> inputPorts = inputPortResource.populateRemainingInputPortsContent(serviceFacade.getInputPorts(groupId));

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
     * Creates a new output port.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param portEntity A outputPortEntity.
     * @return A outputPortEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates an output port",
        response = OutputPortEntity.class,
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
    public Response createOutputPort(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The output port configuration.",
            required = true
        ) OutputPortEntity portEntity) {

        if (portEntity == null || portEntity.getOutputPort() == null) {
            throw new IllegalArgumentException("Port details must be specified.");
        }

        if (portEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (portEntity.getOutputPort().getId() != null) {
            throw new IllegalArgumentException("Output port ID cannot be specified.");
        }

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(portEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            portEntity.getOutputPort().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            portEntity.getOutputPort().setId(UUID.randomUUID().toString());
        }

        // create the output port and generate the json
        final RevisionDTO revision = portEntity.getRevision();
        final ConfigurationSnapshot<PortDTO> controllerResponse = serviceFacade.createOutputPort(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, portEntity.getOutputPort());
        final PortDTO port = controllerResponse.getConfiguration();
        outputPortResource.populateRemainingOutputPortContent(port);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final OutputPortEntity entity = new OutputPortEntity();
        entity.setRevision(updatedRevision);
        entity.setOutputPort(port);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(port.getUri()), entity)).build();
    }

    /**
     * Retrieves all the of output ports in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A outputPortsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all output ports",
        response = OutputPortsEntity.class,
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
    public Response getOutputPorts(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the output ports
        final Set<PortDTO> outputPorts = outputPortResource.populateRemainingOutputPortsContent(serviceFacade.getOutputPorts(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final OutputPortsEntity entity = new OutputPortsEntity();
        entity.setRevision(revision);
        entity.setOutputPorts(outputPorts);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new Funnel.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param funnelEntity A funnelEntity.
     * @return A funnelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/funnels")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a funnel",
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
    public Response createFunnel(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The funnel configuration details.",
            required = true
        ) FunnelEntity funnelEntity) {

        if (funnelEntity == null || funnelEntity.getFunnel() == null) {
            throw new IllegalArgumentException("Funnel details must be specified.");
        }

        if (funnelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (funnelEntity.getFunnel().getId() != null) {
            throw new IllegalArgumentException("Funnel ID cannot be specified.");
        }

        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity if request came through overloaded endpoint
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(funnelEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            funnelEntity.getFunnel().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            funnelEntity.getFunnel().setId(UUID.randomUUID().toString());
        }

        // create the funnel and generate the json
        final RevisionDTO revision = funnelEntity.getRevision();
        final ConfigurationSnapshot<FunnelDTO> controllerResponse = serviceFacade.createFunnel(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, funnelEntity.getFunnel());
        final FunnelDTO funnel = controllerResponse.getConfiguration();
        funnelResource.populateRemainingFunnelContent(funnel);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(updatedRevision);
        entity.setFunnel(funnel);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(funnel.getUri()), entity)).build();
    }

    /**
     * Retrieves all the of funnels in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A funnelsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/funnels")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all funnels",
        response = FunnelsEntity.class,
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
    public Response getFunnels(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the funnels
        final Set<FunnelDTO> funnels = funnelResource.populateRemainingFunnelsContent(serviceFacade.getFunnels(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final FunnelsEntity entity = new FunnelsEntity();
        entity.setRevision(revision);
        entity.setFunnels(funnels);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new Label.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param labelEntity A labelEntity.
     * @return A labelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/labels")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a label",
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
    public Response createLabel(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The label configuration details.",
            required = true
        ) LabelEntity labelEntity) {

        if (labelEntity == null || labelEntity.getLabel() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (labelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (labelEntity.getLabel().getId() != null) {
            throw new IllegalArgumentException("Label ID cannot be specified.");
        }

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(labelEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            labelEntity.getLabel().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            labelEntity.getLabel().setId(UUID.randomUUID().toString());
        }

        // create the label and generate the json
        final RevisionDTO revision = labelEntity.getRevision();
        final ConfigurationSnapshot<LabelDTO> controllerResponse = serviceFacade.createLabel(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, labelEntity.getLabel());
        final LabelDTO label = controllerResponse.getConfiguration();
        labelResource.populateRemainingLabelContent(label);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(updatedRevision);
        entity.setLabel(label);

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(label.getUri()), entity)).build();
    }

    /**
     * Retrieves all the of labels in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A labelsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}/labels")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all labels",
        response = LabelsEntity.class,
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
    public Response getLabels(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the labels
        final Set<LabelDTO> labels = labelResource.populateRemainingLabelsContent(serviceFacade.getLabels(groupId));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final LabelsEntity entity = new LabelsEntity();
        entity.setRevision(revision);
        entity.setLabels(labels);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new remote process group.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param remoteProcessGroupEntity A remoteProcessGroupEntity.
     * @return A remoteProcessGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/remote-process-groups")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a new process group",
        response = RemoteProcessGroupEntity.class,
        authorizations = {
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
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
    public Response createRemoteProcessGroup(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The remote process group configuration details.",
            required = true
        ) RemoteProcessGroupEntity remoteProcessGroupEntity) {

        if (remoteProcessGroupEntity == null || remoteProcessGroupEntity.getRemoteProcessGroup() == null) {
            throw new IllegalArgumentException("Remote process group details must be specified.");
        }

        if (remoteProcessGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        final RemoteProcessGroupDTO requestProcessGroupDTO = remoteProcessGroupEntity.getRemoteProcessGroup();

        if (requestProcessGroupDTO.getId() != null) {
            throw new IllegalArgumentException("Remote process group ID cannot be specified.");
        }

        if (requestProcessGroupDTO.getTargetUri() == null) {
            throw new IllegalArgumentException("The URI of the process group must be specified.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity if request came through overloaded endpoint
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(remoteProcessGroupEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            requestProcessGroupDTO.setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            requestProcessGroupDTO.setId(UUID.randomUUID().toString());
        }

        // parse the uri
        final URI uri;
        try {
            uri = URI.create(requestProcessGroupDTO.getTargetUri());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + requestProcessGroupDTO.getTargetUri());
        }

        // validate each part of the uri
        if (uri.getScheme() == null || uri.getHost() == null) {
            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + requestProcessGroupDTO.getTargetUri());
        }

        if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("The specified remote process group URL is invalid because it is not http or https: " + requestProcessGroupDTO.getTargetUri());
        }

        // normalize the uri to the other controller
        String controllerUri = uri.toString();
        if (controllerUri.endsWith("/")) {
            controllerUri = StringUtils.substringBeforeLast(controllerUri, "/");
        }

        // since the uri is valid, use the normalized version
        requestProcessGroupDTO.setTargetUri(controllerUri);

        // create the remote process group
        final RevisionDTO revision = remoteProcessGroupEntity.getRevision();
        final ConfigurationSnapshot<RemoteProcessGroupDTO> controllerResponse
            = serviceFacade.createRemoteProcessGroup(new Revision(revision.getVersion(), revision.getClientId()), groupId, requestProcessGroupDTO);

        // prepare the response
        final RemoteProcessGroupDTO remoteProcessGroup = controllerResponse.getConfiguration();
        remoteProcessGroupResource.populateRemainingRemoteProcessGroupContent(remoteProcessGroup);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setRemoteProcessGroup(remoteProcessGroup);

        return clusterContext(generateCreatedResponse(URI.create(remoteProcessGroup.getUri()), entity)).build();
    }

    /**
     * Retrieves all the of remote process groups in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true remote group contents (ports) will be included.
     * @return A remoteProcessGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/remote-process-groups")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all remote process groups",
        response = RemoteProcessGroupsEntity.class,
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
    public Response getRemoteProcessGroups(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "Whether to include any encapulated ports or just details about the remote process group.",
            required = false
        )
        @QueryParam("verbose") @DefaultValue(VERBOSE) Boolean verbose,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the labels
        final Set<RemoteProcessGroupDTO> remoteProcessGroups = serviceFacade.getRemoteProcessGroups(groupId);

        // prune response as necessary
        if (!verbose) {
            for (RemoteProcessGroupDTO remoteProcessGroup : remoteProcessGroups) {
                remoteProcessGroup.setContents(null);
            }
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final RemoteProcessGroupsEntity entity = new RemoteProcessGroupsEntity();
        entity.setRevision(revision);
        entity.setRemoteProcessGroups(remoteProcessGroupResource.populateRemainingRemoteProcessGroupsContent(remoteProcessGroups));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new connection.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param connectionEntity A connectionEntity.
     * @return A connectionEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/connections")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Creates a connection",
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
    public Response createConnection(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "The connection configuration details.",
            required = true
        ) ConnectionEntity connectionEntity) {

        if (connectionEntity == null || connectionEntity.getConnection() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (connectionEntity.getConnection().getId() != null) {
            throw new IllegalArgumentException("Connection ID cannot be specified.");
        }

        if (connectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity if request came through overloaded endpoint
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(connectionEntity), getHeaders(headersToOverride)).getResponse();
        }

        // get the connection
        final ConnectionDTO connection = connectionEntity.getConnection();

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyCreateConnection(groupId, connection);
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            connection.setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            connection.setId(UUID.randomUUID().toString());
        }

        // create the new relationship target
        final RevisionDTO revision = connectionEntity.getRevision();
        final ConfigurationSnapshot<ConnectionDTO> controllerResponse = serviceFacade.createConnection(
            new Revision(revision.getVersion(), revision.getClientId()), groupId, connection);
        ConnectionDTO connectionDTO = controllerResponse.getConfiguration();

        // marshall the target and add the source processor
        connectionResource.populateRemainingConnectionContent(connectionDTO);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(updatedRevision);
        entity.setConnection(connectionDTO);

        // extract the href and build the response
        String href = connectionDTO.getUri();

        return clusterContext(generateCreatedResponse(URI.create(href), entity)).build();
    }

    /**
     * Gets all the connections.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A connectionsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/connections")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all connections",
        response = ConnectionsEntity.class,
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
    public Response getConnections(
        @ApiParam(
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // all of the relationships for the specified source processor
        Set<ConnectionDTO> connections = serviceFacade.getConnections(groupId);

        // create the revision
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the client response entity
        ConnectionsEntity entity = new ConnectionsEntity();
        entity.setRevision(revision);
        entity.setConnections(connectionResource.populateRemainingConnectionsContent(connections));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProcessorResource(ProcessorResource processorResource) {
        this.processorResource = processorResource;
    }

    public void setInputPortResource(InputPortResource inputPortResource) {
        this.inputPortResource = inputPortResource;
    }

    public void setOutputPortResource(OutputPortResource outputPortResource) {
        this.outputPortResource = outputPortResource;
    }

    public void setFunnelResource(FunnelResource funnelResource) {
        this.funnelResource = funnelResource;
    }

    public void setLabelResource(LabelResource labelResource) {
        this.labelResource = labelResource;
    }

    public void setRemoteProcessGroupResource(RemoteProcessGroupResource remoteProcessGroupResource) {
        this.remoteProcessGroupResource = remoteProcessGroupResource;
    }

    public void setConnectionResource(ConnectionResource connectionResource) {
        this.connectionResource = connectionResource;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
