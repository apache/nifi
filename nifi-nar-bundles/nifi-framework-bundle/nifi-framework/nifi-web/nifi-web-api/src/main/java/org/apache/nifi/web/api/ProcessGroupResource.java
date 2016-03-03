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
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.FlowSnippetEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
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
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for managing a Group.
 */
@Api(hidden = true)
public class ProcessGroupResource extends ApplicationResource {

    private static final String VERBOSE = "false";
    private static final String RECURSIVE = "false";

    @Context
    private ResourceContext resourceContext;

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private String groupId;

    /**
     * Get the processor resource within the specified group.
     *
     * @return the processor resource within the specified group
     */
    @Path("processors")
    @ApiOperation(
            value = "Gets the processor resource",
            response = ProcessorResource.class
    )
    public ProcessorResource getProcessorResource() {
        ProcessorResource processorResource = resourceContext.getResource(ProcessorResource.class);
        processorResource.setGroupId(groupId);
        return processorResource;
    }

    /**
     * Get the connection sub-resource within the specified group.
     *
     * @return the connection sub-resource within the specified group
     */
    @Path("connections")
    @ApiOperation(
            value = "Gets the connection resource",
            response = ConnectionResource.class
    )
    public ConnectionResource getConnectionResource() {
        ConnectionResource connectionResource = resourceContext.getResource(ConnectionResource.class);
        connectionResource.setGroupId(groupId);
        return connectionResource;
    }

    /**
     * Get the input ports sub-resource within the specified group.
     *
     * @return the input ports sub-resource within the specified group
     */
    @Path("input-ports")
    @ApiOperation(
            value = "Gets the input port resource",
            response = InputPortResource.class
    )
    public InputPortResource getInputPortResource() {
        InputPortResource inputPortResource = resourceContext.getResource(InputPortResource.class);
        inputPortResource.setGroupId(groupId);
        return inputPortResource;
    }

    /**
     * Get the output ports sub-resource within the specified group.
     *
     * @return the output ports sub-resource within the specified group
     */
    @Path("output-ports")
    @ApiOperation(
            value = "Gets the output port resource",
            response = OutputPortResource.class
    )
    public OutputPortResource getOutputPortResource() {
        OutputPortResource outputPortResource = resourceContext.getResource(OutputPortResource.class);
        outputPortResource.setGroupId(groupId);
        return outputPortResource;
    }

    /**
     * Locates the label sub-resource within the specified group.
     *
     * @return the label sub-resource within the specified group
     */
    @Path("labels")
    @ApiOperation(
            value = "Gets the label resource",
            response = LabelResource.class
    )
    public LabelResource getLabelResource() {
        LabelResource labelResource = resourceContext.getResource(LabelResource.class);
        labelResource.setGroupId(groupId);
        return labelResource;
    }

    /**
     * Locates the funnel sub-resource within the specified group.
     *
     * @return the funnel sub-resource within the specified group
     */
    @Path("funnels")
    @ApiOperation(
            value = "Gets the funnel resource",
            response = FunnelResource.class
    )
    public FunnelResource getFunnelResource() {
        FunnelResource funnelResource = resourceContext.getResource(FunnelResource.class);
        funnelResource.setGroupId(groupId);
        return funnelResource;
    }

    /**
     * Locates the remote process group sub-resource within the specified group.
     *
     * @return the remote process group sub-resource within the specified group
     */
    @Path("remote-process-groups")
    @ApiOperation(
            value = "Gets the remote process group resource",
            response = RemoteProcessGroupResource.class
    )
    public RemoteProcessGroupResource getRemoteProcessGroupResource() {
        RemoteProcessGroupResource remoteProcessGroupResource = resourceContext.getResource(RemoteProcessGroupResource.class);
        remoteProcessGroupResource.setGroupId(groupId);
        return remoteProcessGroupResource;
    }

    /**
     * Populates the remaining fields in the specified process groups.
     *
     * @param processGroups groups
     * @return group dto
     */
    public Set<ProcessGroupDTO> populateRemainingProcessGroupsContent(Set<ProcessGroupDTO> processGroups) {
        for (ProcessGroupDTO processGroup : processGroups) {
            populateRemainingProcessGroupContent(processGroup, getProcessGroupReferenceUri(processGroup));
        }
        return processGroups;
    }

    /**
     * Populates the remaining fields in the specified process group.
     *
     * @param processGroup group
     * @param processGroupUri processGroupUri
     * @return group dto
     */
    private ProcessGroupDTO populateRemainingProcessGroupContent(ProcessGroupDTO processGroup, String processGroupUri) {
        FlowSnippetDTO flowSnippet = processGroup.getContents();

        // populate the remaining fields for the processors, connections, process group refs, remote process groups, and labels if appropriate
        if (flowSnippet != null) {
            populateRemainingSnippetContent(flowSnippet);
        }

        // set the process group uri
        processGroup.setUri(processGroupUri);

        return processGroup;
    }

    /**
     * Populates the remaining content of the specified snippet.
     */
    private FlowSnippetDTO populateRemainingSnippetContent(FlowSnippetDTO snippet) {
        getProcessorResource().populateRemainingProcessorsContent(snippet.getProcessors());
        getConnectionResource().populateRemainingConnectionsContent(snippet.getConnections());
        getInputPortResource().populateRemainingInputPortsContent(snippet.getInputPorts());
        getOutputPortResource().populateRemainingOutputPortsContent(snippet.getOutputPorts());
        getRemoteProcessGroupResource().populateRemainingRemoteProcessGroupsContent(snippet.getRemoteProcessGroups());
        getFunnelResource().populateRemainingFunnelsContent(snippet.getFunnels());
        getLabelResource().populateRemainingLabelsContent(snippet.getLabels());

        // go through each process group child and populate its uri
        if (snippet.getProcessGroups() != null) {
            populateRemainingProcessGroupsContent(snippet.getProcessGroups());
        }

        return snippet;
    }

    /**
     * Generates a URI for a process group.
     */
    private String getProcessGroupUri(String processGroupId) {
        return generateResourceUri("controller", "process-groups", processGroupId);
    }

    /**
     * Generates a URI for a process group reference.
     */
    private String getProcessGroupReferenceUri(ProcessGroupDTO processGroup) {
        return generateResourceUri("controller", "process-groups", processGroup.getParentGroupId(), "process-group-references", processGroup.getId());
    }

    /**
     * Retrieves the content of the specified group. This includes all processors, the connections, the process group references, the remote process group references, and the labels.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendent groups and their content will be included if the verbose flag is also set to true.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true processor configuration and property details will be included in the response.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a process group",
            notes = "Gets a process group and includes all components contained in this group. The verbose and recursive flags can be used to adjust "
            + "the default behavior. This endpoint is starting point for obtaining the current flow and consequently includes the current "
            + "flow revision.",
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
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether the response should contain all encapsulated components or just the immediate children.",
                    required = false,
                    allowableValues = "true, false"
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

        // prune response if necessary
        if (!verbose) {
            processGroup.setContents(null);
        }

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();
        processGroupEntity.setRevision(revision);
        processGroupEntity.setProcessGroup(populateRemainingProcessGroupContent(processGroup, getProcessGroupUri(processGroup.getId())));

        return clusterContext(generateOkResponse(processGroupEntity)).build();
    }

    /**
     * Copies the specified snippet within this ProcessGroup. The snippet instance that is instantiated cannot be referenced at a later time, therefore there is no
     * corresponding URI. Instead the request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param snippetId The id of the snippet to copy.
     * @param originX The x coordinate of the origin of the bounding box.
     * @param originY The y coordinate of the origin of the bounding box.
     * @return A flowSnippetEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/snippet-instance")
    @PreAuthorize("hasRole('ROLE_DFM')")
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
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param templateId The id of the template to instantiate.
     * @param originX The x coordinate of the origin of the bounding box.
     * @param originY The y coordinate of the origin of the bounding box.
     * @return A flowSnippetEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/template-instance")
    @PreAuthorize("hasRole('ROLE_DFM')")
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
     * Updates the state of all processors in the process group. Supports modifying whether the processors and process groups are running/stopped and instantiating templates.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param running Optional flag that indicates whether all processors in this group should be started/stopped.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateProcessGroup(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("running") Boolean running) {

        // create the process group dto
        final ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setId(groupId);
        processGroup.setRunning(running);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the entity for the request
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setProcessGroup(processGroup);

        // update the process group
        return updateProcessGroup(httpServletRequest, entity);
    }

    /**
     * Updates the state of all processors in the process group. Supports modifying whether the processors and process groups are running/stopped and instantiating templates.
     *
     * @param httpServletRequest request
     * @param processGroupEntity A processGroupEntity
     * @return A processGroupEntity
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a process group",
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
    public Response updateProcessGroup(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group to update. The only action that is supported at this endpoint is to set the running flag in order "
                    + "to start or stop all descendent schedulable components. This defines the schema of the expected input.",
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
        ProcessGroupDTO requestProcessGroupDTO = processGroupEntity.getProcessGroup();
        if (!groupId.equals(requestProcessGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestProcessGroupDTO.getId(), groupId));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(processGroupEntity), getHeaders(headersToOverride)).getResponse();
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
                new Revision(revision.getVersion(), revision.getClientId()), null, requestProcessGroupDTO);
        final ProcessGroupDTO processGroup = response.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(response.getVersion());

        // create the response entity
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessGroup(populateRemainingProcessGroupContent(processGroup, getProcessGroupUri(processGroup.getId())));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendent groups and their content will be included if the verbose flag is also set to true.
     * @param processGroupReferenceId The id of the process group.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true processor configuration and property details will be included in the response.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
            @PathParam("id") String processGroupReferenceId,
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
        final ConfigurationSnapshot<ProcessGroupDTO> controllerResponse = serviceFacade.getProcessGroup(processGroupReferenceId, recurse);
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
        processGroupEntity.setProcessGroup(populateRemainingProcessGroupContent(processGroup, getProcessGroupReferenceUri(processGroup)));

        return clusterContext(generateOkResponse(processGroupEntity)).build();
    }

    /**
     * Retrieves the content of the specified group reference.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param verbose Optional verbose flag that defaults to false. If the verbose flag is set to true processor configuration and property details will be included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all process groups",
            response = ProcessGroupsEntity.class,
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
    public Response getProcessGroupReferences(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether to include any encapulated components or just details about the process group.",
                    required = false
            )
            @QueryParam("verbose") @DefaultValue(VERBOSE) Boolean verbose) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get this process group contents
        final Set<ProcessGroupDTO> processGroups = serviceFacade.getProcessGroups(groupId);

        // prune the response if necessary
        if (!verbose) {
            for (ProcessGroupDTO processGroup : processGroups) {
                processGroup.setContents(null);
            }
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessGroupsEntity processGroupsEntity = new ProcessGroupsEntity();
        processGroupsEntity.setRevision(revision);
        processGroupsEntity.setProcessGroups(populateRemainingProcessGroupsContent(processGroups));

        return clusterContext(generateOkResponse(processGroupsEntity)).build();
    }

    /**
     * Adds the specified process group.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param name The name of the process group
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @return A processGroupEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response createProcessGroupReference(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("name") String name, @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y) {

        // ensure the position has been specified
        if (x == null || y == null) {
            throw new IllegalArgumentException("The position (x, y) must be specified");
        }

        // create the process group dto
        final ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setName(name);
        processGroup.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the entity for the request
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setProcessGroup(processGroup);

        // create the process group
        return createProcessGroupReference(httpServletRequest, entity);
    }

    /**
     * Adds the specified process group.
     *
     * @param httpServletRequest request
     * @param processGroupEntity A processGroupEntity
     * @return A processGroupEntity
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references")
    @PreAuthorize("hasRole('ROLE_DFM')")
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
    public Response createProcessGroupReference(
            @Context HttpServletRequest httpServletRequest,
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

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            processGroupEntity.getProcessGroup().setId(id);

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
            return clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(processGroupEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
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
        entity.setProcessGroup(populateRemainingProcessGroupContent(processGroup, getProcessGroupReferenceUri(processGroup)));

        // generate a 201 created response
        String uri = processGroup.getUri();
        return clusterContext(generateCreatedResponse(URI.create(uri), entity)).build();
    }

    /**
     * Updates the specified process group.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the process group
     * @param name The name of the process group.
     * @param comments The comments for the process group.
     * @param running Optional flag that indicates whether all processors should be started/stopped.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateProcessGroupReference(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @FormParam("name") String name,
            @FormParam("comments") String comments,
            @FormParam("running") Boolean running,
            @FormParam("x") DoubleParameter x,
            @FormParam("y") DoubleParameter y) {

        // create the process group dto
        final ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setId(id);
        processGroup.setName(name);
        processGroup.setComments(comments);
        processGroup.setRunning(running);

        // require both coordinates to be specified
        if (x != null && y != null) {
            processGroup.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the entity for the request
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setProcessGroup(processGroup);

        // update the process group
        return updateProcessGroupReference(httpServletRequest, id, entity);
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
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
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

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(processGroupEntity), getHeaders(headersToOverride)).getResponse();
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
                new Revision(revision.getVersion(), revision.getClientId()), groupId, requestProcessGroupDTO);
        final ProcessGroupDTO processGroup = response.getConfiguration();

        // create the revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(response.getVersion());

        // create the response entity
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessGroup(populateRemainingProcessGroupContent(processGroup, getProcessGroupReferenceUri(processGroup)));

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
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/process-group-references/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
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
     * @return A processGroupStatusEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/status")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
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
            @QueryParam("clusterNodeId") String clusterNodeId) {

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
     * @return A processorEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/status/history")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
    public Response getProcessGroupStatusHistory(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

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
