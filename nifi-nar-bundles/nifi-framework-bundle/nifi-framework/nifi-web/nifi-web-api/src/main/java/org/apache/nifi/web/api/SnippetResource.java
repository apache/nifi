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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Snippet.
 */
@Api(hidden = true)
public class SnippetResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SnippetResource.class);
    private static final String LINKED = "false";
    private static final String VERBOSE = "false";

    @Context
    private ResourceContext resourceContext;

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    /**
     * Get the processor resource within the specified group.
     *
     * @return the processor resource within the specified group
     */
    private ProcessorResource getProcessorResource(final String groupId) {
        ProcessorResource processorResource = resourceContext.getResource(ProcessorResource.class);
        processorResource.setGroupId(groupId);
        return processorResource;
    }

    /**
     * Get the connection sub-resource within the specified group.
     *
     * @return the connection sub-resource within the specified group
     */
    private ConnectionResource getConnectionResource(final String groupId) {
        ConnectionResource connectionResource = resourceContext.getResource(ConnectionResource.class);
        connectionResource.setGroupId(groupId);
        return connectionResource;
    }

    /**
     * Get the input ports sub-resource within the specified group.
     *
     * @return the input ports sub-resource within the specified group
     */
    private InputPortResource getInputPortResource(final String groupId) {
        InputPortResource inputPortResource = resourceContext.getResource(InputPortResource.class);
        inputPortResource.setGroupId(groupId);
        return inputPortResource;
    }

    /**
     * Get the output ports sub-resource within the specified group.
     *
     * @return the output ports sub-resource within the specified group
     */
    private OutputPortResource getOutputPortResource(final String groupId) {
        OutputPortResource outputPortResource = resourceContext.getResource(OutputPortResource.class);
        outputPortResource.setGroupId(groupId);
        return outputPortResource;
    }

    /**
     * Locates the label sub-resource within the specified group.
     *
     * @return the label sub-resource within the specified group
     */
    private LabelResource getLabelResource(final String groupId) {
        LabelResource labelResource = resourceContext.getResource(LabelResource.class);
        labelResource.setGroupId(groupId);
        return labelResource;
    }

    /**
     * Locates the funnel sub-resource within the specified group.
     *
     * @return the funnel sub-resource within the specified group
     */
    private FunnelResource getFunnelResource(final String groupId) {
        FunnelResource funnelResource = resourceContext.getResource(FunnelResource.class);
        funnelResource.setGroupId(groupId);
        return funnelResource;
    }

    /**
     * Locates the remote process group sub-resource within the specified group.
     *
     * @return the remote process group sub-resource within the specified group
     */
    private RemoteProcessGroupResource getRemoteProcessGroupResource(final String groupId) {
        RemoteProcessGroupResource remoteProcessGroupResource = resourceContext.getResource(RemoteProcessGroupResource.class);
        remoteProcessGroupResource.setGroupId(groupId);
        return remoteProcessGroupResource;
    }

    /**
     * Locates the process group sub-resource within the specified group.
     *
     * @param groupId group id
     * @return the process group sub-resource within the specified group
     */
    private ProcessGroupResource getProcessGroupResource(final String groupId) {
        ProcessGroupResource processGroupResource = resourceContext.getResource(ProcessGroupResource.class);
        processGroupResource.setGroupId(groupId);
        return processGroupResource;
    }

    /**
     * Populates the uri for the specified snippet.
     */
    private SnippetDTO populateRemainingSnippetContent(SnippetDTO snippet) {
        // populate the snippet href
        snippet.setUri(generateResourceUri("controller", "snippets", snippet.getId()));

        String snippetGroupId = snippet.getParentGroupId();
        FlowSnippetDTO snippetContents = snippet.getContents();

        // populate the snippet content uris
        if (snippet.getContents() != null) {
            getProcessorResource(snippetGroupId).populateRemainingProcessorsContent(snippetContents.getProcessors());
            getConnectionResource(snippetGroupId).populateRemainingConnectionsContent(snippetContents.getConnections());
            getInputPortResource(snippetGroupId).populateRemainingInputPortsContent(snippetContents.getInputPorts());
            getOutputPortResource(snippetGroupId).populateRemainingOutputPortsContent(snippetContents.getOutputPorts());
            getRemoteProcessGroupResource(snippetGroupId).populateRemainingRemoteProcessGroupsContent(snippetContents.getRemoteProcessGroups());
            getFunnelResource(snippetGroupId).populateRemainingFunnelsContent(snippetContents.getFunnels());
            getLabelResource(snippetGroupId).populateRemainingLabelsContent(snippetContents.getLabels());
            getProcessGroupResource(snippetGroupId).populateRemainingProcessGroupsContent(snippetContents.getProcessGroups());
        }

        return snippet;
    }

    /**
     * Creates a new snippet based on the specified contents.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param parentGroupId The id of the process group the components in this
     * snippet belong to.
     * @param linked Whether or not this snippet is linked to the underlying
     * data flow. If a linked snippet is deleted, the components that comprise
     * the snippet are also deleted.
     * @param processorIds The ids of any processors in this snippet.
     * @param processGroupIds The ids of any process groups in this snippet.
     * @param remoteProcessGroupIds The ids of any remote process groups in this
     * snippet.
     * @param inputPortIds The ids of any input ports in this snippet.
     * @param outputPortIds The ids of any output ports in this snippet.
     * @param connectionIds The ids of any connections in this snippet.
     * @param labelIds The ids of any labels in this snippet.
     * @param funnelIds The ids of any funnels in this snippet.
     * @return A snippetEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response createSnippet(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("parentGroupId") String parentGroupId,
            @FormParam("linked") @DefaultValue(LINKED) Boolean linked,
            @FormParam("processorIds[]") List<String> processorIds,
            @FormParam("processGroupIds[]") List<String> processGroupIds,
            @FormParam("remoteProcessGroupIds[]") List<String> remoteProcessGroupIds,
            @FormParam("inputPortIds[]") List<String> inputPortIds,
            @FormParam("outputPortIds[]") List<String> outputPortIds,
            @FormParam("connectionIds[]") List<String> connectionIds,
            @FormParam("labelIds[]") List<String> labelIds,
            @FormParam("funnelIds[]") List<String> funnelIds) {

        // create the snippet dto
        final SnippetDTO snippetDTO = new SnippetDTO();
        snippetDTO.setParentGroupId(parentGroupId);
        snippetDTO.setLinked(linked);
        snippetDTO.setProcessors(new HashSet<>(processorIds));
        snippetDTO.setProcessGroups(new HashSet<>(processGroupIds));
        snippetDTO.setRemoteProcessGroups(new HashSet<>(remoteProcessGroupIds));
        snippetDTO.setInputPorts(new HashSet<>(inputPortIds));
        snippetDTO.setOutputPorts(new HashSet<>(outputPortIds));
        snippetDTO.setConnections(new HashSet<>(connectionIds));
        snippetDTO.setLabels(new HashSet<>(labelIds));
        snippetDTO.setFunnels(new HashSet<>(funnelIds));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the snippet entity
        final SnippetEntity snippetEntity = new SnippetEntity();
        snippetEntity.setRevision(revision);
        snippetEntity.setSnippet(snippetDTO);

        // build the response
        return createSnippet(httpServletRequest, snippetEntity);
    }

    /**
     * Creates a snippet based off the specified configuration.
     *
     * @param httpServletRequest request
     * @param snippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a snippet",
            response = SnippetEntity.class,
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
    public Response createSnippet(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The snippet configuration details.",
                    required = true
            )
            final SnippetEntity snippetEntity) {

        if (snippetEntity == null || snippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        if (snippetEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (snippetEntity.getSnippet().getId() != null) {
            throw new IllegalArgumentException("Snippet ID cannot be specified.");
        }

        // ensure the group id has been specified
        if (snippetEntity.getSnippet().getParentGroupId() == null) {
            throw new IllegalArgumentException("The group id must be specified when creating a snippet.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            snippetEntity.getSnippet().setId(id);

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
            return (Response) clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(snippetEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the snippet
        final RevisionDTO revision = snippetEntity.getRevision();
        final ConfigurationSnapshot<SnippetDTO> response = serviceFacade.createSnippet(new Revision(revision.getVersion(), revision.getClientId()), snippetEntity.getSnippet());

        // get the snippet
        final SnippetDTO snippet = response.getConfiguration();

        // always prune the response when creating
        snippet.setContents(null);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(response.getVersion());

        // build the response entity
        SnippetEntity entity = new SnippetEntity();
        entity.setRevision(updatedRevision);
        entity.setSnippet(populateRemainingSnippetContent(snippet));

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(snippet.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified snippet.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param verbose Whether or not to include the contents of the snippet in
     * the response.
     * @param id The id of the snippet to retrieve.
     * @return A snippetEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a snippet",
            response = SnippetEntity.class,
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
    public Response getSnippet(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether to include configuration details for the components specified in the snippet.",
                    required = false
            )
            @QueryParam("verbose") @DefaultValue(VERBOSE) Boolean verbose,
            @ApiParam(
                    value = "The snippet id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the snippet
        final SnippetDTO snippet = serviceFacade.getSnippet(id);

        // prune the response if necessary
        if (!verbose) {
            snippet.setContents(null);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final SnippetEntity entity = new SnippetEntity();
        entity.setRevision(revision);
        entity.setSnippet(populateRemainingSnippetContent(snippet));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified snippet.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param verbose Whether or not to include the contents of the snippet in
     * the response.
     * @param id The id of the snippet to update.
     * @param parentGroupId The id of the process group to move the contents of
     * this snippet to.
     * @param linked Whether or not this snippet is linked to the underlying
     * data flow. If a linked snippet is deleted, the components that comprise
     * the snippet are also deleted.
     * @return A snippetEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateSnippet(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("verbose") @DefaultValue(VERBOSE) Boolean verbose,
            @PathParam("id") String id,
            @FormParam("parentGroupId") String parentGroupId,
            @FormParam("linked") Boolean linked) {

        // create the snippet dto
        final SnippetDTO snippetDTO = new SnippetDTO();
        snippetDTO.setId(id);
        snippetDTO.setParentGroupId(parentGroupId);
        snippetDTO.setLinked(linked);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the snippet entity
        final SnippetEntity entity = new SnippetEntity();
        entity.setRevision(revision);
        entity.setSnippet(snippetDTO);

        // build the response
        return updateSnippet(httpServletRequest, id, entity);
    }

    /**
     * Updates the specified snippet. The contents of the snippet (component
     * ids) cannot be updated once the snippet is created.
     *
     * @param httpServletRequest request
     * @param id The id of the snippet.
     * @param snippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a snippet",
            response = SnippetEntity.class,
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
    public Response updateSnippet(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The snippet id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The snippet configuration details.",
                    required = true
            )
            final SnippetEntity snippetEntity) {

        if (snippetEntity == null || snippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        if (snippetEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final SnippetDTO requestSnippetDTO = snippetEntity.getSnippet();
        if (!id.equals(requestSnippetDTO.getId())) {
            throw new IllegalArgumentException(String.format("The snippet id (%s) in the request body does not equal the "
                    + "snippet id of the requested resource (%s).", requestSnippetDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(snippetEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateSnippet(requestSnippetDTO);
            return generateContinueResponse().build();
        }

        // update the snippet
        final RevisionDTO revision = snippetEntity.getRevision();
        final ConfigurationSnapshot<SnippetDTO> controllerResponse = serviceFacade.updateSnippet(
                new Revision(revision.getVersion(), revision.getClientId()), snippetEntity.getSnippet());

        // get the results
        final SnippetDTO snippet = controllerResponse.getConfiguration();

        // always prune update responses
        snippet.setContents(null);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        SnippetEntity entity = new SnippetEntity();
        entity.setRevision(updatedRevision);
        entity.setSnippet(populateRemainingSnippetContent(snippet));

        if (controllerResponse.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(snippet.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified snippet.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the snippet to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a snippet",
            response = SnippetEntity.class,
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
    public Response removeSnippet(
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
                    value = "The snippet id.",
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
            serviceFacade.verifyDeleteSnippet(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified snippet
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteSnippet(new Revision(clientVersion, clientId.getClientId()), id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        SnippetEntity entity = new SnippetEntity();
        entity.setRevision(revision);

        return clusterContext(generateOkResponse(entity)).build();
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
