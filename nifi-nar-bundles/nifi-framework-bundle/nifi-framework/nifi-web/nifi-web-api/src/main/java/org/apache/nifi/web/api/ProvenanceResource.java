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
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceRequestDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO.LineageRequestType;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.ProvenanceEventEntity;
import org.apache.nifi.web.api.entity.ProvenanceOptionsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for querying data provenance.
 */
@Api(hidden = true)
public class ProvenanceResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ProvenanceResource.class);
    private static final int MAX_MAX_RESULTS = 10000;

    private NiFiProperties properties;
    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;

    /**
     * Populates the uri for the specified provenance.
     */
    private ProvenanceDTO populateRemainingProvenanceContent(ProvenanceDTO provenance) {
        provenance.setUri(generateResourceUri("controller", "provenance", provenance.getId()));
        return provenance;
    }

    /**
     * Populates the uri for the specified lineage.
     */
    private LineageDTO populateRemainingLineageContent(LineageDTO lineage) {
        lineage.setUri(generateResourceUri("controller", "provenance", "lineage", lineage.getId()));
        return lineage;
    }

    /**
     * Gets the provenance search options for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A provenanceOptionsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/search-options")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets the searchable attributes for provenance events",
            response = ProvenanceOptionsEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getSearchOptions(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get provenance search options
        final ProvenanceOptionsDTO searchOptions = serviceFacade.getProvenanceSearchOptions();

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProvenanceOptionsEntity entity = new ProvenanceOptionsEntity();
        entity.setProvenanceOptions(searchOptions);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    /**
     * Creates a new replay request for the content associated with the specified provenance event id.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param clusterNodeId The id of the node in the cluster that has the specified event. Required if clustered.
     * @param eventId The provenance event id.
     * @return A provenanceEventEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/replays")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE') and hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Replays content from a provenance event",
            response = ProvenanceEventEntity.class,
            authorizations = {
                @Authorization(value = "Provenance and Data Flow Manager", type = "ROLE_PROVENANCE and ROLE_DFM")
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
    public Response submitReplay(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @FormParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @FormParam("eventId") LongParameter eventId) {

        // ensure the event id is specified
        if (eventId == null) {
            throw new IllegalArgumentException("The id of the event must be specified.");
        }

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
                return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // submit the provenance replay request
        final ProvenanceEventDTO event = serviceFacade.submitReplay(eventId.getLong());

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);
        entity.setRevision(revision);

        // generate the response
        URI uri = URI.create(generateResourceUri("controller", "provenance", "events", event.getId()));
        return clusterContext(generateCreatedResponse(uri, entity)).build();
    }

    /**
     * Gets the content for the input of the specified event.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/events/{id}/content/input")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets the input content for a provenance event",
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getInputContent(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @PathParam("id") LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

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
        final String uri = generateResourceUri("controller", "provenance", "events", String.valueOf(id.getLong()), "content", "input");

        // get an input stream to the content
        final DownloadableContent content = serviceFacade.getContent(id.getLong(), uri, ContentDirection.INPUT);

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
     * Gets the content for the output of the specified event.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/events/{id}/content/output")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets the output content for a provenance event",
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getOutputContent(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @PathParam("id") LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

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
        final String uri = generateResourceUri("controller", "provenance", "events", String.valueOf(id.getLong()), "content", "output");

        // get an input stream to the content
        final DownloadableContent content = serviceFacade.getContent(id.getLong(), uri, ContentDirection.OUTPUT);

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
     * Creates provenance using the specified query criteria.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param startDate The start date.
     * @param endDate The end date.
     * @param minimumFileSize The minimum size of the content after the event.
     * @param maximumFileSize The maximum size of the content after the event.
     * @param maxResults The maximum number of results to return.
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @param formParams Additionally, the search parameters are specified in the form parameters. Because the search parameters differ based on configuration they are specified in a map-like fashion:
     * <br>
     * <ul>
     * <li>search[filename]=myFile.txt</li>
     * <li>search[eventType]=RECEIVED</li>
     * </ul>
     *
     * @return A provenanceEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    public Response submitProvenanceRequest(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("startDate") DateTimeParameter startDate,
            @FormParam("endDate") DateTimeParameter endDate,
            @FormParam("minimumFileSize") String minimumFileSize,
            @FormParam("maximumFileSize") String maximumFileSize,
            @FormParam("maxResults") IntegerParameter maxResults,
            @FormParam("clusterNodeId") String clusterNodeId,
            MultivaluedMap<String, String> formParams) {

        // ensure the max results has been specified
        if (maxResults == null) {
            throw new IllegalArgumentException("Max results must be specified.");
        } else if (maxResults.getInteger() > MAX_MAX_RESULTS) {
            throw new IllegalArgumentException("The maximum number of results cannot be greater than " + MAX_MAX_RESULTS);
        }

        // create collections for holding the search terms
        final Map<String, String> searchTerms = new LinkedHashMap<>();

        // go through each parameter and look for processor properties
        for (String parameterName : formParams.keySet()) {
            if (StringUtils.isNotBlank(parameterName)) {
                // see if the parameter name starts with an expected parameter type...
                // if so, store the parameter name and value in the corresponding collection
                if (parameterName.startsWith("search[")) {
                    final int startIndex = StringUtils.indexOf(parameterName, "[");
                    final int endIndex = StringUtils.lastIndexOf(parameterName, "]");
                    if (startIndex != -1 && endIndex != -1) {
                        final String searchTerm = StringUtils.substring(parameterName, startIndex + 1, endIndex);
                        searchTerms.put(searchTerm, formParams.getFirst(parameterName));
                    }
                }
            }
        }

        // Build request object from all params
        final ProvenanceRequestDTO request = new ProvenanceRequestDTO();
        request.setSearchTerms(searchTerms);
        request.setMinimumFileSize(minimumFileSize);
        request.setMaximumFileSize(maximumFileSize);
        request.setMaxResults(maxResults.getInteger());

        // add date range
        if (startDate != null) {
            request.setStartDate(startDate.getDateTime());
        }
        if (endDate != null) {
            request.setEndDate(endDate.getDateTime());
        }

        // build the provenance object
        final ProvenanceDTO provenanceDto = new ProvenanceDTO();
        provenanceDto.setClusterNodeId(clusterNodeId);
        provenanceDto.setRequest(request);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the request entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setRevision(revision);
        entity.setProvenance(provenanceDto);

        return submitProvenanceRequest(httpServletRequest, entity);
    }

    /**
     * Creates provenance using the specified query criteria.
     *
     * @param httpServletRequest request
     * @param provenanceEntity A provenanceEntity
     * @return A provenanceEntity
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Submits a provenance query",
            notes = "Provenance queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the provenance request "
                    + "should be deleted by the client who originally submitted it.",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response submitProvenanceRequest(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The provenance query details.",
                    required = true
            ) ProvenanceEntity provenanceEntity) {

        // check the request
        if (provenanceEntity == null) {
            provenanceEntity = new ProvenanceEntity();
        }

        // get the provenance
        ProvenanceDTO provenanceDto = provenanceEntity.getProvenance();
        if (provenanceDto == null) {
            provenanceDto = new ProvenanceDTO();
            provenanceEntity.setProvenance(provenanceDto);
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // determine where this request should be sent
            if (provenanceDto.getClusterNodeId() == null) {
                // replicate to all nodes
                return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(provenanceEntity), getHeaders(headersToOverride)).getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(provenanceDto.getClusterNodeId());
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // send to every node
                return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(provenanceEntity), getHeaders(headersToOverride), targetNodes).getResponse();
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // ensure the id is the same across the cluster
        final String provenanceId;
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            provenanceId = UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            provenanceId = UUID.randomUUID().toString();
        }

        // set the provenance id accordingly
        provenanceDto.setId(provenanceId);

        // submit the provenance request
        final ProvenanceDTO dto = serviceFacade.submitProvenance(provenanceDto);
        dto.setClusterNodeId(provenanceDto.getClusterNodeId());
        populateRemainingProvenanceContent(dto);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        if (provenanceEntity.getRevision() == null) {
            revision.setClientId(new ClientIdParameter().getClientId());
        } else {
            revision.setClientId(provenanceEntity.getRevision().getClientId());
        }

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateCreatedResponse(URI.create(dto.getUri()), entity)).build();
    }

    /**
     * Gets the provenance with the specified id.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets a provenance query",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getProvenance(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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

        // get the provenance
        final ProvenanceDTO dto = serviceFacade.getProvenance(id);
        dto.setClusterNodeId(clusterNodeId);
        populateRemainingProvenanceContent(dto);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Deletes the provenance with the specified id.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Deletes a provenance query",
            response = ProvenanceEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response deleteProvenance(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the provenance
        serviceFacade.deleteProvenance(id);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the details for a provenance event.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the event
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @return A provenanceEventEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/events/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets a provenance event",
            response = ProvenanceEventEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getProvenanceEvent(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where this event exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The provenence event id.",
                    required = true
            )
            @PathParam("id") LongParameter id) {

        // ensure the id is specified
        if (id == null) {
            throw new IllegalArgumentException("Provenance event id must be specified.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // since we're cluster we must specify the cluster node identifier
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

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

        // get the provenance event
        final ProvenanceEventDTO event = serviceFacade.getProvenanceEvent(id.getLong());

        // event clusterNodeId is set in the NCM where the request is replicated. it is not set
        // here because we also need the clusterNodeAddress (address:apiPort) that is unknown
        // at ths point.
        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Submits a lineage request based on an event or a flowfile uuid.
     *
     * When querying for the lineage of an event you must specify the eventId and the eventDirection. The eventDirection must be 'parents' or 'children' and specifies whether we are going up or down
     * the flowfile ancestry. The uuid cannot be specified in these cases.
     *
     * When querying for the lineage of a flowfile you must specify the uuid. The eventId and eventDirection cannot be specified in this case.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param eventId The id of an event to get the lineage for. Must also specify the eventDirection and not the uuid.
     * @param lineageRequest Either 'PARENTS', 'CHILDREN', or 'FLOWFILE'. PARENTS will return the lineage for the flowfiles that are parents of the specified event. CHILDREN will return the lineage of
     * for the flowfiles that are children of the specified event. FLOWFILE will return the lineage for the specified flowfile.
     * @param uuid The uuid of the flowfile to get the lineage for. Must not specify the eventId or eventDirection.
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @return A lineageEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/lineage")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    public Response submitLineageRequest(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("lineageRequestType") String lineageRequest,
            @FormParam("eventId") LongParameter eventId,
            @FormParam("uuid") String uuid,
            @FormParam("clusterNodeId") String clusterNodeId) {

        // create the lineage request
        final LineageRequestDTO request = new LineageRequestDTO();

        // ensure the lineage request type is specified
        try {
            final LineageRequestType direction = LineageRequestType.valueOf(lineageRequest);
            request.setLineageRequestType(direction);
        } catch (final IllegalArgumentException iae) {
            throw new IllegalArgumentException(String.format("The event direction must be one of %s", StringUtils.join(LineageRequestType.values())));
        }

        // set the uuid (may be null if based on event)
        request.setUuid(uuid);

        // set the event id (may be null is based on flowfile)
        if (eventId != null) {
            request.setEventId(eventId.getLong());
        }

        // create the lineage
        final LineageDTO lineage = new LineageDTO();
        lineage.setClusterNodeId(clusterNodeId);
        lineage.setRequest(request);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create a response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(lineage);
        entity.setRevision(revision);

        return submitLineageRequest(httpServletRequest, entity);
    }

    /**
     * Submits a lineage request based on an event or a flowfile uuid.
     *
     * When querying for the lineage of an event you must specify the eventId and the eventDirection. The eventDirection must be 'parents' or 'children' and specifies whether we are going up or down
     * the flowfile ancestry. The uuid cannot be specified in these cases.
     *
     * When querying for the lineage of a flowfile you must specify the uuid. The eventId and eventDirection cannot be specified in this case.
     *
     * @param httpServletRequest request
     * @param lineageEntity A lineageEntity
     * @return A lineageEntity
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/lineage")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Submits a lineage query",
            notes = "Lineage queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the lineage request "
                    + "should be deleted by the client who originally submitted it.",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response submitLineageRequest(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The lineage query details.",
                    required = true
            )
            final LineageEntity lineageEntity) {

        if (lineageEntity == null || lineageEntity.getLineage() == null || lineageEntity.getLineage().getRequest() == null) {
            throw new IllegalArgumentException("Lineage request must be specified.");
        }

        // ensure the request is well formed
        final LineageDTO lineageDto = lineageEntity.getLineage();
        final LineageRequestDTO requestDto = lineageDto.getRequest();

        // ensure the type has been specified
        if (requestDto.getLineageRequestType() == null) {
            throw new IllegalArgumentException("The type of lineage request must be specified.");
        }

        // validate the remainder of the request
        switch (requestDto.getLineageRequestType()) {
            case CHILDREN:
            case PARENTS:
                // ensure the event has been specified
                if (requestDto.getEventId() == null) {
                    throw new IllegalArgumentException("The event id must be specified when the event type is PARENTS or CHILDREN.");
                }
                break;
            case FLOWFILE:
                // ensure the uuid has been specified
                if (requestDto.getUuid() == null) {
                    throw new IllegalArgumentException("The flowfile uuid must be specified when the event type is FLOWFILE.");
                }
                break;
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            if (lineageDto.getClusterNodeId() == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            // get the target node and ensure it exists
            final Node targetNode = clusterManager.getNode(lineageDto.getClusterNodeId());
            if (targetNode == null) {
                throw new UnknownNodeException("The specified cluster node does not exist.");
            }

            final Set<NodeIdentifier> targetNodes = new HashSet<>();
            targetNodes.add(targetNode.getNodeId());

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // send to every node
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(lineageEntity), getHeaders(headersToOverride), targetNodes).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // get the provenance event
        final LineageDTO dto = serviceFacade.submitLineage(lineageDto);
        dto.setClusterNodeId(lineageDto.getClusterNodeId());
        populateRemainingLineageContent(dto);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        if (lineageEntity.getRevision() == null) {
            revision.setClientId(new ClientIdParameter().getClientId());
        } else {
            revision.setClientId(lineageEntity.getRevision().getClientId());
        }

        // create a response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(dto);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the lineage with the specified id.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/lineage/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Gets a lineage query",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getLineage(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // since we're cluster we must specify the cluster node identifier
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            // get the target node and ensure it exists
            final Node targetNode = clusterManager.getNode(clusterNodeId);
            if (targetNode == null) {
                throw new UnknownNodeException("The specified cluster node does not exist.");
            }

            final Set<NodeIdentifier> targetNodes = new HashSet<>();
            targetNodes.add(targetNode.getNodeId());

            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
        }

        // get the lineage
        final LineageDTO dto = serviceFacade.getLineage(id);
        dto.setClusterNodeId(clusterNodeId);
        populateRemainingLineageContent(dto);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(dto);
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Deletes the lineage with the specified id.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/lineage/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Deletes a lineage query",
            response = LineageEntity.class,
            authorizations = {
                @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response deleteLineage(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The id of the node where this query exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @ApiParam(
                    value = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // since we're cluster we must specify the cluster node identifier
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            // get the target node and ensure it exists
            final Node targetNode = clusterManager.getNode(clusterNodeId);
            if (targetNode == null) {
                throw new UnknownNodeException("The specified cluster node does not exist.");
            }

            final Set<NodeIdentifier> targetNodes = new HashSet<>();
            targetNodes.add(targetNode.getNodeId());

            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // delete the lineage
        serviceFacade.deleteLineage(id);

        // create a revision to return
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final LineageEntity entity = new LineageEntity();
        entity.setRevision(revision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }
}
