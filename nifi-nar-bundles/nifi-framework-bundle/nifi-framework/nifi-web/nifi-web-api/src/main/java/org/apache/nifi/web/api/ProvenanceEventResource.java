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
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.ProvenanceEventEntity;
import org.apache.nifi.web.api.entity.SubmitReplayRequestEntity;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;


/**
 * RESTful endpoint for querying data provenance.
 */
@Path("/provenance-events")
@Api(
        value = "/provenance-events",
        description = "Endpoint for accessing data flow provenance."
)
public class ProvenanceEventResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;

    /**
     * Gets the content for the input of the specified event.
     *
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id            The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/content/input")
    @ApiOperation(
            value = "Gets the input content for a provenance event",
            response = StreamingOutput.class,
            authorizations = {
                    @Authorization(value = "Read Component Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the uri of the request
        final String uri = generateResourceUri("provenance", "events", String.valueOf(id.getLong()), "content", "input");

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
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id            The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/content/output")
    @ApiOperation(
            value = "Gets the output content for a provenance event",
            response = StreamingOutput.class,
            authorizations = {
                    @Authorization(value = "Read Component Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the uri of the request
        final String uri = generateResourceUri("provenance", "events", String.valueOf(id.getLong()), "content", "output");

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
     * Gets the details for a provenance event.
     *
     * @param id            The id of the event
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @return A provenanceEventEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a provenance event",
            response = ProvenanceEventEntity.class,
            authorizations = {
                    @Authorization(value = "Read Component Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The id of the node where this event exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @ApiParam(
                    value = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure the id is specified
        if (id == null) {
            throw new IllegalArgumentException("Provenance event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // since we're cluster we must specify the cluster node identifier
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            return replicate(HttpMethod.GET, clusterNodeId);
        }

        // get the provenance event
        final ProvenanceEventDTO event = serviceFacade.getProvenanceEvent(id.getLong());
        event.setClusterNodeId(clusterNodeId);

        // populate the cluster node address
        final ClusterCoordinator coordinator = getClusterCoordinator();
        if (coordinator != null) {
            final NodeIdentifier nodeId = coordinator.getNodeIdentifier(clusterNodeId);
            event.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
        }

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Creates a new replay request for the content associated with the specified provenance event id.
     *
     * @param httpServletRequest  request
     * @param replayRequestEntity The replay request
     * @return A provenanceEventEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("replays")
    @ApiOperation(
            value = "Replays content from a provenance event",
            response = ProvenanceEventEntity.class,
            authorizations = {
                    @Authorization(value = "Read Component Data - /data/{component-type}/{uuid}", type = ""),
                    @Authorization(value = "Write Component Data - /data/{component-type}/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The replay request.",
                    required = true
            ) final SubmitReplayRequestEntity replayRequestEntity) {

        // ensure the event id is specified
        if (replayRequestEntity == null || replayRequestEntity.getEventId() == null) {
            throw new IllegalArgumentException("The id of the event must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (replayRequestEntity.getClusterNodeId() == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.POST, replayRequestEntity, replayRequestEntity.getClusterNodeId());
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // submit the provenance replay request
        final ProvenanceEventDTO event = serviceFacade.submitReplay(replayRequestEntity.getEventId());
        event.setClusterNodeId(replayRequestEntity.getClusterNodeId());

        // populate the cluster node address
        final ClusterCoordinator coordinator = getClusterCoordinator();
        if (coordinator != null) {
            final NodeIdentifier nodeId = coordinator.getNodeIdentifier(replayRequestEntity.getClusterNodeId());
            event.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
        }

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);

        // generate the response
        URI uri = URI.create(generateResourceUri("provenance-events", event.getId()));
        return generateCreatedResponse(uri, entity).build();
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

}
