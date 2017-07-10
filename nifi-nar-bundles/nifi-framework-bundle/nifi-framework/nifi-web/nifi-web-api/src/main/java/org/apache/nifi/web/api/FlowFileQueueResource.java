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
import org.apache.nifi.authorization.ConnectionAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * RESTful endpoint for managing a flowfile queue.
 */
@Path("/flowfile-queues")
@Api(
        value = "/flowfile-queues",
        description = "Endpoint for managing a FlowFile Queue."
)
public class FlowFileQueueResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the URIs for the specified flowfile listing.
     *
     * @param connectionId    connection
     * @param flowFileListing flowfile listing
     * @return dto
     */
    public ListingRequestDTO populateRemainingFlowFileListingContent(final String connectionId, final ListingRequestDTO flowFileListing) {
        // uri of the listing
        flowFileListing.setUri(generateResourceUri("flowfile-queues", connectionId, "listing-requests", flowFileListing.getId()));

        // uri of each flowfile
        if (flowFileListing.getFlowFileSummaries() != null) {
            for (final FlowFileSummaryDTO flowFile : flowFileListing.getFlowFileSummaries()) {
                populateRemainingFlowFileContent(connectionId, flowFile);
            }
        }
        return flowFileListing;
    }

    /**
     * Populate the URIs for the specified flowfile.
     *
     * @param connectionId the connection id
     * @param flowFile     the flowfile
     * @return the dto
     */
    public FlowFileSummaryDTO populateRemainingFlowFileContent(final String connectionId, final FlowFileSummaryDTO flowFile) {
        flowFile.setUri(generateResourceUri("flowfile-queues", connectionId, "flowfiles", flowFile.getUuid()));
        return flowFile;
    }

    /**
     * Gets the specified flowfile from the specified connection.
     *
     * @param connectionId  The connection id
     * @param flowFileUuid  The flowfile uuid
     * @param clusterNodeId The cluster node id where the flowfile resides
     * @return a flowFileDTO
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/flowfiles/{flowfile-uuid}")
    @ApiOperation(
            value = "Gets a FlowFile from a Connection.",
            response = FlowFileEntity.class,
            authorizations = {
                    @Authorization(value = "Read Source Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The flowfile uuid.",
                    required = true
            )
            @PathParam("flowfile-uuid") final String flowFileUuid,
            @ApiParam(
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                // get the target node and ensure it exists
                final NodeIdentifier targetNode = getClusterCoordinator().getNodeIdentifier(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                return replicate(HttpMethod.GET, targetNode);
            }
        }

        // NOTE - deferred authorization so we can consider flowfile attributes in the access decision

        // get the flowfile
        final FlowFileDTO flowfileDto = serviceFacade.getFlowFile(connectionId, flowFileUuid);
        populateRemainingFlowFileContent(connectionId, flowfileDto);

        // create the response entity
        final FlowFileEntity entity = new FlowFileEntity();
        entity.setFlowFile(flowfileDto);

        return generateOkResponse(entity).build();
    }

    /**
     * Gets the content for the specified flowfile in the specified connection.
     *
     * @param clientId      Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId  The connection id
     * @param flowFileUuid  The flowfile uuid
     * @param clusterNodeId The cluster node id
     * @return The content stream
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/flowfiles/{flowfile-uuid}/content")
    @ApiOperation(
            value = "Gets the content for a FlowFile in a Connection.",
            response = StreamingOutput.class,
            authorizations = {
                    @Authorization(value = "Read Source Data - /data/{component-type}/{uuid}", type = "")
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
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The flowfile uuid.",
                    required = true
            )
            @PathParam("flowfile-uuid") final String flowFileUuid,
            @ApiParam(
                    value = "The id of the node where the content exists if clustered.",
                    required = false
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                // get the target node and ensure it exists
                final NodeIdentifier targetNode = getClusterCoordinator().getNodeIdentifier(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                return replicate(HttpMethod.GET, targetNode);
            }
        }

        // NOTE - deferred authorization so we can consider flowfile attributes in the access decision

        // get the uri of the request
        final String uri = generateResourceUri("flowfile-queues", connectionId, "flowfiles", flowFileUuid, "content");

        // get an input stream to the content
        final DownloadableContent content = serviceFacade.getContent(connectionId, flowFileUuid, uri);

        // generate a streaming response
        final StreamingOutput response = new StreamingOutput() {
            @Override
            public void write(final OutputStream output) throws IOException, WebApplicationException {
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
     * @param id                 The id of the connection
     * @return A listRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests")
    @ApiOperation(
            value = "Lists the contents of the queue in this connection.",
            response = ListingRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Read Source Data - /data/{component-type}/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyListQueue(id),
                (connectionEntity) -> {
                    // ensure the id is the same across the cluster
                    final String listingRequestId = generateUuid();

                    // submit the listing request
                    final ListingRequestDTO listingRequest = serviceFacade.createFlowFileListingRequest(connectionEntity.getId(), listingRequestId);
                    populateRemainingFlowFileListingContent(connectionEntity.getId(), listingRequest);

                    // create the response entity
                    final ListingRequestEntity entity = new ListingRequestEntity();
                    entity.setListingRequest(listingRequest);

                    // generate the URI where the response will be
                    final URI location = URI.create(listingRequest.getUri());
                    return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
                }
        );
    }

    /**
     * Checks the status of an outstanding listing request.
     *
     * @param connectionId     The id of the connection
     * @param listingRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests/{listing-request-id}")
    @ApiOperation(
            value = "Gets the current status of a listing request for the specified connection.",
            response = ListingRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Read Source Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The listing request id.",
                    required = true
            )
            @PathParam("listing-request-id") final String listingRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
            final Authorizable dataAuthorizable = connAuth.getSourceData();
            dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the listing request
        final ListingRequestDTO listingRequest = serviceFacade.getFlowFileListingRequest(connectionId, listingRequestId);
        populateRemainingFlowFileListingContent(connectionId, listingRequest);

        // create the response entity
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(listingRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified listing request.
     *
     * @param httpServletRequest request
     * @param connectionId       The connection id
     * @param listingRequestId   The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests/{listing-request-id}")
    @ApiOperation(
            value = "Cancels and/or removes a request to list the contents of this connection.",
            response = ListingRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Read Source Data - /data/{component-type}/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The listing request id.",
                    required = true
            )
            @PathParam("listing-request-id") final String listingRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
                serviceFacade,
                new ListingEntity(connectionId, listingRequestId),
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                null,
                (listingEntity) -> {
                    // delete the listing request
                    final ListingRequestDTO listingRequest = serviceFacade.deleteFlowFileListingRequest(listingEntity.getConnectionId(), listingEntity.getListingRequestId());

                    // prune the results as they were already received when the listing completed
                    listingRequest.setFlowFileSummaries(null);

                    // populate remaining content
                    populateRemainingFlowFileListingContent(listingEntity.getConnectionId(), listingRequest);

                    // create the response entity
                    final ListingRequestEntity entity = new ListingRequestEntity();
                    entity.setListingRequest(listingRequest);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private static class ListingEntity extends Entity {
        final String connectionId;
        final String listingRequestId;

        public ListingEntity(String connectionId, String listingRequestId) {
            this.connectionId = connectionId;
            this.listingRequestId = listingRequestId;
        }

        public String getConnectionId() {
            return connectionId;
        }

        public String getListingRequestId() {
            return listingRequestId;
        }
    }

    /**
     * Creates a request to delete the flowfiles in the queue of the specified connection.
     *
     * @param httpServletRequest request
     * @param id                 The id of the connection
     * @return A dropRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests")
    @ApiOperation(
            value = "Creates a request to drop the contents of the queue in this connection.",
            response = DropRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Write Source Data - /data/{component-type}/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (connectionEntity) -> {
                    // ensure the id is the same across the cluster
                    final String dropRequestId = generateUuid();

                    // submit the drop request
                    final DropRequestDTO dropRequest = serviceFacade.createFlowFileDropRequest(connectionEntity.getId(), dropRequestId);
                    dropRequest.setUri(generateResourceUri("flowfile-queues", connectionEntity.getId(), "drop-requests", dropRequest.getId()));

                    // create the response entity
                    final DropRequestEntity entity = new DropRequestEntity();
                    entity.setDropRequest(dropRequest);

                    // generate the URI where the response will be
                    final URI location = URI.create(dropRequest.getUri());
                    return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
                }
        );
    }

    /**
     * Checks the status of an outstanding drop request.
     *
     * @param connectionId  The id of the connection
     * @param dropRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests/{drop-request-id}")
    @ApiOperation(
            value = "Gets the current status of a drop request for the specified connection.",
            response = DropRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Write Source Data - /data/{component-type}/{uuid}", type = "")
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
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") final String dropRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
            final Authorizable dataAuthorizable = connAuth.getSourceData();
            dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the drop request
        final DropRequestDTO dropRequest = serviceFacade.getFlowFileDropRequest(connectionId, dropRequestId);
        dropRequest.setUri(generateResourceUri("flowfile-queues", connectionId, "drop-requests", dropRequestId));

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dropRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified drop request.
     *
     * @param httpServletRequest request
     * @param connectionId       The connection id
     * @param dropRequestId      The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests/{drop-request-id}")
    @ApiOperation(
            value = "Cancels and/or removes a request to drop the contents of this connection.",
            response = DropRequestEntity.class,
            authorizations = {
                    @Authorization(value = "Write Source Data - /data/{component-type}/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @ApiParam(
                    value = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") final String dropRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
                serviceFacade,
                new DropEntity(connectionId, dropRequestId),
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (dropEntity) -> {
                    // delete the drop request
                    final DropRequestDTO dropRequest = serviceFacade.deleteFlowFileDropRequest(dropEntity.getConnectionId(), dropEntity.getDropRequestId());
                    dropRequest.setUri(generateResourceUri("flowfile-queues", dropEntity.getConnectionId(), "drop-requests", dropEntity.getDropRequestId()));

                    // create the response entity
                    final DropRequestEntity entity = new DropRequestEntity();
                    entity.setDropRequest(dropRequest);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private static class DropEntity extends Entity {
        final String connectionId;
        final String dropRequestId;

        public DropEntity(String connectionId, String dropRequestId) {
            this.connectionId = connectionId;
            this.dropRequestId = dropRequestId;
        }

        public String getConnectionId() {
            return connectionId;
        }

        public String getDropRequestId() {
            return dropRequestId;
        }
    }

    // setters
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
