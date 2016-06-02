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

import java.net.URI;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UpdateResult;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;

/**
 * RESTful endpoint for managing a Connection.
 */
@Path("/connections")
@Api(
    value = "/connections",
    description = "Endpoint for managing a Connection."
)
public class ConnectionResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the URIs for the specified connections.
     *
     * @param connectionEntities connections
     * @return dtos
     */
    public Set<ConnectionEntity> populateRemainingConnectionEntitiesContent(Set<ConnectionEntity> connectionEntities) {
        for (ConnectionEntity connectionEntity : connectionEntities) {
            populateRemainingConnectionEntityContent(connectionEntity);
        }
        return connectionEntities;
    }

    /**
     * Populate the URIs for the specified connection.
     *
     * @param connectionEntity connection
     * @return dto
     */
    public ConnectionEntity populateRemainingConnectionEntityContent(ConnectionEntity connectionEntity) {
        if (connectionEntity.getComponent() != null) {
            populateRemainingConnectionContent(connectionEntity.getComponent());
        }
        return connectionEntity;
    }

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
     * @param id The id of the connection.
     * @return A connectionEntity.
     * @throws InterruptedException if interrupted
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
                    value = "The connection id.",
                    required = true
            )
        @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable conn = lookup.getConnection(id);
            conn.authorize(authorizer, RequestAction.READ);
        });

        // get the specified relationship
        ConnectionEntity entity = serviceFacade.getConnection(id);
        populateRemainingConnectionEntityContent(entity);

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
     * @throws InterruptedException if interrupted
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
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The connection configuration details.",
                    required = true
        ) final ConnectionEntity connectionEntity) throws InterruptedException {

        if (connectionEntity == null || connectionEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (connectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ConnectionDTO connection = connectionEntity.getComponent();
        if (!id.equals(connection.getId())) {
            throw new IllegalArgumentException(String.format("The connection id "
                    + "(%s) in the request body does not equal the connection id of the "
                    + "requested resource (%s).", connection.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, connectionEntity);
        }

        final Revision revision = getRevision(connectionEntity, id);
        return withWriteLock(
            serviceFacade,
            revision,
            lookup -> {
                final Authorizable conn = lookup.getConnection(id);
                conn.authorize(authorizer, RequestAction.WRITE);
            },
            () -> serviceFacade.verifyUpdateConnection(connection),
            () -> {
                // update the relationship target
                final UpdateResult<ConnectionEntity> updateResult = serviceFacade.updateConnection(revision, connection);

                final ConnectionEntity entity = updateResult.getResult();
                populateRemainingConnectionEntityContent(entity);

                // generate the response
                if (updateResult.isNew()) {
                    return clusterContext(generateCreatedResponse(URI.create(entity.getUri()), entity)).build();
                } else {
                    return clusterContext(generateOkResponse(entity)).build();
                }
            });
    }

    /**
     * Removes the specified connection.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection.
     * @return An Entity containing the client id and an updated revision.
     * @throws InterruptedException if interrupted
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
            @QueryParam(VERSION) final LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @ApiParam(
                    value = "The connection id.",
                    required = true
            )
        @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        // determine the specified version
        final Long clientVersion = version == null ? null : version.getLong();
        final Revision revision = new Revision(clientVersion, clientId.getClientId(), id);

        // get the current user
        return withWriteLock(
            serviceFacade,
            revision,
            lookup -> {
                final Authorizable conn = lookup.getConnection(id);
                conn.authorize(authorizer, RequestAction.WRITE);
            },
            () -> serviceFacade.verifyDeleteConnection(id),
            () -> {
                // delete the connection
                final ConnectionEntity entity = serviceFacade.deleteConnection(revision, id);

                // generate the response
                return clusterContext(generateOkResponse(entity)).build();
            }
        );
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
