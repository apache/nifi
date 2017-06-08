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
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

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
import java.util.List;
import java.util.Set;

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
        connectionEntity.setUri(generateResourceUri("connections", connectionEntity.getId()));
        return connectionEntity;
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
    @ApiOperation(
            value = "Gets a connection",
            response = ConnectionEntity.class,
            authorizations = {
                    @Authorization(value = "Read Source - /{component-type}/{uuid}", type = ""),
                    @Authorization(value = "Read Destination - /{component-type}/{uuid}", type = "")
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
            // ensure read access to this connection (checks source and destination)
            final Authorizable authorizable = lookup.getConnection(id).getAuthorizable();
            authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specified relationship
        ConnectionEntity entity = serviceFacade.getConnection(id);
        populateRemainingConnectionEntityContent(entity);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified connection.
     *
     * @param httpServletRequest request
     * @param id                 The id of the connection.
     * @param requestConnectionEntity   A connectionEntity.
     * @return A connectionEntity.
     * @throws InterruptedException if interrupted
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @ApiOperation(
            value = "Updates a connection",
            response = ConnectionEntity.class,
            authorizations = {
                    @Authorization(value = "Write Source - /{component-type}/{uuid}", type = ""),
                    @Authorization(value = "Write Destination - /{component-type}/{uuid}", type = ""),
                    @Authorization(value = "Write New Destination - /{component-type}/{uuid} - if updating Destination", type = ""),
                    @Authorization(value = "Write Process Group - /process-groups/{uuid} - if updating Destination", type = "")
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
            ) final ConnectionEntity requestConnectionEntity) throws InterruptedException {

        if (requestConnectionEntity == null || requestConnectionEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (requestConnectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ConnectionDTO requestConnection = requestConnectionEntity.getComponent();
        if (!id.equals(requestConnection.getId())) {
            throw new IllegalArgumentException(String.format("The connection id "
                    + "(%s) in the request body does not equal the connection id of the "
                    + "requested resource (%s).", requestConnection.getId(), id));
        }

        if (requestConnection.getDestination() != null) {
            if (requestConnection.getDestination().getId() == null) {
                throw new IllegalArgumentException("When specifying a destination component, the destination id is required.");
            }

            if (requestConnection.getDestination().getType() == null) {
                throw new IllegalArgumentException("When specifying a destination component, the type of the destination is required.");
            }
        }

        final List<PositionDTO> proposedBends = requestConnection.getBends();
        if (proposedBends != null) {
            for (final PositionDTO proposedBend : proposedBends) {
                if (proposedBend.getX() == null || proposedBend.getY() == null) {
                    throw new IllegalArgumentException("The x and y coordinate of the each bend must be specified.");
                }
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConnectionEntity);
        }

        final Revision requestRevision = getRevision(requestConnectionEntity, id);
        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                requestRevision,
                lookup -> {
                    // verifies write access to this connection (this checks the current source and destination)
                    ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    connAuth.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // if a destination has been specified and is different
                    final Connectable currentDestination = connAuth.getDestination();
                    if (requestConnection.getDestination() != null && !currentDestination.getIdentifier().equals(requestConnection.getDestination().getId())) {
                        try {
                            final ConnectableType destinationConnectableType = ConnectableType.valueOf(requestConnection.getDestination().getType());

                            // explicitly handle RPGs differently as the connectable id can be ambiguous if self referencing
                            final Authorizable newDestinationAuthorizable;
                            if (ConnectableType.REMOTE_INPUT_PORT.equals(destinationConnectableType)) {
                                newDestinationAuthorizable = lookup.getRemoteProcessGroup(requestConnection.getDestination().getGroupId());
                            } else {
                                newDestinationAuthorizable = lookup.getLocalConnectable(requestConnection.getDestination().getId());
                            }

                            // verify access of the new destination (current destination was already authorized as part of the connection check)
                            newDestinationAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                        } catch (final IllegalArgumentException e) {
                            throw new IllegalArgumentException(String.format("Unrecognized destination type %s. Excepted values are [%s]",
                                    requestConnection.getDestination().getType(), StringUtils.join(ConnectableType.values(), ", ")));
                        }

                        // verify access of the parent group (this is the same check that is performed when creating the connection)
                        connAuth.getParentGroup().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                    }
                },
                () -> serviceFacade.verifyUpdateConnection(requestConnection),
                (revision, connectionEntity) -> {
                    final ConnectionDTO connection = connectionEntity.getComponent();

                    final ConnectionEntity entity = serviceFacade.updateConnection(revision, connection);
                    populateRemainingConnectionEntityContent(entity);

                    // generate the response
                    return generateOkResponse(entity).build();
                });
    }

    /**
     * Removes the specified connection.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id                 The id of the connection.
     * @return An Entity containing the client id and an updated revision.
     * @throws InterruptedException if interrupted
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @ApiOperation(
            value = "Deletes a connection",
            response = ConnectionEntity.class,
            authorizations = {
                    @Authorization(value = "Write Source - /{component-type}/{uuid}", type = ""),
                    @Authorization(value = "Write - Parent Process Group - /process-groups/{uuid}", type = ""),
                    @Authorization(value = "Write Destination - /{component-type}/{uuid}", type = "")
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
        final Revision requestRevision = new Revision(clientVersion, clientId.getClientId(), id);

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        // get the current user
        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                requestRevision,
                lookup -> {
                    // verifies write access to the source and destination
                    final Authorizable authorizable = lookup.getConnection(id).getAuthorizable();

                    // ensure write permission to the connection
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    authorizable.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteConnection(id),
                (revision, connectionEntity) -> {
                    // delete the connection
                    final ConnectionEntity entity = serviceFacade.deleteConnection(revision, connectionEntity.getId());

                    // generate the response
                    return generateOkResponse(entity).build();
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
