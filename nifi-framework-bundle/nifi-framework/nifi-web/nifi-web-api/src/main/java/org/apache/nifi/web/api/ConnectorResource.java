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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.entity.ConfigurationStepEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepNamesEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorPropertyAllowableValuesEntity;
import org.apache.nifi.web.api.entity.ConnectorRunStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.VerifyConnectorConfigStepRequestDTO;
import org.apache.nifi.web.api.entity.VerifyConnectorConfigStepRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * RESTful endpoint for managing a Connector.
 */
@Controller
@Path("/connectors")
@Tag(name = "Connectors")
public class ConnectorResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorResource.class);
    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private FlowResource flowResource;
    private final RequestManager<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>> configVerificationRequestManager =
            new AsyncRequestManager<>(100, 1L, "Connector Configuration Step Verification");

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified connectors.
     *
     * @param connectorEntities connectors
     * @return connector entities
     */
    public Set<ConnectorEntity> populateRemainingConnectorEntitiesContent(final Set<ConnectorEntity> connectorEntities) {
        for (ConnectorEntity connectorEntity : connectorEntities) {
            populateRemainingConnectorEntityContent(connectorEntity);
        }
        return connectorEntities;
    }

    /**
     * Populate the uri's for the specified connector.
     *
     * @param connectorEntity connector
     * @return connector entity
     */
    public ConnectorEntity populateRemainingConnectorEntityContent(final ConnectorEntity connectorEntity) {
        connectorEntity.setUri(generateResourceUri("connectors", connectorEntity.getId()));

        // populate the remaining content
        if (connectorEntity.getComponent() != null) {
            populateRemainingConnectorContent(connectorEntity.getComponent());
        }
        return connectorEntity;
    }

    /**
     * Populates the uri for the specified connector including custom UI information.
     */
    public ConnectorDTO populateRemainingConnectorContent(final ConnectorDTO connector) {
        final BundleDTO bundle = connector.getBundle();
        if (bundle == null) {
            return connector;
        }

        // see if this connector has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(connector.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(connector.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.Connector.equals(uiExtension.getExtensionType())) {
                    final String contextPath = uiExtension.getContextPath();
                    final Map<String, String> routes = uiExtension.getSupportedRoutes();

                    if (routes != null) {
                        final String configurationPath = routes.get("configuration");
                        if (configurationPath != null) {
                            connector.setConfigurationUrl(buildCustomUiUrl(contextPath, configurationPath));
                        }

                        final String detailsPath = routes.get("details");
                        if (detailsPath != null) {
                            connector.setDetailsUrl(buildCustomUiUrl(contextPath, detailsPath));
                        }
                    }
                }
            }
        }

        return connector;
    }

    /**
     * Builds a custom UI URL from the context path and route path.
     * Handles both hash-based routing (path starts with #) and location-based routing.
     *
     * @param contextPath the context path of the custom UI
     * @param routePath the route path (e.g., "#/wizard" for hash-based or "/wizard" for location-based)
     * @return the full URL for the custom UI route
     */
    private String buildCustomUiUrl(final String contextPath, final String routePath) {
        final String baseUrl = generateExternalUiUri(contextPath);
        if (routePath.startsWith("#")) {
            // Hash-based routing: /context-path/#/route
            return baseUrl + "/" + routePath;
        } else {
            // Location-based routing: /context-path/route
            return baseUrl + routePath;
        }
    }

    /**
     * Creates a new connector.
     *
     * @param requestConnectorEntity A connectorEntity.
     * @return A connectorEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Creates a new connector",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /connectors")
            }
    )
    public Response createConnector(
            @Parameter(description = "The connector configuration details.", required = true) final ConnectorEntity requestConnectorEntity) {

        if (requestConnectorEntity == null || requestConnectorEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connector details must be specified.");
        }

        if (requestConnectorEntity.getRevision() == null || (requestConnectorEntity.getRevision().getVersion() == null || requestConnectorEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Connector.");
        }

        final ConnectorDTO requestConnector = requestConnectorEntity.getComponent();
        if (requestConnector.getId() != null) {
            throw new IllegalArgumentException("Connector ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestConnector.getType())) {
            throw new IllegalArgumentException("The type of connector to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestConnectorEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConnectorEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestConnectorEntity,
                lookup -> {
                    final Authorizable connectors = lookup.getConnectors();
                    connectors.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCreateConnector(requestConnector),
                connectorEntity -> {
                    final ConnectorDTO connector = connectorEntity.getComponent();

                    // set the connector id as appropriate
                    connector.setId(generateUuid());

                    // create the new connector
                    final Revision revision = getRevision(connectorEntity, connector.getId());
                    final ConnectorEntity entity = serviceFacade.createConnector(revision, connector);
                    populateRemainingConnectorEntityContent(entity);

                    // generate a 201 created response
                    final String uri = entity.getUri();
                    return generateCreatedResponse(URI.create(uri), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified connector.
     *
     * @param id The id of the connector to retrieve
     * @return A connectorEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Gets a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            }
    )
    public Response getConnector(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the connector
        final ConnectorEntity entity = serviceFacade.getConnector(id);
        populateRemainingConnectorEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified connector.
     *
     * @param id The id of the connector to update.
     * @param requestConnectorEntity A connectorEntity.
     * @return A connectorEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Updates a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid}")
            }
    )
    public Response updateConnector(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The connector configuration details.",
                    required = true
            ) final ConnectorEntity requestConnectorEntity) {

        if (requestConnectorEntity == null || requestConnectorEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connector details must be specified.");
        }

        if (requestConnectorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ConnectorDTO requestConnectorDTO = requestConnectorEntity.getComponent();
        if (!id.equals(requestConnectorDTO.getId())) {
            throw new IllegalArgumentException(String.format("The connector id (%s) in the request body does not equal the "
                    + "connector id of the requested resource (%s).", requestConnectorDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConnectorEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConnectorEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestConnectorEntity, id);
        return withWriteLock(
                serviceFacade,
                requestConnectorEntity,
                requestRevision,
                lookup -> {
                    final Authorizable connector = lookup.getConnector(id);
                    connector.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateConnector(requestConnectorDTO),
                (revision, connectorEntity) -> {
                    final ConnectorDTO connectorDTO = connectorEntity.getComponent();

                    // update the connector
                    final ConnectorEntity entity = serviceFacade.updateConnector(revision, connectorDTO);
                    populateRemainingConnectorEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified connector.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connector to remove.
     * @return A connectorEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Deletes a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid}")
            }
    )
    public Response deleteConnector(
            @Parameter(
                    description = "The revision is used to verify the client is working with the latest version of the flow."
            )
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) final ClientIdParameter clientId,
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ConnectorEntity requestConnectorEntity = new ConnectorEntity();
        requestConnectorEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestConnectorEntity,
                requestRevision,
                lookup -> {
                    final Authorizable connector = lookup.getConnector(id);
                    connector.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteConnector(id),
                (revision, connectorEntity) -> {
                    // delete the specified connector
                    final ConnectorEntity entity = serviceFacade.deleteConnector(revision, connectorEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates run status of a connector.
     *
     * @param id The id of the connector to update.
     * @param requestRunStatus A connectorRunStatusEntity.
     * @return A connectorEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/run-status")
    @Operation(
            summary = "Updates run status of a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid} or /operation/connectors/{uuid}")
            }
    )
    public Response updateRunStatus(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The connector run status.",
                    required = true
            ) final ConnectorRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Connector run status must be specified.");
        }

        if (requestRunStatus.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRunStatus.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRunStatus);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRunStatus.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRunStatus.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRunStatus,
                requestRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final Authorizable connector = lookup.getConnector(id);
                    OperationAuthorizable.authorizeOperation(connector, authorizer, user);
                },
                () -> serviceFacade.verifyUpdateConnector(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, runStatusEntity) -> {
                    // update the connector
                    final ScheduledState scheduledState = ScheduledState.valueOf(runStatusEntity.getState());
                    final ConnectorEntity entity = serviceFacade.scheduleConnector(revision, id, scheduledState);
                    populateRemainingConnectorEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Gets the configuration step names for the specified connector.
     *
     * @param id The id of the connector to retrieve configuration steps from
     * @return A configurationStepNamesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps")
    @Operation(
            summary = "Gets all configuration step names for a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConfigurationStepNamesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            }
    )
    public Response getConnectorConfigurationSteps(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the configuration step names
        final ConfigurationStepNamesEntity entity = serviceFacade.getConnectorConfigurationSteps(id);

        return generateOkResponse(entity).build();
    }

    /**
     * Gets a specific configuration step by name for the specified connector.
     *
     * @param id The id of the connector to retrieve configuration step from
     * @param configurationStepName The name of the configuration step to retrieve
     * @return A configurationStepEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}")
    @Operation(
            summary = "Gets a specific configuration step by name for a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConfigurationStepEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            }
    )
    public Response getConnectorConfigurationStep(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The configuration step name.",
                    required = true
            )
            @PathParam("configurationStepName") final String configurationStepName) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specific configuration step
        final ConfigurationStepEntity entity = serviceFacade.getConnectorConfigurationStep(id, configurationStepName);

        return generateOkResponse(entity).build();
    }

    /**
     * Gets the allowable values for a specific property in a connector's configuration step.
     *
     * @param id The id of the connector
     * @param configurationStepName The name of the configuration step
     * @param propertyGroupName The name of the property group
     * @param propertyName The name of the property
     * @param filter Optional filter for the allowable values
     * @return A ConnectorPropertyAllowableValuesEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}/property-groups/{propertyGroupName}/properties/{propertyName}/allowable-values")
    @Operation(
            summary = "Gets the allowable values for a specific property in a connector's configuration step",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorPropertyAllowableValuesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Gets the allowable values for a specific property that supports dynamic fetching of allowable values. " +
                    "The filter parameter can be used to narrow down the results based on the property's filtering logic.",
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            }
    )
    public Response getConnectorPropertyAllowableValues(
            @Parameter(description = "The connector id.", required = true)
            @PathParam("id") final String id,
            @Parameter(description = "The configuration step name.", required = true)
            @PathParam("configurationStepName") final String configurationStepName,
            @Parameter(description = "The property group name.", required = true)
            @PathParam("propertyGroupName") final String propertyGroupName,
            @Parameter(description = "The property name.", required = true)
            @PathParam("propertyName") final String propertyName,
            @Parameter(description = "Optional filter to narrow down the allowable values.")
            @QueryParam("filter") final String filter) {

        // NOTE: fetching allowable values is handled by the node that receives the request and does not need to be replicated

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the allowable values
        final ConnectorPropertyAllowableValuesEntity entity = serviceFacade.getConnectorPropertyAllowableValues(id, configurationStepName, propertyGroupName, propertyName, filter);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates a specific configuration step by name for the specified connector.
     *
     * @param id The id of the connector to update configuration step for
     * @param configurationStepName The name of the configuration step to update
     * @param requestConfigurationStepEntity The configuration step configuration to update
     * @return A configurationStepEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}")
    @Operation(
            summary = "Updates a specific configuration step by name for a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConfigurationStepEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid}")
            }
    )
    public Response updateConnectorConfigurationStep(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The configuration step name.",
                    required = true
            )
            @PathParam("configurationStepName") final String configurationStepName,
            @Parameter(
                    description = "The configuration step configuration.",
                    required = true
            ) final ConfigurationStepEntity requestConfigurationStepEntity) {

        if (requestConfigurationStepEntity == null || requestConfigurationStepEntity.getConfigurationStep() == null) {
            throw new IllegalArgumentException("Configuration step details must be specified.");
        }

        if (requestConfigurationStepEntity.getParentConnectorRevision() == null) {
            throw new IllegalArgumentException("Parent connector revision must be specified.");
        }

        if (requestConfigurationStepEntity.getParentConnectorId() == null) {
            throw new IllegalArgumentException("Parent connector ID must be specified.");
        }

        if (!id.equals(requestConfigurationStepEntity.getParentConnectorId())) {
            throw new IllegalArgumentException(String.format("The parent connector ID (%s) in the request body does not equal the "
                    + "connector ID of the requested resource (%s).", requestConfigurationStepEntity.getParentConnectorId(), id));
        }

        // ensure the configuration step names match
        final ConfigurationStepConfigurationDTO requestConfigurationStep = requestConfigurationStepEntity.getConfigurationStep();
        if (!configurationStepName.equals(requestConfigurationStep.getConfigurationStepName())) {
            throw new IllegalArgumentException(String.format("The configuration step name (%s) in the request body does not equal the "
                    + "configuration step name of the requested resource (%s).", requestConfigurationStep.getConfigurationStepName(), configurationStepName));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConfigurationStepEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConfigurationStepEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestConfigurationStepEntity.getParentConnectorRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestConfigurationStepEntity,
                requestRevision,
                lookup -> {
                    final Authorizable connector = lookup.getConnector(id);
                    connector.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    // Verify the connector exists and the configuration step exists
                    serviceFacade.getConnectorConfigurationStep(id, configurationStepName);
                },
                (revision, configurationStepEntity) -> {
                    final ConfigurationStepConfigurationDTO configurationStepConfiguration = configurationStepEntity.getConfigurationStep();

                    // update the configuration step
                    final ConfigurationStepEntity entity = serviceFacade.updateConnectorConfigurationStep(revision, id, configurationStepName, configurationStepConfiguration);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Submits a request to perform verification of a specific configuration step for a connector.
     * This is an asynchronous operation that will return immediately with a request ID that can be
     * used to poll for the results.
     *
     * @param id The id of the connector
     * @param configurationStepName The name of the configuration step to verify
     * @param requestEntity The verify config request entity containing the properties to verify
     * @return The verification request entity with the request ID
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}/verify-config")
    @Operation(
            summary = "Performs verification of a configuration step for a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConnectorConfigStepRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of verifying a given Connector Configuration Step. This may be a long-running task. As a result, " +
                    "this endpoint will immediately return a VerifyConnectorConfigStepRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
                    "The client may then periodically poll the status of the request by issuing a GET request to " +
                    "/connectors/{connectorId}/configuration-steps/{stepName}/verify-config/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/connectors/{connectorId}/configuration-steps/{stepName}/verify-config/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid}")
            }
    )
    public Response submitConfigurationStepVerificationRequest(
            @Parameter(description = "The connector id.", required = true)
            @PathParam("id") final String id,
            @Parameter(description = "The configuration step name.", required = true)
            @PathParam("configurationStepName") final String configurationStepName,
            @Parameter(description = "The verify config request entity containing the configuration step to verify.", required = true)
            final VerifyConnectorConfigStepRequestEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Connector configuration step verification request must be specified.");
        }

        final VerifyConnectorConfigStepRequestDTO requestDto = requestEntity.getRequest();
        if (requestDto == null || requestDto.getConfigurationStep() == null) {
            throw new IllegalArgumentException("Connector configuration step must be specified.");
        }

        if (requestDto.getConnectorId() == null) {
            throw new IllegalArgumentException("Connector's identifier must be specified in the request.");
        }

        if (!requestDto.getConnectorId().equals(id)) {
            throw new IllegalArgumentException("Connector's identifier in the request must match the identifier provided in the URL.");
        }

        if (requestDto.getConfigurationStepName() == null) {
            throw new IllegalArgumentException("Configuration step name must be specified in the request.");
        }

        if (!requestDto.getConfigurationStepName().equals(configurationStepName)) {
            throw new IllegalArgumentException("Configuration step name in the request must match the step name provided in the URL.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> {
                    final Authorizable connector = lookup.getConnector(id);
                    connector.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    serviceFacade.verifyCanVerifyConnectorConfigurationStep(id, configurationStepName);
                },
                entity -> performAsyncConfigStepVerification(entity, id, configurationStepName, user)
        );
    }

    /**
     * Returns the verification request with the given ID for a connector configuration step.
     *
     * @param id The id of the connector
     * @param configurationStepName The name of the configuration step
     * @param requestId The id of the verification request
     * @return The verification request entity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}/verify-config/{requestId}")
    @Operation(
            summary = "Returns the Verification Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConnectorConfigStepRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Verification Request with the given ID. Once a Verification Request has been created, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getConfigurationStepVerificationRequest(
            @Parameter(description = "The connector id.", required = true)
            @PathParam("id") final String id,
            @Parameter(description = "The configuration step name.", required = true)
            @PathParam("configurationStepName") final String configurationStepName,
            @Parameter(description = "The ID of the Verification Request", required = true)
            @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final AsynchronousWebRequest<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);
        final VerifyConnectorConfigStepRequestEntity updateRequestEntity = createVerifyConnectorConfigStepRequestEntity(asyncRequest, id, configurationStepName, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    /**
     * Deletes the verification request with the given ID for a connector configuration step.
     *
     * @param id The id of the connector
     * @param configurationStepName The name of the configuration step
     * @param requestId The id of the verification request
     * @return The verification request entity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/configuration-steps/{configurationStepName}/verify-config/{requestId}")
    @Operation(
            summary = "Deletes the Verification Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConnectorConfigStepRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Verification Request with the given ID. After a request is created, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Verification process has completed. If the request is deleted before the request "
                    + "completes, then the Verification request will finish the step that it is currently performing and then will cancel any subsequent steps.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteConfigurationStepVerificationRequest(
            @Parameter(description = "The connector id.", required = true)
            @PathParam("id") final String id,
            @Parameter(description = "The configuration step name.", required = true)
            @PathParam("configurationStepName") final String configurationStepName,
            @Parameter(description = "The ID of the Verification Request", required = true)
            @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final boolean twoPhaseRequest = isTwoPhaseRequest(httpServletRequest);
        final boolean executionPhase = isExecutionPhase(httpServletRequest);

        if (!twoPhaseRequest || executionPhase) {
            final AsynchronousWebRequest<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                    configVerificationRequestManager.removeRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

            if (!asyncRequest.isComplete()) {
                asyncRequest.cancel();
            }

            final VerifyConnectorConfigStepRequestEntity updateRequestEntity = createVerifyConnectorConfigStepRequestEntity(asyncRequest, id, configurationStepName, requestId);
            return generateOkResponse(updateRequestEntity).build();
        }

        if (isValidationPhase(httpServletRequest)) {
            configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);
            return generateContinueResponse().build();
        } else if (isCancellationPhase(httpServletRequest)) {
            return generateOkResponse().build();
        } else {
            throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
        }
    }

    private Response performAsyncConfigStepVerification(final VerifyConnectorConfigStepRequestEntity requestEntity, final String connectorId,
                                                        final String configurationStepName, final NiFiUser user) {
        final String requestId = generateUuid();
        logger.debug("Generated Config Verification Request with ID {} for Connector {} Configuration Step {}", requestId, connectorId, configurationStepName);

        final VerifyConnectorConfigStepRequestDTO requestDto = requestEntity.getRequest();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Connector Configuration Step"));

        final AsynchronousWebRequest<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>> request =
                new StandardAsynchronousWebRequest<>(requestId, requestEntity, connectorId, user, updateSteps);

        final Consumer<AsynchronousWebRequest<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>>> updateTask = asyncRequest -> {
            try {
                final List<PropertyGroupConfigurationDTO> propertyGroupConfigurations = requestDto.getConfigurationStep().getPropertyGroupConfigurations();
                final List<ConfigVerificationResultDTO> results = serviceFacade.performConnectorConfigurationStepVerification(connectorId, configurationStepName, propertyGroupConfigurations);
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Connector Configuration Step", e);
                asyncRequest.fail("Failed to verify Connector Configuration Step due to " + e);
            }
        };

        configVerificationRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, updateTask);

        final VerifyConnectorConfigStepRequestEntity resultsEntity = createVerifyConnectorConfigStepRequestEntity(request, connectorId, configurationStepName, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConnectorConfigStepRequestEntity createVerifyConnectorConfigStepRequestEntity(
            final AsynchronousWebRequest<VerifyConnectorConfigStepRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest,
            final String connectorId, final String configurationStepName, final String requestId) {

        final VerifyConnectorConfigStepRequestDTO requestDto = asyncRequest.getRequest().getRequest();
        final List<ConfigVerificationResultDTO> resultsList = asyncRequest.getResults();

        final VerifyConnectorConfigStepRequestDTO dto = new VerifyConnectorConfigStepRequestDTO();
        dto.setConnectorId(requestDto.getConnectorId());
        dto.setConfigurationStepName(requestDto.getConfigurationStepName());
        dto.setConfigurationStep(requestDto.getConfigurationStep());
        dto.setResults(resultsList);

        dto.setComplete(asyncRequest.isComplete());
        dto.setFailureReason(asyncRequest.getFailureReason());
        dto.setLastUpdated(asyncRequest.getLastUpdated());
        dto.setPercentCompleted(asyncRequest.getPercentComplete());
        dto.setRequestId(requestId);
        dto.setState(asyncRequest.getState());
        dto.setUri(generateResourceUri("connectors", connectorId, "configuration-steps", configurationStepName, "verify-config", requestId));

        final VerifyConnectorConfigStepRequestEntity entity = new VerifyConnectorConfigStepRequestEntity();
        entity.setRequest(dto);
        return entity;
    }

    /**
     * Performs a search request within the encapsulated process group of this connector.
     *
     * @param id The connector id
     * @param value Search string
     * @return A searchResultsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/search-results")
    @Operation(
            summary = "Performs a search against the encapsulated process group of this connector using the specified search term",
            description = "Only search results from authorized components will be returned.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = SearchResultsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            }
    )
    public Response searchConnector(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The search term.",
                    required = false
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) final String value
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access to the connector
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // search within the connector's encapsulated process group
        final SearchResultsDTO results = serviceFacade.searchConnector(id, value);

        // create the entity
        final SearchResultsEntity entity = new SearchResultsEntity();
        entity.setSearchResultsDTO(results);

        // generate the response
        return noCache(Response.ok(entity)).build();
    }

    private ConnectorDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final ConnectorDTO dto = new ConnectorDTO();
        dto.setId(id);
        dto.setState(runStatus);
        return dto;
    }

    /**
     * Applies an update to a connector.
     *
     * @param id The id of the connector to apply update to.
     * @param requestConnectorEntity A connectorEntity containing the revision.
     * @return A connectorEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/apply-update")
    @Operation(
            summary = "Applies an update to a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will apply any pending configuration changes to the connector. The client can poll the connector endpoint to check when the update is complete.",
            security = {
                    @SecurityRequirement(name = "Write - /connectors/{uuid}")
            }
    )
    public Response applyConnectorUpdate(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The connector configuration with revision.",
                    required = true
            ) final ConnectorEntity requestConnectorEntity) {

        if (requestConnectorEntity == null || requestConnectorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Connector entity with revision must be specified.");
        }

        // ensure the ids are the same
        if (requestConnectorEntity.getId() != null && !id.equals(requestConnectorEntity.getId())) {
            throw new IllegalArgumentException(String.format("The connector id (%s) in the request body does not equal the "
                    + "connector id of the requested resource (%s).", requestConnectorEntity.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestConnectorEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConnectorEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestConnectorEntity, id);
        return withWriteLock(
                serviceFacade,
                requestConnectorEntity,
                requestRevision,
                lookup -> {
                    final Authorizable connector = lookup.getConnector(id);
                    connector.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    // Verify the connector exists
                    serviceFacade.getConnector(id);
                },
                (revision, connectorEntity) -> {
                    // apply the connector update
                    final ConnectorEntity entity = serviceFacade.applyConnectorUpdate(revision, id);
                    populateRemainingConnectorEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Retrieves the flow for the process group managed by the specified connector.
     *
     * @param id The id of the connector
     * @param uiOnly Whether to return only UI-specific fields
     * @return A processGroupFlowEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/flow")
    @Operation(
            summary = "Gets the flow for the process group managed by a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessGroupFlowEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            },
            description = "Returns the flow for the process group managed by the specified connector. If the uiOnly query parameter is " +
                    "provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User " +
                    "Interface. As such, the selected fields may change at any time, even during incremental releases, without warning. " +
                    "As a result, this parameter should not be provided by any client other than the UI."
    )
    public Response getFlow(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "Whether to return only UI-specific fields"
            )
            @QueryParam("uiOnly") @DefaultValue("false") final boolean uiOnly) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access to the connector
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the flow for the connector's managed process group
        final ProcessGroupFlowEntity entity = serviceFacade.getConnectorFlow(id, uiOnly);
        flowResource.populateRemainingFlowContent(entity.getProcessGroupFlow());
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the status for the process group managed by the specified connector.
     *
     * @param id The id of the connector
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendant groups and the status of their content will be included.
     * @param nodewise Whether to include breakdown per node
     * @param clusterNodeId The id of a specific node to get status from
     * @return A processGroupStatusEntity
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/status")
    @Operation(
            summary = "Gets the status for the process group managed by a connector",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessGroupStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /connectors/{uuid}")
            },
            description = "Returns the status for the process group managed by the specified connector. The status includes status for all descendent components. " +
                    "When invoked with recursive set to true, it will return the current status of every component in the connector's encapsulated flow."
    )
    public Response getConnectorStatus(
            @Parameter(
                    description = "The connector id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "Whether all descendant groups and the status of their content will be included. Optional, defaults to false"
            )
            @QueryParam("recursive") @DefaultValue("false") final Boolean recursive,
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue("false") final Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ProcessGroupStatusEntity entity = (ProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // authorize access to the connector
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable connector = lookup.getConnector(id);
            connector.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the status for the connector's managed process group
        final ProcessGroupStatusEntity entity = serviceFacade.getConnectorProcessGroupStatus(id, recursive);
        return generateOkResponse(entity).build();
    }

    // -----------------
    // setters
    // -----------------

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired
    public void setFlowResource(final FlowResource flowResource) {
        this.flowResource = flowResource;
    }
}
