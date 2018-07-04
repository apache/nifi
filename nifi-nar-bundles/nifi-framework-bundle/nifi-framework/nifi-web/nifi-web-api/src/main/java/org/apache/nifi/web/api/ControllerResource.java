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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryClientsEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import java.util.Date;
import java.util.Set;

/**
 * RESTful endpoint for managing a Flow Controller.
 */
@Path("/controller")
@Api(
        value = "/controller",
        description = "Provides realtime command and control of this NiFi instance"
)
public class ControllerResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    private ReportingTaskResource reportingTaskResource;
    private ControllerServiceResource controllerServiceResource;

    /**
     * Populate the uri's for the specified registry.
     *
     * @param registryClientEntity registry
     * @return dtos
     */
    public RegistryClientEntity populateRemainingRegistryEntityContent(final RegistryClientEntity registryClientEntity) {
        registryClientEntity.setUri(generateResourceUri("controller", "registry-clients", registryClientEntity.getId()));
        return registryClientEntity;
    }

    /**
     * Authorizes access to the flow.
     */
    private void authorizeController(final RequestAction action) {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controller = lookup.getController();
            controller.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Retrieves the configuration for this NiFi.
     *
     * @return A controllerConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi Controller",
            response = ControllerConfigurationEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller")
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
    public Response getControllerConfig() {

        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerConfigurationEntity entity = serviceFacade.getControllerConfiguration();
        return generateOkResponse(entity).build();
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param httpServletRequest request
     * @param requestConfigEntity       A controllerConfigurationEntity.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi",
            response = ControllerConfigurationEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response updateControllerConfig(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller configuration.",
                    required = true
            ) final ControllerConfigurationEntity requestConfigEntity) {

        if (requestConfigEntity == null || requestConfigEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (requestConfigEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConfigEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConfigEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestConfigEntity.getRevision(), FlowController.class.getSimpleName());
        return withWriteLock(
                serviceFacade,
                requestConfigEntity,
                requestRevision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (revision, configEntity) -> {
                    final ControllerConfigurationEntity entity = serviceFacade.updateControllerConfiguration(revision, configEntity.getComponent());
                    return generateOkResponse(entity).build();
                }
        );
    }

    // ---------------
    // reporting tasks
    // ---------------

    /**
     * Creates a new Reporting Task.
     *
     * @param httpServletRequest  request
     * @param requestReportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks")
    @ApiOperation(
            value = "Creates a new reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller"),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @Authorization(value = "Write - if the Reporting Task is restricted - /restricted-components")
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
    public Response createReportingTask(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The reporting task configuration details.",
                    required = true
            ) final ReportingTaskEntity requestReportingTaskEntity) {

        if (requestReportingTaskEntity == null || requestReportingTaskEntity.getComponent() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (requestReportingTaskEntity.getRevision() == null || (requestReportingTaskEntity.getRevision().getVersion() == null || requestReportingTaskEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Reporting task.");
        }

        final ReportingTaskDTO requestReportingTask = requestReportingTaskEntity.getComponent();
        if (requestReportingTask.getId() != null) {
            throw new IllegalArgumentException("Reporting task ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestReportingTask.getType())) {
            throw new IllegalArgumentException("The type of reporting task to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestReportingTaskEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestReportingTaskEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestReportingTaskEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestReportingTask.getType(), requestReportingTask.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestReportingTask.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestReportingTask.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateReportingTask(requestReportingTask),
                (reportingTaskEntity) -> {
                    final ReportingTaskDTO reportingTask = reportingTaskEntity.getComponent();

                    // set the processor id as appropriate
                    reportingTask.setId(generateUuid());

                    // create the reporting task and generate the json
                    final Revision revision = getRevision(reportingTaskEntity, reportingTask.getId());
                    final ReportingTaskEntity entity = serviceFacade.createReportingTask(revision, reportingTask);
                    reportingTaskResource.populateRemainingReportingTaskEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    // ----------
    // registries
    // ----------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registry-clients")
    @ApiOperation(value = "Gets the listing of available registry clients", response = RegistryClientsEntity.class, authorizations = {
            @Authorization(value = "Read - /flow")
    })
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getRegistryClients() {
        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final Set<RegistryClientEntity> registries = serviceFacade.getRegistryClients();
        registries.forEach(registry -> populateRemainingRegistryEntityContent(registry));

        final RegistryClientsEntity registryEntities = new RegistryClientsEntity();
        registryEntities.setRegistries(registries);

        return generateOkResponse(registryEntities).build();
    }

    /**
     * Creates a new Registry.
     *
     * @param httpServletRequest  request
     * @param requestRegistryClientEntity A registryClientEntity.
     * @return A registryClientEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registry-clients")
    @ApiOperation(
            value = "Creates a new registry client",
            response = RegistryClientEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response createRegistryClient(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The registry configuration details.",
                    required = true
            ) final RegistryClientEntity requestRegistryClientEntity) {

        if (requestRegistryClientEntity == null || requestRegistryClientEntity.getComponent() == null) {
            throw new IllegalArgumentException("Registry details must be specified.");
        }

        if (requestRegistryClientEntity.getRevision() == null || (requestRegistryClientEntity.getRevision().getVersion() == null || requestRegistryClientEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Registry.");
        }

        final RegistryDTO requestRegistryClient = requestRegistryClientEntity.getComponent();
        if (requestRegistryClient.getId() != null) {
            throw new IllegalArgumentException("Registry ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestRegistryClient.getName())) {
            throw new IllegalArgumentException("Registry name must be specified.");
        }

        if (StringUtils.isBlank(requestRegistryClient.getUri())) {
            throw new IllegalArgumentException("Registry URL must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestRegistryClientEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRegistryClientEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestRegistryClientEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (registryEntity) -> {
                    final RegistryDTO registry = registryEntity.getComponent();

                    // set the processor id as appropriate
                    registry.setId(generateUuid());

                    // create the reporting task and generate the json
                    final Revision revision = getRevision(registryEntity, registry.getId());
                    final RegistryClientEntity entity = serviceFacade.createRegistryClient(revision, registry);
                    populateRemainingRegistryEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified registry.
     *
     * @param id The id of the registry to retrieve
     * @return A registryClientEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @ApiOperation(
            value = "Gets a registry client",
            response = RegistryClientEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller")
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
    public Response getRegistryClient(
            @ApiParam(
                    value = "The registry id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        authorizeController(RequestAction.READ);

        // get the registry
        final RegistryClientEntity entity = serviceFacade.getRegistryClient(id);
        populateRemainingRegistryEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified registry.
     *
     * @param httpServletRequest      request
     * @param id                      The id of the controller service to update.
     * @param requestRegistryEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @ApiOperation(
            value = "Updates a registry client",
            response = RegistryClientEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response updateRegistryClient(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The registry id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The registry configuration details.",
                    required = true
            ) final RegistryClientEntity requestRegistryEntity) {

        if (requestRegistryEntity == null || requestRegistryEntity.getComponent() == null) {
            throw new IllegalArgumentException("Registry details must be specified.");
        }

        if (requestRegistryEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RegistryDTO requestRegistryClient = requestRegistryEntity.getComponent();
        if (!id.equals(requestRegistryClient.getId())) {
            throw new IllegalArgumentException(String.format("The registry id (%s) in the request body does not equal the "
                    + "registry id of the requested resource (%s).", requestRegistryClient.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRegistryEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRegistryEntity.isDisconnectedNodeAcknowledged());
        }

        if (requestRegistryClient.getName() != null && StringUtils.isBlank(requestRegistryClient.getName())) {
            throw new IllegalArgumentException("Registry name must be specified.");
        }

        if (requestRegistryClient.getUri() != null && StringUtils.isBlank(requestRegistryClient.getUri())) {
            throw new IllegalArgumentException("Registry URL must be specified.");
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRegistryEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRegistryEntity,
                requestRevision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (revision, registryEntity) -> {
                    final RegistryDTO registry = registryEntity.getComponent();

                    // update the controller service
                    final RegistryClientEntity entity = serviceFacade.updateRegistryClient(revision, registry);
                    populateRemainingRegistryEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified registry.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the registry to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @ApiOperation(
            value = "Deletes a registry client",
            response = RegistryClientEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response deleteRegistryClient(
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
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam(
                    value = "The registry id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final RegistryClientEntity requestRegistryClientEntity = new RegistryClientEntity();
        requestRegistryClientEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestRegistryClientEntity,
                requestRevision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                () -> serviceFacade.verifyDeleteRegistry(id),
                (revision, registryEntity) -> {
                    // delete the specified registry
                    final RegistryClientEntity entity = serviceFacade.deleteRegistryClient(revision, registryEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Creates a Bulletin.
     *
     * @param httpServletRequest  request
     * @param requestBulletinEntity A bulletinEntity.
     * @return A bulletinEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("bulletin")
    @ApiOperation(
            value = "Creates a new bulletin",
            response = BulletinEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response createBulletin(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The reporting task configuration details.",
                    required = true
            ) final BulletinEntity requestBulletinEntity) {

        if (requestBulletinEntity == null || requestBulletinEntity.getBulletin() == null) {
            throw new IllegalArgumentException("Bulletin details must be specified.");
        }

        final BulletinDTO requestBulletin = requestBulletinEntity.getBulletin();
        if (requestBulletin.getId() != null) {
            throw new IllegalArgumentException("A bulletin ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestBulletin.getMessage())) {
            throw new IllegalArgumentException("The bulletin message must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestBulletinEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestBulletinEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (bulletinEntity) -> {
                    final BulletinDTO bulletin = bulletinEntity.getBulletin();
                    final BulletinEntity entity = serviceFacade.createBulletin(bulletin,true);
                    return generateOkResponse(entity).build();
                }
        );
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Creates a new Controller Service.
     *
     * @param httpServletRequest      request
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-services")
    @ApiOperation(
            value = "Creates a new controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller"),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @Authorization(value = "Write - if the Controller Service is restricted - /restricted-components")
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
    public Response createControllerService(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller service configuration details.",
                    required = true
            ) final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null || requestControllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null
                || (requestControllerServiceEntity.getRevision().getVersion() == null || requestControllerServiceEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Controller service.");
        }

        final ControllerServiceDTO requestControllerService = requestControllerServiceEntity.getComponent();
        if (requestControllerService.getId() != null) {
            throw new IllegalArgumentException("Controller service ID cannot be specified.");
        }

        if (requestControllerService.getParentGroupId() != null) {
            throw new IllegalArgumentException("Parent process group ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestControllerService.getType())) {
            throw new IllegalArgumentException("The type of controller service to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestControllerServiceEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestControllerServiceEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestControllerService.getType(), requestControllerService.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestControllerService.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestControllerService.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateControllerService(requestControllerService),
                (controllerServiceEntity) -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();

                    // set the processor id as appropriate
                    controllerService.setId(generateUuid());

                    // create the controller service and generate the json
                    final Revision revision = getRevision(controllerServiceEntity, controllerService.getId());
                    final ControllerServiceEntity entity = serviceFacade.createControllerService(revision, null, controllerService);
                    controllerServiceResource.populateRemainingControllerServiceEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    // -------
    // cluster
    // -------

    /**
     * Gets the contents of this NiFi cluster. This includes all nodes and their status.
     *
     * @return A clusterEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster")
    @ApiOperation(
            value = "Gets the contents of the cluster",
            notes = "Returns the contents of the cluster including all nodes and their status.",
            response = ClusterEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller")
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
    public Response getCluster() {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getClusterCoordinatorNode());
        }

        final ClusterDTO dto = serviceFacade.getCluster();

        // create entity
        final ClusterEntity entity = new ClusterEntity();
        entity.setCluster(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the contents of the specified node in this NiFi cluster.
     *
     * @param id The node id.
     * @return A nodeEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @ApiOperation(
            value = "Gets a node in the cluster",
            response = NodeEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller")
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
    public Response getNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getClusterCoordinatorNode());
        }

        // get the specified relationship
        final NodeDTO dto = serviceFacade.getNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();
        entity.setNode(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the contents of the specified node in this NiFi cluster.
     *
     * @param id         The id of the node
     * @param nodeEntity A nodeEntity
     * @return A nodeEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @ApiOperation(
            value = "Updates a node in the cluster",
            response = NodeEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response updateNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The node configuration. The only configuration that will be honored at this endpoint is the status.",
                    required = true
            ) NodeEntity nodeEntity) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (nodeEntity == null || nodeEntity.getNode() == null) {
            throw new IllegalArgumentException("Node details must be specified.");
        }

        // get the request node
        final NodeDTO requestNodeDTO = nodeEntity.getNode();
        if (!id.equals(requestNodeDTO.getNodeId())) {
            throw new IllegalArgumentException(String.format("The node id (%s) in the request body does "
                    + "not equal the node id of the requested resource (%s).", requestNodeDTO.getNodeId(), id));
        }

        if (isReplicateRequest()) {
            return replicateToCoordinator(HttpMethod.PUT, nodeEntity);
        }

        // update the node
        final NodeDTO node = serviceFacade.updateNode(requestNodeDTO);

        // create the response entity
        NodeEntity entity = new NodeEntity();
        entity.setNode(node);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Removes the specified from this NiFi cluster.
     *
     * @param id The id of the node
     * @return A nodeEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @ApiOperation(
            value = "Removes a node from the cluster",
            response = NodeEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response deleteNode(
            @ApiParam(
                    value = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicateToCoordinator(HttpMethod.DELETE, getRequestParameters());
        }

        serviceFacade.deleteNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -------
    // history
    // -------

    /**
     * Deletes flow history from the specified end date.
     *
     * @param endDate The end date for the purge action.
     * @return A historyEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history")
    @ApiOperation(
            value = "Purges history",
            response = HistoryEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller")
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
    public Response deleteHistory(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "Purge actions before this date/time.",
                    required = true
            )
            @QueryParam("endDate") DateTimeParameter endDate) {

        // ensure the end date is specified
        if (endDate == null) {
            throw new IllegalArgumentException("The end date must be specified.");
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        return withWriteLock(
                serviceFacade,
                new EndDateEntity(endDate.getDateTime()),
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (endDateEntity) -> {
                    // purge the actions
                    serviceFacade.deleteActions(endDateEntity.getEndDate());

                    // generate the response
                    return generateOkResponse(new HistoryEntity()).build();
                }
        );
    }

    private class EndDateEntity extends Entity {
        final Date endDate;

        public EndDateEntity(Date endDate) {
            this.endDate = endDate;
        }

        public Date getEndDate() {
            return endDate;
        }
    }

    // setters

    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setReportingTaskResource(final ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    public void setControllerServiceResource(final ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
