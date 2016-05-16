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
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.UpdateResult;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Controller Service.
 */
@Path("/controller-services")
@Api(
    value = "/controller-services",
    description = "Endpoint for managing a Controller Service."
)
public class ControllerServiceResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceResource.class);

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified controller services.
     *
     * @param controllerServiceEntities controller services
     * @return dtos
     */
    public Set<ControllerServiceEntity> populateRemainingControllerServiceEntitiesContent(final Set<ControllerServiceEntity> controllerServiceEntities) {
        for (ControllerServiceEntity controllerServiceEntity : controllerServiceEntities) {
            if (controllerServiceEntity.getComponent() != null) {
                populateRemainingControllerServiceEntityContent(controllerServiceEntity);
            }
        }
        return controllerServiceEntities;
    }

    /**
     * Populate the uri's for the specified controller service.
     *
     * @param controllerServiceEntity controller service
     * @return dtos
     */
    public ControllerServiceEntity populateRemainingControllerServiceEntityContent(final ControllerServiceEntity controllerServiceEntity) {
        if (controllerServiceEntity.getComponent() != null) {
            populateRemainingControllerServiceContent(controllerServiceEntity.getComponent());
        }
        return controllerServiceEntity;
    }

    /**
     * Populates the uri for the specified controller service.
     *
     * @param controllerServices services
     * @return dtos
     */
    public Set<ControllerServiceDTO> populateRemainingControllerServicesContent(final Set<ControllerServiceDTO> controllerServices) {
        for (ControllerServiceDTO controllerService : controllerServices) {
            populateRemainingControllerServiceContent(controllerService);
        }
        return controllerServices;
    }

    /**
     * Populates the uri for the specified controller service.
     */
    public ControllerServiceDTO populateRemainingControllerServiceContent(final ControllerServiceDTO controllerService) {
        // populate the controller service href
        controllerService.setUri(generateResourceUri("controller-services", controllerService.getId()));

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(controllerService.getType())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(controllerService.getType());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ControllerServiceConfiguration.equals(uiExtension.getExtensionType())) {
                    controllerService.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                }
            }
        }

        return controllerService;
    }

    /**
     * Retrieves the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a controller service",
            response = ControllerServiceEntity.class,
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
    public Response getControllerService(
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller service
        final ControllerServiceEntity entity = serviceFacade.getControllerService(id);
        populateRemainingControllerServiceEntityContent(entity);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the controller service.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a controller service property descriptor",
            response = PropertyDescriptorEntity.class,
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
    public Response getPropertyDescriptor(
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The property name to return the descriptor for.",
                    required = true
            )
            @QueryParam("propertyName") String propertyName) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        // replicate if cluster manager and service is on node
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getControllerServicePropertyDescriptor(id, propertyName);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Gets the state for a controller service.
     *
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_DFM')")
    @ApiOperation(
        value = "Gets the state for a controller service",
        response = ComponentStateDTO.class,
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
    public Response getState(
        @ApiParam(
            value = "The controller service id.",
            required = true
        )
        @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the component state
        final ComponentStateDTO state = serviceFacade.getControllerServiceState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Clears the state for a controller service.
     *
     * @param revisionEntity The revision is used to verify the client is working with the latest version of the flow.
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_DFM')")
    @ApiOperation(
        value = "Clears the state for a controller service",
        response = ComponentStateDTO.class,
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
    public Response clearState(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The revision used to verify the client is working with the latest version of the flow.",
            required = true
        ) ComponentStateEntity revisionEntity,
        @ApiParam(
            value = "The controller service id.",
            required = true
        )
        @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase) {
            serviceFacade.verifyCanClearControllerServiceState(id);
            return generateContinueResponse().build();
        }

        // get the component state
        serviceFacade.clearControllerServiceState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the references of the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a controller service",
            response = ControllerServiceEntity.class,
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
    public Response getControllerServiceReferences(
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller service
        final ControllerServiceReferencingComponentsEntity entity = serviceFacade.getControllerServiceReferencingComponents(id);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the references of the specified controller service.
     *
     * @param httpServletRequest request
     * @param updateReferenceRequest The update request
     * @return A controllerServiceReferencingComponentsEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a controller services references",
            response = ControllerServiceReferencingComponentsEntity.class,
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
    public Response updateControllerServiceReferences(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The controller service request update request.",
                required = true
            ) UpdateControllerServiceReferenceRequestEntity updateReferenceRequest) {

        if (updateReferenceRequest.getId() == null) {
            throw new IllegalArgumentException("The controller service identifier must be specified.");
        }

        if (updateReferenceRequest.getReferencingComponentRevisions() == null) {
            throw new IllegalArgumentException("The controller service referencing components revisions must be specified.");
        }

        // parse the state to determine the desired action
        // need to consider controller service state first as it shares a state with
        // scheduled state (disabled) which is applicable for referencing services
        // but not referencing schedulable components
        ControllerServiceState controllerServiceState = null;
        try {
            controllerServiceState = ControllerServiceState.valueOf(updateReferenceRequest.getState());
        } catch (final IllegalArgumentException iae) {
            // ignore
        }

        ScheduledState scheduledState = null;
        try {
            scheduledState = ScheduledState.valueOf(updateReferenceRequest.getState());
        } catch (final IllegalArgumentException iae) {
            // ignore
        }

        // ensure an action has been specified
        if (scheduledState == null && controllerServiceState == null) {
            throw new IllegalArgumentException("Must specify the updated state. To update referencing Processors "
                    + "and Reporting Tasks the state should be RUNNING or STOPPED. To update the referencing Controller Services the "
                    + "state should be ENABLED or DISABLED.");
        }

        // ensure the controller service state is not ENABLING or DISABLING
        if (controllerServiceState != null && (ControllerServiceState.ENABLING.equals(controllerServiceState) || ControllerServiceState.DISABLING.equals(controllerServiceState))) {
            throw new IllegalArgumentException("Cannot set the referencing services to ENABLING or DISABLING");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateReferenceRequest, getHeaders()).getResponse();
        }

        // convert the referencing revisions
        final Map<String, Revision> referencingRevisions = updateReferenceRequest.getReferencingComponentRevisions().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                final RevisionDTO rev = e.getValue();
                return new Revision(rev.getVersion(), rev.getClientId(), e.getKey());
            }));

        // handle expects request (usually from the cluster manager)
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            referencingRevisions.entrySet().stream().forEach(e -> {
                serviceFacade.claimRevision(e.getValue());
            });
        }
        if (validationPhase) {
            serviceFacade.verifyUpdateControllerServiceReferencingComponents(updateReferenceRequest.getId(), scheduledState, controllerServiceState);
            return generateContinueResponse().build();
        }

        // update the controller service references
        final ControllerServiceReferencingComponentsEntity entity = serviceFacade.updateControllerServiceReferencingComponents(
            referencingRevisions, updateReferenceRequest.getId(), scheduledState, controllerServiceState);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified a new Controller Service.
     *
     * @param httpServletRequest request
     * @param id The id of the controller service to update.
     * @param controllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a controller service",
            response = ControllerServiceEntity.class,
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
    public Response updateControllerService(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The controller service configuration details.",
                    required = true
            ) ControllerServiceEntity controllerServiceEntity) {

        if (controllerServiceEntity == null || controllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (controllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ControllerServiceDTO requestControllerServiceDTO = controllerServiceEntity.getComponent();
        if (!id.equals(requestControllerServiceDTO.getId())) {
            throw new IllegalArgumentException(String.format("The controller service id (%s) in the request body does not equal the "
                    + "controller service id of the requested resource (%s).", requestControllerServiceDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), controllerServiceEntity, getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = getRevision(controllerServiceEntity, id);
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            serviceFacade.claimRevision(revision);
        }
        if (validationPhase) {
            serviceFacade.verifyUpdateControllerService(requestControllerServiceDTO);
            return generateContinueResponse().build();
        }

        // update the controller service
        final UpdateResult<ControllerServiceEntity> updateResult = serviceFacade.updateControllerService(revision, requestControllerServiceDTO);

        // build the response entity
        final ControllerServiceEntity entity = updateResult.getResult();
        populateRemainingControllerServiceContent(entity.getComponent());

        if (updateResult.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(entity.getComponent().getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified controller service.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the controller service to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a controller service",
            response = ControllerServiceEntity.class,
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
    public Response removeControllerService(
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
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            serviceFacade.claimRevision(revision);
        }
        if (validationPhase) {
            serviceFacade.verifyDeleteControllerService(id);
            return generateContinueResponse().build();
        }

        // delete the specified controller service
        final ControllerServiceEntity entity = serviceFacade.deleteControllerService(revision, id);
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
