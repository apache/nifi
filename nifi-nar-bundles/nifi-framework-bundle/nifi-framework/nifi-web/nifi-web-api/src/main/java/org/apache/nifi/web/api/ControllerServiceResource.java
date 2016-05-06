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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
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
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.Availability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;

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
     * Populates the uri for the specified controller service.
     *
     * @param controllerServices services
     * @return dtos
     */
    public Set<ControllerServiceDTO> populateRemainingControllerServicesContent(final String availability, final Set<ControllerServiceDTO> controllerServices) {
        for (ControllerServiceDTO controllerService : controllerServices) {
            populateRemainingControllerServiceContent(availability, controllerService);
        }
        return controllerServices;
    }

    /**
     * Populates the uri for the specified controller service.
     */
    public ControllerServiceDTO populateRemainingControllerServiceContent(final String availability, final ControllerServiceDTO controllerService) {
        // populate the controller service href
        controllerService.setUri(generateResourceUri("controller-services", availability, controllerService.getId()));
        controllerService.setAvailability(availability);

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
     * Parses the availability and ensure that the specified availability makes
     * sense for the given NiFi instance.
     *
     * @param availability avail
     * @return avail
     */
    public Availability parseAvailability(final String availability) {
        final Availability avail;
        try {
            avail = Availability.valueOf(availability.toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(String.format("Availability: Value must be one of [%s]", StringUtils.join(Availability.values(), ", ")));
        }

        // ensure this nifi is an NCM is specifying NCM availability
        if (!properties.isClusterManager() && Availability.NCM.equals(avail)) {
            throw new IllegalArgumentException("Availability of NCM is only applicable when the NiFi instance is the cluster manager.");
        }

        return avail;
    }

    /**
     * Retrieves the specified controller service.
     *
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
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
                    value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller service
        final ControllerServiceDTO controllerService = serviceFacade.getControllerService(id);

        // create the response entity
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setControllerService(populateRemainingControllerServiceContent(availability, controllerService));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param availability avail
     * @param id The id of the controller service.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/descriptors")
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
                    value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
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

        final Availability avail = parseAvailability(availability);

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        // replicate if cluster manager and service is on node
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
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
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/state")
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
            value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
            allowableValues = "NCM, NODE",
            required = true
        )
        @PathParam("availability") String availability,
        @ApiParam(
            value = "The controller service id.",
            required = true
        )
        @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
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
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/state/clear-requests")
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
        )
        Entity revisionEntity,
        @ApiParam(
            value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
            allowableValues = "NCM, NODE",
            required = true
        )
        @PathParam("availability") String availability,
        @ApiParam(
            value = "The controller service id.",
            required = true
        )
        @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final Revision revision = getRevision(revisionEntity.getRevision(), id);
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            serviceFacade.claimRevision(revision);
        }
        if (validationPhase) {
            serviceFacade.verifyCanClearControllerServiceState(id);
            return generateContinueResponse().build();
        }

        // get the component state
        final ConfigurationSnapshot<Void> snapshot = serviceFacade.clearControllerServiceState(revision, id);

        // create the revision
        final RevisionDTO responseRevision = new RevisionDTO();
        responseRevision.setClientId(revision.getClientId());
        responseRevision.setVersion(snapshot.getVersion());

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setRevision(responseRevision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the references of the specified controller service.
     *
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/references")
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
                    value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
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
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param updateReferenceRequest The update request
     * @return A controllerServiceReferencingComponentsEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/references")
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
                value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                allowableValues = "NCM, NODE",
                required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                value = "The controller service request update request.",
                required = true
            ) UpdateControllerServiceReferenceRequestEntity updateReferenceRequest) {

        if (updateReferenceRequest.getId() == null) {
            throw new IllegalArgumentException("The controller service identifier must be specified.");
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

        // determine the availability
        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(updateReferenceRequest), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final Revision controllerServiceRevision = getRevision(updateReferenceRequest.getRevision(), updateReferenceRequest.getId());
        final boolean validationPhase = isValidationPhase(httpServletRequest);
        if (validationPhase || !isTwoPhaseRequest(httpServletRequest)) {
            serviceFacade.claimRevision(controllerServiceRevision);
        }
        if (validationPhase) {
            serviceFacade.verifyUpdateControllerServiceReferencingComponents(updateReferenceRequest.getId(), scheduledState, controllerServiceState);
            return generateContinueResponse().build();
        }

        // get the controller service
        final ControllerServiceReferencingComponentsEntity entity = serviceFacade.updateControllerServiceReferencingComponents(
            controllerServiceRevision, updateReferenceRequest.getId(), scheduledState, controllerServiceState);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified a new Controller Service.
     *
     * @param httpServletRequest request
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to update.
     * @param controllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
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
                    value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The controller service configuration details.",
                    required = true
            ) ControllerServiceEntity controllerServiceEntity) {

        final Availability avail = parseAvailability(availability);

        if (controllerServiceEntity == null || controllerServiceEntity.getControllerService() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (controllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ControllerServiceDTO requestControllerServiceDTO = controllerServiceEntity.getControllerService();
        if (!id.equals(requestControllerServiceDTO.getId())) {
            throw new IllegalArgumentException(String.format("The controller service id (%s) in the request body does not equal the "
                    + "controller service id of the requested resource (%s).", requestControllerServiceDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(controllerServiceEntity), getHeaders(headersToOverride)).getResponse();
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
        populateRemainingControllerServiceContent(availability, entity.getControllerService());

        if (updateResult.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(entity.getControllerService().getUri()), entity)).build();
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
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
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
                    value = "Whether the controller service is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
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
