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
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.servlet.ServletContext;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.UiExtensionType;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.util.Availability;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Controller Service.
 */
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
     * @param controllerServices
     * @return
     */
    private Set<ControllerServiceDTO> populateRemainingControllerServicesContent(final String availability, final Set<ControllerServiceDTO> controllerServices) {
        for (ControllerServiceDTO controllerService : controllerServices) {
            populateRemainingControllerServiceContent(availability, controllerService);
        }
        return controllerServices;
    }

    /**
     * Populates the uri for the specified controller service.
     */
    private ControllerServiceDTO populateRemainingControllerServiceContent(final String availability, final ControllerServiceDTO controllerService) {
        // populate the controller service href
        controllerService.setUri(generateResourceUri("controller", "controller-services", availability, controllerService.getId()));
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
     * @param availability
     * @return
     */
    private Availability parseAvailability(final String availability) {
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
     * Retrieves all the of controller services in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @return A controllerServicesEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerServicesEntity.class)
    public Response getControllerServices(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId, @PathParam("availability") String availability) {
        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the controller services
        final Set<ControllerServiceDTO> controllerServices = populateRemainingControllerServicesContent(availability, serviceFacade.getControllerServices());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setRevision(revision);
        entity.setControllerServices(controllerServices);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new controller service.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param type The type of controller service to create.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerServiceEntity.class)
    public Response createControllerService(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability,
            @FormParam("type") String type) {

        // create the controller service DTO
        final ControllerServiceDTO controllerServiceDTO = new ControllerServiceDTO();
        controllerServiceDTO.setType(type);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the controller service entity
        final ControllerServiceEntity controllerServiceEntity = new ControllerServiceEntity();
        controllerServiceEntity.setRevision(revision);
        controllerServiceEntity.setControllerService(controllerServiceDTO);

        return createControllerService(httpServletRequest, availability, controllerServiceEntity);
    }

    /**
     * Creates a new Controller Service.
     *
     * @param httpServletRequest
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param controllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerServiceEntity.class)
    public Response createControllerService(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("availability") String availability,
            ControllerServiceEntity controllerServiceEntity) {

        final Availability avail = parseAvailability(availability);

        if (controllerServiceEntity == null || controllerServiceEntity.getControllerService() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (controllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (controllerServiceEntity.getControllerService().getId() != null) {
            throw new IllegalArgumentException("Controller service ID cannot be specified.");
        }

        if (StringUtils.isBlank(controllerServiceEntity.getControllerService().getType())) {
            throw new IllegalArgumentException("The type of controller service to create must be specified.");
        }

        // get the revision
        final RevisionDTO revision = controllerServiceEntity.getRevision();

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            controllerServiceEntity.getControllerService().setId(id);

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
            return (Response) clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(controllerServiceEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the controller service and generate the json
        final ConfigurationSnapshot<ControllerServiceDTO> controllerResponse = serviceFacade.createControllerService(
                new Revision(revision.getVersion(), revision.getClientId()), controllerServiceEntity.getControllerService());
        final ControllerServiceDTO controllerService = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(updatedRevision);
        entity.setControllerService(populateRemainingControllerServiceContent(availability, controllerService));

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(controllerService.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified controller service.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerServiceEntity.class)
    public Response getControllerService(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller service
        final ControllerServiceDTO controllerService = serviceFacade.getControllerService(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(revision);
        entity.setControllerService(populateRemainingControllerServiceContent(availability, controllerService));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability
     * @param id The id of the controller service.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/descriptors")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(PropertyDescriptorEntity.class)
    public Response getPropertyDescriptor(
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id,
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

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setRevision(revision);
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the references of the specified controller service.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/references")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerServiceEntity.class)
    public Response getControllerServiceReferences(
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the controller service
        final Set<ControllerServiceReferencingComponentDTO> controllerServiceReferences = serviceFacade.getControllerServiceReferencingComponents(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setRevision(revision);
        entity.setControllerServiceReferencingComponents(controllerServiceReferences);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the references of the specified controller service.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to retrieve
     * @param state Sets the state of referencing components. A value of RUNNING
     * or STOPPED will update referencing schedulable components (Processors and
     * Reporting Tasks). A value of ENABLED or DISABLED will update referencing
     * controller services.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/references")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(ControllerServiceEntity.class)
    public Response updateControllerServiceReferences(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id,
            @FormParam("state") @DefaultValue(StringUtils.EMPTY) String state) {

        // parse the state to determine the desired action
        // need to consider controller service state first as it shares a state with
        // scheduled state (disabled) which is applicable for referencing services
        // but not referencing schedulable components
        ControllerServiceState controllerServiceState = null;
        try {
            controllerServiceState = ControllerServiceState.valueOf(state);
        } catch (final IllegalArgumentException iae) {
            // ignore
        }

        ScheduledState scheduledState = null;
        try {
            scheduledState = ScheduledState.valueOf(state);
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
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateControllerServiceReferencingComponents(id, scheduledState, controllerServiceState);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // get the controller service
        final ConfigurationSnapshot<Set<ControllerServiceReferencingComponentDTO>> response
                = serviceFacade.updateControllerServiceReferencingComponents(new Revision(clientVersion, clientId.getClientId()), id, scheduledState, controllerServiceState);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(response.getVersion());

        // create the response entity
        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setRevision(revision);
        entity.setControllerServiceReferencingComponents(response.getConfiguration());

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified controller service.
     *
     * @param httpServletRequest
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to update.
     * @param name The name of the controller service
     * @param annotationData The annotation data for the controller service
     * @param comments The comments for the controller service
     * @param state The state of this controller service. Should be ENABLED or
     * DISABLED.
     * @param markedForDeletion Array of property names whose value should be
     * removed.
     * @param formParams Additionally, the processor properties and styles are
     * specified in the form parameters. Because the property names and styles
     * differ from processor to processor they are specified in a map-like
     * fashion:
     * <br>
     * <ul>
     * <li>properties[required.file.path]=/path/to/file</li>
     * <li>properties[required.hostname]=localhost</li>
     * <li>properties[required.port]=80</li>
     * <li>properties[optional.file.path]=/path/to/file</li>
     * <li>properties[optional.hostname]=localhost</li>
     * <li>properties[optional.port]=80</li>
     * <li>properties[user.defined.pattern]=^.*?s.*$</li>
     * </ul>
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerServiceEntity.class)
    public Response updateControllerService(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id, @FormParam("name") String name,
            @FormParam("annotationData") String annotationData, @FormParam("comments") String comments,
            @FormParam("state") String state, @FormParam("markedForDeletion[]") List<String> markedForDeletion,
            MultivaluedMap<String, String> formParams) {

        // create collections for holding the controller service properties
        final Map<String, String> updatedProperties = new LinkedHashMap<>();

        // go through each parameter and look for processor properties
        for (String parameterName : formParams.keySet()) {
            if (StringUtils.isNotBlank(parameterName)) {
                // see if the parameter name starts with an expected parameter type...
                // if so, store the parameter name and value in the corresponding collection
                if (parameterName.startsWith("properties")) {
                    final int startIndex = StringUtils.indexOf(parameterName, "[");
                    final int endIndex = StringUtils.lastIndexOf(parameterName, "]");
                    if (startIndex != -1 && endIndex != -1) {
                        final String propertyName = StringUtils.substring(parameterName, startIndex + 1, endIndex);
                        updatedProperties.put(propertyName, formParams.getFirst(parameterName));
                    }
                }
            }
        }

        // set the properties to remove
        for (String propertyToDelete : markedForDeletion) {
            updatedProperties.put(propertyToDelete, null);
        }

        // create the controller service DTO
        final ControllerServiceDTO controllerServiceDTO = new ControllerServiceDTO();
        controllerServiceDTO.setId(id);
        controllerServiceDTO.setName(name);
        controllerServiceDTO.setAnnotationData(annotationData);
        controllerServiceDTO.setComments(comments);
        controllerServiceDTO.setState(state);

        // only set the properties when appropriate
        if (!updatedProperties.isEmpty()) {
            controllerServiceDTO.setProperties(updatedProperties);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the controller service entity
        final ControllerServiceEntity controllerServiceEntity = new ControllerServiceEntity();
        controllerServiceEntity.setRevision(revision);
        controllerServiceEntity.setControllerService(controllerServiceDTO);

        // update the controller service
        return updateControllerService(httpServletRequest, availability, id, controllerServiceEntity);
    }

    /**
     * Updates the specified a new Controller Service.
     *
     * @param httpServletRequest
     * @param availability Whether the controller service is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the controller service to update.
     * @param controllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerServiceEntity.class)
    public Response updateControllerService(
            @Context HttpServletRequest httpServletRequest,
            @PathParam("availability") String availability,
            @PathParam("id") String id,
            ControllerServiceEntity controllerServiceEntity) {

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
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateControllerService(requestControllerServiceDTO);
            return generateContinueResponse().build();
        }

        // update the controller service
        final RevisionDTO revision = controllerServiceEntity.getRevision();
        final ConfigurationSnapshot<ControllerServiceDTO> controllerResponse = serviceFacade.updateControllerService(
                new Revision(revision.getVersion(), revision.getClientId()), requestControllerServiceDTO);

        // get the results
        final ControllerServiceDTO responseControllerServiceDTO = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(updatedRevision);
        entity.setControllerService(populateRemainingControllerServiceContent(availability, responseControllerServiceDTO));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Removes the specified controller service.
     *
     * @param httpServletRequest
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
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @TypeHint(ControllerServiceEntity.class)
    public Response removeControllerService(
            @Context HttpServletRequest httpServletRequest,
            @QueryParam(VERSION) LongParameter version,
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyDeleteControllerService(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified controller service
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteControllerService(new Revision(clientVersion, clientId.getClientId()), id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ControllerServiceEntity entity = new ControllerServiceEntity();
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
