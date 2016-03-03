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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.Availability;
import org.springframework.security.access.prepost.PreAuthorize;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;

/**
 * RESTful endpoint for managing a Reporting Task.
 */
@Api(hidden = true)
public class ReportingTaskResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ServletContext servletContext;

    /**
     * Populates the uri for the specified reporting task.
     *
     * @param reportingTasks tasks
     * @return tasks
     */
    private Set<ReportingTaskDTO> populateRemainingReportingTasksContent(final String availability, final Set<ReportingTaskDTO> reportingTasks) {
        for (ReportingTaskDTO reportingTask : reportingTasks) {
            populateRemainingReportingTaskContent(availability, reportingTask);
        }
        return reportingTasks;
    }

    /**
     * Populates the uri for the specified reporting task.
     */
    private ReportingTaskDTO populateRemainingReportingTaskContent(final String availability, final ReportingTaskDTO reportingTask) {
        // populate the reporting task href
        reportingTask.setUri(generateResourceUri("controller", "reporting-tasks", availability, reportingTask.getId()));
        reportingTask.setAvailability(availability);

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(reportingTask.getType())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(reportingTask.getType());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ReportingTaskConfiguration.equals(uiExtension.getExtensionType())) {
                    reportingTask.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                }
            }
        }

        return reportingTask;
    }

    /**
     * Parses the availability and ensure that the specified availability makes
     * sense for the given NiFi instance.
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
     * Retrieves all the of reporting tasks in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @return A reportingTasksEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all reporting tasks",
            response = ReportingTasksEntity.class,
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
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getReportingTasks(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the reporting tasks
        final Set<ReportingTaskDTO> reportingTasks = populateRemainingReportingTasksContent(availability, serviceFacade.getReportingTasks());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ReportingTasksEntity entity = new ReportingTasksEntity();
        entity.setRevision(revision);
        entity.setReportingTasks(reportingTasks);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new reporting task.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param type The type of reporting task to create.
     * @return A reportingTaskEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response createReportingTask(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability,
            @FormParam("type") String type) {

        // create the reporting task DTO
        final ReportingTaskDTO reportingTaskDTO = new ReportingTaskDTO();
        reportingTaskDTO.setType(type);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the reporting task entity
        final ReportingTaskEntity reportingTaskEntity = new ReportingTaskEntity();
        reportingTaskEntity.setRevision(revision);
        reportingTaskEntity.setReportingTask(reportingTaskDTO);

        return createReportingTask(httpServletRequest, availability, reportingTaskEntity);
    }

    /**
     * Creates a new Reporting Task.
     *
     * @param httpServletRequest request
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param reportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a new reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM")
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
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The reporting task configuration details.",
                    required = true
            ) ReportingTaskEntity reportingTaskEntity) {

        final Availability avail = parseAvailability(availability);

        if (reportingTaskEntity == null || reportingTaskEntity.getReportingTask() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (reportingTaskEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (reportingTaskEntity.getReportingTask().getId() != null) {
            throw new IllegalArgumentException("Reporting task ID cannot be specified.");
        }

        if (StringUtils.isBlank(reportingTaskEntity.getReportingTask().getType())) {
            throw new IllegalArgumentException("The type of reporting task to create must be specified.");
        }

        // get the revision
        final RevisionDTO revision = reportingTaskEntity.getRevision();

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            reportingTaskEntity.getReportingTask().setId(id);

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
            return clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(reportingTaskEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the reporting task and generate the json
        final ConfigurationSnapshot<ReportingTaskDTO> controllerResponse = serviceFacade.createReportingTask(
                new Revision(revision.getVersion(), revision.getClientId()), reportingTaskEntity.getReportingTask());
        final ReportingTaskDTO reportingTask = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setRevision(updatedRevision);
        entity.setReportingTask(populateRemainingReportingTaskContent(availability, reportingTask));

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(reportingTask.getUri()), entity)).build();
    }

    /**
     * Retrieves the specified reporting task.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param id The id of the reporting task to retrieve
     * @return A reportingTaskEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a reporting task",
            response = ReportingTaskEntity.class,
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
    public Response getReportingTask(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the reporting task
        final ReportingTaskDTO reportingTask = serviceFacade.getReportingTask(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setRevision(revision);
        entity.setReportingTask(populateRemainingReportingTaskContent(availability, reportingTask));

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability availability
     * @param id The id of the reporting task.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/descriptors")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a reporting task property descriptor",
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
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") String propertyName) {

        final Availability avail = parseAvailability(availability);

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        // replicate if cluster manager and task is on node
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getReportingTaskPropertyDescriptor(id, propertyName);

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
     * Gets the state for a reporting task.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param availability Whether the reporting task is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the reporting task
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/state")
    @PreAuthorize("hasAnyRole('ROLE_DFM')")
    @ApiOperation(
        value = "Gets the state for a reporting task",
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
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
            allowableValues = "NCM, NODE",
            required = true
        )
        @PathParam("availability") String availability,
        @ApiParam(
            value = "The reporting task id.",
            required = true
        )
        @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the component state
        final ComponentStateDTO state = serviceFacade.getReportingTaskState(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setRevision(revision);
        entity.setComponentState(state);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Clears the state for a reporting task.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param availability Whether the reporting task is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the reporting task
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}/state/clear-requests")
    @PreAuthorize("hasAnyRole('ROLE_DFM')")
    @ApiOperation(
        value = "Clears the state for a reporting task",
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
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The revision is used to verify the client is working with the latest version of the flow.",
            required = true
        )
        @FormParam(VERSION) LongParameter version,
        @ApiParam(
            value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
            allowableValues = "NCM, NODE",
            required = true
        )
        @PathParam("availability") String availability,
        @ApiParam(
            value = "The reporting task id.",
            required = true
        )
        @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyCanClearReportingTaskState(id);
            return generateContinueResponse().build();
        }

        // get the revision specified by the user
        Long revision = null;
        if (version != null) {
            revision = version.getLong();
        }

        // get the component state
        final ConfigurationSnapshot<Void> snapshot = serviceFacade.clearReportingTaskState(new Revision(revision, clientId.getClientId()), id);

        // create the revision
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId(clientId.getClientId());
        revisionDTO.setVersion(snapshot.getVersion());

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setRevision(revisionDTO);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified reporting task.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param id The id of the reporting task to update.
     * @param name The name of the reporting task
     * @param annotationData The annotation data for the reporting task
     * @param markedForDeletion Array of property names whose value should be
     * removed.
     * @param state The updated scheduled state
     * @param schedulingStrategy The scheduling strategy for this reporting task
     * @param schedulingPeriod The scheduling period for this reporting task
     * @param comments The comments for this reporting task
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
     * @return A reportingTaskEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateReportingTask(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("availability") String availability, @PathParam("id") String id, @FormParam("name") String name,
            @FormParam("annotationData") String annotationData, @FormParam("markedForDeletion[]") List<String> markedForDeletion,
            @FormParam("state") String state, @FormParam("schedulingStrategy") String schedulingStrategy,
            @FormParam("schedulingPeriod") String schedulingPeriod, @FormParam("comments") String comments,
            MultivaluedMap<String, String> formParams) {

        // create collections for holding the reporting task properties
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

        // create the reporting task DTO
        final ReportingTaskDTO reportingTaskDTO = new ReportingTaskDTO();
        reportingTaskDTO.setId(id);
        reportingTaskDTO.setName(name);
        reportingTaskDTO.setState(state);
        reportingTaskDTO.setSchedulingStrategy(schedulingStrategy);
        reportingTaskDTO.setSchedulingPeriod(schedulingPeriod);
        reportingTaskDTO.setAnnotationData(annotationData);
        reportingTaskDTO.setComments(comments);

        // only set the properties when appropriate
        if (!updatedProperties.isEmpty()) {
            reportingTaskDTO.setProperties(updatedProperties);
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the reporting task entity
        final ReportingTaskEntity reportingTaskEntity = new ReportingTaskEntity();
        reportingTaskEntity.setRevision(revision);
        reportingTaskEntity.setReportingTask(reportingTaskDTO);

        // update the reporting task
        return updateReportingTask(httpServletRequest, availability, id, reportingTaskEntity);
    }

    /**
     * Updates the specified a Reporting Task.
     *
     * @param httpServletRequest request
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param id The id of the reporting task to update.
     * @param reportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a reporting task",
            response = ReportingTaskEntity.class,
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
    public Response updateReportingTask(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The reporting task configuration details.",
                    required = true
            ) ReportingTaskEntity reportingTaskEntity) {

        final Availability avail = parseAvailability(availability);

        if (reportingTaskEntity == null || reportingTaskEntity.getReportingTask() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (reportingTaskEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ReportingTaskDTO requestReportingTaskDTO = reportingTaskEntity.getReportingTask();
        if (!id.equals(requestReportingTaskDTO.getId())) {
            throw new IllegalArgumentException(String.format("The reporting task id (%s) in the request body does not equal the "
                    + "reporting task id of the requested resource (%s).", requestReportingTaskDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(reportingTaskEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateReportingTask(requestReportingTaskDTO);
            return generateContinueResponse().build();
        }

        // update the reporting task
        final RevisionDTO revision = reportingTaskEntity.getRevision();
        final ConfigurationSnapshot<ReportingTaskDTO> controllerResponse = serviceFacade.updateReportingTask(
                new Revision(revision.getVersion(), revision.getClientId()), requestReportingTaskDTO);

        // get the results
        final ReportingTaskDTO responseReportingTaskDTO = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setRevision(updatedRevision);
        entity.setReportingTask(populateRemainingReportingTaskContent(availability, responseReportingTaskDTO));

        if (controllerResponse.isNew()) {
            return clusterContext(generateCreatedResponse(URI.create(responseReportingTaskDTO.getUri()), entity)).build();
        } else {
            return clusterContext(generateOkResponse(entity)).build();
        }
    }

    /**
     * Removes the specified reporting task.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param availability Whether the reporting task is available on the NCM
     * only (ncm) or on the nodes only (node). If this instance is not clustered
     * all tasks should use the node availability.
     * @param id The id of the reporting task to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{availability}/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a reporting task",
            response = ReportingTaskEntity.class,
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
    public Response removeReportingTask(
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
                    value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
                    allowableValues = "NCM, NODE",
                    required = true
            )
            @PathParam("availability") String availability,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id) {

        final Availability avail = parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyDeleteReportingTask(id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the specified reporting task
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteReportingTask(new Revision(clientVersion, clientId.getClientId()), id);

        // get the updated revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // build the response entity
        final ReportingTaskEntity entity = new ReportingTaskEntity();
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
