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
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.Availability;

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
import java.util.Set;

/**
 * RESTful endpoint for managing a Reporting Task.
 */
@Path("/reporting-tasks")
@Api(
    value = "/reporting-tasks",
    description = "Endpoint for managing a Reporting Task."
)
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
    public Set<ReportingTaskDTO> populateRemainingReportingTasksContent(final String availability, final Set<ReportingTaskDTO> reportingTasks) {
        for (ReportingTaskDTO reportingTask : reportingTasks) {
            populateRemainingReportingTaskContent(availability, reportingTask);
        }
        return reportingTasks;
    }

    /**
     * Populates the uri for the specified reporting task.
     */
    public ReportingTaskDTO populateRemainingReportingTaskContent(final String availability, final ReportingTaskDTO reportingTask) {
        // populate the reporting task href
        reportingTask.setUri(generateResourceUri("reporting-tasks", availability, reportingTask.getId()));
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/descriptors")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/state")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_DFM')")
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
     * @param revisionEntity The revision is used to verify the client is working with the latest version of the flow.
     * @param availability Whether the reporting task is available on the
     * NCM only (ncm) or on the nodes only (node). If this instance is not
     * clustered all services should use the node availability.
     * @param id The id of the reporting task
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}/state/clear-requests")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_DFM')")
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
            value = "The revision used to verify the client is working with the latest version of the flow.",
            required = true
        )
        Entity revisionEntity,
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

        // get the component state
        final RevisionDTO requestRevision = revisionEntity.getRevision();
        final ConfigurationSnapshot<Void> snapshot = serviceFacade.clearReportingTaskState(new Revision(requestRevision.getVersion(), requestRevision.getClientId()), id);

        // create the revision
        final RevisionDTO responseRevision = new RevisionDTO();
        responseRevision.setClientId(requestRevision.getClientId());
        responseRevision.setVersion(snapshot.getVersion());

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setRevision(responseRevision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
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
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
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
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(reportingTaskEntity), getHeaders()).getResponse();
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{availability}/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
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
