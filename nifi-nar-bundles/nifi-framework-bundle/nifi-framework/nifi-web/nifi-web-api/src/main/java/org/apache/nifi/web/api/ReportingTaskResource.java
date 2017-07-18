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
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;

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
    private Authorizer authorizer;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified reporting tasks.
     *
     * @param reportingTaskEntities reporting tasks
     * @return dtos
     */
    public Set<ReportingTaskEntity> populateRemainingReportingTaskEntitiesContent(final Set<ReportingTaskEntity> reportingTaskEntities) {
        for (ReportingTaskEntity reportingTaskEntity : reportingTaskEntities) {
            populateRemainingReportingTaskEntityContent(reportingTaskEntity);
        }
        return reportingTaskEntities;
    }

    /**
     * Populate the uri's for the specified reporting task.
     *
     * @param reportingTaskEntity reporting task
     * @return dtos
     */
    public ReportingTaskEntity populateRemainingReportingTaskEntityContent(final ReportingTaskEntity reportingTaskEntity) {
        reportingTaskEntity.setUri(generateResourceUri("reporting-tasks", reportingTaskEntity.getId()));

        // populate the remaining content
        if (reportingTaskEntity.getComponent() != null) {
            populateRemainingReportingTaskContent(reportingTaskEntity.getComponent());
        }
        return reportingTaskEntity;
    }

    /**
     * Populates the uri for the specified reporting task.
     */
    public ReportingTaskDTO populateRemainingReportingTaskContent(final ReportingTaskDTO reportingTask) {
        final BundleDTO bundle = reportingTask.getBundle();

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(reportingTask.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(reportingTask.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ReportingTaskConfiguration.equals(uiExtension.getExtensionType())) {
                    reportingTask.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                }
            }
        }

        return reportingTask;
    }

    /**
     * Retrieves the specified reporting task.
     *
     * @param id The id of the reporting task to retrieve
     * @return A reportingTaskEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /reporting-tasks/{uuid}", type = "")
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
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable reportingTask = lookup.getReportingTask(id).getAuthorizable();
            reportingTask.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the reporting task
        final ReportingTaskEntity reportingTask = serviceFacade.getReportingTask(id);
        populateRemainingReportingTaskEntityContent(reportingTask);

        return generateOkResponse(reportingTask).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id           The id of the reporting task.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    @ApiOperation(
            value = "Gets a reporting task property descriptor",
            response = PropertyDescriptorEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /reporting-tasks/{uuid}", type = "")
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
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") final String propertyName) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable reportingTask = lookup.getReportingTask(id).getAuthorizable();
            reportingTask.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getReportingTaskPropertyDescriptor(id, propertyName);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a reporting task.
     *
     * @param id The id of the reporting task
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    @ApiOperation(
            value = "Gets the state for a reporting task",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /reporting-tasks/{uuid}", type = "")
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
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable reportingTask = lookup.getReportingTask(id).getAuthorizable();
            reportingTask.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getReportingTaskState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a reporting task.
     *
     * @param httpServletRequest servlet request
     * @param id                 The id of the reporting task
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @ApiOperation(
            value = "Clears the state for a reporting task",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /reporting-tasks/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ReportingTaskEntity requestReportTaskEntity = new ReportingTaskEntity();
        requestReportTaskEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestReportTaskEntity,
                lookup -> {
                    final Authorizable processor = lookup.getReportingTask(id).getAuthorizable();
                    processor.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearReportingTaskState(id),
                (reportingTaskEntity) -> {
                    // get the component state
                    serviceFacade.clearReportingTaskState(reportingTaskEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified a Reporting Task.
     *
     * @param httpServletRequest  request
     * @param id                  The id of the reporting task to update.
     * @param requestReportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates a reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /reporting-tasks/{uuid}", type = ""),
                    @Authorization(value = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}", type = "")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The reporting task configuration details.",
                    required = true
            ) final ReportingTaskEntity requestReportingTaskEntity) {

        if (requestReportingTaskEntity == null || requestReportingTaskEntity.getComponent() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (requestReportingTaskEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ReportingTaskDTO requestReportingTaskDTO = requestReportingTaskEntity.getComponent();
        if (!id.equals(requestReportingTaskDTO.getId())) {
            throw new IllegalArgumentException(String.format("The reporting task id (%s) in the request body does not equal the "
                    + "reporting task id of the requested resource (%s).", requestReportingTaskDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestReportingTaskEntity);
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestReportingTaskEntity, id);
        return withWriteLock(
                serviceFacade,
                requestReportingTaskEntity,
                requestRevision,
                lookup -> {
                    // authorize reporting task
                    final ComponentAuthorizable authorizable = lookup.getReportingTask(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestReportingTaskDTO.getProperties(), authorizable, authorizer, lookup);
                },
                () -> serviceFacade.verifyUpdateReportingTask(requestReportingTaskDTO),
                (revision, reportingTaskEntity) -> {
                    final ReportingTaskDTO reportingTaskDTO = reportingTaskEntity.getComponent();

                    // update the reporting task
                    final ReportingTaskEntity entity = serviceFacade.updateReportingTask(revision, reportingTaskDTO);
                    populateRemainingReportingTaskEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified reporting task.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the reporting task to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /reporting-tasks/{uuid}", type = ""),
                    @Authorization(value = "Write - /controller", type = ""),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}", type = "")
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
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final ReportingTaskEntity requestReportingTaskEntity = new ReportingTaskEntity();
        requestReportingTaskEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestReportingTaskEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable reportingTask = lookup.getReportingTask(id);

                    // ensure write permission to the reporting task
                    reportingTask.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    reportingTask.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(reportingTask, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteReportingTask(id),
                (revision, reportingTaskEntity) -> {
                    // delete the specified reporting task
                    final ReportingTaskEntity entity = serviceFacade.deleteReportingTask(revision, reportingTaskEntity.getId());
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
