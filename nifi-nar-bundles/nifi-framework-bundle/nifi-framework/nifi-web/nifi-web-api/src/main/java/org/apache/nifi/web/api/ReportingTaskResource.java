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
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConfigurationAnalysisDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * RESTful endpoint for managing a Reporting Task.
 */
@Path("/reporting-tasks")
@Api(
        value = "/reporting-tasks",
        description = "Endpoint for managing a Reporting Task."
)
public class ReportingTaskResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ReportingTaskResource.class);
    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";
    private RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> updateRequestManager =
        new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Reporting Task Config Thread");

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
        if (bundle == null) {
            return reportingTask;
        }

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
                    @Authorization(value = "Read - /reporting-tasks/{uuid}")
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
                    @Authorization(value = "Read - /reporting-tasks/{uuid}")
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
            @QueryParam("propertyName") final String propertyName,
            @ApiParam(
                    value = "Property Descriptor requested sensitive status",
                    defaultValue = "false"
            )
            @QueryParam("sensitive") final boolean sensitive
    ) {

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
        final PropertyDescriptorDTO descriptor = serviceFacade.getReportingTaskPropertyDescriptor(id, propertyName, sensitive);

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
                    @Authorization(value = "Write - /reporting-tasks/{uuid}")
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
                    @Authorization(value = "Write - /reporting-tasks/{uuid}")
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
                    @Authorization(value = "Write - /reporting-tasks/{uuid}"),
                    @Authorization(value = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}")
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
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestReportingTaskEntity.isDisconnectedNodeAcknowledged());
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
                    @Authorization(value = "Write - /reporting-tasks/{uuid}"),
                    @Authorization(value = "Write - /controller"),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}")
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
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
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

    /**
     * Updates the operational status for the specified ReportingTask with the specified values.
     *
     * @param httpServletRequest  request
     * @param id                  The id of the reporting task to update.
     * @param requestRunStatus A runStatusEntity.
     * @return A reportingTaskEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/run-status")
    @ApiOperation(
            value = "Updates run status of a reporting task",
            response = ReportingTaskEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /reporting-tasks/{uuid} or  or /operation/reporting-tasks/{uuid}")
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
    public Response updateRunStatus(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The reporting task run status.",
                    required = true
            ) final ReportingTaskRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Reporting task run status must be specified.");
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
                    // authorize reporting task
                    final Authorizable authorizable = lookup.getReportingTask(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateReportingTask(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, reportingTaskRunStatusEntity) -> {
                    // update the reporting task
                    final ReportingTaskEntity entity = serviceFacade.updateReportingTask(revision, createDTOWithDesiredRunStatus(id, reportingTaskRunStatusEntity.getState()));
                    populateRemainingReportingTaskEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/analysis")
    @ApiOperation(
        value = "Performs analysis of the component's configuration, providing information about which attributes are referenced.",
        response = ConfigurationAnalysisEntity.class,
        authorizations = {
            @Authorization(value = "Read - /reporting-tasks/{uuid}")
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
    public Response analyzeConfiguration(
        @ApiParam(value = "The reporting task id.", required = true) @PathParam("id") final String reportingTaskId,
        @ApiParam(value = "The configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Reporting Tasks's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Reporting Tasks's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(reportingTaskId)) {
            throw new IllegalArgumentException("Reporting Tasks's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Reporting Tasks's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
            serviceFacade,
            configurationAnalysis,
            lookup -> {
                final ComponentAuthorizable reportingTask = lookup.getReportingTask(reportingTaskId);
                reportingTask.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            },
            () -> { },
            entity -> {
                final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeReportingTaskConfiguration(analysis.getComponentId(), analysis.getProperties());
                return generateOkResponse(resultsEntity).build();
            }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/verification-requests")
    @ApiOperation(
        value = "Performs verification of the Reporting Task's configuration",
        response = VerifyConfigRequestEntity.class,
        notes = "This will initiate the process of verifying a given Reporting Task configuration. This may be a long-running task. As a result, this endpoint will immediately return a " +
            "ReportingTaskConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
            "The client may then periodically poll the status of the request by " +
            "issuing a GET request to /reporting-tasks/{serviceId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
            "/reporting-tasks/{serviceId}/verification-requests/{requestId}.",
        authorizations = {
            @Authorization(value = "Read - /reporting-tasks/{uuid}")
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
    public Response submitConfigVerificationRequest(
        @ApiParam(value = "The reporting task id.", required = true) @PathParam("id") final String reportingTaskId,
        @ApiParam(value = "The reporting task configuration verification request.", required = true) final VerifyConfigRequestEntity reportingTaskConfigRequest) {

        if (reportingTaskConfigRequest == null) {
            throw new IllegalArgumentException("Reporting Task's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = reportingTaskConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Reporting Task Properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Reporting Task's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(reportingTaskId)) {
            throw new IllegalArgumentException("Reporting Task's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, reportingTaskConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
            serviceFacade,
            reportingTaskConfigRequest,
            lookup -> {
                final ComponentAuthorizable reportingTask = lookup.getReportingTask(reportingTaskId);
                reportingTask.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                serviceFacade.verifyCanVerifyReportingTaskConfig(reportingTaskId);
            },
            entity -> performAsyncConfigVerification(entity, user)
        );
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/config/verification-requests/{requestId}")
    @ApiOperation(
        value = "Returns the Verification Request with the given ID",
        response = VerifyConfigRequestEntity.class,
        notes = "Returns the Verification Request with the given ID. Once an Verification Request has been created, "
            + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
            + "current state of the request, and any failures. ",
        authorizations = {
            @Authorization(value = "Only the user that submitted the request can get it")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getVerificationRequest(
        @ApiParam("The ID of the Reporting Task") @PathParam("id") final String reportingTaskId,
        @ApiParam("The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
            updateRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

        final VerifyConfigRequestEntity updateRequestEntity = createVerifyReportingTaskConfigRequestEntity(asyncRequest, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/config/verification-requests/{requestId}")
    @ApiOperation(
        value = "Deletes the Verification Request with the given ID",
        response = VerifyConfigRequestEntity.class,
        notes = "Deletes the Verification Request with the given ID. After a request is created, it is expected "
            + "that the client will properly clean up the request by DELETE'ing it, once the Verification process has completed. If the request is deleted before the request "
            + "completes, then the Verification request will finish the step that it is currently performing and then will cancel any subsequent steps.",
        authorizations = {
            @Authorization(value = "Only the user that submitted the request can remove it")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteValidationRequest(
        @ApiParam("The ID of the Reporting Task") @PathParam("id") final String reportingTaskId,
        @ApiParam("The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final boolean twoPhaseRequest = isTwoPhaseRequest(httpServletRequest);
        final boolean executionPhase = isExecutionPhase(httpServletRequest);

        // If this is a standalone node, or if this is the execution phase of the request, perform the actual request.
        if (!twoPhaseRequest || executionPhase) {
            // request manager will ensure that the current is the user that submitted this request
            final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                updateRequestManager.removeRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

            if (asyncRequest == null) {
                throw new ResourceNotFoundException("Could not find request of type " + VERIFICATION_REQUEST_TYPE + " with ID " + requestId);
            }

            if (!asyncRequest.isComplete()) {
                asyncRequest.cancel();
            }

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyReportingTaskConfigRequestEntity(asyncRequest, requestId);
            return generateOkResponse(updateRequestEntity).build();
        }

        if (isValidationPhase(httpServletRequest)) {
            // Perform authorization by attempting to get the request
            updateRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);
            return generateContinueResponse().build();
        } else if (isCancellationPhase(httpServletRequest)) {
            return generateOkResponse().build();
        } else {
            throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
        }
    }



    public Response performAsyncConfigVerification(final VerifyConfigRequestEntity configRequest, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may take an indeterminate amount of time.
        final String requestId = generateUuid();

        final VerifyConfigRequestDTO requestDto = configRequest.getRequest();
        final String taskId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Reporting Task Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
            new StandardAsynchronousWebRequest<>(requestId, configRequest, taskId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> updateTask = asyncRequest -> {
            try {
                final List<ConfigVerificationResultDTO> results = serviceFacade.performReportingTaskConfigVerification(taskId, requestDto.getProperties());
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Reporting Task configuration", e);
                asyncRequest.fail("Failed to verify Reporting Task configuration due to " + e);
            }
        };

        updateRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, updateTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyReportingTaskConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyReportingTaskConfigRequestEntity(
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest, final String requestId) {

        final VerifyConfigRequestDTO requestDto = asyncRequest.getRequest().getRequest();
        final List<ConfigVerificationResultDTO> resultsList = asyncRequest.getResults();

        final VerifyConfigRequestDTO dto = new VerifyConfigRequestDTO();
        dto.setComponentId(requestDto.getComponentId());
        dto.setProperties(requestDto.getProperties());
        dto.setResults(resultsList);

        dto.setComplete(asyncRequest.isComplete());
        dto.setFailureReason(asyncRequest.getFailureReason());
        dto.setLastUpdated(asyncRequest.getLastUpdated());
        dto.setPercentCompleted(asyncRequest.getPercentComplete());
        dto.setRequestId(requestId);
        dto.setState(asyncRequest.getState());
        dto.setUri(generateResourceUri("reporting-tasks", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
    }

    private ReportingTaskDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setId(id);
        dto.setState(runStatus);
        return dto;
    }

    // setters

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
