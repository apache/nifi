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

import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BannerDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ClusterSummaryDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.search.NodeSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.AboutEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;
import org.apache.nifi.web.api.entity.ClusterSearchResultsEntity;
import org.apache.nifi.web.api.entity.ComponentHistoryEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskTypesEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.TemplatesEntity;
import org.apache.nifi.web.api.request.BulletinBoardPatternParameter;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Flow.
 */
@Path("/flow")
@Api(
    value = "/flow",
    description = "Endpoint for accessing the flow structure and component status."
)
public class FlowResource extends ApplicationResource {

    private static final String RECURSIVE = "false";

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Context
    private ResourceContext resourceContext;

    private ProcessorResource processorResource;
    private InputPortResource inputPortResource;
    private OutputPortResource outputPortResource;
    private FunnelResource funnelResource;
    private LabelResource labelResource;
    private RemoteProcessGroupResource remoteProcessGroupResource;
    private ConnectionResource connectionResource;
    private TemplateResource templateResource;
    private ProcessGroupResource processGroupResource;
    private ControllerServiceResource controllerServiceResource;
    private ReportingTaskResource reportingTaskResource;

    /**
     * Populates the remaining fields in the specified process group.
     *
     * @param flow group
     * @return group dto
     */
    private ProcessGroupFlowDTO populateRemainingFlowContent(ProcessGroupFlowDTO flow) {
        FlowDTO flowStructure = flow.getFlow();

        // populate the remaining fields for the processors, connections, process group refs, remote process groups, and labels if appropriate
        if (flowStructure != null) {
            populateRemainingFlowStructure(flowStructure);
        }

        // set the process group uri
        flow.setUri(generateResourceUri("flow", "process-groups",  flow.getId()));

        return flow;
    }

    /**
     * Populates the remaining content of the specified snippet.
     */
    private FlowDTO populateRemainingFlowStructure(FlowDTO flowStructure) {
        processorResource.populateRemainingProcessorEntitiesContent(flowStructure.getProcessors());
        connectionResource.populateRemainingConnectionEntitiesContent(flowStructure.getConnections());
        inputPortResource.populateRemainingInputPortEntitiesContent(flowStructure.getInputPorts());
        outputPortResource.populateRemainingOutputPortEntitiesContent(flowStructure.getOutputPorts());
        remoteProcessGroupResource.populateRemainingRemoteProcessGroupEntitiesContent(flowStructure.getRemoteProcessGroups());
        funnelResource.populateRemainingFunnelEntitiesContent(flowStructure.getFunnels());
        labelResource.populateRemainingLabelEntitiesContent(flowStructure.getLabels());
        processGroupResource.populateRemainingProcessGroupEntitiesContent(flowStructure.getProcessGroups());

        // go through each process group child and populate its uri
        for (final ProcessGroupEntity processGroupEntity : flowStructure.getProcessGroups()) {
            final ProcessGroupDTO processGroup = processGroupEntity.getComponent();
            if (processGroup != null) {
                processGroup.setContents(null);
            }
        }

        return flowStructure;
    }

    /**
     * Authorizes access to the flow.
     */
    private void authorizeFlow() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final Map<String,String> userContext;
        if (!StringUtils.isBlank(user.getClientAddress())) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
            .resource(ResourceFactory.getFlowResource())
            .identity(user.getIdentity())
            .anonymous(user.isAnonymous())
            .accessAttempt(true)
            .action(RequestAction.READ)
            .userContext(userContext)
            .build();

        final AuthorizationResult result = authorizer.authorize(request);
        if (!Result.Approved.equals(result.getResult())) {
            final String message = StringUtils.isNotBlank(result.getExplanation()) ? result.getExplanation() : "Access is denied";
            throw new AccessDeniedException(message);
        }
    }

    // ----
    // flow
    // ----

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("client-id")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Generates a client id.",
            response = String.class,
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
    public Response generateClientId() {
        authorizeFlow();
        return clusterContext(generateOkResponse(generateUuid())).build();
    }

    /**
     * Retrieves the configuration for the flow.
     *
     * @return A flowConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi flow",
            response = FlowConfigurationEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN"),
                    @Authorization(value = "ROLE_NIFI", type = "ROLE_NIFI")
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
    public Response getFlowConfig() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final FlowConfigurationEntity entity = serviceFacade.getFlowConfiguration();
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the identity of the user making the request.
     *
     * @return An identityEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("current-user")
    @ApiOperation(
            value = "Retrieves the user identity of the user making the request",
            response = CurrentUserEntity.class
    )
    public Response getCurrentUser() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the response entity
        final CurrentUserEntity entity = serviceFacade.getCurrentUser();

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param groupId The id of the process group.
     * @return A processGroupEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @ApiOperation(
        value = "Gets a process group",
        response = ProcessGroupFlowEntity.class,
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
    public Response getFlow(
        @ApiParam(
            value = "The process group id.",
            required = false
        )
        @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get this process group flow
        final ProcessGroupFlowEntity entity = serviceFacade.getProcessGroupFlow(groupId);
        populateRemainingFlowContent(entity.getProcessGroupFlow());
        return clusterContext(generateOkResponse(entity)).build();
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Retrieves all the of controller services in this NiFi.
     *
     * @return A controllerServicesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller/controller-services")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all controller services",
        response = ControllerServicesEntity.class,
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
    public Response getControllerServicesFromController() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the controller services
        final Set<ControllerServiceEntity> controllerServices = serviceFacade.getControllerServices(null);
        controllerServiceResource.populateRemainingControllerServiceEntitiesContent(controllerServices);

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setControllerServices(controllerServices);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves all the of controller services in this NiFi.
     *
     * @return A controllerServicesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/controller-services")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets all controller services",
        response = ControllerServicesEntity.class,
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
    public Response getControllerServicesFromGroup(
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

        // get all the controller services
        final Set<ControllerServiceEntity> controllerServices = serviceFacade.getControllerServices(groupId);
        controllerServiceResource.populateRemainingControllerServiceEntitiesContent(controllerServices);

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setControllerServices(controllerServices);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // ---------------
    // reporting-tasks
    // ---------------

    /**
     * Retrieves all the of reporting tasks in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @return A reportingTasksEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the reporting tasks
        final Set<ReportingTaskEntity> reportingTasks = serviceFacade.getReportingTasks();
        reportingTaskResource.populateRemainingReportingTaskEntitiesContent(reportingTasks);

        // create the response entity
        final ReportingTasksEntity entity = new ReportingTasksEntity();
        entity.setReportingTasks(reportingTasks);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Updates the specified process group.
     *
     * @param httpServletRequest request
     * @param id The id of the process group.
     * @param scheduleComponentsEntity A scheduleComponentsEntity.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
        value = "Updates a process group",
        response = ScheduleComponentsEntity.class,
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
    public Response scheduleComponents(
        @Context HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String id,
        ScheduleComponentsEntity scheduleComponentsEntity) {

        authorizeFlow();

        // ensure the same id is being used
        if (!id.equals(scheduleComponentsEntity.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                + "not equal the process group id of the requested resource (%s).", scheduleComponentsEntity.getId(), id));
        }

        final ScheduledState state;
        if (scheduleComponentsEntity.getState() == null) {
            throw new IllegalArgumentException("The scheduled state must be specified.");
        } else {
            try {
                state = ScheduledState.valueOf(scheduleComponentsEntity.getState());
            } catch (final IllegalArgumentException iae) {
                throw new IllegalArgumentException(String.format("The scheduled must be one of [%s].", StringUtils.join(EnumSet.of(ScheduledState.RUNNING, ScheduledState.STOPPED), ", ")));
            }
        }

        // ensure its a supported scheduled state
        if (ScheduledState.DISABLED.equals(state) || ScheduledState.STARTING.equals(state) || ScheduledState.STOPPING.equals(state)) {
            throw new IllegalArgumentException(String.format("The scheduled must be one of [%s].", StringUtils.join(EnumSet.of(ScheduledState.RUNNING, ScheduledState.STOPPED), ", ")));
        }

        // if the components are not specified, gather all components and their current revision
        if (scheduleComponentsEntity.getComponents() == null) {
            // get the current revisions for the components being updated
            final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(id, group -> {
                final Set<String> componentIds = new HashSet<>();

                // ensure authorized for each processor we will attempt to schedule
                group.findAllProcessors().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PROCESSORS : ProcessGroup.UNSCHEDULABLE_PROCESSORS)
                    .filter(processor -> processor.isAuthorized(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser()))
                    .forEach(processor -> {
                        componentIds.add(processor.getIdentifier());
                    });

                // ensure authorized for each input port we will attempt to schedule
                group.findAllInputPorts().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PORTS : ProcessGroup.UNSCHEDULABLE_PORTS)
                    .filter(inputPort -> inputPort.isAuthorized(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser()))
                    .forEach(inputPort -> {
                        componentIds.add(inputPort.getIdentifier());
                    });

                // ensure authorized for each output port we will attempt to schedule
                group.findAllOutputPorts().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PORTS : ProcessGroup.UNSCHEDULABLE_PORTS)
                    .filter(outputPort -> outputPort.isAuthorized(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser()))
                    .forEach(outputPort -> {
                        componentIds.add(outputPort.getIdentifier());
                    });

                return componentIds;
            });

            // build the component mapping
            final Map<String, RevisionDTO> componentsToSchedule = new HashMap<>();
            revisions.forEach(revision -> {
                final RevisionDTO dto = new RevisionDTO();
                dto.setClientId(revision.getClientId());
                dto.setVersion(revision.getVersion());
                componentsToSchedule.put(revision.getComponentId(), dto);
            });

            // set the components and their current revision
            scheduleComponentsEntity.setComponents(componentsToSchedule);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, scheduleComponentsEntity);
        }

        final Map<String, RevisionDTO> componentsToSchedule = scheduleComponentsEntity.getComponents();
        final Map<String, Revision> componentRevisions = componentsToSchedule.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getRevision(e.getValue(), e.getKey())));
        final Set<Revision> revisions = new HashSet<>(componentRevisions.values());

        return withWriteLock(
            serviceFacade,
            revisions,
            lookup -> {
                // ensure access to every component being scheduled
                componentsToSchedule.keySet().forEach(componentId -> {
                    final Authorizable connectable = lookup.getConnectable(componentId);
                    connectable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                });
            },
            () -> serviceFacade.verifyScheduleComponents(id, state, componentRevisions.keySet()),
            () -> {
                // update the process group
                final ScheduleComponentsEntity entity = serviceFacade.scheduleComponents(id, state, componentRevisions);
                return clusterContext(generateOkResponse(entity)).build();
            }
        );
    }

    // ------
    // search
    // ------

    /**
     * Performs a search request in this flow.
     *
     * @param value Search string
     * @return A searchResultsEntity
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-results")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Performs a search against this NiFi using the specified search term",
            response = SearchResultsEntity.class,
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
    public Response searchFlow(@QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) throws InterruptedException {
        authorizeFlow();

        // query the controller
        final SearchResultsDTO results = serviceFacade.searchController(value);

        // create the entity
        final SearchResultsEntity entity = new SearchResultsEntity();
        entity.setSearchResultsDTO(results);

        // generate the response
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    /**
     * Retrieves the status for this NiFi.
     *
     * @return A controllerStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the current status of this NiFi",
            response = ControllerStatusEntity.class,
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
    public Response getControllerStatus() throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerStatusDTO controllerStatus = serviceFacade.getControllerStatus();

        // create the response entity
        final ControllerStatusEntity entity = new ControllerStatusEntity();
        entity.setControllerStatus(controllerStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the status for this NiFi.
     *
     * @return A controllerStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/summary")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the current status of this NiFi",
            response = ControllerStatusEntity.class,
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
    public Response getClusterSummary() throws InterruptedException {

        authorizeFlow();

        final ClusterSummaryDTO clusterConfiguration = new ClusterSummaryDTO();
        final ClusterCoordinator clusterCoordinator = getClusterCoordinator();

        if (clusterCoordinator != null && clusterCoordinator.isConnected()) {
            final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = clusterCoordinator.getConnectionStates();
            int totalNodeCount = 0;
            for (final List<NodeIdentifier> nodeList : stateMap.values()) {
                totalNodeCount += nodeList.size();
            }
            final List<NodeIdentifier> connectedNodeIds = stateMap.get(NodeConnectionState.CONNECTED);
            final int connectedNodeCount = (connectedNodeIds == null) ? 0 : connectedNodeIds.size();

            clusterConfiguration.setConnectedNodeCount(connectedNodeCount);
            clusterConfiguration.setTotalNodeCount(totalNodeCount);
            clusterConfiguration.setConnectedNodes(connectedNodeCount + " / " + totalNodeCount);
        }

        clusterConfiguration.setClustered(isClustered());
        clusterConfiguration.setConnectedToCluster(isConnectedToCluster());

        // create the response entity
        final ClusteSummaryEntity entity = new ClusteSummaryEntity();
        entity.setClusterSummary(clusterConfiguration);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the controller level bulletins.
     *
     * @return A controllerBulletinsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller/bulletins")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves Controller level bulletins",
            response = ControllerBulletinsEntity.class,
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
    public Response getBulletins() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerBulletinsEntity entity = serviceFacade.getControllerBulletins();
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the banners for this NiFi.
     *
     * @return A bannerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("banners")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves the banners for this NiFi",
            response = BannerEntity.class,
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
    public Response getBanners() {

        authorizeFlow();

        // get the banner from the properties - will come from the NCM when clustered
        final String bannerText = getProperties().getBannerText();

        // create the DTO
        final BannerDTO bannerDTO = new BannerDTO();
        bannerDTO.setHeaderText(bannerText);
        bannerDTO.setFooterText(bannerText);

        // create the response entity
        final BannerEntity entity = new BannerEntity();
        entity.setBanners(bannerDTO);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of processors that this NiFi supports.
     *
     * @return A processorTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processor-types")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves the types of processors that this NiFi supports",
            response = ProcessorTypesEntity.class,
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
    public Response getProcessorTypes() throws InterruptedException {
        authorizeFlow();

        // create response entity
        final ProcessorTypesEntity entity = new ProcessorTypesEntity();
        entity.setProcessorTypes(serviceFacade.getProcessorTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of controller services that this NiFi supports.
     *
     * @param serviceType Returns only services that implement this type
     * @return A controllerServicesTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-service-types")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves the types of controller services that this NiFi supports",
            response = ControllerServiceTypesEntity.class,
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
    public Response getControllerServiceTypes(
            @ApiParam(
                    value = "If specified, will only return controller services of this type.",
                    required = false
            )
        @QueryParam("serviceType") String serviceType) throws InterruptedException {
        authorizeFlow();

        // create response entity
        final ControllerServiceTypesEntity entity = new ControllerServiceTypesEntity();
        entity.setControllerServiceTypes(serviceFacade.getControllerServiceTypes(serviceType));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of reporting tasks that this NiFi supports.
     *
     * @return A controllerServicesTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-task-types")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves the types of reporting tasks that this NiFi supports",
            response = ReportingTaskTypesEntity.class,
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
    public Response getReportingTaskTypes() throws InterruptedException {
        authorizeFlow();

        // create response entity
        final ReportingTaskTypesEntity entity = new ReportingTaskTypesEntity();
        entity.setReportingTaskTypes(serviceFacade.getReportingTaskTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the types of prioritizers that this NiFi supports.
     *
     * @return A prioritizerTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("prioritizers")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves the types of prioritizers that this NiFi supports",
            response = PrioritizerTypesEntity.class,
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
    public Response getPrioritizers() throws InterruptedException {
        authorizeFlow();

        // create response entity
        final PrioritizerTypesEntity entity = new PrioritizerTypesEntity();
        entity.setPrioritizerTypes(serviceFacade.getWorkQueuePrioritizerTypes());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves details about this NiFi to put in the About dialog.
     *
     * @return An aboutEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("about")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Retrieves details about this NiFi to put in the About dialog",
            response = AboutEntity.class,
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
    public Response getAboutInfo() {
        authorizeFlow();

        // create the about dto
        final AboutDTO aboutDTO = new AboutDTO();
        aboutDTO.setTitle("NiFi"); // TODO - where to load title from
        aboutDTO.setVersion(getProperties().getUiTitle());
        aboutDTO.setUri(generateResourceUri());

        // get the content viewer url
        aboutDTO.setContentViewerUrl(getProperties().getProperty(NiFiProperties.CONTENT_VIEWER_URL));

        // create the response entity
        final AboutEntity entity = new AboutEntity();
        entity.setAbout(aboutDTO);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // --------------
    // bulletin board
    // --------------

    /**
     * Retrieves all the of bulletins in this NiFi.
     *
     * @param after Supporting querying for bulletins after a particular
     *            bulletin id.
     * @param limit The max number of bulletins to return.
     * @param sourceName Source name filter. Supports a regular expression.
     * @param message Message filter. Supports a regular expression.
     * @param sourceId Source id filter. Supports a regular expression.
     * @param groupId Group id filter. Supports a regular expression.
     * @return A bulletinBoardEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("bulletin-board")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets current bulletins",
        response = BulletinBoardEntity.class,
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
    public Response getBulletinBoard(
        @ApiParam(
            value = "Includes bulletins with an id after this value.",
            required = false
        )
        @QueryParam("after") LongParameter after,
        @ApiParam(
            value = "Includes bulletins originating from this sources whose name match this regular expression.",
            required = false
        )
        @QueryParam("sourceName") BulletinBoardPatternParameter sourceName,
        @ApiParam(
            value = "Includes bulletins whose message that match this regular expression.",
            required = false
        )
        @QueryParam("message") BulletinBoardPatternParameter message,
        @ApiParam(
            value = "Includes bulletins originating from this sources whose id match this regular expression.",
            required = false
        )
        @QueryParam("sourceId") BulletinBoardPatternParameter sourceId,
        @ApiParam(
            value = "Includes bulletins originating from this sources whose group id match this regular expression.",
            required = false
        )
        @QueryParam("groupId") BulletinBoardPatternParameter groupId,
        @ApiParam(
            value = "The number of bulletins to limit the response to.",
            required = false
        )
        @QueryParam("limit") IntegerParameter limit) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // build the bulletin query
        final BulletinQueryDTO query = new BulletinQueryDTO();

        if (sourceId != null) {
            query.setSourceId(sourceId.getRawPattern());
        }
        if (groupId != null) {
            query.setGroupId(groupId.getRawPattern());
        }
        if (sourceName != null) {
            query.setName(sourceName.getRawPattern());
        }
        if (message != null) {
            query.setMessage(message.getRawPattern());
        }
        if (after != null) {
            query.setAfter(after.getLong());
        }
        if (limit != null) {
            query.setLimit(limit.getInteger());
        }

        // get the bulletin board
        final BulletinBoardDTO bulletinBoard = serviceFacade.getBulletinBoard(query);

        // create the response entity
        BulletinBoardEntity entity = new BulletinBoardEntity();
        entity.setBulletinBoard(bulletinBoard);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // ------
    // status
    // ------

    /**
     * Retrieves the specified processor status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A processorStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processors/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for a processor",
        response = ProcessorStatusEntity.class,
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
    public Response getProcessorStatus(
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The processor id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ProcessorStatusEntity entity = (ProcessorStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessorStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified processor status
        final ProcessorStatusDTO processorStatus = serviceFacade.getProcessorStatus(id);

        // generate the response entity
        final ProcessorStatusEntity entity = new ProcessorStatusEntity();
        entity.setProcessorStatus(processorStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified input port status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A portStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for an input port",
        response = PortStatusEntity.class,
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
    public Response getInputPortStatus(
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The input port id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified input port status
        final PortStatusDTO portStatus = serviceFacade.getInputPortStatus(id);

        // generate the response entity
        final PortStatusEntity entity = new PortStatusEntity();
        entity.setPortStatus(portStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified output port status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A portStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for an output port",
        response = PortStatusEntity.class,
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
    public Response getOutputPortStatus(
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The output port id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified output port status
        final PortStatusDTO portStatus = serviceFacade.getOutputPortStatus(id);

        // generate the response entity
        final PortStatusEntity entity = new PortStatusEntity();
        entity.setPortStatus(portStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified remote process group status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A remoteProcessGroupStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("remote-process-groups/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for a remote process group",
        response = ProcessorStatusEntity.class,
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
    public Response getRemoteProcessGroupStatus(
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The remote process group id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final RemoteProcessGroupStatusEntity entity = (RemoteProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getRemoteProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified remote process group status
        final RemoteProcessGroupStatusDTO remoteProcessGroupStatus = serviceFacade.getRemoteProcessGroupStatus(id);

        // generate the response entity
        final RemoteProcessGroupStatusEntity entity = new RemoteProcessGroupStatusEntity();
        entity.setRemoteProcessGroupStatus(remoteProcessGroupStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the status report for this NiFi.
     *
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendant groups and the status of their content will be included.
     * @param groupId The group id
     * @return A processGroupStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @ApiOperation(
        value = "Gets the status for a process group",
        notes = "The status for a process group includes status for all descendent components. When invoked on the root group with "
            + "recursive set to true, it will return the current status of every component in the flow.",
        response = ProcessGroupStatusEntity.class,
        authorizations = {
            @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
            @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
            @Authorization(value = "Administrator", type = "ROLE_ADMIN"),
            @Authorization(value = "NiFi", type = "ROLE_NIFI")
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
    public Response getProcessGroupStatus(
        @ApiParam(
            value = "Whether all descendant groups and the status of their content will be included. Optional, defaults to false",
            required = false
        )
        @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive,
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

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

        // get the status
        final ProcessGroupStatusDTO statusReport = serviceFacade.getProcessGroupStatus(groupId);

        // prune the response as necessary
        if (!recursive) {
            pruneChildGroups(statusReport.getAggregateSnapshot());
            if (statusReport.getNodeSnapshots() != null) {
                for (final NodeProcessGroupStatusSnapshotDTO nodeSnapshot : statusReport.getNodeSnapshots()) {
                    pruneChildGroups(nodeSnapshot.getStatusSnapshot());
                }
            }
        }

        // create the response entity
        final ProcessGroupStatusEntity entity = new ProcessGroupStatusEntity();
        entity.setProcessGroupStatus(statusReport);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    private void pruneChildGroups(final ProcessGroupStatusSnapshotDTO snapshot) {
        for (final ProcessGroupStatusSnapshotDTO childProcessGroupStatus : snapshot.getProcessGroupStatusSnapshots()) {
            childProcessGroupStatus.setConnectionStatusSnapshots(null);
            childProcessGroupStatus.setProcessGroupStatusSnapshots(null);
            childProcessGroupStatus.setInputPortStatusSnapshots(null);
            childProcessGroupStatus.setOutputPortStatusSnapshots(null);
            childProcessGroupStatus.setProcessorStatusSnapshots(null);
            childProcessGroupStatus.setRemoteProcessGroupStatusSnapshots(null);
        }
    }

    /**
     * Retrieves the specified connection status.
     *
     * @param id The id of the connection history to retrieve.
     * @return A connectionStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("connections/{id}/status")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status for a connection",
        response = ConnectionStatusEntity.class,
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
    public Response getConnectionStatus(
        @ApiParam(
            value = "Whether or not to include the breakdown per node. Optional, defaults to false",
            required = false
        )
        @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
        @ApiParam(
            value = "The id of the node where to get the status.",
            required = false
        )
        @QueryParam("clusterNodeId") String clusterNodeId,
        @ApiParam(
            value = "The connection id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ConnectionStatusEntity entity = (ConnectionStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getConnectionStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified connection status
        final ConnectionStatusDTO connectionStatus = serviceFacade.getConnectionStatus(id);

        // generate the response entity
        final ConnectionStatusEntity entity = new ConnectionStatusEntity();
        entity.setConnectionStatus(connectionStatus);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // --------------
    // status history
    // --------------

    /**
     * Retrieves the specified processor status history.
     *
     * @param id The id of the processor history to retrieve.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processors/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status history for a processor",
        response = StatusHistoryEntity.class,
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
    public Response getProcessorStatusHistory(
        @ApiParam(
            value = "The processor id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryDTO processorStatusHistory = serviceFacade.getProcessorStatusHistory(id);

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setStatusHistory(processorStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param groupId The group id
     * @return A processorEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets status history for a remote process group",
        response = StatusHistoryEntity.class,
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
    public Response getProcessGroupStatusHistory(
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryDTO processGroupStatusHistory = serviceFacade.getProcessGroupStatusHistory(groupId);

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setStatusHistory(processGroupStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param id The id of the remote process group to retrieve the status fow.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("remote-process-groups/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets the status history",
        response = StatusHistoryEntity.class,
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
    public Response getRemoteProcessGroupStatusHistory(
        @ApiParam(
            value = "The remote process group id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryDTO remoteProcessGroupStatusHistory = serviceFacade.getRemoteProcessGroupStatusHistory(id);

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setStatusHistory(remoteProcessGroupStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified connection status history.
     *
     * @param id The id of the connection to retrieve.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("connections/{id}/status/history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
        value = "Gets the status history for a connection",
        response = StatusHistoryEntity.class,
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
    public Response getConnectionStatusHistory(
        @ApiParam(
            value = "The connection id.",
            required = true
        )
        @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryDTO connectionStatusHistory = serviceFacade.getConnectionStatusHistory(id);

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setStatusHistory(connectionStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // -------
    // history
    // -------

    /**
     * Queries the history of this Controller.
     *
     * @param offset The offset into the data. This parameter is required and is
     * used in conjunction with count.
     * @param count The number of rows that should be returned. This parameter
     * is required and is used in conjunction with page.
     * @param sortColumn The column to sort on. This parameter is optional. If
     * not specified the results will be returned with the most recent first.
     * @param sortOrder The sort order.
     * @param startDate The start date/time for the query. The start date/time
     * must be formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional
     * and must be specified in the timezone of the server. The server's
     * timezone can be determined by inspecting the result of a status or
     * history request.
     * @param endDate The end date/time for the query. The end date/time must be
     * formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional and must
     * be specified in the timezone of the server. The server's timezone can be
     * determined by inspecting the result of a status or history request.
     * @param userIdentity The user name of the user who's actions are being
     * queried. This parameter is optional.
     * @param sourceId The id of the source being queried (usually a processor
     * id). This parameter is optional.
     * @return A historyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history",
            response = HistoryEntity.class,
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
    public Response queryHistory(
            @ApiParam(
                    value = "The offset into the result set.",
                    required = true
            )
            @QueryParam("offset") IntegerParameter offset,
            @ApiParam(
                    value = "The number of actions to return.",
                    required = true
            )
            @QueryParam("count") IntegerParameter count,
            @ApiParam(
                    value = "The field to sort on.",
                    required = false
            )
            @QueryParam("sortColumn") String sortColumn,
            @ApiParam(
                    value = "The direction to sort.",
                    required = false
            )
            @QueryParam("sortOrder") String sortOrder,
            @ApiParam(
                    value = "Include actions after this date.",
                    required = false
            )
            @QueryParam("startDate") DateTimeParameter startDate,
            @ApiParam(
                    value = "Include actions before this date.",
                    required = false
            )
            @QueryParam("endDate") DateTimeParameter endDate,
            @ApiParam(
                    value = "Include actions performed by this user.",
                    required = false
            )
            @QueryParam("userIdentity") String userIdentity,
            @ApiParam(
                    value = "Include actions on this component.",
                    required = false
            )
            @QueryParam("sourceId") String sourceId) {

        authorizeFlow();

        // ensure the page is specified
        if (offset == null) {
            throw new IllegalArgumentException("The desired offset must be specified.");
        } else if (offset.getInteger() < 0) {
            throw new IllegalArgumentException("The desired offset must be an integer value greater than or equal to 0.");
        }

        // ensure the row count is specified
        if (count == null) {
            throw new IllegalArgumentException("The desired row count must be specified.");
        } else if (count.getInteger() < 1) {
            throw new IllegalArgumentException("The desired row count must be an integer value greater than 0.");
        }

        // normalize the sort order
        if (sortOrder != null) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new IllegalArgumentException("The sort order must be 'asc' or 'desc'.");
            }
        }

        // ensure the start and end dates are specified
        if (endDate != null && startDate != null) {
            if (endDate.getDateTime().before(startDate.getDateTime())) {
                throw new IllegalArgumentException("The start date/time must come before the end date/time.");
            }
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // create a history query
        final HistoryQueryDTO query = new HistoryQueryDTO();
        query.setSortColumn(sortColumn);
        query.setSortOrder(sortOrder);
        query.setOffset(offset.getInteger());
        query.setCount(count.getInteger());

        // optionally set the start date
        if (startDate != null) {
            query.setStartDate(startDate.getDateTime());
        }

        // optionally set the end date
        if (endDate != null) {
            query.setEndDate(endDate.getDateTime());
        }

        // optionally set the user id
        if (userIdentity != null) {
            query.setUserIdentity(userIdentity);
        }

        // optionally set the processor id
        if (sourceId != null) {
            query.setSourceId(sourceId);
        }

        // perform the query
        final HistoryDTO history = serviceFacade.getActions(query);

        // create the response entity
        final HistoryEntity entity = new HistoryEntity();
        entity.setHistory(history);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the action for the corresponding id.
     *
     * @param id The id of the action to get.
     * @return An actionEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @Path("history/{id}")
    @ApiOperation(
            value = "Gets an action",
            response = ActionEntity.class,
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
    public Response getAction(
            @ApiParam(
                    value = "The action id.",
                    required = true
            )
            @PathParam("id") IntegerParameter id) {

        authorizeFlow();

        // ensure the id was specified
        if (id == null) {
            throw new IllegalArgumentException("The action id must be specified.");
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // get the specified action
        final ActionDTO action = serviceFacade.getAction(id.getInteger());

        // create the response entity
        final ActionEntity entity = new ActionEntity();
        entity.setAction(action);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the actions for the specified component.
     *
     * @param componentId The id of the component.
     * @return An processorHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history/components/{componentId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history for a processor",
            response = ComponentHistoryEntity.class,
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
    public Response getComponentHistory(
            @ApiParam(
                    value = "The component id.",
                    required = true
            )
            @PathParam("componentId") final String componentId) {

        authorizeFlow();

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // create the response entity
        final ComponentHistoryEntity entity = new ComponentHistoryEntity();
        entity.setComponentHistory(serviceFacade.getComponentHistory(componentId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ---------
    // templates
    // ---------

    /**
     * Retrieves all the of templates in this NiFi.
     *
     * @return A templatesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("templates")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all templates",
            response = TemplatesEntity.class,
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
    public Response getTemplates() {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        authorizeFlow();

        // get all the templates
        final Set<TemplateEntity> templates = serviceFacade.getTemplates();
        templateResource.populateRemainingTemplateEntitiesContent(templates);

        // create the response entity
        final TemplatesEntity entity = new TemplatesEntity();
        entity.setTemplates(templates);
        entity.setGenerated(new Date());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // --------------------
    // search cluster nodes
    // --------------------

    /**
     * Searches the cluster for a node with a given address.
     *
     * @param value Search value that will be matched against a node's address
     * @return Nodes that match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/search-results")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Searches the cluster for a node with the specified address",
            response = ClusterSearchResultsEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "DFM", type = "ROLE_DFM"),
                    @Authorization(value = "Admin", type = "ROLE_ADMIN")
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
    public Response searchCluster(
            @ApiParam(
                    value = "Node address to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        authorizeFlow();

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        final List<NodeSearchResultDTO> nodeMatches = new ArrayList<>();

        // get the nodes in the cluster
        final ClusterDTO cluster = serviceFacade.getCluster();

        // check each to see if it matches the search term
        for (NodeDTO node : cluster.getNodes()) {
            // ensure the node is connected
            if (!NodeConnectionState.CONNECTED.name().equals(node.getStatus())) {
                continue;
            }

            // determine the current nodes address
            final String address = node.getAddress() + ":" + node.getApiPort();

            // count the node if there is no search or it matches the address
            if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(address, value)) {
                final NodeSearchResultDTO nodeMatch = new NodeSearchResultDTO();
                nodeMatch.setId(node.getNodeId());
                nodeMatch.setAddress(address);
                nodeMatches.add(nodeMatch);
            }
        }

        // build the response
        ClusterSearchResultsEntity results = new ClusterSearchResultsEntity();
        results.setNodeResults(nodeMatches);

        // generate an 200 - OK response
        return noCache(Response.ok(results)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setProcessorResource(ProcessorResource processorResource) {
        this.processorResource = processorResource;
    }

    public void setInputPortResource(InputPortResource inputPortResource) {
        this.inputPortResource = inputPortResource;
    }

    public void setOutputPortResource(OutputPortResource outputPortResource) {
        this.outputPortResource = outputPortResource;
    }

    public void setFunnelResource(FunnelResource funnelResource) {
        this.funnelResource = funnelResource;
    }

    public void setLabelResource(LabelResource labelResource) {
        this.labelResource = labelResource;
    }

    public void setRemoteProcessGroupResource(RemoteProcessGroupResource remoteProcessGroupResource) {
        this.remoteProcessGroupResource = remoteProcessGroupResource;
    }

    public void setConnectionResource(ConnectionResource connectionResource) {
        this.connectionResource = connectionResource;
    }

    public void setTemplateResource(TemplateResource templateResource) {
        this.templateResource = templateResource;
    }

    public void setProcessGroupResource(ProcessGroupResource processGroupResource) {
        this.processGroupResource = processGroupResource;
    }

    public void setControllerServiceResource(ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    public void setReportingTaskResource(ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
