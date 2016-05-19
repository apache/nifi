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
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BannerDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
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
import org.apache.nifi.web.api.entity.AuthorityEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.IdentityEntity;
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
import org.apache.nifi.web.api.request.BulletinBoardPatternParameter;
import org.apache.nifi.web.api.request.ClientIdParameter;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
            .resource(ResourceFactory.getFlowResource())
            .identity(user.getIdentity())
            .anonymous(user.isAnonymous())
            .accessAttempt(true)
            .action(RequestAction.READ)
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
     * Retrieves the identity of the user making the request.
     *
     * @return An identityEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("identity")
    @ApiOperation(
            value = "Retrieves the user identity of the user making the request",
            response = IdentityEntity.class
    )
    public Response getIdentity() {

        authorizeFlow();

        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the response entity
        IdentityEntity entity = new IdentityEntity();
        entity.setUserId(user.getIdentity());
        entity.setIdentity(user.getUserName());
        entity.setAnonymous(user.isAnonymous());

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**x
     * Retrieves the user details, including the authorities, about the user making the request.
     *
     * @return A authoritiesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("authorities")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_PROXY', 'ROLE_NIFI', 'ROLE_PROVENANCE')")
    @ApiOperation(
            value = "Retrieves the user details, including the authorities, about the user making the request",
            response = AuthorityEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN"),
                    @Authorization(value = "Proxy", type = "ROLE_PROXY"),
                    @Authorization(value = "NiFi", type = "ROLE_NIFI"),
                    @Authorization(value = "Provenance", type = "ROLE_PROVENANCE")
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
    public Response getAuthorities() {

        // TODO - Remove
        authorizeFlow();

        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the response entity
        AuthorityEntity entity = new AuthorityEntity();
        entity.setUserId(user.getIdentity());
        entity.setAuthorities(new HashSet<>(Arrays.asList("ROLE_MONITOR", "ROLE_DFM", "ROLE_ADMIN", "ROLE_PROXY", "ROLE_NIFI", "ROLE_PROVENANCE")));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendent groups and their content will be included if the verbose flag is also set to true.
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
            value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
            required = false
        )
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "The process group id.",
            required = false
        )
        @PathParam("id") String groupId,
        @ApiParam(
            value = "Whether the response should contain all encapsulated components or just the immediate children.",
            required = false
        )
        @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get this process group flow
        final ProcessGroupFlowEntity entity = serviceFacade.getProcessGroupFlow(groupId, recursive);
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
            // TODO - this will break while clustered until nodes are able to process/replicate requests
            // get the current revisions for the components being updated
            final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(id, group -> {
                final Set<String> componentIds = new HashSet<>();

                // ensure authorized for each processor we will attempt to schedule
                group.findAllProcessors().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PROCESSORS : ProcessGroup.UNSCHEDULABLE_PROCESSORS)
                    .filter(processor -> processor.isAuthorized(authorizer, RequestAction.WRITE))
                    .forEach(processor -> {
                        componentIds.add(processor.getIdentifier());
                    });

                // ensure authorized for each input port we will attempt to schedule
                group.findAllInputPorts().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PORTS : ProcessGroup.UNSCHEDULABLE_PORTS)
                    .filter(inputPort -> inputPort.isAuthorized(authorizer, RequestAction.WRITE))
                    .forEach(inputPort -> {
                        componentIds.add(inputPort.getIdentifier());
                    });

                // ensure authorized for each output port we will attempt to schedule
                group.findAllOutputPorts().stream()
                    .filter(ScheduledState.RUNNING.equals(state) ? ProcessGroup.SCHEDULABLE_PORTS : ProcessGroup.UNSCHEDULABLE_PORTS)
                    .filter(outputPort -> outputPort.isAuthorized(authorizer, RequestAction.WRITE))
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
                // ensure access to the flow
                authorizeFlow();

                // ensure access to every component being scheduled
                componentsToSchedule.keySet().forEach(componentId -> {
                    final Authorizable connectable = lookup.getConnectable(componentId);
                    connectable.authorize(authorizer, RequestAction.WRITE);
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
            response = Entity.class,
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
     * Retrieves all the of templates in this NiFi.
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
                final NodeResponse nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).awaitMergedResponse();
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
