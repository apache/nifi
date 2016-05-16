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
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BannerDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
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
import org.apache.nifi.web.api.entity.ReportingTaskTypesEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.request.BulletinBoardPatternParameter;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
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

    /**
     * Retrieves the contents of the specified group.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendent groups and their content will be included if the verbose flag is also set to true.
     * @param groupId The id of the process group.
     * @return A processGroupEntity.
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
        @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get this process group contents
        final ConfigurationSnapshot<ProcessGroupFlowDTO> controllerResponse = serviceFacade.getProcessGroupFlow(groupId, recursive);
        final ProcessGroupFlowDTO flow = controllerResponse.getConfiguration();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());
        revision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupFlowEntity processGroupEntity = new ProcessGroupFlowEntity();
        processGroupEntity.setProcessGroupFlow(populateRemainingFlowContent(flow));

        return clusterContext(generateOkResponse(processGroupEntity)).build();
    }

    /**
     * Retrieves all the of controller services in this NiFi.
     *
     * @return A controllerServicesEntity.
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
    public Response getControllerServices(
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") String groupId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the controller services
        final Set<ControllerServiceEntity> controllerServices = serviceFacade.getControllerServices(groupId);
        controllerServiceResource.populateRemainingControllerServiceEntitiesContent(controllerServices);

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setControllerServices(controllerServices);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

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

        final String clientId;
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            clientId = UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            clientId = UUID.randomUUID().toString();
        }

        return clusterContext(generateOkResponse(clientId)).build();
    }

    // ------
    // search
    // ------

    /**
     * Performs a search request in this flow.
     *
     * @param value Search string
     * @return A searchResultsEntity
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
    public Response searchFlow(@QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {
        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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
    public Response getControllerStatus() {

        authorizeFlow();

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final ControllerStatusDTO controllerStatus = serviceFacade.getControllerStatus();

        // create the response entity
        final ControllerStatusEntity entity = new ControllerStatusEntity();
        entity.setControllerStatus(controllerStatus);

        // generate the response
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
    @Path("identity")
    @ApiOperation(
            value = "Retrieves the user identity of the user making the request",
            response = IdentityEntity.class
    )
    public Response getIdentity() {

        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the response entity
        IdentityEntity entity = new IdentityEntity();
        entity.setUserId(user.getIdentity());
        entity.setIdentity(user.getUserName());

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
        final String bannerText = properties.getBannerText();

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
    public Response getProcessorTypes() {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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
            @QueryParam("serviceType") String serviceType) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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
    public Response getReportingTaskTypes() {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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
    public Response getPrioritizers() {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

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

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final ControllerConfigurationDTO controllerConfig = serviceFacade.getControllerConfiguration();

        // create the about dto
        final AboutDTO aboutDTO = new AboutDTO();
        aboutDTO.setTitle(controllerConfig.getName());
        aboutDTO.setVersion(properties.getUiTitle());
        aboutDTO.setUri(generateResourceUri());

        // get the content viewer url
        aboutDTO.setContentViewerUrl(properties.getProperty(NiFiProperties.CONTENT_VIEWER_URL));

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
     * bulletin id.
     * @param limit The max number of bulletins to return.
     * @param sourceName Source name filter. Supports a regular expression.
     * @param message Message filter. Supports a regular expression.
     * @param sourceId Source id filter. Supports a regular expression.
     * @param groupId Group id filter. Supports a regular expression.
     * @return A bulletinBoardEntity.
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
        @QueryParam("limit") IntegerParameter limit) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final ProcessorStatusEntity entity = (ProcessorStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessorStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final RemoteProcessGroupStatusEntity entity = (RemoteProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getRemoteProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String groupId) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final ProcessGroupStatusEntity entity = (ProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final ConnectionStatusEntity entity = (ConnectionStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getConnectionStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final Node targetNode = clusterManager.getNode(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                final Set<NodeIdentifier> targetNodes = new HashSet<>();
                targetNodes.add(targetNode.getNodeId());

                // replicate the request to the specific node
                return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders(), targetNodes).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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
        @PathParam("id") String groupId) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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
        @PathParam("id") String id) {

        authorizeFlow();

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
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

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
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

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
