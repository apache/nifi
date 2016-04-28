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
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AuthorityEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.CounterEntity;
import org.apache.nifi.web.api.entity.CountersEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.web.util.Availability;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.Response;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * RESTful endpoint for managing a Flow Controller.
 */
@Path("/controller")
@Api(
    value = "/controller",
    description = "Provides realtime command and control of this NiFi instance"
)
public class ControllerResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    private ReportingTaskResource reportingTaskResource;

    @Context
    private ResourceContext resourceContext;

    /**
     * Creates a new archive of this flow controller. Note, this is a POST operation that returns a URI that is not representative of the thing that was actually created. The archive that is created
     * cannot be referenced at a later time, therefore there is no corresponding URI. Instead the request URI is returned.
     *
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @param revisionEntity The revision is used to verify the client is working with the latest version of the flow.
     * @return A processGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("archive")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a new archive of this NiFi flow configuration",
            notes = "This POST operation returns a URI that is not representative of the thing "
            + "that was actually created. The archive that is created cannot be referenced "
            + "at a later time, therefore there is no corresponding URI. Instead the "
            + "request URI is returned.",
            response = ProcessGroupEntity.class,
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
    public Response createArchive(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                value = "The revision used to verify the client is working with the latest version of the flow.",
                required = true
            ) Entity revisionEntity) {

        // ensure the revision was specified
        if (revisionEntity == null || revisionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // don't need to convert from POST to PUT because no resource ID exists on the nodes
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the archive
        final RevisionDTO requestRevision = revisionEntity.getRevision();
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.createArchive(new Revision(requestRevision.getVersion(), requestRevision.getClientId()));

        // create the revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(requestRevision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ProcessGroupEntity controllerEntity = new ProcessGroupEntity();
        controllerEntity.setRevision(updatedRevision);

        // generate the response
        URI uri = URI.create(generateResourceUri("controller", "archive"));
        return clusterContext(generateCreatedResponse(uri, controllerEntity)).build();
    }

    /**
     * Retrieves the counters report for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A countersEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("counters")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the current counters for this NiFi",
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
    public Response getCounters(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                value = "Whether or not to include the breakdown per node. Optional, defaults to false",
                required = false
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @ApiParam(
                value = "The id of the node where to get the status.",
                required = false
            )
            @QueryParam("clusterNodeId") String clusterNodeId) {

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders());
                final CountersEntity entity = (CountersEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getCounters().setNodeSnapshots(null);
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

        final CountersDTO countersReport = serviceFacade.getCounters();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final CountersEntity entity = new CountersEntity();
        entity.setRevision(revision);
        entity.setCounters(countersReport);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Update the specified counter. This will reset the counter value to 0.
     *
     * @param httpServletRequest request
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the counter.
     * @return A counterEntity.
     */
    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("counters/{id}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates the specified counter. This will reset the counter value to 0",
            response = CounterEntity.class,
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
    public Response updateCounter(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // reset the specified counter
        final CounterDTO counter = serviceFacade.updateCounter(id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final CounterEntity entity = new CounterEntity();
        entity.setRevision(revision);
        entity.setCounter(counter);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the configuration for this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A controllerConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN', 'ROLE_NIFI')")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi",
            response = ControllerConfigurationEntity.class,
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
    public Response getControllerConfig(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        final ControllerConfigurationDTO controllerConfig = serviceFacade.getControllerConfiguration();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);
        entity.setConfig(controllerConfig);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param httpServletRequest request
     * @param configEntity A controllerConfigurationEntity.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Retrieves the configuration for this NiFi",
            response = ControllerConfigurationEntity.class,
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
    public Response updateControllerConfig(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller configuration.",
                    required = true
            ) ControllerConfigurationEntity configEntity) {

        if (configEntity == null || configEntity.getConfig() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (configEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(configEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        final RevisionDTO revision = configEntity.getRevision();
        final ConfigurationSnapshot<ControllerConfigurationDTO> controllerResponse
                = serviceFacade.updateControllerConfiguration(new Revision(revision.getVersion(), revision.getClientId()), configEntity.getConfig());
        final ControllerConfigurationDTO controllerConfig = controllerResponse.getConfiguration();

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // create the response entity
        final ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(updatedRevision);
        entity.setConfig(controllerConfig);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**x
     * Retrieves the user details, including the authorities, about the user making the request.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
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
    public Response getAuthorities(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // note that the cluster manager will handle this request directly
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        AuthorityEntity entity = new AuthorityEntity();
        entity.setRevision(revision);
        entity.setUserId(user.getIdentity());
        entity.setAuthorities(new HashSet<>(Arrays.asList("ROLE_MONITOR", "ROLE_DFM", "ROLE_ADMIN", "ROLE_PROXY", "ROLE_NIFI", "ROLE_PROVENANCE")));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // ---------------
    // reporting tasks
    // ---------------

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
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/{availability}")
    // TODO - @PreAuthorize("hasRole('ROLE_DFM')")
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

        final Availability avail = reportingTaskResource.parseAvailability(availability);

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

        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.POST, getAbsolutePath(), updateClientId(reportingTaskEntity), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // set the processor id as appropriate
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            reportingTaskEntity.getReportingTask().setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        } else {
            reportingTaskEntity.getReportingTask().setId(UUID.randomUUID().toString());
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
        entity.setReportingTask(reportingTaskResource.populateRemainingReportingTaskContent(availability, reportingTask));

        // build the response
        return clusterContext(generateCreatedResponse(URI.create(reportingTask.getUri()), entity)).build();
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/{availability}")
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
        @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
        @ApiParam(
            value = "Whether the reporting task is available on the NCM or nodes. If the NiFi is standalone the availability should be NODE.",
            allowableValues = "NCM, NODE",
            required = true
        )
        @PathParam("availability") String availability) {

        final Availability avail = reportingTaskResource.parseAvailability(availability);

        // replicate if cluster manager
        if (properties.isClusterManager() && Availability.NODE.equals(avail)) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get all the reporting tasks
        final Set<ReportingTaskDTO> reportingTasks = reportingTaskResource.populateRemainingReportingTasksContent(availability, serviceFacade.getReportingTasks());

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

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setReportingTaskResource(ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
