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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
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

import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ConfigurationSnapshot;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DoubleParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Processor.
 */
@Api(hidden = true)
public class ProcessorResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorResource.class);

    private static final List<Long> POSSIBLE_RUN_DURATIONS = Arrays.asList(0L, 25L, 50L, 100L, 250L, 500L, 1000L, 2000L);

    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;
    private String groupId;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified processors and their relationships.
     *
     * @param processors processors
     * @return dtos
     */
    public Set<ProcessorDTO> populateRemainingProcessorsContent(Set<ProcessorDTO> processors) {
        for (ProcessorDTO processor : processors) {
            populateRemainingProcessorContent(processor);
        }
        return processors;
    }

    /**
     * Populate the uri's for the specified processor and its relationships.
     */
    private ProcessorDTO populateRemainingProcessorContent(ProcessorDTO processor) {
        // populate the remaining properties
        processor.setUri(generateResourceUri("controller", "process-groups", processor.getParentGroupId(), "processors", processor.getId()));

        // get the config details and see if there is a custom ui for this processor type
        ProcessorConfigDTO config = processor.getConfig();
        if (config != null) {
            // consider legacy custom ui fist
            String customUiUrl = servletContext.getInitParameter(processor.getType());
            if (StringUtils.isNotBlank(customUiUrl)) {
                config.setCustomUiUrl(customUiUrl);
            } else {
                // see if this processor has any ui extensions
                final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
                if (uiExtensionMapping.hasUiExtension(processor.getType())) {
                    final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(processor.getType());
                    for (final UiExtension uiExtension : uiExtensions) {
                        if (UiExtensionType.ProcessorConfiguration.equals(uiExtension.getExtensionType())) {
                            config.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                        }
                    }
                }
            }
        }

        return processor;
    }

    /**
     * Retrieves all the processors in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A processorsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets all processors",
            response = ProcessorsEntity.class,
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
    public Response getProcessors(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the processors
        final Set<ProcessorDTO> processorDTOs = serviceFacade.getProcessors(groupId);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessorsEntity entity = new ProcessorsEntity();
        entity.setRevision(revision);
        entity.setProcessors(populateRemainingProcessorsContent(processorDTOs));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Creates a new processor.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param name The name of the new processor.
     * @param type The type of the new processor. This type should refer to one of the types in the GET /controller/processor-types response.
     * @param x The x coordinate for this funnels position.
     * @param y The y coordinate for this funnels position.
     * @return A processorEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response createProcessor(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @FormParam("name") String name, @FormParam("type") String type,
            @FormParam("x") DoubleParameter x, @FormParam("y") DoubleParameter y) {

        // ensure the position has been specified
        if (x == null || y == null) {
            throw new IllegalArgumentException("The position (x, y) must be specified");
        }

        // create the processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setName(name);
        processorDTO.setType(type);
        processorDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the entity dto
        final ProcessorEntity processorEntity = new ProcessorEntity();
        processorEntity.setRevision(revision);
        processorEntity.setProcessor(processorDTO);

        // create the processor
        return createProcessor(httpServletRequest, processorEntity);
    }

    /**
     * Creates a new processor.
     *
     * @param httpServletRequest request
     * @param processorEntity A processorEntity.
     * @return A processorEntity.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Creates a new processor",
            response = ProcessorEntity.class,
            authorizations = {
                @Authorization(value = "ROLE_DFM", type = "ROLE_DFM")
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
    public Response createProcessor(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The processor configuration details.",
                    required = true
            )
            ProcessorEntity processorEntity) {

        if (processorEntity == null || processorEntity.getProcessor() == null) {
            throw new IllegalArgumentException("Processor details must be specified.");
        }

        if (processorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (processorEntity.getProcessor().getId() != null) {
            throw new IllegalArgumentException("Processor ID cannot be specified.");
        }

        if (StringUtils.isBlank(processorEntity.getProcessor().getType())) {
            throw new IllegalArgumentException("The type of processor to create must be specified.");
        }

        // if cluster manager, convert POST to PUT (to maintain same ID across nodes) and replicate
        if (properties.isClusterManager()) {

            // create ID for resource
            final String id = UUID.randomUUID().toString();

            // set ID for resource
            processorEntity.getProcessor().setId(id);

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
            return (Response) clusterManager.applyRequest(HttpMethod.PUT, putUri, updateClientId(processorEntity), getHeaders(headersToOverride)).getResponse();

        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // create the new processor
        final RevisionDTO revision = processorEntity.getRevision();
        final ConfigurationSnapshot<ProcessorDTO> controllerResponse = serviceFacade.createProcessor(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, processorEntity.getProcessor());
        final ProcessorDTO processor = controllerResponse.getConfiguration();
        populateRemainingProcessorContent(processor);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // generate the response entity
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessor(processor);

        // generate a 201 created response
        String uri = processor.getUri();
        return clusterContext(generateCreatedResponse(URI.create(uri), entity)).build();
    }

    /**
     * Retrieves the specified processor.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor to retrieve.
     * @return A processorEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets a processor",
            response = ProcessorEntity.class,
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
    public Response getProcessor(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The processor id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the specified processor
        final ProcessorDTO processor = serviceFacade.getProcessor(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setProcessor(populateRemainingProcessorContent(processor));

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Retrieves the specified processor status history.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor history to retrieve.
     * @return A statusHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}/status/history")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
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
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The processor id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            throw new IllegalClusterResourceRequestException("This request is only supported in standalone mode.");
        }

        // get the specified processor status history
        final StatusHistoryDTO processorStatusHistory = serviceFacade.getProcessorStatusHistory(groupId, id);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // generate the response entity
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setRevision(revision);
        entity.setStatusHistory(processorStatusHistory);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}/descriptors")
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets the descriptor for a processor property",
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
                    value = "The processor id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") String propertyName) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getProcessorPropertyDescriptor(groupId, id, propertyName);

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
     * Updates the specified processor with the specified values.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor to update.
     * @param x The x coordinate for this processors position.
     * @param y The y coordinate for this processors position.
     * @param name The name of the processor.
     * @param concurrentlySchedulableTaskCount The number of concurrentlySchedulableTasks
     * @param schedulingPeriod The scheduling period
     * @param schedulingStrategy The scheduling strategy
     * @param penaltyDuration The penalty duration
     * @param yieldDuration The yield duration
     * @param runDurationMillis The run duration in milliseconds
     * @param bulletinLevel The bulletin level
     * @param comments Any comments about this processor.
     * @param markedForDeletion Array of property names whose value should be removed.
     * @param state The processors state.
     * @param formParams Additionally, the processor properties and styles are specified in the form parameters. Because the property names and styles differ from processor to processor they are
     * specified in a map-like fashion:
     * <br>
     * <ul>
     * <li>properties[required.file.path]=/path/to/file</li>
     * <li>properties[required.hostname]=localhost</li>
     * <li>properties[required.port]=80</li>
     * <li>properties[optional.file.path]=/path/to/file</li>
     * <li>properties[optional.hostname]=localhost</li>
     * <li>properties[optional.port]=80</li>
     * <li>properties[user.defined.pattern]=^.*?s.*$</li>
     * <li>style[background-color]=#aaaaaa</li>
     * </ul>
     *
     * @return A processorEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    public Response updateProcessor(
            @Context HttpServletRequest httpServletRequest,
            @FormParam(VERSION) LongParameter version,
            @FormParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") String id,
            @FormParam("x") DoubleParameter x,
            @FormParam("y") DoubleParameter y,
            @FormParam("name") String name,
            @FormParam("concurrentlySchedulableTaskCount") IntegerParameter concurrentlySchedulableTaskCount,
            @FormParam("schedulingPeriod") String schedulingPeriod,
            @FormParam("penaltyDuration") String penaltyDuration,
            @FormParam("yieldDuration") String yieldDuration,
            @FormParam("runDurationMillis") LongParameter runDurationMillis,
            @FormParam("bulletinLevel") String bulletinLevel,
            @FormParam("schedulingStrategy") String schedulingStrategy,
            @FormParam("comments") String comments,
            @FormParam("markedForDeletion[]") List<String> markedForDeletion,
            @FormParam("state") String state,
            MultivaluedMap<String, String> formParams) {

        // create collections for holding the processor settings/properties
        final Map<String, String> processorProperties = new LinkedHashMap<>();
        final Map<String, String> processorStyle = new LinkedHashMap<>();

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
                        processorProperties.put(propertyName, formParams.getFirst(parameterName));
                    }
                } else if (parameterName.startsWith("style")) {
                    final int startIndex = StringUtils.indexOf(parameterName, "[");
                    final int endIndex = StringUtils.lastIndexOf(parameterName, "]");
                    if (startIndex != -1 && endIndex != -1) {
                        final String styleName = StringUtils.substring(parameterName, startIndex + 1, endIndex);
                        processorStyle.put(styleName, formParams.getFirst(parameterName));
                    }
                }
            }
        }

        // set the properties to remove
        for (String propertyToDelete : markedForDeletion) {
            processorProperties.put(propertyToDelete, null);
        }

        // create the processor config dto
        final ProcessorConfigDTO configDTO = new ProcessorConfigDTO();
        configDTO.setSchedulingPeriod(schedulingPeriod);
        configDTO.setPenaltyDuration(penaltyDuration);
        configDTO.setYieldDuration(yieldDuration);
        configDTO.setBulletinLevel(bulletinLevel);
        configDTO.setComments(comments);

        // if the run duration is specified
        if (runDurationMillis != null) {
            // ensure the value is supported
            if (!POSSIBLE_RUN_DURATIONS.contains(runDurationMillis.getLong())) {
                throw new IllegalArgumentException("The run duration must be one of: " + StringUtils.join(POSSIBLE_RUN_DURATIONS, ", ") + " millis.");
            }
            configDTO.setRunDurationMillis(runDurationMillis.getLong());
        }

        if (concurrentlySchedulableTaskCount != null) {
            configDTO.setConcurrentlySchedulableTaskCount(concurrentlySchedulableTaskCount.getInteger());
        }

        // only set the properties when appropriate
        if (!processorProperties.isEmpty()) {
            configDTO.setProperties(processorProperties);
        }

        // create the processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(id);
        processorDTO.setName(name);
        processorDTO.setState(state);
        processorDTO.setConfig(configDTO);

        // only set the styles when appropriate
        if (!processorStyle.isEmpty()) {
            processorDTO.setStyle(processorStyle);
        }

        // require both coordinates to be specified
        if (x != null && y != null) {
            processorDTO.setPosition(new PositionDTO(x.getDouble(), y.getDouble()));
        }

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        if (version != null) {
            revision.setVersion(version.getLong());
        }

        // create the entity dto
        final ProcessorEntity dtoEntity = new ProcessorEntity();
        dtoEntity.setRevision(revision);
        dtoEntity.setProcessor(processorDTO);

        // update the processor
        return updateProcessor(httpServletRequest, id, dtoEntity);
    }

    /**
     * Updates the specified processor with the specified values.
     *
     * @param httpServletRequest request
     * @param id The id of the processor to update.
     * @param processorEntity A processorEntity.
     * @return A processorEntity.
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Updates a processor",
            response = ProcessorEntity.class,
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
    public Response updateProcessor(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The processor id.",
                    required = true
            )
            @PathParam("id") String id,
            @ApiParam(
                    value = "The processor configuration details.",
                    required = true
            )
            ProcessorEntity processorEntity) {

        if (processorEntity == null || processorEntity.getProcessor() == null) {
            throw new IllegalArgumentException("Processor details must be specified.");
        }

        if (processorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the same id is being used
        final ProcessorDTO requestProcessorDTO = processorEntity.getProcessor();
        if (!id.equals(requestProcessorDTO.getId())) {
            throw new IllegalArgumentException(String.format("The processor id (%s) in the request body does "
                    + "not equal the processor id of the requested resource (%s).", requestProcessorDTO.getId(), id));
        }

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            // the run on primary mode cannot change when there is a disconnected primary
            final ProcessorConfigDTO config = requestProcessorDTO.getConfig();
            if (config != null && SchedulingStrategy.PRIMARY_NODE_ONLY.name().equals(config.getSchedulingStrategy())) {
                Node primaryNode = clusterManager.getPrimaryNode();
                if (primaryNode != null && primaryNode.getStatus() != Node.Status.CONNECTED) {
                    throw new IllegalClusterStateException("Unable to update processor because primary node is not connected.");
                }
            }

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // replicate the request
            return clusterManager.applyRequest(HttpMethod.PUT, getAbsolutePath(), updateClientId(processorEntity), getHeaders(headersToOverride)).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyUpdateProcessor(groupId, requestProcessorDTO);
            return generateContinueResponse().build();
        }

        // update the processor
        final RevisionDTO revision = processorEntity.getRevision();
        final ConfigurationSnapshot<ProcessorDTO> controllerResponse = serviceFacade.updateProcessor(
                new Revision(revision.getVersion(), revision.getClientId()), groupId, requestProcessorDTO);

        // get the processor dto
        final ProcessorDTO responseProcessorDTO = controllerResponse.getConfiguration();
        populateRemainingProcessorContent(responseProcessorDTO);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(revision.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // generate the response entity
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(updatedRevision);
        entity.setProcessor(responseProcessorDTO);

        return clusterContext(generateOkResponse(entity)).build();
    }

    /**
     * Removes the specified processor.
     *
     * @param httpServletRequest request
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor to remove.
     * @return A processorEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{id}")
    @PreAuthorize("hasRole('ROLE_DFM')")
    @ApiOperation(
            value = "Deletes a processor",
            response = ProcessorEntity.class,
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
    public Response deleteProcessor(
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
                    value = "The processor id.",
                    required = true
            )
            @PathParam("id") String id) {

        // replicate if cluster manager
        if (properties.isClusterManager()) {
            return clusterManager.applyRequest(HttpMethod.DELETE, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(WebClusterManager.NCM_EXPECTS_HTTP_HEADER);
        if (expects != null) {
            serviceFacade.verifyDeleteProcessor(groupId, id);
            return generateContinueResponse().build();
        }

        // determine the specified version
        Long clientVersion = null;
        if (version != null) {
            clientVersion = version.getLong();
        }

        // delete the processor
        final ConfigurationSnapshot<Void> controllerResponse = serviceFacade.deleteProcessor(new Revision(clientVersion, clientId.getClientId()), groupId, id);

        // get the updated revision
        final RevisionDTO updatedRevision = new RevisionDTO();
        updatedRevision.setClientId(clientId.getClientId());
        updatedRevision.setVersion(controllerResponse.getVersion());

        // generate the response entity
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(updatedRevision);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
