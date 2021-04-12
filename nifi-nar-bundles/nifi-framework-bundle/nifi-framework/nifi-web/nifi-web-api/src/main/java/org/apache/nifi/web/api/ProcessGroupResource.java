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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.AuthorizeParameterReference;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.ConnectionAuthorizable;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.SnippetAuthorizable;
import org.apache.nifi.authorization.TemplateContentsAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryUtils;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.variable.VariableRegistryUpdateRequest;
import org.apache.nifi.registry.variable.VariableRegistryUpdateStep;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessGroupReplaceRequestDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.CopySnippetRequestEntity;
import org.apache.nifi.web.api.entity.CreateTemplateRequestEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.FunnelsEntity;
import org.apache.nifi.web.api.entity.InputPortsEntity;
import org.apache.nifi.web.api.entity.InstantiateTemplateRequestEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;
import org.apache.nifi.web.api.entity.OutputPortsEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupUploadEntity;
import org.apache.nifi.web.api.entity.ProcessGroupsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VariableRegistryUpdateRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.apache.nifi.web.util.Pause;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * RESTful endpoint for managing a Group.
 */
@Path("/process-groups")
@Api(
        value = "/process-groups",
        description = "Endpoint for managing a Process Group."
)
public class ProcessGroupResource extends FlowUpdateResource<ProcessGroupImportEntity, ProcessGroupReplaceRequestEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupResource.class);

    private ProcessorResource processorResource;
    private InputPortResource inputPortResource;
    private OutputPortResource outputPortResource;
    private FunnelResource funnelResource;
    private LabelResource labelResource;
    private RemoteProcessGroupResource remoteProcessGroupResource;
    private ConnectionResource connectionResource;
    private TemplateResource templateResource;
    private ControllerServiceResource controllerServiceResource;

    private final ConcurrentMap<String, VariableRegistryUpdateRequest> varRegistryUpdateRequests = new ConcurrentHashMap<>();
    private static final int MAX_VARIABLE_REGISTRY_UPDATE_REQUESTS = 100;
    private static final long VARIABLE_REGISTRY_UPDATE_REQUEST_EXPIRATION = TimeUnit.MINUTES.toMillis(1L);
    private final ExecutorService variableRegistryThreadPool = new ThreadPoolExecutor(1, 50, 5L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(MAX_VARIABLE_REGISTRY_UPDATE_REQUESTS),
        new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName("Variable Registry Update Thread");
                thread.setDaemon(true);
                return thread;
            }
        });
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        MAPPER.setAnnotationIntrospector(new JaxbAnnotationIntrospector(MAPPER.getTypeFactory()));
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Populates the remaining fields in the specified process groups.
     *
     * @param processGroupEntities groups
     * @return group dto
     */
    public Set<ProcessGroupEntity> populateRemainingProcessGroupEntitiesContent(Set<ProcessGroupEntity> processGroupEntities) {
        for (ProcessGroupEntity processGroupEntity : processGroupEntities) {
            populateRemainingProcessGroupEntityContent(processGroupEntity);
        }
        return processGroupEntities;
    }

    /**
     * Populates the remaining fields in the specified process group.
     *
     * @param processGroupEntity group
     * @return group dto
     */
    public ProcessGroupEntity populateRemainingProcessGroupEntityContent(ProcessGroupEntity processGroupEntity) {
        processGroupEntity.setUri(generateResourceUri("process-groups", processGroupEntity.getId()));
        return processGroupEntity;
    }


    /**
     * Populates the remaining content of the specified snippet.
     */
    private FlowDTO populateRemainingSnippetContent(FlowDTO flow) {
        processorResource.populateRemainingProcessorEntitiesContent(flow.getProcessors());
        connectionResource.populateRemainingConnectionEntitiesContent(flow.getConnections());
        inputPortResource.populateRemainingInputPortEntitiesContent(flow.getInputPorts());
        outputPortResource.populateRemainingOutputPortEntitiesContent(flow.getOutputPorts());
        remoteProcessGroupResource.populateRemainingRemoteProcessGroupEntitiesContent(flow.getRemoteProcessGroups());
        funnelResource.populateRemainingFunnelEntitiesContent(flow.getFunnels());
        labelResource.populateRemainingLabelEntitiesContent(flow.getLabels());

        // go through each process group child and populate its uri
        if (flow.getProcessGroups() != null) {
            populateRemainingProcessGroupEntitiesContent(flow.getProcessGroups());
        }

        return flow;
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param groupId The id of the process group.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getProcessGroup(
            @ApiParam(
                    value = "The process group id.",
                    required = false
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get this process group contents
        final ProcessGroupEntity entity = serviceFacade.getProcessGroup(groupId);
        populateRemainingProcessGroupEntityContent(entity);

        if (entity.getComponent() != null) {
            entity.getComponent().setContents(null);
        }

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified group as a versioned flow snapshot for download.
     *
     * @param groupId The id of the process group
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/download")
    @ApiOperation(
        value = "Gets a process group for download",
        response = String.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}")
        }
    )
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response exportProcessGroup(@ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId) {
        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            // ensure access to process groups (nested), encapsulated controller services and referenced parameter contexts
            final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
            authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true,
                    false, true, false, true);
        });

        // get the versioned flow
        final VersionedFlowSnapshot currentVersionedFlowSnapshot = serviceFacade.getCurrentFlowSnapshotByGroupId(groupId);

        // determine the name of the attachment - possible issues with spaces in file names
        final VersionedProcessGroup currentVersionedProcessGroup = currentVersionedFlowSnapshot.getFlowContents();
        final String flowName = currentVersionedProcessGroup.getName();
        final String filename = flowName.replaceAll("\\s", "_") + ".json";

        return generateOkResponse(currentVersionedFlowSnapshot).header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filename)).build();
    }

    /**
     * Retrieves a list of local modifications to the Process Group since it was last synchronized with the Flow Registry
     *
     * @param groupId The id of the process group.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/local-modifications")
    @ApiOperation(
            value = "Gets a list of local modifications to the Process Group since it was last synchronized with the Flow Registry",
            response = FlowComparisonEntity.class,
            authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}"),
            @Authorization(value = "Read - /{component-type}/{uuid} - For all encapsulated components")
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
    public Response getLocalModifications(
            @ApiParam(
                    value = "The process group id.",
                    required = false
            )
            @PathParam("id") final String groupId) throws IOException, NiFiRegistryException {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
            authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, false, false, true, false, false);
        });

        final FlowComparisonEntity entity = serviceFacade.getLocalModifications(groupId);
        return generateOkResponse(entity).build();
    }


    /**
     * Retrieves the Variable Registry for the group with the given ID
     *
     * @param groupId the ID of the Process Group
     * @return the Variable Registry for the group
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/variable-registry")
    @ApiOperation(value = "Gets a process group's variable registry",
        response = VariableRegistryEntity.class,
        notes = NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getVariableRegistry(
        @ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId,
        @ApiParam(value = "Whether or not to include ancestor groups", required = false) @QueryParam("includeAncestorGroups") @DefaultValue("true") final boolean includeAncestorGroups) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get this process group's variable registry
        final VariableRegistryEntity entity = serviceFacade.getVariableRegistry(groupId, includeAncestorGroups);
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified process group.
     *
     * @param httpServletRequest request
     * @param id                 The id of the process group.
     * @param requestProcessGroupEntity A processGroupEntity.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates a process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response updateProcessGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The process group configuration details.",
                    required = true
            ) final ProcessGroupEntity requestProcessGroupEntity) {

        if (requestProcessGroupEntity == null || requestProcessGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("Process group details must be specified.");
        }

        if (requestProcessGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the same id is being used
        final ProcessGroupDTO requestProcessGroupDTO = requestProcessGroupEntity.getComponent();
        if (!id.equals(requestProcessGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestProcessGroupDTO.getId(), id));
        }

        final PositionDTO proposedPosition = requestProcessGroupDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestProcessGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestProcessGroupEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestProcessGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                requestProcessGroupEntity,
                requestRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    Authorizable authorizable = lookup.getProcessGroup(id).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.WRITE, user);

                    // Ensure that user has READ permission on current Parameter Context (if any) because user is un-binding.
                    final ParameterContextReferenceEntity referencedParamContext = requestProcessGroupDTO.getParameterContext();
                    if (referencedParamContext != null) {
                        // Lookup the current Parameter Context and determine whether or not the Parameter Context is changing
                        final String groupId = requestProcessGroupDTO.getId();
                        final ProcessGroupEntity currentGroupEntity = serviceFacade.getProcessGroup(groupId);
                        final ProcessGroupDTO groupDto = currentGroupEntity.getComponent();
                        final ParameterContextReferenceEntity currentParamContext = groupDto.getParameterContext();
                        final String currentParamContextId = currentParamContext == null ? null : currentParamContext.getId();
                        final boolean parameterContextChanging = !Objects.equals(referencedParamContext.getId(), currentParamContextId);

                        // If Parameter Context is changing...
                        if (parameterContextChanging) {
                            // In order to bind to a Parameter Context, the user must have the READ policy to that Parameter Context.
                            if (referencedParamContext.getId() != null) {
                                lookup.getParameterContext(referencedParamContext.getId()).authorize(authorizer, RequestAction.READ, user);
                            }

                            // If currently referencing a Parameter Context, we must authorize that the user has READ permissions on the Parameter Context in order to un-bind to it.
                            if (currentParamContextId != null) {
                                lookup.getParameterContext(currentParamContextId).authorize(authorizer, RequestAction.READ, user);
                            }

                            // Because the user will be changing the behavior of any component in this group that is currently referencing any Parameter, we must ensure that the user has
                            // both READ and WRITE policies for each of those components.
                            for (final AffectedComponentEntity affectedComponentEntity : serviceFacade.getProcessorsReferencingParameter(groupId)) {
                                final Authorizable processorAuthorizable = lookup.getProcessor(affectedComponentEntity.getId()).getAuthorizable();
                                processorAuthorizable.authorize(authorizer, RequestAction.READ, user);
                                processorAuthorizable.authorize(authorizer, RequestAction.WRITE, user);
                            }

                            for (final AffectedComponentEntity affectedComponentEntity : serviceFacade.getControllerServicesReferencingParameter(groupId)) {
                                final Authorizable serviceAuthorizable = lookup.getControllerService(affectedComponentEntity.getId()).getAuthorizable();
                                serviceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                                serviceAuthorizable.authorize(authorizer, RequestAction.WRITE, user);
                            }
                        }
                    }
                },
                () -> serviceFacade.verifyUpdateProcessGroup(requestProcessGroupDTO),
                (revision, processGroupEntity) -> {
                    // update the process group
                    final ProcessGroupEntity entity = serviceFacade.updateProcessGroup(revision, processGroupEntity.getComponent());
                    populateRemainingProcessGroupEntityContent(entity);

                    // prune response as necessary
                    if (entity.getComponent() != null) {
                        entity.getComponent().setContents(null);
                    }

                    return generateOkResponse(entity).build();
                }
        );
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{groupId}/variable-registry/update-requests/{updateId}")
    @ApiOperation(value = "Gets a process group's variable registry",
        response = VariableRegistryUpdateRequestEntity.class,
        notes = NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response getVariableRegistryUpdateRequest(
        @ApiParam(value = "The process group id.", required = true) @PathParam("groupId") final String groupId,
        @ApiParam(value = "The ID of the Variable Registry Update Request", required = true) @PathParam("updateId") final String updateId) {

        if (groupId == null || updateId == null) {
            throw new IllegalArgumentException("Group ID and Update ID must both be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, user);
        });

        final VariableRegistryUpdateRequest request = varRegistryUpdateRequests.get(updateId);
        if (request == null) {
            throw new ResourceNotFoundException("Could not find a Variable Registry Update Request with identifier " + updateId);
        }

        if (!groupId.equals(request.getProcessGroupId())) {
            throw new ResourceNotFoundException("Could not find a Variable Registry Update Request with identifier " + updateId + " for Process Group with identifier " + groupId);
        }

        if (!user.equals(request.getUser())) {
            throw new IllegalArgumentException("Only the user that submitted the update request can retrieve it.");
        }

        final VariableRegistryUpdateRequestEntity entity = new VariableRegistryUpdateRequestEntity();
        entity.setRequest(dtoFactory.createVariableRegistryUpdateRequestDto(request));
        entity.setProcessGroupRevision(request.getProcessGroupRevision());
        entity.getRequest().setUri(generateResourceUri("process-groups", groupId, "variable-registry", updateId));
        return generateOkResponse(entity).build();
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{groupId}/variable-registry/update-requests/{updateId}")
    @ApiOperation(value = "Deletes an update request for a process group's variable registry. If the request is not yet complete, it will automatically be cancelled.",
        response = VariableRegistryUpdateRequestEntity.class,
        notes = NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response deleteVariableRegistryUpdateRequest(
        @ApiParam(
                value = "The process group id.",
                required = true
        )
        @PathParam("groupId") final String groupId,
        @ApiParam(
                value = "The ID of the Variable Registry Update Request",
                required = true)
        @PathParam("updateId") final String updateId,
        @ApiParam(
                value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                required = false
        )
        @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged) {

        if (groupId == null || updateId == null) {
            throw new IllegalArgumentException("Group ID and Update ID must both be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, user);
            processGroup.authorize(authorizer, RequestAction.WRITE, user);
        });

        final VariableRegistryUpdateRequest request = varRegistryUpdateRequests.remove(updateId);
        if (request == null) {
            throw new ResourceNotFoundException("Could not find a Variable Registry Update Request with identifier " + updateId);
        }

        if (!groupId.equals(request.getProcessGroupId())) {
            throw new ResourceNotFoundException("Could not find a Variable Registry Update Request with identifier " + updateId + " for Process Group with identifier " + groupId);
        }

        if (!user.equals(request.getUser())) {
            throw new IllegalArgumentException("Only the user that submitted the update request can remove it.");
        }

        request.cancel();

        final VariableRegistryUpdateRequestEntity entity = new VariableRegistryUpdateRequestEntity();
        entity.setRequest(dtoFactory.createVariableRegistryUpdateRequestDto(request));
        entity.setProcessGroupRevision(request.getProcessGroupRevision());
        entity.getRequest().setUri(generateResourceUri("process-groups", groupId, "variable-registry", updateId));
        return generateOkResponse(entity).build();
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/variable-registry")
    @ApiOperation(value = "Updates the contents of a Process Group's variable Registry", response = VariableRegistryEntity.class, notes = NON_GUARANTEED_ENDPOINT, authorizations = {
        @Authorization(value = "Write - /process-groups/{uuid}")
    })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response updateVariableRegistry(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId,
        @ApiParam(value = "The variable registry configuration details.", required = true) final VariableRegistryEntity requestVariableRegistryEntity) {

        if (requestVariableRegistryEntity == null || requestVariableRegistryEntity.getVariableRegistry() == null) {
            throw new IllegalArgumentException("Variable Registry details must be specified.");
        }

        if (requestVariableRegistryEntity.getProcessGroupRevision() == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified.");
        }

        // ensure the same id is being used
        final VariableRegistryDTO requestRegistryDto = requestVariableRegistryEntity.getVariableRegistry();
        if (!groupId.equals(requestRegistryDto.getProcessGroupId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestRegistryDto.getProcessGroupId(), groupId));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestVariableRegistryEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestVariableRegistryEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestVariableRegistryEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
            serviceFacade,
            requestVariableRegistryEntity,
            requestRevision,
            lookup -> {
                Authorizable authorizable = lookup.getProcessGroup(groupId).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
            },
            null,
            (revision, variableRegistryEntity) -> {
                final VariableRegistryDTO variableRegistry = variableRegistryEntity.getVariableRegistry();

                // update the process group
                final VariableRegistryEntity entity = serviceFacade.updateVariableRegistry(revision, variableRegistry);
                return generateOkResponse(entity).build();
            });
    }


    /**
     * Updates the variable registry for the specified process group.
     *
     * @param httpServletRequest request
     * @param groupId The id of the process group.
     * @param requestVariableRegistryEntity the Variable Registry Entity
     * @return A Variable Registry Entry.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/variable-registry/update-requests")
    @ApiOperation(value = "Submits a request to update a process group's variable registry",
        response = VariableRegistryUpdateRequestEntity.class,
        notes = NON_GUARANTEED_ENDPOINT,
        authorizations = {
            @Authorization(value = "Write - /process-groups/{uuid}")
        })
    @ApiResponses(value = {
        @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
        @ApiResponse(code = 401, message = "Client could not be authenticated."),
        @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
        @ApiResponse(code = 404, message = "The specified resource could not be found."),
        @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response submitUpdateVariableRegistryRequest(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId,
        @ApiParam(value = "The variable registry configuration details.", required = true) final VariableRegistryEntity requestVariableRegistryEntity) {

        if (requestVariableRegistryEntity == null || requestVariableRegistryEntity.getVariableRegistry() == null) {
            throw new IllegalArgumentException("Variable Registry details must be specified.");
        }

        if (requestVariableRegistryEntity.getProcessGroupRevision() == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestVariableRegistryEntity.isDisconnectedNodeAcknowledged());
        }

        // In order to update variables in a variable registry, we have to perform the following steps:
        // 1. Determine Affected Components (this includes any Processors and Controller Services and any components that reference an affected Controller Service).
        //    1a. Determine ID's of components
        //    1b. Determine Revision's of associated components
        // 2. Stop All Active Affected Processors
        // 3. Disable All Active Affected Controller Services
        // 4. Update the Variables
        // 5. Re-Enable all previously Active Affected Controller Services (services only, not dependent components)
        // 6. Re-Enable all previously Active Processors that Depended on the Controller Services

        // Determine the affected components (and their associated revisions)
        final VariableRegistryEntity computedEntity = serviceFacade.populateAffectedComponents(requestVariableRegistryEntity.getVariableRegistry());
        final VariableRegistryDTO computedRegistryDto = computedEntity.getVariableRegistry();
        if (computedRegistryDto == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        final Set<AffectedComponentEntity> allAffectedComponents = serviceFacade.getComponentsAffectedByVariableRegistryUpdate(requestVariableRegistryEntity.getVariableRegistry());
        final Set<AffectedComponentDTO> activeAffectedComponents = serviceFacade.getActiveComponentsAffectedByVariableRegistryUpdate(requestVariableRegistryEntity.getVariableRegistry());

        final Map<String, List<AffectedComponentDTO>> activeAffectedComponentsByType = activeAffectedComponents.stream()
            .collect(Collectors.groupingBy(comp -> comp.getReferenceType()));

        final List<AffectedComponentDTO> activeAffectedProcessors = activeAffectedComponentsByType.get(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        final List<AffectedComponentDTO> activeAffectedServices = activeAffectedComponentsByType.get(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // define access authorize for execution below
        final AuthorizeAccess authorizeAccess = lookup -> {
            final Authorizable groupAuthorizable = lookup.getProcessGroup(groupId).getAuthorizable();
            groupAuthorizable.authorize(authorizer, RequestAction.WRITE, user);

            // For every component that is affected, the user must have READ permissions and WRITE permissions
            // (because this action requires stopping the component).
            if (activeAffectedProcessors != null) {
                for (final AffectedComponentDTO activeAffectedComponent : activeAffectedProcessors) {
                    final Authorizable authorizable = lookup.getProcessor(activeAffectedComponent.getId()).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.READ, user);
                    authorizable.authorize(authorizer, RequestAction.WRITE, user);
                }
            }

            if (activeAffectedServices != null) {
                for (final AffectedComponentDTO activeAffectedComponent : activeAffectedServices) {
                    final Authorizable authorizable = lookup.getControllerService(activeAffectedComponent.getId()).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.READ, user);
                    authorizable.authorize(authorizer, RequestAction.WRITE, user);
                }
            }
        };

        if (isReplicateRequest()) {
            // authorize access
            serviceFacade.authorizeAccess(authorizeAccess);

            // update the variable registry
            final VariableRegistryUpdateRequest updateRequest = createVariableRegistryUpdateRequest(groupId, allAffectedComponents, user);
            updateRequest.getIdentifyRelevantComponentsStep().setComplete(true);
            final URI originalUri = getAbsolutePath();

            // Submit the task to be run in the background
            final Runnable taskWrapper = () -> {
                try {
                    // set the user authentication token
                    final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(user));
                    SecurityContextHolder.getContext().setAuthentication(authentication);

                    updateVariableRegistryReplicated(groupId, originalUri, activeAffectedProcessors, activeAffectedServices, updateRequest, requestVariableRegistryEntity);

                    // ensure the request is marked complete
                    updateRequest.setComplete(true);
                } catch (final Exception e) {
                    logger.error("Failed to update variable registry", e);

                    updateRequest.setComplete(true);
                    updateRequest.setFailureReason("An unexpected error has occurred: " + e);
                } finally {
                    // clear the authentication token
                    SecurityContextHolder.getContext().setAuthentication(null);
                }
            };

            variableRegistryThreadPool.submit(taskWrapper);

            final VariableRegistryUpdateRequestEntity responseEntity = new VariableRegistryUpdateRequestEntity();
            responseEntity.setRequest(dtoFactory.createVariableRegistryUpdateRequestDto(updateRequest));
            responseEntity.setProcessGroupRevision(updateRequest.getProcessGroupRevision());
            responseEntity.getRequest().setUri(generateResourceUri("process-groups", groupId, "variable-registry", "update-requests", updateRequest.getRequestId()));

            final URI location = URI.create(responseEntity.getRequest().getUri());
            return Response.status(Status.ACCEPTED).location(location).entity(responseEntity).build();
        }

        final UpdateVariableRegistryRequestWrapper requestWrapper =
                new UpdateVariableRegistryRequestWrapper(allAffectedComponents, activeAffectedProcessors, activeAffectedServices, requestVariableRegistryEntity);

        final Revision requestRevision = getRevision(requestVariableRegistryEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
            serviceFacade,
            requestWrapper,
            requestRevision,
            authorizeAccess,
            null,
            (revision, wrapper) ->
                    updateVariableRegistryLocal(groupId, wrapper.getAllAffectedComponents(), wrapper.getActiveAffectedProcessors(),
                        wrapper.getActiveAffectedServices(), user, revision, wrapper.getVariableRegistryEntity())
        );
    }

    private Pause createPause(final VariableRegistryUpdateRequest updateRequest) {
        return new Pause() {
            @Override
            public boolean pause() {
                if (updateRequest.isComplete()) {
                    return false;
                }

                try {
                    Thread.sleep(500);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }

                return !updateRequest.isComplete();
            }
        };
    }

    private void updateVariableRegistryReplicated(final String groupId, final URI originalUri, final Collection<AffectedComponentDTO> affectedProcessors,
                                                  final Collection<AffectedComponentDTO> affectedServices, final VariableRegistryUpdateRequest updateRequest,
                                                  final VariableRegistryEntity requestEntity) throws InterruptedException, IOException {
        final Pause pause = createPause(updateRequest);

        // stop processors
        if (affectedProcessors != null) {
            logger.info("In order to update Variable Registry for Process Group with ID {}, replicating request to stop {} affected Processors", groupId, affectedProcessors.size());
            scheduleProcessors(groupId, originalUri, updateRequest, pause, affectedProcessors, ScheduledState.STOPPED, updateRequest.getStopProcessorsStep());
        } else {
            logger.info("In order to update Variable Registry for Process Group with ID {}, no Processors are affected.", groupId);
            updateRequest.getStopProcessorsStep().setComplete(true);
        }

        // disable controller services
        if (affectedServices != null) {
            logger.info("In order to update Variable Registry for Process Group with ID {}, replicating request to stop {} affected Controller Services", groupId, affectedServices.size());
            activateControllerServices(groupId, originalUri, updateRequest, pause, affectedServices, ControllerServiceState.DISABLED, updateRequest.getDisableServicesStep());
        } else {
            logger.info("In order to update Variable Registry for Process Group with ID {}, no Controller Services are affected.", groupId);
            updateRequest.getDisableServicesStep().setComplete(true);
        }

        // apply updates
        logger.info("In order to update Variable Registry for Process Group with ID {}, replicating request to apply updates to variable registry", groupId);
        applyVariableRegistryUpdate(groupId, originalUri, updateRequest, requestEntity);

        // re-enable controller services
        if (affectedServices != null) {
            logger.info("In order to update Variable Registry for Process Group with ID {}, replicating request to re-enable {} affected services", groupId, affectedServices.size());
            activateControllerServices(groupId, originalUri, updateRequest, pause, affectedServices, ControllerServiceState.ENABLED, updateRequest.getEnableServicesStep());
        } else {
            logger.info("In order to update Variable Registry for Process Group with ID {}, no Controller Services are affected.", groupId);
            updateRequest.getEnableServicesStep().setComplete(true);
        }

        // restart processors
        if (affectedProcessors != null) {
            logger.info("In order to update Variable Registry for Process Group with ID {}, replicating request to restart {} affected processors", groupId, affectedProcessors.size());
            scheduleProcessors(groupId, originalUri, updateRequest, pause, affectedProcessors, ScheduledState.RUNNING, updateRequest.getStartProcessorsStep());
        } else {
            logger.info("In order to update Variable Registry for Process Group with ID {}, no Processors are affected.", groupId);
            updateRequest.getStartProcessorsStep().setComplete(true);
        }
    }

    /**
     * Periodically polls the process group with the given ID, waiting for all processors whose ID's are given to have the given Scheduled State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param processorIds the ID of all Processors whose state should be equal to the given desired state
     * @param desiredState the desired state for all processors with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for processors to reach the desired state
     */
    private boolean waitForProcessorStatus(final URI originalUri, final String groupId, final Set<String> processorIds, final ScheduledState desiredState,
                                           final VariableRegistryUpdateRequest updateRequest, final Pause pause) throws InterruptedException {
        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/process-groups/" + groupId + "/processors", "includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        boolean continuePolling = true;
        while (continuePolling) {

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ProcessorsEntity processorsEntity = getResponseEntity(clusterResponse, ProcessorsEntity.class);
            final Set<ProcessorEntity> processorEntities = processorsEntity.getProcessors();

            if (isProcessorActionComplete(processorEntities, updateRequest, processorIds, desiredState)) {
                logger.debug("All {} processors of interest now have the desired state of {}", processorIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    /**
     * Periodically polls the process group with the given ID, waiting for all processors whose ID's are given to have the given Scheduled State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param processorIds the ID of all Processors whose state should be equal to the given desired state
     * @param desiredState the desired state for all processors with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for processors to reach the desired state
     */
    private boolean waitForLocalProcessor(final String groupId, final Set<String> processorIds, final ScheduledState desiredState,
                                          final VariableRegistryUpdateRequest updateRequest, final Pause pause) {

        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ProcessorEntity> processorEntities = serviceFacade.getProcessors(groupId, true);

            if (isProcessorActionComplete(processorEntities, updateRequest, processorIds, desiredState)) {
                logger.debug("All {} processors of interest now have the desired state of {}", processorIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private boolean isProcessorActionComplete(final Set<ProcessorEntity> processorEntities, final VariableRegistryUpdateRequest updateRequest,
                                              final Set<String> processorIds, final ScheduledState desiredState) {

        final String desiredStateName = desiredState.name();

        // update the affected processors
        processorEntities.stream()
                .filter(entity -> updateRequest.getAffectedComponents().containsKey(entity.getId()))
                .forEach(entity -> {
                    final AffectedComponentEntity affectedComponentEntity = updateRequest.getAffectedComponents().get(entity.getId());
                    affectedComponentEntity.setRevision(entity.getRevision());

                    // only consider update this component if the user had permissions to it
                    if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                        final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                        affectedComponent.setState(entity.getStatus().getAggregateSnapshot().getRunStatus());
                        affectedComponent.setActiveThreadCount(entity.getStatus().getAggregateSnapshot().getActiveThreadCount());

                        if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                            affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                        }
                    }
                });

        final boolean allProcessorsMatch = processorEntities.stream()
                .filter(entity -> processorIds.contains(entity.getId()))
                .allMatch(entity -> {
                    final ProcessorStatusDTO status = entity.getStatus();

                    final String runStatus = status.getAggregateSnapshot().getRunStatus();
                    final boolean stateMatches = desiredStateName.equalsIgnoreCase(runStatus);
                    if (!stateMatches) {
                        return false;
                    }

                    return desiredState != ScheduledState.STOPPED || status.getAggregateSnapshot().getActiveThreadCount() == 0;
                });

        return allProcessorsMatch;
    }

    /**
     * Updates the affected controller services in the specified updateRequest with the serviceEntities.
     *
     * @param serviceEntities service entities
     * @param updateRequest update request
     */
    private void updateAffectedControllerServices(final Set<ControllerServiceEntity> serviceEntities, final VariableRegistryUpdateRequest updateRequest) {
        // update the affected components
        serviceEntities.stream()
                .filter(entity -> updateRequest.getAffectedComponents().containsKey(entity.getId()))
                .forEach(entity -> {
                    final AffectedComponentEntity affectedComponentEntity = updateRequest.getAffectedComponents().get(entity.getId());
                    affectedComponentEntity.setRevision(entity.getRevision());

                    // only consider update this component if the user had permissions to it
                    if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                        final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                        affectedComponent.setState(entity.getComponent().getState());

                        if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                            affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                        }
                    }
                });
    }

    /**
     * Periodically polls the process group with the given ID, waiting for all controller services whose ID's are given to have the given Controller Service State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param serviceIds the ID of all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForControllerServiceStatus(final URI originalUri, final String groupId, final Set<String> serviceIds,
                                                   final ControllerServiceState desiredState, final VariableRegistryUpdateRequest updateRequest, final Pause pause) throws InterruptedException {

        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", "includeAncestorGroups=false&includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        boolean continuePolling = true;
        while (continuePolling) {

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ControllerServicesEntity controllerServicesEntity = getResponseEntity(clusterResponse, ControllerServicesEntity.class);
            final Set<ControllerServiceEntity> serviceEntities = controllerServicesEntity.getControllerServices();

            // update the affected controller services
            updateAffectedControllerServices(serviceEntities, updateRequest);

            final String desiredStateName = desiredState.name();
            final boolean allServicesMatch = serviceEntities.stream()
                .map(entity -> entity.getComponent())
                .filter(service -> serviceIds.contains(service.getId()))
                .map(service -> service.getState())
                .allMatch(state -> state.equals(desiredStateName));

            if (allServicesMatch) {
                logger.debug("All {} controller services of interest now have the desired state of {}", serviceIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }


    /**
     * Periodically polls the process group with the given ID, waiting for all controller services whose ID's are given to have the given Controller Service State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param serviceIds the ID of all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForLocalControllerServiceStatus(final String groupId, final Set<String> serviceIds, final ControllerServiceState desiredState,
                                                        final VariableRegistryUpdateRequest updateRequest, final Pause pause) {

        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ControllerServiceEntity> serviceEntities = serviceFacade.getControllerServices(groupId, false, true);

            // update the affected controller services
            updateAffectedControllerServices(serviceEntities, updateRequest);

            final String desiredStateName = desiredState.name();
            final boolean allServicesMatch = serviceEntities.stream()
                .map(entity -> entity.getComponent())
                .filter(service -> serviceIds.contains(service.getId()))
                .map(service -> service.getState())
                .allMatch(state -> desiredStateName.equals(state));


            if (allServicesMatch) {
                logger.debug("All {} controller services of interest now have the desired state of {}", serviceIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private VariableRegistryUpdateRequest createVariableRegistryUpdateRequest(final String groupId, final Set<AffectedComponentEntity> affectedComponents, final NiFiUser user) {
        final VariableRegistryUpdateRequest updateRequest = new VariableRegistryUpdateRequest(UUID.randomUUID().toString(), groupId, affectedComponents, user);

        // before adding to the request map, purge any old requests. Must do this by creating a List of ID's
        // and then removing those ID's one-at-a-time in order to avoid ConcurrentModificationException.
        final Date oneMinuteAgo = new Date(System.currentTimeMillis() - VARIABLE_REGISTRY_UPDATE_REQUEST_EXPIRATION);
        final List<String> completedRequestIds = varRegistryUpdateRequests.entrySet().stream()
            .filter(entry -> entry.getValue().isComplete())
            .filter(entry -> entry.getValue().getLastUpdated().before(oneMinuteAgo))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        completedRequestIds.forEach(varRegistryUpdateRequests::remove);

        final int requestCount = varRegistryUpdateRequests.size();
        if (requestCount > MAX_VARIABLE_REGISTRY_UPDATE_REQUESTS) {
            throw new IllegalStateException("There are already " + requestCount + " update requests for variable registries. "
                + "Cannot issue any more requests until the older ones are deleted or expire");
        }

        this.varRegistryUpdateRequests.put(updateRequest.getRequestId(), updateRequest);
        return updateRequest;
    }

    private Response updateVariableRegistryLocal(final String groupId, final Set<AffectedComponentEntity> affectedComponents, final List<AffectedComponentDTO> affectedProcessors,
                                                 final List<AffectedComponentDTO> affectedServices, final NiFiUser user, final Revision requestRevision, final VariableRegistryEntity requestEntity) {

        final Set<String> affectedProcessorIds = affectedProcessors == null ? Collections.emptySet() : affectedProcessors.stream()
            .map(component -> component.getId())
            .collect(Collectors.toSet());
        Map<String, Revision> processorRevisionMap = getRevisions(groupId, affectedProcessorIds);

        final Set<String> affectedServiceIds = affectedServices == null ? Collections.emptySet() : affectedServices.stream()
            .map(component -> component.getId())
            .collect(Collectors.toSet());
        Map<String, Revision> serviceRevisionMap = getRevisions(groupId, affectedServiceIds);

        // update the variable registry
        final VariableRegistryUpdateRequest updateRequest = createVariableRegistryUpdateRequest(groupId, affectedComponents, user);
        updateRequest.getIdentifyRelevantComponentsStep().setComplete(true);
        final Pause pause = createPause(updateRequest);

        final Runnable updateTask = new Runnable() {
            @Override
            public void run() {
                try {
                    // set the user authentication token
                    final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(user));
                    SecurityContextHolder.getContext().setAuthentication(authentication);

                    // Stop processors
                    performUpdateVariableRegistryStep(groupId, updateRequest, updateRequest.getStopProcessorsStep(), "Stopping Processors",
                        () -> stopProcessors(updateRequest, groupId, processorRevisionMap, pause));

                    // Update revision map because this will have modified the revisions of our components.
                    final Map<String, Revision> updatedProcessorRevisionMap = getRevisions(groupId, affectedProcessorIds);

                    // Disable controller services
                    performUpdateVariableRegistryStep(groupId, updateRequest, updateRequest.getDisableServicesStep(), "Disabling Controller Services",
                        () -> disableControllerServices(updateRequest, groupId, serviceRevisionMap, pause));

                    // Update revision map because this will have modified the revisions of our components.
                    final Map<String, Revision> updatedServiceRevisionMap = getRevisions(groupId, affectedServiceIds);

                    // Apply the updates
                    performUpdateVariableRegistryStep(groupId, updateRequest, updateRequest.getApplyUpdatesStep(), "Applying updates to Variable Registry",
                        () -> {
                            final VariableRegistryEntity entity = serviceFacade.updateVariableRegistry(requestRevision, requestEntity.getVariableRegistry());
                            updateRequest.setProcessGroupRevision(entity.getProcessGroupRevision());
                        });

                    // Re-enable the controller services
                    performUpdateVariableRegistryStep(groupId, updateRequest, updateRequest.getEnableServicesStep(), "Re-enabling Controller Services",
                        () -> enableControllerServices(updateRequest, groupId, updatedServiceRevisionMap, pause));

                    // Restart processors
                    performUpdateVariableRegistryStep(groupId, updateRequest, updateRequest.getStartProcessorsStep(), "Restarting Processors",
                        () -> startProcessors(updateRequest, groupId, updatedProcessorRevisionMap, pause));

                    // Set complete
                    updateRequest.setComplete(true);
                    updateRequest.setLastUpdated(new Date());
                } catch (final Exception e) {
                    logger.error("Failed to update Variable Registry for Proces Group with ID " + groupId, e);

                    updateRequest.setComplete(true);
                    updateRequest.setFailureReason("An unexpected error has occurred: " + e);
                } finally {
                    // clear the authentication token
                    SecurityContextHolder.getContext().setAuthentication(null);
                }
            }
        };

        // Submit the task to be run in the background
        variableRegistryThreadPool.submit(updateTask);

        final VariableRegistryUpdateRequestEntity responseEntity = new VariableRegistryUpdateRequestEntity();
        responseEntity.setRequest(dtoFactory.createVariableRegistryUpdateRequestDto(updateRequest));
        responseEntity.setProcessGroupRevision(updateRequest.getProcessGroupRevision());
        responseEntity.getRequest().setUri(generateResourceUri("process-groups", groupId, "variable-registry", "update-requests", updateRequest.getRequestId()));

        final URI location = URI.create(responseEntity.getRequest().getUri());
        return Response.status(Status.ACCEPTED).location(location).entity(responseEntity).build();
    }

    private Map<String, Revision> getRevisions(final String groupId, final Set<String> componentIds) {
        final Set<Revision> processorRevisions = serviceFacade.getRevisionsFromGroup(groupId, group -> componentIds);
        return processorRevisions.stream().collect(Collectors.toMap(revision -> revision.getComponentId(), Function.identity()));
    }

    private void performUpdateVariableRegistryStep(final String groupId, final VariableRegistryUpdateRequest request, final VariableRegistryUpdateStep step,
        final String stepDescription, final Runnable action) {

        if (request.isComplete()) {
            logger.info("In updating Variable Registry for Process Group with ID {}"
                + ", skipping the following step because the request has completed already: {}", groupId, stepDescription);
            return;
        }

        try {
            logger.info("In order to update Variable Registry for Process Group with ID {}, {}", groupId, stepDescription);

            action.run();
            step.setComplete(true);
        } catch (final Exception e) {
            logger.error("Failed to update variable registry for Process Group with ID {}", groupId, e);

            step.setComplete(true);
            step.setFailureReason(e.getMessage());

            request.setComplete(true);
            request.setFailureReason("Failed to update Variable Registry because failed while performing step: " + stepDescription);
        }

        request.setLastUpdated(new Date());
    }

    private void stopProcessors(final VariableRegistryUpdateRequest updateRequest, final String processGroupId,
        final Map<String, Revision> processorRevisions, final Pause pause) {

        if (processorRevisions.isEmpty()) {
            return;
        }

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.STOPPED, processorRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.STOPPED, processorRevisions);
        waitForLocalProcessor(processGroupId, processorRevisions.keySet(), ScheduledState.STOPPED, updateRequest, pause);
    }

    private void startProcessors(final VariableRegistryUpdateRequest request, final String processGroupId, final Map<String, Revision> processorRevisions, final Pause pause) {
        if (processorRevisions.isEmpty()) {
            return;
        }

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.RUNNING, processorRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.RUNNING, processorRevisions);
        waitForLocalProcessor(processGroupId, processorRevisions.keySet(), ScheduledState.RUNNING, request, pause);
    }

    private void disableControllerServices(final VariableRegistryUpdateRequest updateRequest, final String processGroupId,
                                           final Map<String, Revision> serviceRevisions, final Pause pause) {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.DISABLED, serviceRevisions.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.DISABLED, serviceRevisions);
        waitForLocalControllerServiceStatus(processGroupId, serviceRevisions.keySet(), ControllerServiceState.DISABLED, updateRequest, pause);
    }

    private void enableControllerServices(final VariableRegistryUpdateRequest updateRequest, final String processGroupId,
                                          final Map<String, Revision> serviceRevisions, final Pause pause) {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.ENABLED, serviceRevisions.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.ENABLED, serviceRevisions);
        waitForLocalControllerServiceStatus(processGroupId, serviceRevisions.keySet(), ControllerServiceState.ENABLED, updateRequest, pause);
    }


    private void scheduleProcessors(final String groupId, final URI originalUri, final VariableRegistryUpdateRequest updateRequest,
                                    final Pause pause, final Collection<AffectedComponentDTO> affectedProcessors, final ScheduledState desiredState,
                                    final VariableRegistryUpdateStep updateStep) throws InterruptedException {

        final Set<String> affectedProcessorIds = affectedProcessors.stream()
            .map(component -> component.getId())
            .collect(Collectors.toSet());

        final Map<String, Revision> processorRevisionMap = getRevisions(groupId, affectedProcessorIds);
        final Map<String, RevisionDTO> processorRevisionDtoMap = processorRevisionMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> dtoFactory.createRevisionDTO(entry.getValue())));

        final ScheduleComponentsEntity scheduleProcessorsEntity = new ScheduleComponentsEntity();
        scheduleProcessorsEntity.setComponents(processorRevisionDtoMap);
        scheduleProcessorsEntity.setId(groupId);
        scheduleProcessorsEntity.setState(desiredState.name());

        URI scheduleGroupUri;
        try {
            scheduleGroupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId, null, originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
        final NodeResponse clusterResponse;
        if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
            clusterResponse = getRequestReplicator().replicate(HttpMethod.PUT, scheduleGroupUri, scheduleProcessorsEntity, headers).awaitMergedResponse();
        } else {
            clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.PUT, scheduleGroupUri, scheduleProcessorsEntity, headers).awaitMergedResponse();
        }

        final int stopProcessorStatus = clusterResponse.getStatus();
        if (stopProcessorStatus != Status.OK.getStatusCode()) {
            updateRequest.getStopProcessorsStep().setFailureReason("Failed while " + updateStep.getDescription());

            updateStep.setComplete(true);
            updateRequest.setFailureReason("Failed while " + updateStep.getDescription());
            return;
        }

        updateRequest.setLastUpdated(new Date());
        final boolean processorsTransitioned = waitForProcessorStatus(originalUri, groupId, affectedProcessorIds, desiredState, updateRequest, pause);
        updateStep.setComplete(true);

        if (!processorsTransitioned) {
            updateStep.setFailureReason("Failed while " + updateStep.getDescription());

            updateRequest.setComplete(true);
            updateRequest.setFailureReason("Failed while " + updateStep.getDescription());
        }
    }

    private void activateControllerServices(final String groupId, final URI originalUri, final VariableRegistryUpdateRequest updateRequest,
        final Pause pause, final Collection<AffectedComponentDTO> affectedServices, final ControllerServiceState desiredState, final VariableRegistryUpdateStep updateStep)
            throws InterruptedException {

        final Set<String> affectedServiceIds = affectedServices.stream()
            .map(component -> component.getId())
            .collect(Collectors.toSet());

        final Map<String, Revision> serviceRevisionMap = getRevisions(groupId, affectedServiceIds);
        final Map<String, RevisionDTO> serviceRevisionDtoMap = serviceRevisionMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> dtoFactory.createRevisionDTO(entry.getValue())));

        final ActivateControllerServicesEntity activateServicesEntity = new ActivateControllerServicesEntity();
        activateServicesEntity.setComponents(serviceRevisionDtoMap);
        activateServicesEntity.setId(groupId);
        activateServicesEntity.setState(desiredState.name());

        URI controllerServicesUri;
        try {
            controllerServicesUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", null, originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
        final NodeResponse clusterResponse;
        if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
            clusterResponse = getRequestReplicator().replicate(HttpMethod.PUT, controllerServicesUri, activateServicesEntity, headers).awaitMergedResponse();
        } else {
            clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.PUT, controllerServicesUri, activateServicesEntity, headers).awaitMergedResponse();
        }

        final int disableServicesStatus = clusterResponse.getStatus();
        if (disableServicesStatus != Status.OK.getStatusCode()) {
            updateStep.setFailureReason("Failed while " + updateStep.getDescription());

            updateStep.setComplete(true);
            updateRequest.setFailureReason("Failed while " + updateStep.getDescription());
            return;
        }

        updateRequest.setLastUpdated(new Date());
        final boolean serviceTransitioned = waitForControllerServiceStatus(originalUri, groupId, affectedServiceIds, desiredState, updateRequest, pause);
        updateStep.setComplete(true);

        if (!serviceTransitioned) {
            updateStep.setFailureReason("Failed while " + updateStep.getDescription());

            updateRequest.setComplete(true);
            updateRequest.setFailureReason("Failed while " + updateStep.getDescription());
        }
    }

    private void applyVariableRegistryUpdate(final String groupId, final URI originalUri, final VariableRegistryUpdateRequest updateRequest,
        final VariableRegistryEntity updateEntity) throws InterruptedException, IOException {

        // convert request accordingly
        URI applyUpdatesUri;
        try {
            applyUpdatesUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/process-groups/" + groupId + "/variable-registry", null, originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
        final NodeResponse clusterResponse;
        if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
            clusterResponse = getRequestReplicator().replicate(HttpMethod.PUT, applyUpdatesUri, updateEntity, headers).awaitMergedResponse();
        } else {
            clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), HttpMethod.PUT, applyUpdatesUri, updateEntity, headers).awaitMergedResponse();
        }

        final int applyUpdatesStatus = clusterResponse.getStatus();
        updateRequest.setLastUpdated(new Date());
        updateRequest.getApplyUpdatesStep().setComplete(true);

        if (applyUpdatesStatus == Status.OK.getStatusCode()) {
            // grab the current process group revision
            final VariableRegistryEntity entity = getResponseEntity(clusterResponse, VariableRegistryEntity.class);
            updateRequest.setProcessGroupRevision(entity.getProcessGroupRevision());
        } else {
            final String message = getResponseEntity(clusterResponse, String.class);

            // update the request progress
            updateRequest.getApplyUpdatesStep().setFailureReason("Failed to apply updates to the Variable Registry: " + message);
            updateRequest.setComplete(true);
            updateRequest.setFailureReason("Failed to apply updates to the Variable Registry: " + message);
        }
    }

    /**
     * Extracts the response entity from the specified node response.
     *
     * @param nodeResponse node response
     * @param clazz class
     * @param <T> type of class
     * @return the response entity
     */
    @SuppressWarnings("unchecked")
    protected <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            entity = nodeResponse.getClientResponse().readEntity(clazz);
        }
        return entity;
    }

    /**
     * Creates a request to drop the flowfiles from all connection queues within a process group (recursively).
     *
     * @param httpServletRequest request
     * @param processGroupId     The id of the process group to be removed.
     * @return A dropRequestEntity.
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/empty-all-connections-requests")
    @ApiOperation(
        value = "Creates a request to drop all flowfiles of all connection queues in this process group.",
        response = ProcessGroupEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid} - For this and all encapsulated process groups"),
            @Authorization(value = "Write Source Data - /data/{component-type}/{uuid} - For all encapsulated connections")
        }
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 202, message = "The request has been accepted. An HTTP response header will contain the URI where the status can be polled."),
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
        }
    )
    public Response createEmptyAllConnectionsRequest(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") final String processGroupId
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ProcessGroupEntity requestProcessGroupEntity = new ProcessGroupEntity();
        requestProcessGroupEntity.setId(processGroupId);

        return withWriteLock(
            serviceFacade,
            requestProcessGroupEntity,
            lookup -> authorizeHandleDropAllFlowFilesRequest(processGroupId, lookup),
            null,
            (processGroupEntity) -> {
                // ensure the id is the same across the cluster
                final String dropRequestId = generateUuid();

                // submit the drop request
                final DropRequestDTO dropRequest = serviceFacade.createDropAllFlowFilesInProcessGroup(processGroupEntity.getId(), dropRequestId);
                dropRequest.setUri(generateResourceUri("process-groups", processGroupEntity.getId(), "empty-all-connections-requests", dropRequest.getId()));

                // create the response entity
                final DropRequestEntity entity = new DropRequestEntity();
                entity.setDropRequest(dropRequest);

                // generate the URI where the response will be
                final URI location = URI.create(dropRequest.getUri());
                return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
            }
        );
    }

    /**
     * Checks the status of an outstanding request for dropping all flowfiles within a process group.
     *
     * @param processGroupId  The id of the process group
     * @param dropRequestId   The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/empty-all-connections-requests/{drop-request-id}")
    @ApiOperation(
        value = "Gets the current status of a drop all flowfiles request.",
        response = DropRequestEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid} - For this and all encapsulated process groups"),
            @Authorization(value = "Write Source Data - /data/{component-type}/{uuid} - For all encapsulated connections")
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
    public Response getDropAllFlowfilesRequest(
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") final String processGroupId,
        @ApiParam(
            value = "The drop request id.",
            required = true
        )
        @PathParam("drop-request-id") final String dropRequestId
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> authorizeHandleDropAllFlowFilesRequest(processGroupId, lookup));

        // get the drop request
        final DropRequestDTO dropRequest = serviceFacade.getDropAllFlowFilesRequest(processGroupId, dropRequestId);
        dropRequest.setUri(generateResourceUri("process-groups", processGroupId, "empty-all-connections-requests", dropRequest.getId()));

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dropRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Cancels the specified request for dropping all flowfiles within a process group.
     *
     * @param httpServletRequest request
     * @param processGroupId     The process group id
     * @param dropRequestId      The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/empty-all-connections-requests/{drop-request-id}")
    @ApiOperation(
        value = "Cancels and/or removes a request to drop all flowfiles.",
        response = DropRequestEntity.class,
        authorizations = {
            @Authorization(value = "Read - /process-groups/{uuid} - For this and all encapsulated process groups"),
            @Authorization(value = "Write Source Data - /data/{component-type}/{uuid} - For all encapsulated connections")
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
    public Response removeDropRequest(
        @Context final HttpServletRequest httpServletRequest,
        @ApiParam(
            value = "The process group id.",
            required = true
        )
        @PathParam("id") final String processGroupId,
        @ApiParam(
            value = "The drop request id.",
            required = true
        )
        @PathParam("drop-request-id") final String dropRequestId
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
            serviceFacade,
            new DropEntity(processGroupId, dropRequestId),
            lookup -> authorizeHandleDropAllFlowFilesRequest(processGroupId, lookup),
            null,
            (dropEntity) -> {
                // delete the drop request
                final DropRequestDTO dropRequest = serviceFacade.deleteDropAllFlowFilesRequest(dropEntity.getEntityId(), dropEntity.getDropRequestId());
                dropRequest.setUri(generateResourceUri("process-groups", dropEntity.getEntityId(), "empty-all-connections-requests", dropRequest.getId()));

                // create the response entity
                final DropRequestEntity entity = new DropRequestEntity();
                entity.setDropRequest(dropRequest);

                return generateOkResponse(entity).build();
            }
        );
    }

    private void authorizeHandleDropAllFlowFilesRequest(String processGroupId, AuthorizableLookup lookup) {
        final ProcessGroupAuthorizable processGroup = lookup.getProcessGroup(processGroupId);

        authorizeProcessGroup(processGroup, authorizer, lookup, RequestAction.READ, false, false, false, false, false);

        processGroup.getEncapsulatedProcessGroups()
            .forEach(encapsulatedProcessGroup -> authorizeProcessGroup(encapsulatedProcessGroup, authorizer, lookup, RequestAction.READ, false, false, false, false, false));

        processGroup.getEncapsulatedConnections().stream()
            .map(ConnectionAuthorizable::getSourceData)
            .forEach(connectionSourceData -> connectionSourceData.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser()));
    }

    /**
     * Removes the specified process group reference.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id                 The id of the process group to be removed.
     * @return A processGroupEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Write - Parent Process Group - /process-groups/{uuid}"),
                    @Authorization(value = "Read - any referenced Controller Services by any encapsulated components - /controller-services/{uuid}"),
                    @Authorization(value = "Write - /{component-type}/{uuid} - For all encapsulated components")
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
    public Response removeProcessGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The revision is used to verify the client is working with the latest version of the flow.",
                    required = false
            )
            @QueryParam(VERSION) final LongParameter version,
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ProcessGroupEntity requestProcessGroupEntity = new ProcessGroupEntity();
        requestProcessGroupEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestProcessGroupEntity,
                requestRevision,
                lookup -> {
                    final ProcessGroupAuthorizable processGroupAuthorizable = lookup.getProcessGroup(id);

                    // ensure write to this process group and all encapsulated components including templates and controller services. additionally, ensure
                    // read to any referenced services by encapsulated components
                    authorizeProcessGroup(processGroupAuthorizable, authorizer, lookup, RequestAction.WRITE, true, true, true, false, false);

                    // ensure write permission to the parent process group, if applicable... if this is the root group the
                    // request will fail later but still need to handle authorization here
                    final Authorizable parentAuthorizable = processGroupAuthorizable.getAuthorizable().getParentAuthorizable();
                    if (parentAuthorizable != null) {
                        parentAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                    }
                },
                () -> serviceFacade.verifyDeleteProcessGroup(id),
                (revision, processGroupEntity) -> {
                    // delete the process group
                    final ProcessGroupEntity entity = serviceFacade.deleteProcessGroup(revision, processGroupEntity.getId());

                    // prune response as necessary
                    if (entity.getComponent() != null) {
                        entity.getComponent().setContents(null);
                    }

                    // create the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Adds the specified process group.
     *
     * @param httpServletRequest request
     * @param groupId The group id
     * @param requestProcessGroupEntity A processGroupEntity
     * @return A processGroupEntity
     * @throws IOException if the request indicates that the Process Group should be imported from a Flow Registry and NiFi is unable to communicate with the Flow Registry
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups")
    @ApiOperation(
            value = "Creates a process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createProcessGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The process group configuration details.",
                    required = true
        ) final ProcessGroupEntity requestProcessGroupEntity) {

        if (requestProcessGroupEntity == null || requestProcessGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("Process group details must be specified.");
        }

        if (requestProcessGroupEntity.getRevision() == null || (requestProcessGroupEntity.getRevision().getVersion() == null || requestProcessGroupEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Process group.");
        }

        if (requestProcessGroupEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Process group ID cannot be specified.");
        }

        final PositionDTO proposedPosition = requestProcessGroupEntity.getComponent().getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        // if the group name isn't specified, ensure the group is being imported from version control
        if (StringUtils.isBlank(requestProcessGroupEntity.getComponent().getName()) && requestProcessGroupEntity.getComponent().getVersionControlInformation() == null) {
            throw new IllegalArgumentException("The group name is required when the group is not imported from version control.");
        }

        if (requestProcessGroupEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestProcessGroupEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestProcessGroupEntity.getComponent().getParentGroupId(), groupId));
        }
        requestProcessGroupEntity.getComponent().setParentGroupId(groupId);

        // Step 1: Ensure that user has write permissions to the Process Group. If not, then immediately fail.
        // Step 2: Retrieve flow from Flow Registry
        // Step 3: Resolve Bundle info
        // Step 4: Update contents of the ProcessGroupDTO passed in to include the components that need to be added.
        // Step 5: If any of the components is a Restricted Component, then we must authorize the user
        //         for write access to the RestrictedComponents resource
        // Step 6: Replicate the request or call serviceFacade.updateProcessGroup

        final VersionControlInformationDTO versionControlInfo = requestProcessGroupEntity.getComponent().getVersionControlInformation();
        if (versionControlInfo != null && requestProcessGroupEntity.getVersionedFlowSnapshot() == null) {
            // Step 1: Ensure that user has write permissions to the Process Group. If not, then immediately fail.
            // Step 2: Retrieve flow from Flow Registry
            final VersionedFlowSnapshot flowSnapshot = getFlowFromRegistry(versionControlInfo);

            // Step 3: Resolve Bundle info
            serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());

            // If there are any Controller Services referenced that are inherited from the parent group, resolve those to point to the appropriate Controller Service, if we are able to.
            serviceFacade.resolveInheritedControllerServices(flowSnapshot, groupId, NiFiUserUtils.getNiFiUser());

            // Step 4: Update contents of the ProcessGroupDTO passed in to include the components that need to be added.
            requestProcessGroupEntity.setVersionedFlowSnapshot(flowSnapshot);
        }

        if (versionControlInfo != null) {
            final VersionedFlowSnapshot flowSnapshot = requestProcessGroupEntity.getVersionedFlowSnapshot();
            serviceFacade.verifyImportProcessGroup(versionControlInfo, flowSnapshot.getFlowContents(), groupId);
        }

        // Step 6: Replicate the request or call serviceFacade.updateProcessGroup
        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestProcessGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestProcessGroupEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestProcessGroupEntity,
                lookup -> authorizeAccess(groupId, requestProcessGroupEntity, lookup),
                () -> {
                    final VersionedFlowSnapshot versionedFlowSnapshot = requestProcessGroupEntity.getVersionedFlowSnapshot();
                    if (versionedFlowSnapshot != null) {
                        serviceFacade.verifyComponentTypes(versionedFlowSnapshot.getFlowContents());
                    }
                },
                processGroupEntity -> {
                    final ProcessGroupDTO processGroup = processGroupEntity.getComponent();

                    // set the processor id as appropriate
                    processGroup.setId(generateUuid());

                    // ensure the group name comes from the versioned flow
                    final VersionedFlowSnapshot flowSnapshot = processGroupEntity.getVersionedFlowSnapshot();
                    if (flowSnapshot != null && StringUtils.isNotBlank(flowSnapshot.getFlowContents().getName()) && StringUtils.isBlank(processGroup.getName())) {
                        processGroup.setName(flowSnapshot.getFlowContents().getName());
                    }

                    // create the process group contents
                    final Revision revision = getRevision(processGroupEntity, processGroup.getId());
                    ProcessGroupEntity entity = serviceFacade.createProcessGroup(revision, groupId, processGroup);

                    if (flowSnapshot != null) {
                        final RevisionDTO revisionDto = entity.getRevision();
                        final String newGroupId = entity.getComponent().getId();
                        final Revision newGroupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), newGroupId);

                        // We don't want the Process Group's position to be updated because we want to keep the position where the user
                        // placed the Process Group. However, we do want to use the name of the Process Group that is in the Flow Contents.
                        // To accomplish this, we call updateProcessGroupContents() passing 'true' for the updateSettings flag but null out the position.
                        flowSnapshot.getFlowContents().setPosition(null);
                        entity = serviceFacade.updateProcessGroupContents(newGroupRevision, newGroupId, versionControlInfo, flowSnapshot,
                                getIdGenerationSeed().orElse(null), false, true, true);
                    }

                    populateRemainingProcessGroupEntityContent(entity);

                    // generate a 201 created response
                    String uri = entity.getUri();
                    return generateCreatedResponse(URI.create(uri), entity).build();
                }
        );
    }

    private VersionedFlowSnapshot getFlowFromRegistry(final VersionControlInformationDTO versionControlInfo) {
        final VersionedFlowSnapshot flowSnapshot = serviceFacade.getVersionedFlowSnapshot(versionControlInfo, true);
        final Bucket bucket = flowSnapshot.getBucket();
        final VersionedFlow flow = flowSnapshot.getFlow();

        versionControlInfo.setBucketName(bucket.getName());
        versionControlInfo.setFlowName(flow.getName());
        versionControlInfo.setFlowDescription(flow.getDescription());

        versionControlInfo.setRegistryName(serviceFacade.getFlowRegistryName(versionControlInfo.getRegistryId()));
        final VersionedFlowState flowState = flowSnapshot.isLatest() ? VersionedFlowState.UP_TO_DATE : VersionedFlowState.STALE;
        versionControlInfo.setState(flowState.name());

        return flowSnapshot;
    }


    /**
     * Retrieves all the child process groups of the process group with the given id.
     *
     * @param groupId the parent process group id
     * @return An entity containing all the child process group entities.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups")
    @ApiOperation(
            value = "Gets all process groups",
            response = ProcessGroupsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getProcessGroups(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the process groups
        final Set<ProcessGroupEntity> entities = serviceFacade.getProcessGroups(groupId);

        // always prune the contents
        for (final ProcessGroupEntity entity : entities) {
            if (entity.getComponent() != null) {
                entity.getComponent().setContents(null);
            }
        }

        // create the response entity
        final ProcessGroupsEntity entity = new ProcessGroupsEntity();
        entity.setProcessGroups(populateRemainingProcessGroupEntitiesContent(entities));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ----------
    // processors
    // ----------

    /**
     * Creates a new processor.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestProcessorEntity    A processorEntity.
     * @return A processorEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/processors")
    @ApiOperation(
            value = "Creates a new processor",
            response = ProcessorEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @Authorization(value = "Write - if the Processor is restricted - /restricted-components")
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
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The processor configuration details.",
                    required = true
            ) final ProcessorEntity requestProcessorEntity) {

        if (requestProcessorEntity == null || requestProcessorEntity.getComponent() == null) {
            throw new IllegalArgumentException("Processor details must be specified.");
        }

        if (requestProcessorEntity.getRevision() == null || (requestProcessorEntity.getRevision().getVersion() == null || requestProcessorEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Processor.");
        }

        final ProcessorDTO requestProcessor = requestProcessorEntity.getComponent();
        if (requestProcessor.getId() != null) {
            throw new IllegalArgumentException("Processor ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestProcessor.getType())) {
            throw new IllegalArgumentException("The type of processor to create must be specified.");
        }

        final PositionDTO proposedPosition = requestProcessor.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestProcessor.getParentGroupId() != null && !groupId.equals(requestProcessor.getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestProcessor.getParentGroupId(), groupId));
        }
        requestProcessor.setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestProcessorEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestProcessorEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestProcessorEntity,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, user);

                    final Authorizable parameterContext = groupAuthorizable.getProcessGroup().getParameterContext();
                    final ProcessorConfigDTO configDto = requestProcessor.getConfig();
                    if (parameterContext != null && configDto != null) {
                        AuthorizeParameterReference.authorizeParameterReferences(configDto.getProperties(), authorizer, parameterContext, user);
                    }

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestProcessor.getType(), requestProcessor.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        final ProcessorConfigDTO config = requestProcessor.getConfig();
                        if (config != null && config.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(config.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateProcessor(requestProcessor),
                processorEntity -> {
                    final ProcessorDTO processor = processorEntity.getComponent();

                    // set the processor id as appropriate
                    processor.setId(generateUuid());

                    // create the new processor
                    final Revision revision = getRevision(processorEntity, processor.getId());
                    final ProcessorEntity entity = serviceFacade.createProcessor(revision, groupId, processor);
                    processorResource.populateRemainingProcessorEntityContent(entity);

                    // generate a 201 created response
                    String uri = entity.getUri();
                    return generateCreatedResponse(URI.create(uri), entity).build();
                }
        );
    }

    /**
     * Retrieves all the processors in this NiFi.
     *
     * @param groupId group id
     * @return A processorsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/processors")
    @ApiOperation(
            value = "Gets all processors",
            response = ProcessorsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getProcessors(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam("Whether or not to include processors from descendant process groups") @QueryParam("includeDescendantGroups") @DefaultValue("false") boolean includeDescendantGroups) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the processors
        final Set<ProcessorEntity> processors = serviceFacade.getProcessors(groupId, includeDescendantGroups);

        // create the response entity
        final ProcessorsEntity entity = new ProcessorsEntity();
        entity.setProcessors(processorResource.populateRemainingProcessorEntitiesContent(processors));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -----------
    // input ports
    // -----------

    /**
     * Creates a new input port.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestPortEntity         A inputPortEntity.
     * @return A inputPortEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports")
    @ApiOperation(
            value = "Creates an input port",
            response = PortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createInputPort(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The input port configuration details.",
                    required = true
            ) final PortEntity requestPortEntity) {

        if (requestPortEntity == null || requestPortEntity.getComponent() == null) {
            throw new IllegalArgumentException("Port details must be specified.");
        }

        if (requestPortEntity.getRevision() == null || (requestPortEntity.getRevision().getVersion() == null || requestPortEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Input port.");
        }

        if (requestPortEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Input port ID cannot be specified.");
        }

        final PositionDTO proposedPosition = requestPortEntity.getComponent().getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestPortEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestPortEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestPortEntity.getComponent().getParentGroupId(), groupId));
        }
        requestPortEntity.getComponent().setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestPortEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestPortEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                portEntity -> {
                    // set the processor id as appropriate
                    portEntity.getComponent().setId(generateUuid());

                    // create the input port and generate the json
                    final Revision revision = getRevision(portEntity, portEntity.getComponent().getId());
                    final PortEntity entity = serviceFacade.createInputPort(revision, groupId, portEntity.getComponent());
                    inputPortResource.populateRemainingInputPortEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves all the of input ports in this NiFi.
     *
     * @return A inputPortsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports")
    @ApiOperation(
            value = "Gets all input ports",
            response = InputPortsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getInputPorts(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the input ports
        final Set<PortEntity> inputPorts = serviceFacade.getInputPorts(groupId);

        final InputPortsEntity entity = new InputPortsEntity();
        entity.setInputPorts(inputPortResource.populateRemainingInputPortEntitiesContent(inputPorts));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ------------
    // output ports
    // ------------

    /**
     * Creates a new output port.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestPortEntity         A outputPortEntity.
     * @return A outputPortEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports")
    @ApiOperation(
            value = "Creates an output port",
            response = PortEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createOutputPort(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The output port configuration.",
                    required = true
            ) final PortEntity requestPortEntity) {

        if (requestPortEntity == null || requestPortEntity.getComponent() == null) {
            throw new IllegalArgumentException("Port details must be specified.");
        }

        if (requestPortEntity.getRevision() == null || (requestPortEntity.getRevision().getVersion() == null || requestPortEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Output port.");
        }

        if (requestPortEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Output port ID cannot be specified.");
        }

        final PositionDTO proposedPosition = requestPortEntity.getComponent().getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestPortEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestPortEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestPortEntity.getComponent().getParentGroupId(), groupId));
        }
        requestPortEntity.getComponent().setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestPortEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestPortEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                portEntity -> {
                    // set the processor id as appropriate
                    portEntity.getComponent().setId(generateUuid());

                    // create the output port and generate the json
                    final Revision revision = getRevision(portEntity, portEntity.getComponent().getId());
                    final PortEntity entity = serviceFacade.createOutputPort(revision, groupId, portEntity.getComponent());
                    outputPortResource.populateRemainingOutputPortEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves all the of output ports in this NiFi.
     *
     * @return A outputPortsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports")
    @ApiOperation(
            value = "Gets all output ports",
            response = OutputPortsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getOutputPorts(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the output ports
        final Set<PortEntity> outputPorts = serviceFacade.getOutputPorts(groupId);

        // create the response entity
        final OutputPortsEntity entity = new OutputPortsEntity();
        entity.setOutputPorts(outputPortResource.populateRemainingOutputPortEntitiesContent(outputPorts));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -------
    // funnels
    // -------

    /**
     * Creates a new Funnel.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestFunnelEntity       A funnelEntity.
     * @return A funnelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/funnels")
    @ApiOperation(
            value = "Creates a funnel",
            response = FunnelEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createFunnel(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The funnel configuration details.",
                    required = true
            ) final FunnelEntity requestFunnelEntity) {

        if (requestFunnelEntity == null || requestFunnelEntity.getComponent() == null) {
            throw new IllegalArgumentException("Funnel details must be specified.");
        }

        if (requestFunnelEntity.getRevision() == null || (requestFunnelEntity.getRevision().getVersion() == null || requestFunnelEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Funnel.");
        }

        if (requestFunnelEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Funnel ID cannot be specified.");
        }

        final PositionDTO proposedPosition = requestFunnelEntity.getComponent().getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestFunnelEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestFunnelEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestFunnelEntity.getComponent().getParentGroupId(), groupId));
        }
        requestFunnelEntity.getComponent().setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestFunnelEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFunnelEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestFunnelEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                funnelEntity -> {
                    // set the processor id as appropriate
                    funnelEntity.getComponent().setId(generateUuid());

                    // create the funnel and generate the json
                    final Revision revision = getRevision(funnelEntity, funnelEntity.getComponent().getId());
                    final FunnelEntity entity = serviceFacade.createFunnel(revision, groupId, funnelEntity.getComponent());
                    funnelResource.populateRemainingFunnelEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves all the of funnels in this NiFi.
     *
     * @return A funnelsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/funnels")
    @ApiOperation(
            value = "Gets all funnels",
            response = FunnelsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getFunnels(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the funnels
        final Set<FunnelEntity> funnels = serviceFacade.getFunnels(groupId);

        // create the response entity
        final FunnelsEntity entity = new FunnelsEntity();
        entity.setFunnels(funnelResource.populateRemainingFunnelEntitiesContent(funnels));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ------
    // labels
    // ------

    /**
     * Creates a new Label.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestLabelEntity        A labelEntity.
     * @return A labelEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/labels")
    @ApiOperation(
            value = "Creates a label",
            response = LabelEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createLabel(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The label configuration details.",
                    required = true
            ) final LabelEntity requestLabelEntity) {

        if (requestLabelEntity == null || requestLabelEntity.getComponent() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (requestLabelEntity.getRevision() == null || (requestLabelEntity.getRevision().getVersion() == null || requestLabelEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Label.");
        }

        if (requestLabelEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Label ID cannot be specified.");
        }

        final PositionDTO proposedPosition = requestLabelEntity.getComponent().getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestLabelEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestLabelEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestLabelEntity.getComponent().getParentGroupId(), groupId));
        }
        requestLabelEntity.getComponent().setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestLabelEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestLabelEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestLabelEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                labelEntity -> {
                    // set the processor id as appropriate
                    labelEntity.getComponent().setId(generateUuid());

                    // create the label and generate the json
                    final Revision revision = getRevision(labelEntity, labelEntity.getComponent().getId());
                    final LabelEntity entity = serviceFacade.createLabel(revision, groupId, labelEntity.getComponent());
                    labelResource.populateRemainingLabelEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves all the of labels in this NiFi.
     *
     * @return A labelsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/labels")
    @ApiOperation(
            value = "Gets all labels",
            response = LabelsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getLabels(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the labels
        final Set<LabelEntity> labels = serviceFacade.getLabels(groupId);

        // create the response entity
        final LabelsEntity entity = new LabelsEntity();
        entity.setLabels(labelResource.populateRemainingLabelEntitiesContent(labels));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ---------------------
    // remote process groups
    // ---------------------

    /**
     * Creates a new remote process group.
     *
     * @param httpServletRequest       request
     * @param groupId                  The group id
     * @param requestRemoteProcessGroupEntity A remoteProcessGroupEntity.
     * @return A remoteProcessGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/remote-process-groups")
    @ApiOperation(
            value = "Creates a new process group",
            response = RemoteProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response createRemoteProcessGroup(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The remote process group configuration details.",
                    required = true
            ) final RemoteProcessGroupEntity requestRemoteProcessGroupEntity) {

        if (requestRemoteProcessGroupEntity == null || requestRemoteProcessGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("Remote process group details must be specified.");
        }

        if (requestRemoteProcessGroupEntity.getRevision() == null
                || (requestRemoteProcessGroupEntity.getRevision().getVersion() == null || requestRemoteProcessGroupEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Remote process group.");
        }

        final RemoteProcessGroupDTO requestRemoteProcessGroupDTO = requestRemoteProcessGroupEntity.getComponent();

        if (requestRemoteProcessGroupDTO.getId() != null) {
            throw new IllegalArgumentException("Remote process group ID cannot be specified.");
        }

        if (requestRemoteProcessGroupDTO.getTargetUri() == null) {
            throw new IllegalArgumentException("The URI of the process group must be specified.");
        }

        final PositionDTO proposedPosition = requestRemoteProcessGroupDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (requestRemoteProcessGroupDTO.getParentGroupId() != null && !groupId.equals(requestRemoteProcessGroupDTO.getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestRemoteProcessGroupDTO.getParentGroupId(), groupId));
        }
        requestRemoteProcessGroupDTO.setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestRemoteProcessGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemoteProcessGroupEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                remoteProcessGroupEntity -> {
                    final RemoteProcessGroupDTO remoteProcessGroupDTO = remoteProcessGroupEntity.getComponent();

                    // set the processor id as appropriate
                    remoteProcessGroupDTO.setId(generateUuid());

                    // parse the uri to check if the uri is valid
                    final String targetUris = remoteProcessGroupDTO.getTargetUris();
                    SiteToSiteRestApiClient.parseClusterUrls(targetUris);

                    // since the uri is valid, use it
                    remoteProcessGroupDTO.setTargetUris(targetUris);

                    // create the remote process group
                    final Revision revision = getRevision(remoteProcessGroupEntity, remoteProcessGroupDTO.getId());
                    final RemoteProcessGroupEntity entity = serviceFacade.createRemoteProcessGroup(revision, groupId, remoteProcessGroupDTO);
                    remoteProcessGroupResource.populateRemainingRemoteProcessGroupEntityContent(entity);

                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves all the of remote process groups in this NiFi.
     *
     * @return A remoteProcessGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/remote-process-groups")
    @ApiOperation(
            value = "Gets all remote process groups",
            response = RemoteProcessGroupsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getRemoteProcessGroups(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the remote process groups
        final Set<RemoteProcessGroupEntity> remoteProcessGroups = serviceFacade.getRemoteProcessGroups(groupId);

        // prune response as necessary
        for (RemoteProcessGroupEntity remoteProcessGroupEntity : remoteProcessGroups) {
            if (remoteProcessGroupEntity.getComponent() != null) {
                remoteProcessGroupEntity.getComponent().setContents(null);
            }
        }

        // create the response entity
        final RemoteProcessGroupsEntity entity = new RemoteProcessGroupsEntity();
        entity.setRemoteProcessGroups(remoteProcessGroupResource.populateRemainingRemoteProcessGroupEntitiesContent(remoteProcessGroups));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -----------
    // connections
    // -----------

    /**
     * Creates a new connection.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestConnectionEntity   A connectionEntity.
     * @return A connectionEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/connections")
    @ApiOperation(
            value = "Creates a connection",
            response = ConnectionEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Write Source - /{component-type}/{uuid}"),
                    @Authorization(value = "Write Destination - /{component-type}/{uuid}")
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
    public Response createConnection(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The connection configuration details.",
                    required = true
            ) final ConnectionEntity requestConnectionEntity) {

        if (requestConnectionEntity == null || requestConnectionEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (requestConnectionEntity.getRevision() == null || (requestConnectionEntity.getRevision().getVersion() == null || requestConnectionEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Connection.");
        }

        if (requestConnectionEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("Connection ID cannot be specified.");
        }

        final List<PositionDTO> proposedBends = requestConnectionEntity.getComponent().getBends();
        if (proposedBends != null) {
            for (final PositionDTO proposedBend : proposedBends) {
                if (proposedBend.getX() == null || proposedBend.getY() == null) {
                    throw new IllegalArgumentException("The x and y coordinate of the each bend must be specified.");
                }
            }
        }

        if (requestConnectionEntity.getComponent().getParentGroupId() != null && !groupId.equals(requestConnectionEntity.getComponent().getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestConnectionEntity.getComponent().getParentGroupId(), groupId));
        }
        requestConnectionEntity.getComponent().setParentGroupId(groupId);

        // get the connection
        final ConnectionDTO requestConnection = requestConnectionEntity.getComponent();

        if (requestConnection.getSource() == null || requestConnection.getSource().getId() == null) {
            throw new IllegalArgumentException("The source of the connection must be specified.");
        }

        if (requestConnection.getSource().getType() == null) {
            throw new IllegalArgumentException("The type of the source of the connection must be specified.");
        }

        final ConnectableType sourceConnectableType;
        try {
            sourceConnectableType = ConnectableType.valueOf(requestConnection.getSource().getType());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Unrecognized source type %s. Expected values are [%s]",
                    requestConnection.getSource().getType(), StringUtils.join(ConnectableType.values(), ", ")));
        }

        if (requestConnection.getDestination() == null || requestConnection.getDestination().getId() == null) {
            throw new IllegalArgumentException("The destination of the connection must be specified.");
        }

        if (requestConnection.getDestination().getType() == null) {
            throw new IllegalArgumentException("The type of the destination of the connection must be specified.");
        }

        final ConnectableType destinationConnectableType;
        try {
            destinationConnectableType = ConnectableType.valueOf(requestConnection.getDestination().getType());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Unrecognized destination type %s. Expected values are [%s]",
                    requestConnection.getDestination().getType(), StringUtils.join(ConnectableType.values(), ", ")));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestConnectionEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConnectionEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                lookup -> {
                    // ensure write access to the group
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // explicitly handle RPGs differently as the connectable id can be ambiguous if self referencing
                    final Authorizable source;
                    if (ConnectableType.REMOTE_OUTPUT_PORT.equals(sourceConnectableType)) {
                        source = lookup.getRemoteProcessGroup(requestConnection.getSource().getGroupId());
                    } else {
                        source = lookup.getLocalConnectable(requestConnection.getSource().getId());
                    }

                    // ensure write access to the source
                    if (source == null) {
                        throw new ResourceNotFoundException("Cannot find source component with ID [" + requestConnection.getSource().getId() + "]");
                    }
                    source.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // explicitly handle RPGs differently as the connectable id can be ambiguous if self referencing
                    final Authorizable destination;
                    if (ConnectableType.REMOTE_INPUT_PORT.equals(destinationConnectableType)) {
                        destination = lookup.getRemoteProcessGroup(requestConnection.getDestination().getGroupId());
                    } else {
                        destination = lookup.getLocalConnectable(requestConnection.getDestination().getId());
                    }

                    // ensure write access to the destination
                    if (destination == null) {
                        throw new ResourceNotFoundException("Cannot find destination component with ID [" + requestConnection.getDestination().getId() + "]");
                    }

                    destination.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCreateConnection(groupId, requestConnection),
                connectionEntity -> {
                    final ConnectionDTO connection = connectionEntity.getComponent();

                    // set the connection id as appropriate
                    connection.setId(generateUuid());

                    // create the new relationship target
                    final Revision revision = getRevision(connectionEntity, connection.getId());
                    final ConnectionEntity entity = serviceFacade.createConnection(revision, groupId, connection);
                    connectionResource.populateRemainingConnectionEntityContent(entity);

                    // extract the href and build the response
                    String uri = entity.getUri();
                    return generateCreatedResponse(URI.create(uri), entity).build();
                }
        );
    }

    /**
     * Gets all the connections.
     *
     * @return A connectionsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/connections")
    @ApiOperation(
            value = "Gets all connections",
            response = ConnectionsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}")
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
    public Response getConnections(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // all of the relationships for the specified source processor
        Set<ConnectionEntity> connections = serviceFacade.getConnections(groupId);

        // create the client response entity
        ConnectionsEntity entity = new ConnectionsEntity();
        entity.setConnections(connectionResource.populateRemainingConnectionEntitiesContent(connections));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ----------------
    // snippet instance
    // ----------------

    /**
     * Copies the specified snippet within this ProcessGroup. The snippet instance that is instantiated cannot be referenced at a later time, therefore there is no
     * corresponding URI. Instead the request URI is returned.
     * <p>
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest request
     * @param groupId            The group id
     * @param requestCopySnippetEntity  The copy snippet request
     * @return A flowSnippetEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/snippet-instance")
    @ApiOperation(
            value = "Copies a snippet and discards it.",
            response = FlowEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - /{component-type}/{uuid} - For each component in the snippet and their descendant components"),
                    @Authorization(value = "Write - if the snippet contains any restricted Processors - /restricted-components")
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
    public Response copySnippet(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") String groupId,
            @ApiParam(
                    value = "The copy snippet request.",
                    required = true
            ) CopySnippetRequestEntity requestCopySnippetEntity) {

        // ensure the position has been specified
        if (requestCopySnippetEntity == null || requestCopySnippetEntity.getOriginX() == null || requestCopySnippetEntity.getOriginY() == null) {
            throw new IllegalArgumentException("The  origin position (x, y) must be specified");
        }

        if (requestCopySnippetEntity.getSnippetId() == null) {
            throw new IllegalArgumentException("The snippet id must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestCopySnippetEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestCopySnippetEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestCopySnippetEntity,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();
                    final SnippetAuthorizable snippet = authorizeSnippetUsage(lookup, groupId, requestCopySnippetEntity.getSnippetId(), false, true);

                    final Consumer<ComponentAuthorizable> authorizeRestricted = authorizable -> {
                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }
                    };

                    // consider each processor. note - this request will not create new controller services so we do not need to check
                    // for if there are not restricted controller services. it will however, need to authorize the user has access
                    // to any referenced services and this is done within authorizeSnippetUsage above.
                    // Also ensure that user has READ permissions to the Parameter Contexts in order to copy them.
                    snippet.getSelectedProcessors().forEach(authorizeRestricted);
                    for (final ProcessGroupAuthorizable groupAuthorizable : snippet.getSelectedProcessGroups()) {
                        groupAuthorizable.getEncapsulatedProcessors().forEach(authorizeRestricted);

                        final ParameterContext parameterContext = groupAuthorizable.getProcessGroup().getParameterContext();
                        if (parameterContext != null) {
                            parameterContext.authorize(authorizer, RequestAction.READ, user);
                        }

                        for (final ProcessGroupAuthorizable encapsulatedGroupAuth : groupAuthorizable.getEncapsulatedProcessGroups()) {
                            final ParameterContext encapsulatedGroupParameterContext = encapsulatedGroupAuth.getProcessGroup().getParameterContext();
                            if (encapsulatedGroupParameterContext != null) {
                                encapsulatedGroupParameterContext.authorize(authorizer, RequestAction.READ, user);
                            }

                        }
                    }
                },
                null,
                copySnippetRequestEntity -> {
                    // copy the specified snippet
                    final FlowEntity flowEntity = serviceFacade.copySnippet(
                            groupId, copySnippetRequestEntity.getSnippetId(), copySnippetRequestEntity.getOriginX(), copySnippetRequestEntity.getOriginY(), getIdGenerationSeed().orElse(null));

                    // get the snippet
                    final FlowDTO flow = flowEntity.getFlow();

                    // prune response as necessary
                    for (ProcessGroupEntity childGroupEntity : flow.getProcessGroups()) {
                        childGroupEntity.getComponent().setContents(null);
                    }

                    // create the response entity
                    populateRemainingSnippetContent(flow);

                    // generate the response
                    return generateCreatedResponse(getAbsolutePath(), flowEntity).build();
                }
        );
    }

    // -----------------
    // template instance
    // -----------------

    /**
     * Discovers the compatible bundle details for the components in the specified snippet.
     *
     * @param snippet the snippet
     */
    private void discoverCompatibleBundles(final FlowSnippetDTO snippet) {
        if (snippet.getProcessors() != null) {
            snippet.getProcessors().forEach(processor -> {
                final BundleCoordinate coordinate = serviceFacade.getCompatibleBundle(processor.getType(), processor.getBundle());
                processor.setBundle(new BundleDTO(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion()));
            });
        }

        if (snippet.getControllerServices() != null) {
            snippet.getControllerServices().forEach(controllerService -> {
                final BundleCoordinate coordinate = serviceFacade.getCompatibleBundle(controllerService.getType(), controllerService.getBundle());
                controllerService.setBundle(new BundleDTO(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion()));
            });
        }

        if (snippet.getProcessGroups() != null) {
            snippet.getProcessGroups().forEach(processGroup -> {
                discoverCompatibleBundles(processGroup.getContents());
            });
        }
    }

    /**
     * Instantiates the specified template within this ProcessGroup. The template instance that is instantiated cannot be referenced at a later time, therefore there is no
     * corresponding URI. Instead the request URI is returned.
     * <p>
     * Alternatively, we could have performed a PUT request. However, PUT requests are supposed to be idempotent and this endpoint is certainly not.
     *
     * @param httpServletRequest               request
     * @param groupId                          The group id
     * @param requestInstantiateTemplateRequestEntity The instantiate template request
     * @return A flowEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/template-instance")
    @ApiOperation(
            value = "Instantiates a template",
            response = FlowEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - /templates/{uuid}"),
                    @Authorization(value = "Write - if the template contains any restricted components - /restricted-components")
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
    public Response instantiateTemplate(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") String groupId,
            @ApiParam(
                    value = "The instantiate template request.",
                    required = true
            ) InstantiateTemplateRequestEntity requestInstantiateTemplateRequestEntity) {

        // ensure the position has been specified
        if (requestInstantiateTemplateRequestEntity == null || requestInstantiateTemplateRequestEntity.getOriginX() == null || requestInstantiateTemplateRequestEntity.getOriginY() == null) {
            throw new IllegalArgumentException("The origin position (x, y) must be specified.");
        }

        // ensure the template id was provided
        if (requestInstantiateTemplateRequestEntity.getTemplateId() == null) {
            throw new IllegalArgumentException("The template id must be specified.");
        }

        // ensure the template encoding version is valid
        if (requestInstantiateTemplateRequestEntity.getEncodingVersion() != null) {
            try {
                FlowEncodingVersion.parse(requestInstantiateTemplateRequestEntity.getEncodingVersion());
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("The template encoding version is not valid. The expected format is <number>.<number>");
            }
        }

        // populate the encoding version if necessary
        if (requestInstantiateTemplateRequestEntity.getEncodingVersion() == null) {
            // if the encoding version is not specified, use the latest encoding version as these options were
            // not available pre 1.x, will be overridden if populating from the underlying template below
            requestInstantiateTemplateRequestEntity.setEncodingVersion(TemplateDTO.MAX_ENCODING_VERSION);
        }

        // populate the component bundles if necessary
        if (requestInstantiateTemplateRequestEntity.getSnippet() == null) {
            // get the desired template in order to determine the supported bundles
            final TemplateDTO requestedTemplate = serviceFacade.exportTemplate(requestInstantiateTemplateRequestEntity.getTemplateId());
            final FlowSnippetDTO requestTemplateContents = requestedTemplate.getSnippet();

            // determine the compatible bundles to use for each component in this template, this ensures the nodes in the cluster
            // instantiate the components from the same bundles
            discoverCompatibleBundles(requestTemplateContents);

            // update the requested template as necessary - use the encoding version from the underlying template
            requestInstantiateTemplateRequestEntity.setEncodingVersion(requestedTemplate.getEncodingVersion());
            requestInstantiateTemplateRequestEntity.setSnippet(requestTemplateContents);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestInstantiateTemplateRequestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestInstantiateTemplateRequestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestInstantiateTemplateRequestEntity,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    // ensure write on the group
                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, user);

                    final Authorizable template = lookup.getTemplate(requestInstantiateTemplateRequestEntity.getTemplateId());
                    template.authorize(authorizer, RequestAction.READ, user);

                    // ensure read on the template
                    final TemplateContentsAuthorizable templateContents = lookup.getTemplateContents(requestInstantiateTemplateRequestEntity.getSnippet());

                    final Consumer<ComponentAuthorizable> authorizeRestricted = authorizable -> {
                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }
                    };

                    // ensure restricted access if necessary
                    templateContents.getEncapsulatedProcessors().forEach(authorizeRestricted);
                    templateContents.getEncapsulatedControllerServices().forEach(authorizeRestricted);

                    final Authorizable parameterContext = groupAuthorizable.getProcessGroup().getParameterContext();
                    if (parameterContext != null) {
                        AuthorizeParameterReference.authorizeParameterReferences(requestInstantiateTemplateRequestEntity.getSnippet(), authorizer, parameterContext, user);
                    }
                },
                () -> serviceFacade.verifyCanInstantiate(groupId, requestInstantiateTemplateRequestEntity.getSnippet()),
                instantiateTemplateRequestEntity -> {
                    final FlowSnippetDTO snippet = instantiateTemplateRequestEntity.getSnippet();

                    // Check if the snippet contains any public port violating public port unique constraint with the current flow
                    verifyPublicPortUniqueness(snippet);

                    // create the template and generate the json
                    final FlowEntity entity = serviceFacade.createTemplateInstance(groupId, instantiateTemplateRequestEntity.getOriginX(), instantiateTemplateRequestEntity.getOriginY(),
                        instantiateTemplateRequestEntity.getEncodingVersion(), snippet, getIdGenerationSeed().orElse(null));

                    final FlowDTO flowSnippet = entity.getFlow();

                    // prune response as necessary
                    for (ProcessGroupEntity childGroupEntity : flowSnippet.getProcessGroups()) {
                        childGroupEntity.getComponent().setContents(null);
                    }

                    // create the response entity
                    populateRemainingSnippetContent(flowSnippet);

                    // generate the response
                    return generateCreatedResponse(getAbsolutePath(), entity).build();
                }
        );
    }

    private void verifyPublicPortUniqueness(FlowSnippetDTO snippet) {
        snippet.getInputPorts().stream().filter(portDTO -> Boolean.TRUE.equals(portDTO.getAllowRemoteAccess()))
            .forEach(portDTO -> {
                try {
                    serviceFacade.verifyPublicInputPortUniqueness(portDTO.getId(), portDTO.getName());
                } catch (IllegalStateException e) {
                    throw toPublicPortUniqueConstraintViolationException("input", portDTO);
                }
            });

        snippet.getOutputPorts().stream().filter(portDTO -> Boolean.TRUE.equals(portDTO.getAllowRemoteAccess()))
            .forEach(portDTO -> {
                try {
                    serviceFacade.verifyPublicOutputPortUniqueness(portDTO.getId(), portDTO.getName());
                } catch (IllegalStateException e) {
                    throw toPublicPortUniqueConstraintViolationException("output", portDTO);
                }
            });

        snippet.getProcessGroups().forEach(processGroupDTO -> verifyPublicPortUniqueness(processGroupDTO.getContents()));
    }

    private IllegalStateException toPublicPortUniqueConstraintViolationException(final String portType, final PortDTO portDTO) {
        return new IllegalStateException(String.format("The %s port [%s] named '%s' will violate the public port unique constraint." +
            " Rename the existing port name, or the one in the template to instantiate the template in this flow.", portType, portDTO.getId(), portDTO.getName()));
    }

    // ---------
    // templates
    // ---------

    private SnippetAuthorizable authorizeSnippetUsage(final AuthorizableLookup lookup, final String groupId, final String snippetId,
                                                      final boolean authorizeTransitiveServices, final boolean authorizeParameterReferences) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure write access to the target process group
        lookup.getProcessGroup(groupId).getAuthorizable().authorize(authorizer, RequestAction.WRITE, user);

        // ensure read permission to every component in the snippet including referenced services
        final SnippetAuthorizable snippet = lookup.getSnippet(snippetId);
        authorizeSnippet(snippet, authorizer, lookup, RequestAction.READ, true, authorizeTransitiveServices, authorizeParameterReferences);
        return snippet;
    }

    /**
     * Creates a new template based off of the specified template.
     *
     * @param httpServletRequest          request
     * @param requestCreateTemplateRequestEntity request to create the template
     * @return A templateEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/templates")
    @ApiOperation(
            value = "Creates a template and discards the specified snippet.",
            response = TemplateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - /{component-type}/{uuid} - For each component in the snippet and their descendant components")
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
    public Response createTemplate(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The create template request.",
                    required = true
            ) final CreateTemplateRequestEntity requestCreateTemplateRequestEntity) {

        if (requestCreateTemplateRequestEntity == null || requestCreateTemplateRequestEntity.getSnippetId() == null) {
            throw new IllegalArgumentException("The snippet identifier must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestCreateTemplateRequestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestCreateTemplateRequestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestCreateTemplateRequestEntity,
                lookup -> {
                    authorizeSnippetUsage(lookup, groupId, requestCreateTemplateRequestEntity.getSnippetId(), true, false);
                },
                () -> serviceFacade.verifyCanAddTemplate(groupId, requestCreateTemplateRequestEntity.getName()),
                createTemplateRequestEntity -> {
                    // create the template and generate the json
                    final TemplateDTO template = serviceFacade.createTemplate(createTemplateRequestEntity.getName(), createTemplateRequestEntity.getDescription(),
                            createTemplateRequestEntity.getSnippetId(), groupId, getIdGenerationSeed());
                    templateResource.populateRemainingTemplateContent(template);

                    // build the response entity
                    final TemplateEntity entity = new TemplateEntity();
                    entity.setTemplate(template);

                    // build the response
                    return generateCreatedResponse(URI.create(template.getUri()), entity).build();
                }
        );
    }

    /**
     * Imports the specified template.
     *
     * @param httpServletRequest request
     * @param in                 The template stream
     * @return A templateEntity or an errorResponse XML snippet.
     * @throws InterruptedException if interrupted
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_XML)
    @Path("{id}/templates/upload")
    @ApiOperation(
            value = "Uploads a template",
            response = TemplateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
            }
    )
    @ApiImplicitParams(
            value = {
                    @ApiImplicitParam(
                            name = "template",
                            value = "The binary content of the template file being uploaded.",
                            required = true,
                            type = "file",
                            paramType = "formData")
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
    public Response uploadTemplate(
            @Context final HttpServletRequest httpServletRequest,
            @Context final UriInfo uriInfo,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @FormDataParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @FormDataParam("template") final InputStream in) throws InterruptedException {

        // unmarshal the template
        final TemplateDTO template;
        try {
            // TODO: Potentially refactor the template parsing to a service layer outside of the resource for web request handling
            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            XMLStreamReader xsr = XmlUtils.createSafeReader(in);
            JAXBElement<TemplateDTO> templateElement = unmarshaller.unmarshal(xsr, TemplateDTO.class);
            template = templateElement.getValue();
        } catch (JAXBException jaxbe) {
            logger.warn("An error occurred while parsing a template.", jaxbe);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"The specified template is not in a valid format.\"/>", Response.Status.BAD_REQUEST.getStatusCode());
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        } catch (IllegalArgumentException iae) {
            logger.warn("Unable to import template.", iae);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"%s\"/>", Response.Status.BAD_REQUEST.getStatusCode(), sanitizeErrorResponse(iae.getMessage()));
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        } catch (Exception e) {
            logger.warn("An error occurred while importing a template.", e);
            String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"Unable to import the specified template: %s\"/>",
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), sanitizeErrorResponse(e.getMessage()));
            return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        // build the response entity
        TemplateEntity entity = new TemplateEntity();
        entity.setTemplate(template);
        entity.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);

        if (isReplicateRequest()) {
            // convert request accordingly
            final UriBuilder uriBuilder = uriInfo.getBaseUriBuilder();
            uriBuilder.segment("process-groups", groupId, "templates", "import");
            final URI importUri = uriBuilder.build();

            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_XML);

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
            // to the cluster nodes themselves.
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                return getRequestReplicator().replicate(HttpMethod.POST, importUri, entity, getHeaders(headersToOverride)).awaitMergedResponse().getResponse();
            } else {
                return getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.POST, importUri, entity, getHeaders(headersToOverride)).awaitMergedResponse().getResponse();
            }
        }

        // otherwise import the template locally
        return importTemplate(httpServletRequest, groupId, entity);
    }

    /**
     * Returns the sanitized error response which can safely be displayed on the error page.
     *
     * @param errorResponse the initial error response
     * @return the HTML-escaped error response
     */
    private String sanitizeErrorResponse(String errorResponse) {
        if (errorResponse == null || StringUtils.isEmpty(errorResponse)) {
            return "";
        }

        return StringEscapeUtils.escapeHtml4(errorResponse);
    }

    /**
     * Imports the specified template.
     *
     * @param httpServletRequest request
     * @param requestTemplateEntity     A templateEntity.
     * @return A templateEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @Path("{id}/templates/import")
    @ApiOperation(
            value = "Imports a template",
            response = TemplateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response importTemplate(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            final TemplateEntity requestTemplateEntity) {

        // verify the template was specified
        if (requestTemplateEntity == null || requestTemplateEntity.getTemplate() == null || requestTemplateEntity.getTemplate().getSnippet() == null) {
            throw new IllegalArgumentException("Template details must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestTemplateEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestTemplateEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestTemplateEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanAddTemplate(groupId, requestTemplateEntity.getTemplate().getName()),
                templateEntity -> {
                    try {
                        // import the template
                        final TemplateDTO template = serviceFacade.importTemplate(templateEntity.getTemplate(), groupId, getIdGenerationSeed());
                        templateResource.populateRemainingTemplateContent(template);

                        // build the response entity
                        TemplateEntity entity = new TemplateEntity();
                        entity.setTemplate(template);

                        // build the response
                        return generateCreatedResponse(URI.create(template.getUri()), entity).build();
                    } catch (IllegalArgumentException | IllegalStateException e) {
                        logger.info("Unable to import template: " + e);
                        String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"%s\"/>", Response.Status.BAD_REQUEST.getStatusCode(), sanitizeErrorResponse(e.getMessage()));
                        return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
                    } catch (Exception e) {
                        logger.warn("An error occurred while importing a template.", e);
                        String responseXml = String.format("<errorResponse status=\"%s\" statusText=\"Unable to import the specified template: %s\"/>",
                                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), sanitizeErrorResponse(e.getMessage()));
                        return Response.status(Response.Status.OK).entity(responseXml).type("application/xml").build();
                    }
                }
        );
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Creates a new Controller Service.
     *
     * @param httpServletRequest      request
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/controller-services")
    @ApiOperation(
            value = "Creates a new controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @Authorization(value = "Write - if the Controller Service is restricted - /restricted-components")
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
    public Response createControllerService(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The controller service configuration details.",
                    required = true
            ) final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null || requestControllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null
                || (requestControllerServiceEntity.getRevision().getVersion() == null || requestControllerServiceEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Controller service.");
        }

        final ControllerServiceDTO requestControllerService = requestControllerServiceEntity.getComponent();
        if (requestControllerService.getId() != null) {
            throw new IllegalArgumentException("Controller service ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestControllerService.getType())) {
            throw new IllegalArgumentException("The type of controller service to create must be specified.");
        }

        if (requestControllerService.getParentGroupId() != null && !groupId.equals(requestControllerService.getParentGroupId())) {
            throw new IllegalArgumentException(String.format("If specified, the parent process group id %s must be the same as specified in the URI %s",
                    requestControllerService.getParentGroupId(), groupId));
        }
        requestControllerService.setParentGroupId(groupId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestControllerServiceEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestControllerServiceEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, user);

                    final Authorizable parameterContext = groupAuthorizable.getProcessGroup().getParameterContext();
                    if (parameterContext != null) {
                        AuthorizeParameterReference.authorizeParameterReferences(requestControllerService.getProperties(), authorizer, parameterContext, user);
                    }

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestControllerService.getType(), requestControllerService.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestControllerService.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestControllerService.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateControllerService(requestControllerService),
                controllerServiceEntity -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();

                    // set the processor id as appropriate
                    controllerService.setId(generateUuid());

                    // create the controller service and generate the json
                    final Revision revision = getRevision(controllerServiceEntity, controllerService.getId());
                    final ControllerServiceEntity entity = serviceFacade.createControllerService(revision, groupId, controllerService);
                    controllerServiceResource.populateRemainingControllerServiceEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Initiates the request to replace the Process Group with the given ID with the Process Group in the given import entity
     *
     * @param groupId          The id of the process group to replace
     * @param importEntity     A request entity containing revision info and the process group to replace with
     * @return A ProcessGroupReplaceRequestEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/replace-requests")
    @ApiOperation(
            value = "Initiate the Replace Request of a Process Group with the given ID",
            response = ProcessGroupReplaceRequestEntity.class,
            notes = "This will initiate the action of replacing a process group with the given process group. This can be a lengthy "
                    + "process, as it will stop any Processors and disable any Controller Services necessary to perform the action and then restart them. As a result, "
                    + "the endpoint will immediately return a ProcessGroupReplaceRequestEntity, and the process of replacing the flow will occur "
                    + "asynchronously in the background. The client may then periodically poll the status of the request by issuing a GET request to "
                    + "/process-groups/replace-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to "
                    + "/process-groups/replace-requests/{requestId}. " + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}"),
                    @Authorization(value = "Write - /process-groups/{uuid}"),
                    @Authorization(value = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                    @Authorization(value = "Write - /{component-type}/{uuid} - For all encapsulated components"),
                    @Authorization(value = "Write - if the template contains any restricted components - /restricted-components"),
                    @Authorization(value = "Read - /parameter-contexts/{uuid} - For any Parameter Context that is referenced by a Property that is changed, added, or removed")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response initiateReplaceProcessGroup(@ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId,
                                                @ApiParam(value = "The process group replace request entity", required = true) final ProcessGroupImportEntity importEntity) {
        if (importEntity == null) {
            throw new IllegalArgumentException("Process Group Import Entity is required");
        }

        // replacing a flow under version control is not permitted via import. Versioned flows have additional requirements to allow
        // them only to be replaced by a different version of the same flow.
        if (serviceFacade.isAnyProcessGroupUnderVersionControl(groupId)) {
            throw new IllegalStateException("Cannot replace a Process Group via import while it or its descendants are under Version Control.");
        }

        final VersionedFlowSnapshot versionedFlowSnapshot = importEntity.getVersionedFlowSnapshot();
        if (versionedFlowSnapshot == null) {
            throw new IllegalArgumentException("Versioned Flow Snapshot must be supplied");
        }

        // remove any registry-specific versioning content which could be present if the flow was exported from registry
        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.setSnapshotMetadata(null);
        sanitizeRegistryInfo(versionedFlowSnapshot.getFlowContents());

        return initiateFlowUpdate(groupId, importEntity, true, "replace-requests",
                "/nifi-api/process-groups/" + groupId + "/flow-contents", importEntity::getVersionedFlowSnapshot);
    }

    /**
     * Recursively clear the registry info in the given versioned process group and all nested versioned process groups
     *
     * @param versionedProcessGroup the process group to sanitize
     */
    private void sanitizeRegistryInfo(final VersionedProcessGroup versionedProcessGroup) {
        versionedProcessGroup.setVersionedFlowCoordinates(null);

        for (final VersionedProcessGroup innerVersionedProcessGroup : versionedProcessGroup.getProcessGroups()) {
            sanitizeRegistryInfo(innerVersionedProcessGroup);
        }
    }

    /**
     * Uploads the specified versioned flow definition and adds it to a new process group.
     *
     * @param in The flow definition stream
     * @return A processGroupEntity
     * @throws IOException if there is an error during deserialization of the InputStream
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups/upload")
    @ApiOperation(
            value = "Uploads a versioned flow definition and creates a process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response uploadProcessGroup(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            @ApiParam(
                    value = "The process group name.",
                    required = true
            )
            @FormDataParam("groupName") final String groupName,
            @ApiParam(
                    value = "The process group X position.",
                    required = true
            )
            @FormDataParam("positionX") final Double positionX,
            @ApiParam(
                    value = "The process group Y position.",
                    required = true
            )
            @FormDataParam("positionY") final Double positionY,
            @ApiParam(
                    value = "The client id.",
                    required = true
            )
            @FormDataParam("clientId") final String clientId,
            @ApiParam(
                    value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.",
                    required = false
            )
            @FormDataParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @FormDataParam("file") final InputStream in) throws InterruptedException {

        // ensure the group name is specified
        if (StringUtils.isBlank(groupName)) {
            throw new IllegalArgumentException("The process group name is required.");
        }

        if (StringUtils.isBlank(groupId)) {
            throw new IllegalArgumentException("The parent process group id is required");
        }

        if (positionX == null) {
            throw new IllegalArgumentException("The x coordinate of the proposed position must be specified.");
        }

        if (positionY == null) {
            throw new IllegalArgumentException("The y coordinate of the proposed position must be specified.");
        }

        if (StringUtils.isBlank(clientId)) {
            throw new IllegalArgumentException("The client id must be specified");
        }

        // deserialize InputStream to a VersionedFlowSnapshot
        VersionedFlowSnapshot deserializedSnapshot;

        try {
            deserializedSnapshot = MAPPER.readValue(in, VersionedFlowSnapshot.class);
        } catch (IOException e) {
            logger.warn("Deserialization of uploaded JSON failed", e);
            throw new IllegalArgumentException("Deserialization of uploaded JSON failed", e);
        }

        // clear Registry info
        sanitizeRegistryInfo(deserializedSnapshot.getFlowContents());

        // resolve Bundle info
        serviceFacade.discoverCompatibleBundles(deserializedSnapshot.getFlowContents());

        // if there are any Controller Services referenced that are inherited from the parent group,
        // resolve those to point to the appropriate Controller Service, if we are able to.
        serviceFacade.resolveInheritedControllerServices(deserializedSnapshot, groupId, NiFiUserUtils.getNiFiUser());

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        // create a PositionDTO
        final PositionDTO positionDTO = new PositionDTO();
        positionDTO.setX(positionX);
        positionDTO.setY(positionY);

        // create a RevisionDTO
        RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId(clientId);
        revisionDTO.setVersion((long) 0);

        // build the response entity for a replicate request
        ProcessGroupUploadEntity pgUploadEntity = new ProcessGroupUploadEntity();
        pgUploadEntity.setGroupId(groupId);
        pgUploadEntity.setGroupName(groupName);
        pgUploadEntity.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);
        pgUploadEntity.setFlowSnapshot(deserializedSnapshot);
        pgUploadEntity.setPositionDTO(positionDTO);
        pgUploadEntity.setRevisionDTO(revisionDTO);

        // replicate the request
        if (isReplicateRequest()) {
            // convert request accordingly
            final UriBuilder uriBuilder = uriInfo.getBaseUriBuilder();
            uriBuilder.segment("process-groups", groupId, "process-groups", "import");
            final URI importUri = uriBuilder.build();

            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
            // to the cluster nodes themselves.
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                return getRequestReplicator().replicate(HttpMethod.POST, importUri, pgUploadEntity, getHeaders(headersToOverride)).awaitMergedResponse().getResponse();
            } else {
                return getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.POST, importUri, pgUploadEntity, getHeaders(headersToOverride)).awaitMergedResponse().getResponse();
            }
        }

        // otherwise import the process group locally
        return importProcessGroup(groupId, pgUploadEntity);
    }

    /**
     * Imports the specified process group.
     *
     * @param processGroupUploadEntity     A ProcessGroupUploadEntity.
     * @return A processGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/process-groups/import")
    @ApiOperation(
            value = "Imports a specified process group",
            response = ProcessGroupEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /process-groups/{uuid}")
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
    public Response importProcessGroup(
            @ApiParam(
                    value = "The process group id.",
                    required = true
            )
            @PathParam("id") final String groupId,
            final ProcessGroupUploadEntity processGroupUploadEntity) {

        // verify the process group was specified
        if (processGroupUploadEntity == null || processGroupUploadEntity.getFlowSnapshot() == null) {
            throw new IllegalArgumentException("Process group details must be specified.");
        }

        final VersionedFlowSnapshot versionedFlowSnapshot = processGroupUploadEntity.getFlowSnapshot();

        // clear Registry info
        sanitizeRegistryInfo(versionedFlowSnapshot.getFlowContents());

        // resolve Bundle info
        serviceFacade.discoverCompatibleBundles(versionedFlowSnapshot.getFlowContents());

        // if there are any Controller Services referenced that are inherited from the parent group,
        // resolve those to point to the appropriate Controller Service, if we are able to.
        serviceFacade.resolveInheritedControllerServices(versionedFlowSnapshot, groupId, NiFiUserUtils.getNiFiUser());

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, processGroupUploadEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(processGroupUploadEntity.getDisconnectedNodeAcknowledged());
        }

        // create a new ProcessGroupEntity
        final ProcessGroupEntity newProcessGroupEntity = createProcessGroupEntity(groupId, processGroupUploadEntity.getGroupName(), processGroupUploadEntity.getPositionDTO(), versionedFlowSnapshot);

        return withWriteLock(
                serviceFacade,
                newProcessGroupEntity,
                lookup -> authorizeAccess(groupId, newProcessGroupEntity, lookup),
                () -> {
                    final VersionedFlowSnapshot newVersionedFlowSnapshot = newProcessGroupEntity.getVersionedFlowSnapshot();
                    if (newVersionedFlowSnapshot != null) {
                        serviceFacade.verifyComponentTypes(newVersionedFlowSnapshot.getFlowContents());
                    }
                },
                processGroupEntity -> {
                    final ProcessGroupDTO processGroup = processGroupEntity.getComponent();

                    // set the processor id as appropriate
                    processGroup.setId(generateUuid());

                    // get the versioned flow
                    final VersionedFlowSnapshot flowSnapshot = processGroupEntity.getVersionedFlowSnapshot();

                    // create the process group contents
                    final Revision revision = new Revision((long) 0, processGroupUploadEntity.getRevisionDTO().getClientId(), processGroup.getId());

                    ProcessGroupEntity entity = serviceFacade.createProcessGroup(revision, groupId, processGroup);

                    if (flowSnapshot != null) {
                        final RevisionDTO revisionDto = entity.getRevision();
                        final String newGroupId = entity.getComponent().getId();
                        final Revision newGroupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), newGroupId);

                        // We don't want the Process Group's position to be updated because we want to keep the position where the user
                        // placed the Process Group. We do not want to use the name of the Process Group that is in the Flow Contents.
                        // To accomplish this, we call updateProcessGroupContents() passing 'false' for the updateSettings flag, set
                        // the Process Group name, and null out the position.
                        flowSnapshot.getFlowContents().setPosition(null);
                        flowSnapshot.getFlowContents().setName(processGroupUploadEntity.getGroupName());

                        entity = serviceFacade.updateProcessGroupContents(newGroupRevision, newGroupId, null, flowSnapshot,
                                getIdGenerationSeed().orElse(null), false, false, true);
                    }

                    populateRemainingProcessGroupEntityContent(entity);

                    // generate a 201 created response
                    String uri = entity.getUri();
                    return generateCreatedResponse(URI.create(uri), entity).build();
                }
        );

    }


    /**
     * Replace the Process Group contents with the given ID with the specified Process Group contents.
     *
     * This is the endpoint used in a cluster update replication scenario.
     *
     * @param groupId          The id of the process group to replace
     * @param importEntity     A request entity containing revision info and the process group to replace with
     * @return A ProcessGroupImportEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/flow-contents")
    @ApiOperation(
            value = "Replace Process Group contents with the given ID with the specified Process Group contents",
            response = ProcessGroupImportEntity.class,
            notes = "This endpoint is used for replication within a cluster, when replacing a flow with a new flow. It expects that the flow being"
                    + "replaced is not under version control and that the given snapshot will not modify any Processor that is currently running "
                    + "or any Controller Service that is enabled. "
                    + NON_GUARANTEED_ENDPOINT,
            authorizations = {
                    @Authorization(value = "Read - /process-groups/{uuid}"),
                    @Authorization(value = "Write - /process-groups/{uuid}")
            })
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
            @ApiResponse(code = 401, message = "Client could not be authenticated."),
            @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
            @ApiResponse(code = 404, message = "The specified resource could not be found."),
            @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
    })
    public Response replaceProcessGroup(@ApiParam(value = "The process group id.", required = true) @PathParam("id") final String groupId,
                                        @ApiParam(value = "The process group replace request entity.", required = true) final ProcessGroupImportEntity importEntity) {
        // Verify the request
        if (importEntity == null) {
            throw new IllegalArgumentException("Process Group Import Entity is required");
        }

        final RevisionDTO revisionDto = importEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified.");
        }

        final VersionedFlowSnapshot requestFlowSnapshot = importEntity.getVersionedFlowSnapshot();
        if (requestFlowSnapshot == null) {
            throw new IllegalArgumentException("Versioned Flow Snapshot must be supplied.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, importEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(importEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(importEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
                serviceFacade,
                importEntity,
                requestRevision,
                lookup -> {
                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    // We do not enforce that the Process Group is 'not dirty' because at this point,
                    // the client has explicitly indicated the dataflow that the Process Group should
                    // provide and provided the Revision to ensure that they have the most up-to-date
                    // view of the Process Group.
                    serviceFacade.verifyCanUpdate(groupId, requestFlowSnapshot, true, false);
                },
                (revision, entity) -> {
                    final ProcessGroupEntity updatedGroup =
                            performUpdateFlow(groupId, revision, importEntity, entity.getVersionedFlowSnapshot(),
                                    getIdGenerationSeed().orElse(null), false, true);

                    // response to replication request is an entity with revision info but no versioned flow snapshot
                    final ProcessGroupImportEntity responseEntity = new ProcessGroupImportEntity();
                    responseEntity.setProcessGroupRevision(updatedGroup.getRevision());

                    return generateOkResponse(responseEntity).build();
                });
    }

    /**
     * Retrieve a request to replace a Process Group by request ID.
     *
     * @param replaceRequestId  The ID of the replace request
     * @return A ProcessGroupReplaceRequestEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("replace-requests/{id}")
    @ApiOperation(
            value = "Returns the Replace Request with the given ID",
            response = ProcessGroupReplaceRequestEntity.class,
            notes = "Returns the Replace Request with the given ID. Once a Replace Request has been created by performing a POST to /process-groups/{id}/replace-requests, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. "
                    + NON_GUARANTEED_ENDPOINT,
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
    public Response getReplaceProcessGroupRequest(
            @ApiParam("The ID of the Replace Request") @PathParam("id") final String replaceRequestId) {
        return retrieveFlowUpdateRequest("replace-requests", replaceRequestId);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("replace-requests/{id}")
    @ApiOperation(
            value = "Deletes the Replace Request with the given ID",
            response = ProcessGroupReplaceRequestEntity.class,
            notes = "Deletes the Replace Request with the given ID. After a request is created via a POST to /process-groups/{id}/replace-requests, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Replace process has completed. If the request is deleted before the request "
                    + "completes, then the Replace request will finish the step that it is currently performing and then will cancel any subsequent steps. "
                    + NON_GUARANTEED_ENDPOINT,
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
    public Response deleteReplaceProcessGroupRequest(
            @ApiParam(value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.", required = false)
                @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @ApiParam("The ID of the Update Request") @PathParam("id") final String replaceRequestId) {
        return deleteFlowUpdateRequest("replace-requests", replaceRequestId, disconnectedNodeAcknowledged.booleanValue());
    }

    /**
     * Perform actual flow update of the specified flow. This is used for the initial flow update and replication updates.
     */
    @Override
    protected ProcessGroupEntity performUpdateFlow(final String groupId, final Revision revision, final ProcessGroupImportEntity requestEntity,
                                                   final VersionedFlowSnapshot flowSnapshot, final String idGenerationSeed,
                                                   final boolean verifyNotModified, final boolean updateDescendantVersionedFlows) {
        logger.info("Replacing Process Group with ID {} with imported Process Group with ID {}", groupId, flowSnapshot.getFlowContents().getIdentifier());

        // Update Process Group to the new flow (including name) and update variable registry with any Variables that were added or removed
        return serviceFacade.updateProcessGroupContents(revision, groupId, null, flowSnapshot, idGenerationSeed, verifyNotModified,
                true, updateDescendantVersionedFlows);
    }

    /**
     * Create the entity that is used for update flow replication. The initial replace request entity can be re-used for the replication request.
     */
    @Override
    protected Entity createReplicateUpdateFlowEntity(final Revision revision, final ProcessGroupImportEntity requestEntity,
                                                     final VersionedFlowSnapshot flowSnapshot) {
        return requestEntity;
    }

    /**
     * Create the entity that captures the status and result of a replace request
     *
     * @return a new instance of a ProcessGroupReplaceRequestEntity
     */
    @Override
    protected ProcessGroupReplaceRequestEntity createUpdateRequestEntity() {
        return new ProcessGroupReplaceRequestEntity();
    }

    /**
     * Finalize a completed update request for an existing replace request. This is used when retrieving and deleting a replace request.
     *
     * A completed request will contain the updated VersionedFlowSnapshot
     *
     * @param requestEntity     the request entity to finalize
     */
    @Override
    protected void finalizeCompletedUpdateRequest(final ProcessGroupReplaceRequestEntity requestEntity) {
        final ProcessGroupReplaceRequestDTO updateRequestDto = requestEntity.getRequest();
        if (updateRequestDto.isComplete()) {
            final VersionedFlowSnapshot versionedFlowSnapshot =
                    serviceFacade.getCurrentFlowSnapshotByGroupId(updateRequestDto.getProcessGroupId());
            requestEntity.setVersionedFlowSnapshot(versionedFlowSnapshot);
        }
    }

    private static class UpdateVariableRegistryRequestWrapper extends Entity {
        private final Set<AffectedComponentEntity> allAffectedComponents;
        private final List<AffectedComponentDTO> activeAffectedProcessors;
        private final List<AffectedComponentDTO> activeAffectedServices;
        private final VariableRegistryEntity variableRegistryEntity;

        public UpdateVariableRegistryRequestWrapper(final Set<AffectedComponentEntity> allAffectedComponents, final List<AffectedComponentDTO> activeAffectedProcessors,
                                                    final List<AffectedComponentDTO> activeAffectedServices, VariableRegistryEntity variableRegistryEntity) {

            this.allAffectedComponents = allAffectedComponents;
            this.activeAffectedProcessors = activeAffectedProcessors;
            this.activeAffectedServices = activeAffectedServices;
            this.variableRegistryEntity = variableRegistryEntity;
        }

        public Set<AffectedComponentEntity> getAllAffectedComponents() {
            return allAffectedComponents;
        }

        public List<AffectedComponentDTO> getActiveAffectedProcessors() {
            return activeAffectedProcessors;
        }

        public List<AffectedComponentDTO> getActiveAffectedServices() {
            return activeAffectedServices;
        }

        public VariableRegistryEntity getVariableRegistryEntity() {
            return variableRegistryEntity;
        }
    }

    /**
     * Creates a new ProcessGroupEntity with the specified VersionedFlowSnapshot.
     *
     * @param groupId               the group id string
     * @param groupName             the process group name string
     * @param positionDTO           the process group PositionDTO
     * @param deserializedSnapshot  the deserialized snapshot
     *
     * @return a new ProcessGroupEntity
     */
    private ProcessGroupEntity createProcessGroupEntity(
            String groupId, String groupName, PositionDTO positionDTO, VersionedFlowSnapshot deserializedSnapshot) {

        final ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();

        // create a ProcessGroupDTO
        final ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setParentGroupId(groupId);
        processGroupDTO.setName(groupName);

        processGroupEntity.setComponent(processGroupDTO);
        processGroupEntity.setVersionedFlowSnapshot(deserializedSnapshot);

        // set the ProcessGroupEntity position
        processGroupEntity.getComponent().setPosition(positionDTO);

        return processGroupEntity;
    }

    /**
     * Authorizes access to a Parameter Context and RestrictedComponents resource.
     *
     * @param groupId               the group id string
     * @param processGroupEntity    the ProcessGroupEntity
     * @param lookup                the lookup
     */
    private void authorizeAccess(String groupId, ProcessGroupEntity processGroupEntity, AuthorizableLookup lookup) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
        processGroup.authorize(authorizer, RequestAction.WRITE, user);

        // if request specifies a Parameter Context, need to authorize that user has READ policy for the Parameter Context.
        final ParameterContextReferenceEntity referencedParamContext = processGroupEntity.getComponent().getParameterContext();
        if (referencedParamContext != null && referencedParamContext.getId() != null) {
            lookup.getParameterContext(referencedParamContext.getId()).authorize(authorizer, RequestAction.READ, user);
        }

        // if any of the components is a Restricted Component, then we must authorize the user
        // for write access to the RestrictedComponents resource
        final VersionedFlowSnapshot versionedFlowSnapshot = processGroupEntity.getVersionedFlowSnapshot();
        if (versionedFlowSnapshot != null) {
            final Set<ConfigurableComponent> restrictedComponents = FlowRegistryUtils.getRestrictedComponents(versionedFlowSnapshot.getFlowContents(), serviceFacade);
            restrictedComponents.forEach(restrictedComponent -> {
                final ComponentAuthorizable restrictedComponentAuthorizable = lookup.getConfigurableComponent(restrictedComponent);
                authorizeRestrictions(authorizer, restrictedComponentAuthorizable);
            });

            final Map<String, VersionedParameterContext> parameterContexts = versionedFlowSnapshot.getParameterContexts();
            if (parameterContexts != null) {
                parameterContexts.values().forEach(context -> AuthorizeParameterReference.authorizeParameterContextAddition(context, serviceFacade, authorizer, lookup, user));
            }
        }
    }

    // setters

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

    public void setControllerServiceResource(ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    private static class DropEntity extends Entity {
        final String entityId;
        final String dropRequestId;

        public DropEntity(String entityId, String dropRequestId) {
            this.entityId = entityId;
            this.dropRequestId = dropRequestId;
        }

        public String getEntityId() {
            return entityId;
        }

        public String getDropRequestId() {
            return dropRequestId;
        }
    }
}
