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
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
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
 * RESTful endpoint for managing a Flow Analysis Rule.
 */
@Path("/flow-analysis-rules")
@Api(
        value = "/flow-analysis-rules",
        description = "Endpoint for managing a Flow Analysis Rule."
)
public class FlowAnalysisRuleResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified flow analysis rules.
     *
     * @param flowAnalysisRuleEntities flow analysis rules
     * @return dtos
     */
    public Set<FlowAnalysisRuleEntity> populateRemainingFlowAnalysisRuleEntitiesContent(final Set<FlowAnalysisRuleEntity> flowAnalysisRuleEntities) {
        for (FlowAnalysisRuleEntity flowAnalysisRuleEntity : flowAnalysisRuleEntities) {
            populateRemainingFlowAnalysisRuleEntityContent(flowAnalysisRuleEntity);
        }
        return flowAnalysisRuleEntities;
    }

    /**
     * Populate the uri's for the specified flow analysis rule.
     *
     * @param flowAnalysisRuleEntity flow analysis rule
     * @return dtos
     */
    public FlowAnalysisRuleEntity populateRemainingFlowAnalysisRuleEntityContent(final FlowAnalysisRuleEntity flowAnalysisRuleEntity) {
        flowAnalysisRuleEntity.setUri(generateResourceUri("flow-analysis-rules", flowAnalysisRuleEntity.getId()));

        // populate the remaining content
        if (flowAnalysisRuleEntity.getComponent() != null) {
            populateRemainingFlowAnalysisRuleContent(flowAnalysisRuleEntity.getComponent());
        }
        return flowAnalysisRuleEntity;
    }

    /**
     * Populates the uri for the specified flow analysis rule.
     */
    public FlowAnalysisRuleDTO populateRemainingFlowAnalysisRuleContent(final FlowAnalysisRuleDTO flowAnalysisRule) {
        final BundleDTO bundle = flowAnalysisRule.getBundle();
        if (bundle == null) {
            return flowAnalysisRule;
        }

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(flowAnalysisRule.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(flowAnalysisRule.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.FlowAnalysisRuleConfiguration.equals(uiExtension.getExtensionType())) {
                    flowAnalysisRule.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                }
            }
        }

        return flowAnalysisRule;
    }

    /**
     * Retrieves the specified flow analysis rule.
     *
     * @param id The id of the flow analysis rule to retrieve
     * @return A flowAnalysisRuleEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a flow analysis rule",
            response = FlowAnalysisRuleEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /flow-analysis-rules/{uuid}")
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
    public Response getFlowAnalysisRule(
            @ApiParam(
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable flowAnalysisRule = lookup.getFlowAnalysisRule(id).getAuthorizable();
            flowAnalysisRule.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the flow analysis rule
        final FlowAnalysisRuleEntity flowAnalysisRule = serviceFacade.getFlowAnalysisRule(id);
        populateRemainingFlowAnalysisRuleEntityContent(flowAnalysisRule);

        return generateOkResponse(flowAnalysisRule).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id           The id of the flow analysis rule.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    @ApiOperation(
            value = "Gets a flow analysis rule property descriptor",
            response = PropertyDescriptorEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /flow-analysis-rules/{uuid}")
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
                    value = "The flow analysis rule id.",
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
            final Authorizable flowAnalysisRule = lookup.getFlowAnalysisRule(id).getAuthorizable();
            flowAnalysisRule.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getFlowAnalysisRulePropertyDescriptor(id, propertyName);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a flow analysis rule.
     *
     * @param id The id of the flow analysis rule
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    @ApiOperation(
            value = "Gets the state for a flow analysis rule",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /flow-analysis-rules/{uuid}")
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
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable flowAnalysisRule = lookup.getFlowAnalysisRule(id).getAuthorizable();
            flowAnalysisRule.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getFlowAnalysisRuleState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a flow analysis rule.
     *
     * @param httpServletRequest servlet request
     * @param id                 The id of the flow analysis rule
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @ApiOperation(
            value = "Clears the state for a flow analysis rule",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /flow-analysis-rules/{uuid}")
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
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final FlowAnalysisRuleEntity requestReportTaskEntity = new FlowAnalysisRuleEntity();
        requestReportTaskEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestReportTaskEntity,
                lookup -> {
                    final Authorizable processor = lookup.getFlowAnalysisRule(id).getAuthorizable();
                    processor.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearFlowAnalysisRuleState(id),
                (flowAnalysisRuleEntity) -> {
                    // get the component state
                    serviceFacade.clearFlowAnalysisRuleState(flowAnalysisRuleEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified a Flow Analysis Rule.
     *
     * @param httpServletRequest  request
     * @param id                  The id of the flow analysis rule to update.
     * @param requestFlowAnalysisRuleEntity A flowAnalysisRuleEntity.
     * @return A flowAnalysisRuleEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates a flow analysis rule",
            response = FlowAnalysisRuleEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /flow-analysis-rules/{uuid}"),
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
    public Response updateFlowAnalysisRule(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The flow analysis rule configuration details.",
                    required = true
            ) final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity) {

        if (requestFlowAnalysisRuleEntity == null || requestFlowAnalysisRuleEntity.getComponent() == null) {
            throw new IllegalArgumentException("Flow analysis rule details must be specified.");
        }

        if (requestFlowAnalysisRuleEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final FlowAnalysisRuleDTO requestFlowAnalysisRuleDTO = requestFlowAnalysisRuleEntity.getComponent();
        if (!id.equals(requestFlowAnalysisRuleDTO.getId())) {
            throw new IllegalArgumentException(String.format("The flow analysis rule id (%s) in the request body does not equal the "
                    + "flow analysis rule id of the requested resource (%s).", requestFlowAnalysisRuleDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestFlowAnalysisRuleEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFlowAnalysisRuleEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestFlowAnalysisRuleEntity, id);
        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                requestRevision,
                lookup -> {
                    // authorize flow analysis rule
                    final ComponentAuthorizable authorizable = lookup.getFlowAnalysisRule(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestFlowAnalysisRuleDTO.getProperties(), authorizable, authorizer, lookup);
                },
                () -> serviceFacade.verifyUpdateFlowAnalysisRule(requestFlowAnalysisRuleDTO),
                (revision, flowAnalysisRuleEntity) -> {
                    final FlowAnalysisRuleDTO flowAnalysisRuleDTO = flowAnalysisRuleEntity.getComponent();

                    // update the flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.updateFlowAnalysisRule(revision, flowAnalysisRuleDTO);
                    populateRemainingFlowAnalysisRuleEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified flow analysis rule.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the flow analysis rule to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a flow analysis rule",
            response = FlowAnalysisRuleEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /flow-analysis-rules/{uuid}"),
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
    public Response removeFlowAnalysisRule(
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
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity = new FlowAnalysisRuleEntity();
        requestFlowAnalysisRuleEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable flowAnalysisRule = lookup.getFlowAnalysisRule(id);

                    // ensure write permission to the flow analysis rule
                    flowAnalysisRule.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    flowAnalysisRule.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(flowAnalysisRule, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteFlowAnalysisRule(id),
                (revision, flowAnalysisRuleEntity) -> {
                    // delete the specified flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.deleteFlowAnalysisRule(revision, flowAnalysisRuleEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified FlowAnalysisRule with the specified values.
     *
     * @param httpServletRequest  request
     * @param id                  The id of the flow analysis rule to update.
     * @param requestRunStatus A runStatusEntity.
     * @return A flowAnalysisRuleEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/run-status")
    @ApiOperation(
            value = "Updates run status of a flow analysis rule",
            response = FlowAnalysisRuleEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /flow-analysis-rules/{uuid} or  or /operation/flow-analysis-rules/{uuid}")
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
                    value = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The flow analysis rule run status.",
                    required = true
            ) final FlowAnalysisRuleRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Flow analysis rule run status must be specified.");
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
                    // authorize flow analysis rule
                    final Authorizable authorizable = lookup.getFlowAnalysisRule(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateFlowAnalysisRule(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, flowAnalysisRuleRunStatusEntity) -> {
                    // update the flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.updateFlowAnalysisRule(revision, createDTOWithDesiredRunStatus(id, flowAnalysisRuleRunStatusEntity.getState()));
                    populateRemainingFlowAnalysisRuleEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private FlowAnalysisRuleDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final FlowAnalysisRuleDTO dto = new FlowAnalysisRuleDTO();
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
