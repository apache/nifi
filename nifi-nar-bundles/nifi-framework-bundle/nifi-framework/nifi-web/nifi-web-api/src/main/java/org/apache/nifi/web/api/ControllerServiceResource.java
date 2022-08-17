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
import org.apache.nifi.authorization.AuthorizeParameterReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Controller Service.
 */
@Path("/controller-services")
@Api(
        value = "/controller-services",
        description = "Endpoint for managing a Controller Service."
)
public class ControllerServiceResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceResource.class);
    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";
    private RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> updateRequestManager =
        new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Controller Service Config Thread");

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified controller services.
     *
     * @param controllerServiceEntities controller services
     * @return dtos
     */
    public Set<ControllerServiceEntity> populateRemainingControllerServiceEntitiesContent(final Set<ControllerServiceEntity> controllerServiceEntities) {
        for (ControllerServiceEntity controllerServiceEntity : controllerServiceEntities) {
            populateRemainingControllerServiceEntityContent(controllerServiceEntity);
        }
        return controllerServiceEntities;
    }

    /**
     * Populate the uri's for the specified controller service.
     *
     * @param controllerServiceEntity controller service
     * @return dtos
     */
    public ControllerServiceEntity populateRemainingControllerServiceEntityContent(final ControllerServiceEntity controllerServiceEntity) {
        // populate the controller service href
        controllerServiceEntity.setUri(generateResourceUri("controller-services", controllerServiceEntity.getId()));

        // populate the remaining content
        if (controllerServiceEntity.getComponent() != null) {
            populateRemainingControllerServiceContent(controllerServiceEntity.getComponent());
        }
        return controllerServiceEntity;
    }

    /**
     * Populates the uri for the specified controller service.
     */
    public ControllerServiceDTO populateRemainingControllerServiceContent(final ControllerServiceDTO controllerService) {
        final BundleDTO bundle = controllerService.getBundle();

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(controllerService.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(controllerService.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ControllerServiceConfiguration.equals(uiExtension.getExtensionType())) {
                    controllerService.setCustomUiUrl(uiExtension.getContextPath() + "/configure");
                }
            }
        }

        return controllerService;
    }

    /**
     * Retrieves the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Gets a controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller-services/{uuid}")
            },
            notes = "If the uiOnly query parameter is provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User Interface. As such, " +
                "the selected fields may change at any time, even during incremental releases, without warning. As a result, this parameter should not be provided by any client other than the UI."
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
    public Response getControllerService(
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @QueryParam("uiOnly") @DefaultValue("false") final boolean uiOnly) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the controller service
        final ControllerServiceEntity entity = serviceFacade.getControllerService(id);
        if (uiOnly) {
            stripNonUiRelevantFields(entity);
        }
        populateRemainingControllerServiceEntityContent(entity);

        return generateOkResponse(entity).build();
    }


    /**
     * Returns the descriptor for the specified property.
     *
     * @param id           The id of the controller service.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    @ApiOperation(
            value = "Gets a controller service property descriptor",
            response = PropertyDescriptorEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller-services/{uuid}")
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
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The property name to return the descriptor for.",
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
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getControllerServicePropertyDescriptor(id, propertyName, sensitive);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a controller service.
     *
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    @ApiOperation(
            value = "Gets the state for a controller service",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller-services/{uuid}")
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
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getControllerServiceState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a controller service.
     *
     * @param httpServletRequest servlet request
     * @param id                 The id of the controller service
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @ApiOperation(
            value = "Clears the state for a controller service",
            response = ComponentStateEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller-services/{uuid}")
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
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ControllerServiceEntity requestControllerServiceEntity = new ControllerServiceEntity();
        requestControllerServiceEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                lookup -> {
                    final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
                    controllerService.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearControllerServiceState(id),
                (controllerServiceEntity) -> {
                    // get the component state
                    serviceFacade.clearControllerServiceState(controllerServiceEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Retrieves the references of the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    @ApiOperation(
            value = "Gets a controller service",
            response = ControllerServiceReferencingComponentsEntity.class,
            authorizations = {
                    @Authorization(value = "Read - /controller-services/{uuid}")
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
    public Response getControllerServiceReferences(
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the controller service
        final ControllerServiceReferencingComponentsEntity entity = serviceFacade.getControllerServiceReferencingComponents(id);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the references of the specified controller service.
     *
     * @param httpServletRequest     request
     * @param requestUpdateReferenceRequest The update request
     * @return A controllerServiceReferencingComponentsEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    @ApiOperation(
            value = "Updates a controller services references",
            response = ControllerServiceReferencingComponentsEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /{component-type}/{uuid} or /operate/{component-type}/{uuid} - For each referencing component specified")
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
    public Response updateControllerServiceReferences(
            @Context final HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The controller service request update request.",
                    required = true
            ) final UpdateControllerServiceReferenceRequestEntity requestUpdateReferenceRequest) {

        if (requestUpdateReferenceRequest == null || requestUpdateReferenceRequest.getId() == null) {
            throw new IllegalArgumentException("The controller service identifier must be specified.");
        }

        if (requestUpdateReferenceRequest.getReferencingComponentRevisions() == null) {
            throw new IllegalArgumentException("The controller service referencing components revisions must be specified.");
        }

        // parse the state to determine the desired action
        // need to consider controller service state first as it shares a state with
        // scheduled state (disabled) which is applicable for referencing services
        // but not referencing schedulable components
        ControllerServiceState requestControllerServiceState = null;
        try {
            requestControllerServiceState = ControllerServiceState.valueOf(requestUpdateReferenceRequest.getState());
        } catch (final IllegalArgumentException iae) {
            // ignore
        }

        ScheduledState requestScheduledState = null;
        try {
            requestScheduledState = ScheduledState.valueOf(requestUpdateReferenceRequest.getState());
        } catch (final IllegalArgumentException iae) {
            // ignore
        }

        // ensure an action has been specified
        if (requestScheduledState == null && requestControllerServiceState == null) {
            throw new IllegalArgumentException("Must specify the updated state. To update referencing Processors "
                    + "and Reporting Tasks the state should be RUNNING or STOPPED. To update the referencing Controller Services the "
                    + "state should be ENABLED or DISABLED.");
        }

        // ensure the controller service state is not ENABLING or DISABLING
        if (requestControllerServiceState != null
                && (ControllerServiceState.ENABLING.equals(requestControllerServiceState) || ControllerServiceState.DISABLING.equals(requestControllerServiceState))) {

            throw new IllegalArgumentException("Cannot set the referencing services to ENABLING or DISABLING");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUpdateReferenceRequest);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUpdateReferenceRequest.isDisconnectedNodeAcknowledged());
        }

        // convert the referencing revisions
        final Map<String, Revision> requestReferencingRevisions = requestUpdateReferenceRequest.getReferencingComponentRevisions().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    final RevisionDTO rev = e.getValue();
                    return new Revision(rev.getVersion(), rev.getClientId(), e.getKey());
                }));
        final Set<Revision> requestRevisions = new HashSet<>(requestReferencingRevisions.values());

        final ScheduledState verifyScheduledState = requestScheduledState;
        final ControllerServiceState verifyControllerServiceState = requestControllerServiceState;
        return withWriteLock(
                serviceFacade,
                requestUpdateReferenceRequest,
                requestRevisions,
                lookup -> {
                    requestReferencingRevisions.entrySet().stream().forEach(e -> {
                        final Authorizable controllerService = lookup.getControllerServiceReferencingComponent(id, e.getKey());
                        OperationAuthorizable.authorizeOperation(controllerService, authorizer, NiFiUserUtils.getNiFiUser());
                    });
                },
                () -> serviceFacade.verifyUpdateControllerServiceReferencingComponents(requestUpdateReferenceRequest.getId(), verifyScheduledState, verifyControllerServiceState),
                (revisions, updateReferenceRequest) -> {
                    ScheduledState scheduledState = null;
                    try {
                        scheduledState = ScheduledState.valueOf(updateReferenceRequest.getState());
                    } catch (final IllegalArgumentException e) {
                        // ignore
                    }

                    ControllerServiceState controllerServiceState = null;
                    try {
                        controllerServiceState = ControllerServiceState.valueOf(updateReferenceRequest.getState());
                    } catch (final IllegalArgumentException iae) {
                        // ignore
                    }

                    final Map<String, Revision> referencingRevisions = updateReferenceRequest.getReferencingComponentRevisions().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                                final RevisionDTO rev = e.getValue();
                                return new Revision(rev.getVersion(), rev.getClientId(), e.getKey());
                            }));

                    // update the controller service references
                    final ControllerServiceReferencingComponentsEntity entity = serviceFacade.updateControllerServiceReferencingComponents(
                            referencingRevisions, updateReferenceRequest.getId(), scheduledState, controllerServiceState);

                    if (updateReferenceRequest.getUiOnly() == Boolean.TRUE) {
                        entity.getControllerServiceReferencingComponents().forEach(this::stripNonUiRelevantFields);
                    }

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified a new Controller Service.
     *
     * @param httpServletRequest      request
     * @param id                      The id of the controller service to update.
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Updates a controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller-services/{uuid}"),
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
    public Response updateControllerService(
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The controller service configuration details.",
                    required = true
            ) final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null || requestControllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ControllerServiceDTO requestControllerServiceDTO = requestControllerServiceEntity.getComponent();
        if (!id.equals(requestControllerServiceDTO.getId())) {
            throw new IllegalArgumentException(String.format("The controller service id (%s) in the request body does not equal the "
                    + "controller service id of the requested resource (%s).", requestControllerServiceDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestControllerServiceEntity);
        }  else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestControllerServiceEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestControllerServiceEntity, id);
        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                requestRevision,
                lookup -> {
                    // authorize the service
                    final ComponentAuthorizable authorizable = lookup.getControllerService(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestControllerServiceDTO.getProperties(), authorizable, authorizer, lookup);
                        AuthorizeParameterReference.authorizeParameterReferences(requestControllerServiceDTO.getProperties(), authorizer, authorizable.getParameterContext(),
                            NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateControllerService(requestControllerServiceDTO),
                (revision, controllerServiceEntity) -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();

                    // update the controller service
                    final ControllerServiceEntity entity = serviceFacade.updateControllerService(revision, controllerService);
                    populateRemainingControllerServiceEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified controller service.
     *
     * @param httpServletRequest request
     * @param version            The revision is used to verify the client is working with
     *                           the latest version of the flow.
     * @param clientId           Optional client id. If the client id is not specified, a
     *                           new one will be generated. This value (whether specified or generated) is
     *                           included in the response.
     * @param id                 The id of the controller service to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @ApiOperation(
            value = "Deletes a controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller-services/{uuid}"),
                    @Authorization(value = "Write - Parent Process Group if scoped by Process Group - /process-groups/{uuid}"),
                    @Authorization(value = "Write - Controller if scoped by Controller - /controller"),
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
    public Response removeControllerService(
            @Context HttpServletRequest httpServletRequest,
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
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ControllerServiceEntity requestControllerServiceEntity = new ControllerServiceEntity();
        requestControllerServiceEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable controllerService = lookup.getControllerService(id);

                    // ensure write permission to the controller service
                    controllerService.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    controllerService.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(controllerService, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteControllerService(id),
                (revision, controllerServiceEntity) -> {
                    // delete the specified controller service
                    final ControllerServiceEntity entity = serviceFacade.deleteControllerService(revision, controllerServiceEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified controller service with the specified values.
     *
     * @param httpServletRequest      request
     * @param id                      The id of the controller service to update.
     * @param requestRunStatus    A runStatusEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/run-status")
    @ApiOperation(
            value = "Updates run status of a controller service",
            response = ControllerServiceEntity.class,
            authorizations = {
                    @Authorization(value = "Write - /controller-services/{uuid} or /operation/controller-services/{uuid}")
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
            @Context HttpServletRequest httpServletRequest,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @ApiParam(
                    value = "The controller service run status.",
                    required = true
            ) final ControllerServiceRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Controller service run status must be specified.");
        }

        if (requestRunStatus.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRunStatus.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRunStatus);
        }  else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRunStatus.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRunStatus.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRunStatus,
                requestRevision,
                lookup -> {
                    // authorize the service
                    final Authorizable authorizable = lookup.getControllerService(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateControllerService(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, runStatusEntity) -> {
                    // update the controller service
                    final ControllerServiceEntity entity = serviceFacade.updateControllerService(revision, createDTOWithDesiredRunStatus(id, runStatusEntity.getState()));
                    populateRemainingControllerServiceEntityContent(entity);

                    if (runStatusEntity.getUiOnly() == Boolean.TRUE) {
                        stripNonUiRelevantFields(entity);
                    }

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
            @Authorization(value = "Read - /controller-services/{uuid}")
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
        @ApiParam(value = "The controller service id.", required = true) @PathParam("id") final String controllerServiceId,
        @ApiParam(value = "The configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Controller Service's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Controller Service's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(controllerServiceId)) {
            throw new IllegalArgumentException("Controller Service's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Controller Service's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
            serviceFacade,
            configurationAnalysis,
            lookup -> {
                final ComponentAuthorizable controllerService = lookup.getControllerService(controllerServiceId);
                controllerService.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            },
            () -> { },
            entity -> {
                final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeControllerServiceConfiguration(analysis.getComponentId(), analysis.getProperties());
                return generateOkResponse(resultsEntity).build();
            }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/verification-requests")
    @ApiOperation(
        value = "Performs verification of the Controller Service's configuration",
        response = VerifyConfigRequestEntity.class,
        notes = "This will initiate the process of verifying a given Controller Service configuration. This may be a long-running task. As a result, this endpoint will immediately return a " +
            "ControllerServiceConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
            "The client may then periodically poll the status of the request by " +
            "issuing a GET request to /controller-services/{serviceId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
            "/controller-services/{serviceId}/verification-requests/{requestId}.",
        authorizations = {
            @Authorization(value = "Read - /controller-services/{uuid}")
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
        @ApiParam(value = "The controller service id.", required = true) @PathParam("id") final String controllerServiceId,
        @ApiParam(value = "The controller service configuration verification request.", required = true) final VerifyConfigRequestEntity controllerServiceConfigRequest) {

        if (controllerServiceConfigRequest == null) {
            throw new IllegalArgumentException("Controller Service's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = controllerServiceConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Controller Service properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Controller Service's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(controllerServiceId)) {
            throw new IllegalArgumentException("Controller Service's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, controllerServiceConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
            serviceFacade,
            controllerServiceConfigRequest,
            lookup -> {
                final ComponentAuthorizable controllerService = lookup.getControllerService(controllerServiceId);
                controllerService.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            },
            () -> {
                serviceFacade.verifyCanVerifyControllerServiceConfig(controllerServiceId);
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
        @ApiParam("The ID of the Controller Service") @PathParam("id") final String controllerServiceId,
        @ApiParam("The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
            updateRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

        final VerifyConfigRequestEntity updateRequestEntity = createVerifyControllerServiceConfigRequestEntity(asyncRequest, requestId);
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
        @ApiParam("The ID of the Controller Service") @PathParam("id") final String controllerServiceId,
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

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyControllerServiceConfigRequestEntity(asyncRequest, requestId);
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
        final String serviceId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Controller Service Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
            new StandardAsynchronousWebRequest<>(requestId, configRequest, serviceId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> updateTask = asyncRequest -> {
            try {
                final Map<String, String> attributes = requestDto.getAttributes() == null ? Collections.emptyMap() : requestDto.getAttributes();
                final List<ConfigVerificationResultDTO> results = serviceFacade.performControllerServiceConfigVerification(serviceId, requestDto.getProperties(), attributes);
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Controller Service configuration", e);
                asyncRequest.fail("Failed to verify Controller Service configuration due to " + e);
            }
        };

        updateRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, updateTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyControllerServiceConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyControllerServiceConfigRequestEntity(
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
        dto.setUri(generateResourceUri("controller-services", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
    }

    private ControllerServiceDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
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
