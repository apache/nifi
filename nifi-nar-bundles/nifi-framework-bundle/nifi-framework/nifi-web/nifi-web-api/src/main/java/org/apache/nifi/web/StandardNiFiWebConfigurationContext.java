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
package org.apache.nifi.web;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.reporting.ReportingTaskProvider;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.ApplicationResource.ReplicationTarget;
import org.apache.nifi.web.api.dto.AllowableValueDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.util.ClientResponseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implements the NiFiWebConfigurationContext interface to support a context in both standalone and clustered environments.
 */
public class StandardNiFiWebConfigurationContext implements NiFiWebConfigurationContext {

    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiWebConfigurationContext.class);

    private NiFiProperties properties;
    private NiFiServiceFacade serviceFacade;
    private ClusterCoordinator clusterCoordinator;
    private RequestReplicator requestReplicator;
    private ControllerServiceProvider controllerServiceProvider;
    private ReportingTaskProvider reportingTaskProvider;
    private AuditService auditService;
    private Authorizer authorizer;
    private VariableRegistry variableRegistry;

    private void authorizeFlowAccess(final NiFiUser user) {
        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable flow = lookup.getFlow();
            flow.authorize(authorizer, RequestAction.READ, user);
        });
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier, final String componentId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        authorizeFlowAccess(user);
        return controllerServiceProvider.getControllerServiceForComponent(serviceIdentifier, componentId);
    }

    @Override
    public void saveActions(final NiFiWebRequestContext requestContext, final Collection<ConfigurationAction> configurationActions) {
        Objects.requireNonNull(configurationActions, "Actions cannot be null.");

        // ensure the path could be
        if (requestContext.getExtensionType() == null) {
            throw new IllegalArgumentException("The UI extension type must be specified.");
        }

        Component componentType = null;
        switch (requestContext.getExtensionType()) {
            case ProcessorConfiguration:
                // authorize access
                serviceFacade.authorizeAccess(lookup -> {
                    final Authorizable authorizable = lookup.getProcessor(requestContext.getId()).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                });

                componentType = Component.Processor;
                break;
            case ControllerServiceConfiguration:
                // authorize access
                serviceFacade.authorizeAccess(lookup -> {
                    final Authorizable authorizable = lookup.getControllerService(requestContext.getId()).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                });

                componentType = Component.ControllerService;
                break;
            case ReportingTaskConfiguration:
                // authorize access
                serviceFacade.authorizeAccess(lookup -> {
                    final Authorizable authorizable = lookup.getReportingTask(requestContext.getId()).getAuthorizable();
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                });

                componentType = Component.ReportingTask;
                break;
        }

        if (componentType == null) {
            throw new IllegalArgumentException("UI extension type must support Processor, ControllerService, or ReportingTask configuration.");
        }

        // - when running standalone or cluster ncm - actions from custom UIs are stored locally
        // - clustered nodes do not serve custom UIs directly to users so they should never be invoking this method
        final Date now = new Date();
        final Collection<Action> actions = new HashSet<>(configurationActions.size());
        for (final ConfigurationAction configurationAction : configurationActions) {
            final FlowChangeExtensionDetails extensionDetails = new FlowChangeExtensionDetails();
            extensionDetails.setType(configurationAction.getType());

            final FlowChangeConfigureDetails configureDetails = new FlowChangeConfigureDetails();
            configureDetails.setName(configurationAction.getName());
            configureDetails.setPreviousValue(configurationAction.getPreviousValue());
            configureDetails.setValue(configurationAction.getValue());

            final FlowChangeAction action = new FlowChangeAction();
            action.setTimestamp(now);
            action.setSourceId(configurationAction.getId());
            action.setSourceName(configurationAction.getName());
            action.setSourceType(componentType);
            action.setOperation(Operation.Configure);
            action.setUserIdentity(getCurrentUserIdentity());
            action.setComponentDetails(extensionDetails);
            action.setActionDetails(configureDetails);
            actions.add(action);
        }

        if (!actions.isEmpty()) {
            try {
                // record the operations
                auditService.addActions(actions);
            } catch (final Throwable t) {
                logger.warn("Unable to record actions: " + t.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.warn(StringUtils.EMPTY, t);
                }
            }
        }
    }

    @Override
    public String getCurrentUserIdentity() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        authorizeFlowAccess(user);
        return user.getIdentity();
    }

    @Override
    public ComponentDetails getComponentDetails(final NiFiWebRequestContext requestContext) throws ResourceNotFoundException, ClusterRequestException {
        final String id = requestContext.getId();

        if (StringUtils.isBlank(id)) {
            throw new ResourceNotFoundException(String.format("Configuration request context config did not have a component ID."));
        }

        // ensure the path could be
        if (requestContext.getExtensionType() == null) {
            throw new IllegalArgumentException("The UI extension type must be specified.");
        }

        // get the component facade for interacting directly with that type of object
        ComponentFacade componentFacade = null;
        switch (requestContext.getExtensionType()) {
            case ProcessorConfiguration:
                componentFacade = new ProcessorFacade();
                break;
            case ControllerServiceConfiguration:
                componentFacade = new ControllerServiceFacade();
                break;
            case ReportingTaskConfiguration:
                componentFacade = new ReportingTaskFacade();
                break;
        }

        if (componentFacade == null) {
            throw new IllegalArgumentException("UI extension type must support Processor, ControllerService, or ReportingTask configuration.");
        }

        return componentFacade.getComponentDetails(requestContext);
    }

    @Override
    public ComponentDetails updateComponent(final NiFiWebConfigurationRequestContext requestContext, final String annotationData, Map<String, String> properties)
            throws ResourceNotFoundException, InvalidRevisionException, ClusterRequestException {

        final String id = requestContext.getId();

        if (StringUtils.isBlank(id)) {
            throw new ResourceNotFoundException(String.format("Configuration request context did not have a component ID."));
        }

        // ensure the path could be
        if (requestContext.getExtensionType() == null) {
            throw new IllegalArgumentException("The UI extension type must be specified.");
        }

        // get the component facade for interacting directly with that type of object
        ComponentFacade componentFacade = null;
        switch (requestContext.getExtensionType()) {
            case ProcessorConfiguration:
                componentFacade = new ProcessorFacade();
                break;
            case ControllerServiceConfiguration:
                componentFacade = new ControllerServiceFacade();
                break;
            case ReportingTaskConfiguration:
                componentFacade = new ReportingTaskFacade();
                break;
        }

        if (componentFacade == null) {
            throw new IllegalArgumentException("UI extension type must support Processor, ControllerService, or ReportingTask configuration.");
        }

        return componentFacade.updateComponent(requestContext, annotationData, properties);
    }

    private ReplicationTarget getReplicationTarget() {
        return clusterCoordinator.isActiveClusterCoordinator() ? ReplicationTarget.CLUSTER_NODES : ReplicationTarget.CLUSTER_COORDINATOR;
    }

    private NodeResponse replicate(final String method, final URI uri, final Object entity, final Map<String, String> headers) throws InterruptedException {
        final NodeIdentifier coordinatorNode = clusterCoordinator.getElectedActiveCoordinatorNode();
        if (coordinatorNode == null) {
            throw new NoClusterCoordinatorException();
        }

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
        // to the cluster nodes themselves.
        if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
            return requestReplicator.replicate(method, uri, entity, headers).awaitMergedResponse();
        } else {
            return requestReplicator.forwardToCoordinator(coordinatorNode, method, uri, entity, headers).awaitMergedResponse();
        }
    }

    /**
     * Facade over accessing different types of NiFi components.
     */
    private interface ComponentFacade {

        /**
         * Gets the component details using the specified request context.
         *
         * @param requestContext context
         * @return the component details using the specified request context
         */
        ComponentDetails getComponentDetails(NiFiWebRequestContext requestContext);

        /**
         * Sets the annotation data using the specified request context.
         *
         * @param requestContext context
         * @param annotationData data
         * @param properties properties
         * @return details
         */
        ComponentDetails updateComponent(NiFiWebConfigurationRequestContext requestContext, String annotationData, Map<String, String> properties);
    }

    /**
     * Interprets the request/response with the underlying Processor model.
     */
    private class ProcessorFacade implements ComponentFacade {

        @Override
        public ComponentDetails getComponentDetails(final NiFiWebRequestContext requestContext) {
            final String id = requestContext.getId();

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable authorizable = lookup.getProcessor(id).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            });

            final ProcessorDTO processor;
            if (properties.isClustered() && clusterCoordinator != null && clusterCoordinator.isConnected()) {
                // create the request URL
                URI requestUrl;
                try {
                    final String path = "/nifi-api/processors/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // set the request parameters
                final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.GET, requestUrl, parameters, getHeaders(requestContext));
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return processor
                ProcessorEntity entity = (ProcessorEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ProcessorEntity.class);
                }
                processor = entity.getComponent();
            } else {
                processor = serviceFacade.getProcessor(id).getComponent();
            }

            // return the processor info
            return getComponentConfiguration(processor);
        }

        @Override
        public ComponentDetails updateComponent(final NiFiWebConfigurationRequestContext requestContext, final String annotationData, Map<String, String> properties) {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            final Revision revision = requestContext.getRevision();
            final String id = requestContext.getId();

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                // authorize the processor
                final ComponentAuthorizable authorizable = lookup.getProcessor(id);
                authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                // authorize any referenced service
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(properties, authorizable, authorizer, lookup);
            });

            final ProcessorDTO processor;
            if (StandardNiFiWebConfigurationContext.this.properties.isClustered()) {
                // create the request URL
                URI requestUrl;
                try {
                    final String path = "/nifi-api/processors/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // create the revision
                final RevisionDTO revisionDto = new RevisionDTO();
                revisionDto.setClientId(revision.getClientId());
                revisionDto.setVersion(revision.getVersion());

                // create the processor entity
                final ProcessorEntity processorEntity = new ProcessorEntity();
                processorEntity.setRevision(revisionDto);

                // create the processor dto
                ProcessorDTO processorDto = buildProcessorDto(id,annotationData,properties);
                processorEntity.setComponent(processorDto);

                // set the content type to json
                final Map<String, String> headers = getHeaders(requestContext);
                headers.put("Content-Type", "application/json");

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.PUT, requestUrl, processorEntity, headers);
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return processor
                ProcessorEntity entity = (ProcessorEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ProcessorEntity.class);
                }
                processor = entity.getComponent();
            } else {
                // update processor within write lock
                ProcessorDTO processorDTO = buildProcessorDto(id, annotationData, properties);
                final ProcessorEntity entity = serviceFacade.updateProcessor(revision, processorDTO);
                processor = entity.getComponent();
            }

            // return the processor info
            return getComponentConfiguration(processor);
        }

        private ProcessorDTO buildProcessorDto(String id, final String annotationData, Map<String, String> properties){
            ProcessorDTO processorDto = new ProcessorDTO();
            processorDto.setId(id);
            ProcessorConfigDTO configDto = new ProcessorConfigDTO();
            processorDto.setConfig(configDto);
            configDto.setAnnotationData(annotationData);
            configDto.setProperties(properties);
            return  processorDto;

        }

        private ComponentDetails getComponentConfiguration(final ProcessorDTO processor) {
            final ProcessorConfigDTO processorConfig = processor.getConfig();
            return new ComponentDetails.Builder()
                    .id(processor.getId())
                    .name(processor.getName())
                    .type(processor.getType())
                    .state(processor.getState())
                    .annotationData(processorConfig.getAnnotationData())
                    .properties(processorConfig.getProperties())
                    .descriptors(buildComponentDescriptorMap(processorConfig))
                    .validateErrors(processor.getValidationErrors()).build();
        }

        private Map<String,ComponentDescriptor> buildComponentDescriptorMap(final ProcessorConfigDTO processorConfig){

            final Map<String, ComponentDescriptor> descriptors = new HashMap<>();

            for(String key : processorConfig.getDescriptors().keySet()){

                PropertyDescriptorDTO descriptor = processorConfig.getDescriptors().get(key);
                List<AllowableValueEntity> allowableValuesEntity = descriptor.getAllowableValues();
                Map<String,String> allowableValues = new HashMap<>();

                if(allowableValuesEntity != null) {
                    for (AllowableValueEntity allowableValueEntity : allowableValuesEntity) {
                        final AllowableValueDTO allowableValueDTO = allowableValueEntity.getAllowableValue();
                        allowableValues.put(allowableValueDTO.getValue(), allowableValueDTO.getDisplayName());
                    }
                }

                ComponentDescriptor componentDescriptor = new ComponentDescriptor.Builder()
                        .name(descriptor.getName())
                        .displayName(descriptor.getDisplayName())
                        .defaultValue(descriptor.getDefaultValue())
                        .allowableValues(allowableValues)
                        .build();


                descriptors.put(key,componentDescriptor);
            }

            return descriptors;
        }

    }

    /**
     * Interprets the request/response with the underlying ControllerService model.
     */
    private class ControllerServiceFacade implements ComponentFacade {

        @Override
        public ComponentDetails getComponentDetails(final NiFiWebRequestContext requestContext) {
            final String id = requestContext.getId();
            final ControllerServiceDTO controllerService;

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable authorizable = lookup.getControllerService(id).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            });

            // if the lookup has the service that means we are either a node or
            // the ncm and the service is available there only
            if (controllerServiceProvider.getControllerService(id) != null) {
                controllerService = serviceFacade.getControllerService(id).getComponent();
            } else {
                // if this is a standalone instance the service should have been found above... there should
                // no cluster to replicate the request to
                if (!properties.isClustered()) {
                    throw new ResourceNotFoundException(String.format("Controller service[%s] could not be found on this NiFi.", id));
                }

                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/controller-services/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // set the request parameters
                final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.GET, requestUrl, parameters, getHeaders(requestContext));
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return controller service
                ControllerServiceEntity entity = (ControllerServiceEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ControllerServiceEntity.class);
                }
                controllerService = entity.getComponent();
            }

            // return the controller service info
            return getComponentConfiguration(controllerService);
        }

        @Override
        public ComponentDetails updateComponent(final NiFiWebConfigurationRequestContext requestContext, final String annotationData, Map<String, String> properties) {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            final Revision revision = requestContext.getRevision();
            final String id = requestContext.getId();

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                // authorize the controller service
                final ComponentAuthorizable authorizable = lookup.getControllerService(id);
                authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                // authorize any referenced service
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(properties, authorizable, authorizer, lookup);
            });

            final ControllerServiceDTO controllerService;
            if (controllerServiceProvider.getControllerService(id) != null) {
                final ControllerServiceDTO controllerServiceDto = new ControllerServiceDTO();
                controllerServiceDto.setId(id);
                controllerServiceDto.setAnnotationData(annotationData);
                controllerServiceDto.setProperties(properties);

                // update controller service
                final ControllerServiceEntity entity = serviceFacade.updateControllerService(revision, controllerServiceDto);
                controllerService = entity.getComponent();
            } else {
                // if this is a standalone instance the service should have been found above... there should
                // no cluster to replicate the request to
                if (!StandardNiFiWebConfigurationContext.this.properties.isClustered()) {
                    throw new ResourceNotFoundException(String.format("Controller service[%s] could not be found on this NiFi.", id));
                }

                // since this PUT request can be interpreted as a request to create a controller service
                // we need to be sure that this service exists on the node before the request is replicated.
                // this is done by attempting to get the details. if the service doesn't exist it will
                // throw a ResourceNotFoundException
                getComponentDetails(requestContext);

                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/controller-services/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // create the revision
                final RevisionDTO revisionDto = new RevisionDTO();
                revisionDto.setClientId(revision.getClientId());
                revisionDto.setVersion(revision.getVersion());

                // create the controller service entity
                final ControllerServiceEntity controllerServiceEntity = new ControllerServiceEntity();
                controllerServiceEntity.setRevision(revisionDto);

                // create the controller service dto
                final ControllerServiceDTO controllerServiceDto = new ControllerServiceDTO();
                controllerServiceEntity.setComponent(controllerServiceDto);
                controllerServiceDto.setId(id);
                controllerServiceDto.setAnnotationData(annotationData);
                controllerServiceDto.setProperties(properties);

                // set the content type to json
                final Map<String, String> headers = getHeaders(requestContext);
                headers.put("Content-Type", "application/json");

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.PUT, requestUrl, controllerServiceEntity, headers);
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return controller service
                ControllerServiceEntity entity = (ControllerServiceEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ControllerServiceEntity.class);
                }
                controllerService = entity.getComponent();
            }

            // return the controller service info
            return getComponentConfiguration(controllerService);
        }

        private ComponentDetails getComponentConfiguration(final ControllerServiceDTO controllerService) {
            return new ComponentDetails.Builder()
                    .id(controllerService.getId())
                    .name(controllerService.getName())
                    .type(controllerService.getType())
                    .state(controllerService.getState())
                    .annotationData(controllerService.getAnnotationData())
                    .properties(controllerService.getProperties())
                    .validateErrors(controllerService.getValidationErrors()).build();
        }
    }

    /**
     * Interprets the request/response with the underlying ControllerService model.
     */
    private class ReportingTaskFacade implements ComponentFacade {

        @Override
        public ComponentDetails getComponentDetails(final NiFiWebRequestContext requestContext) {
            final String id = requestContext.getId();
            final ReportingTaskDTO reportingTask;

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                final Authorizable authorizable = lookup.getReportingTask(id).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
            });

            // if the provider has the service that means we are either a node or
            // the ncm and the service is available there only
            if (reportingTaskProvider.getReportingTaskNode(id) != null) {
                reportingTask = serviceFacade.getReportingTask(id).getComponent();
            } else {
                // if this is a standalone instance the task should have been found above... there should
                // no cluster to replicate the request to
                if (!properties.isClustered()) {
                    throw new ResourceNotFoundException(String.format("Reporting task[%s] could not be found on this NiFi.", id));
                }

                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/reporting-tasks/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // set the request parameters
                final MultivaluedMap<String, String> parameters = new MultivaluedMapImpl();

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.GET, requestUrl, parameters, getHeaders(requestContext));
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return reporting task
                ReportingTaskEntity entity = (ReportingTaskEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ReportingTaskEntity.class);
                }
                reportingTask = entity.getComponent();
            }

            // return the reporting task info
            return getComponentConfiguration(reportingTask);
        }

        @Override
        public ComponentDetails updateComponent(final NiFiWebConfigurationRequestContext requestContext, final String annotationData, Map<String, String> properties) {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            final Revision revision = requestContext.getRevision();
            final String id = requestContext.getId();

            // authorize access
            serviceFacade.authorizeAccess(lookup -> {
                // authorize the reporting task
                final ComponentAuthorizable authorizable = lookup.getReportingTask(id);
                authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                // authorize any referenced service
                AuthorizeControllerServiceReference.authorizeControllerServiceReferences(properties, authorizable, authorizer, lookup);
            });

            final ReportingTaskDTO reportingTask;
            if (reportingTaskProvider.getReportingTaskNode(id) != null) {
                final ReportingTaskDTO reportingTaskDto = new ReportingTaskDTO();
                reportingTaskDto.setId(id);
                reportingTaskDto.setAnnotationData(annotationData);
                reportingTaskDto.setProperties(properties);

                // obtain write lock
                serviceFacade.verifyRevision(revision, user);
                final ReportingTaskEntity entity = serviceFacade.updateReportingTask(revision, reportingTaskDto);
                reportingTask = entity.getComponent();
            } else {
                // if this is a standalone instance the task should have been found above... there should
                // no cluster to replicate the request to
                if (!StandardNiFiWebConfigurationContext.this.properties.isClustered()) {
                    throw new ResourceNotFoundException(String.format("Reporting task[%s] could not be found on this NiFi.", id));
                }

                // since this PUT request can be interpreted as a request to create a reporting task
                // we need to be sure that this task exists on the node before the request is replicated.
                // this is done by attempting to get the details. if the service doesn't exist it will
                // throw a ResourceNotFoundException
                getComponentDetails(requestContext);

                // create the request URL
                URI requestUrl;
                try {
                    String path = "/nifi-api/reporting-tasks/" + URLEncoder.encode(id, "UTF-8");
                    requestUrl = new URI(requestContext.getScheme(), null, "localhost", 0, path, null, null);
                } catch (final URISyntaxException | UnsupportedEncodingException use) {
                    throw new ClusterRequestException(use);
                }

                // create the revision
                final RevisionDTO revisionDto = new RevisionDTO();
                revisionDto.setClientId(revision.getClientId());
                revisionDto.setVersion(revision.getVersion());

                // create the reporting task entity
                final ReportingTaskEntity reportingTaskEntity = new ReportingTaskEntity();
                reportingTaskEntity.setRevision(revisionDto);

                // create the reporting task dto
                final ReportingTaskDTO reportingTaskDto = new ReportingTaskDTO();
                reportingTaskEntity.setComponent(reportingTaskDto);
                reportingTaskDto.setId(id);
                reportingTaskDto.setAnnotationData(annotationData);
                reportingTaskDto.setProperties(properties);

                // set the content type to json
                final Map<String, String> headers = getHeaders(requestContext);
                headers.put("Content-Type", "application/json");

                // replicate request
                NodeResponse nodeResponse;
                try {
                    nodeResponse = replicate(HttpMethod.PUT, requestUrl, reportingTaskEntity, headers);
                } catch (final InterruptedException e) {
                    throw new IllegalClusterStateException("Request was interrupted while waiting for response from node");
                }

                // check for issues replicating request
                checkResponse(nodeResponse, id);

                // return reporting task
                ReportingTaskEntity entity = (ReportingTaskEntity) nodeResponse.getUpdatedEntity();
                if (entity == null) {
                    entity = nodeResponse.getClientResponse().getEntity(ReportingTaskEntity.class);
                }
                reportingTask = entity.getComponent();
            }

            // return the processor info
            return getComponentConfiguration(reportingTask);
        }

        private ComponentDetails getComponentConfiguration(final ReportingTaskDTO reportingTask) {
            return new ComponentDetails.Builder()
                    .id(reportingTask.getId())
                    .name(reportingTask.getName())
                    .type(reportingTask.getType())
                    .state(reportingTask.getState())
                    .annotationData(reportingTask.getAnnotationData())
                    .properties(reportingTask.getProperties())
                    .validateErrors(reportingTask.getValidationErrors()).build();
        }
    }

    /**
     * Gets the headers for the request to replicate to each node while clustered.
     */
    private Map<String, String> getHeaders(final NiFiWebRequestContext config) {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json,application/xml");
        return headers;
    }

    /**
     * Checks the specified response and drains the stream appropriately.
     */
    private void checkResponse(final NodeResponse nodeResponse, final String id) {
        if (nodeResponse.hasThrowable()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ClusterRequestException(nodeResponse.getThrowable());
        } else if (nodeResponse.getClientResponse().getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new InvalidRevisionException(String.format("Conflict: the flow may have been updated by another user."));
        } else if (nodeResponse.getClientResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ResourceNotFoundException("Unable to find component with id: " + id);
        } else if (nodeResponse.getClientResponse().getStatus() != Response.Status.OK.getStatusCode()) {
            ClientResponseUtils.drainClientResponse(nodeResponse.getClientResponse());
            throw new ClusterRequestException("Method resulted in an unsuccessful HTTP response code: " + nodeResponse.getClientResponse().getStatus());
        }
    }

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    public void setRequestReplicator(final RequestReplicator requestReplicator) {
        this.requestReplicator = requestReplicator;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    public void setControllerServiceProvider(final ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    public void setReportingTaskProvider(final ReportingTaskProvider reportingTaskProvider) {
        this.reportingTaskProvider = reportingTaskProvider;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setVariableRegistry(final VariableRegistry variableRegistry){
        this.variableRegistry = variableRegistry;
    }
}
