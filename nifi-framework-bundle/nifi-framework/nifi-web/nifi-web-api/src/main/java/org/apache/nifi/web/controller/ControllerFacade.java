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
package org.apache.nifi.web.controller;

import jakarta.ws.rs.WebApplicationException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ContentAvailability;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.FlowController.GroupStatusCounts;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.serialization.StandardVersionedReportingTaskImporter;
import org.apache.nifi.controller.serialization.VersionedReportingTaskImporter;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceResolver;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.analytics.StatusAnalytics;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsEngine;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.provenance.AttributeDTO;
import org.apache.nifi.web.api.dto.provenance.LatestProvenanceEventsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceRequestDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceResultsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceSearchValueDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceSearchableFieldDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO.LineageRequestType;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.query.SearchQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ControllerFacade implements Authorizable {

    private static final Logger logger = LoggerFactory.getLogger(ControllerFacade.class);

    private static final int MAX_REPLAY_EVENT_COUNT = 10;

    // nifi components
    private FlowController flowController;
    private FlowService flowService;
    private Authorizer authorizer;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;
    private SearchQueryParser searchQueryParser;
    private ControllerSearchService controllerSearchService;
    private RuntimeManifestService runtimeManifestService;

    private ProcessGroup getRootGroup() {
        return flowController.getFlowManager().getRootGroup();
    }

    /**
     * Returns the group id that contains the specified processor.
     *
     * @param processorId processor id
     * @return group id
     */
    public String findProcessGroupIdForProcessor(String processorId) {
        final ProcessGroup rootGroup = getRootGroup();
        final ProcessorNode processor = rootGroup.findProcessor(processorId);
        if (processor == null) {
            return null;
        } else {
            return processor.getProcessGroup().getIdentifier();
        }
    }

    public Connectable findLocalConnectable(String componentId) {
        return flowController.getFlowManager().findConnectable(componentId);
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return flowController.getControllerServiceProvider();
    }

    public ControllerServiceResolver getControllerServiceResolver() {
        return flowController.getControllerServiceResolver();
    }

    public ExtensionManager getExtensionManager() {
        return flowController.getExtensionManager();
    }

    public FlowManager getFlowManager() {
        return flowController.getFlowManager();
    }

    /**
     * Sets the name of this controller.
     *
     * @param name name
     */
    public void setName(String name) {
        getRootGroup().setName(name);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return flowController.getParentAuthorizable();
    }

    @Override
    public Resource getResource() {
        return flowController.getResource();
    }

    /**
     * Sets the comments of this controller.
     *
     * @param comments comments
     */
    public void setComments(String comments) {
        getRootGroup().setComments(comments);
    }

    /**
     * Gets the cached temporary instance of the component for the given type and bundle.
     *
     * @param type   type of the component
     * @param bundle the bundle of the component
     * @return the temporary component
     * @throws IllegalStateException if no temporary component exists for the given type and bundle
     */
    public ConfigurableComponent getTemporaryComponent(final String type, final BundleDTO bundle) {
        final ExtensionManager extensionManager = getExtensionManager();
        final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(extensionManager, type, bundle);
        final ConfigurableComponent configurableComponent = extensionManager.getTempComponent(type, bundleCoordinate);

        if (configurableComponent == null) {
            throw new IllegalStateException("Unable to obtain temporary component for " + type);
        }

        return configurableComponent;
    }

    /**
     * Sets the max timer driven thread count of this controller.
     *
     * @param maxTimerDrivenThreadCount count
     */
    public void setMaxTimerDrivenThreadCount(int maxTimerDrivenThreadCount) {
        flowController.setMaxTimerDrivenThreadCount(maxTimerDrivenThreadCount);
    }

    /**
     * Gets the root group id.
     *
     * @return group id
     */
    public String getRootGroupId() {
        return flowController.getFlowManager().getRootGroupId();
    }

    /**
     * Gets the remotely accessible InputPorts in any ProcessGroup.
     *
     * @return input ports
     */
    public Set<Port> getPublicInputPorts() {
        return flowController.getFlowManager().getPublicInputPorts();
    }

    /**
     * Gets the remotely accessible OutputPorts in any ProcessGroup.
     *
     * @return output ports
     */
    public Set<Port> getPublicOutputPorts() {
        return flowController.getFlowManager().getPublicOutputPorts();
    }

    /**
     * Returns the status history for the node.
     *
     * @return status history
     */
    public StatusHistoryDTO getNodeStatusHistory() {
        final boolean authorized = isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        final StatusHistoryDTO statusHistory = flowController.getNodeStatusHistory();

        if (!authorized) {
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_TYPE, "Node");
        }

        return statusHistory;
    }

    /**
     * Returns the status history for the specified processor.
     *
     * @param processorId processor id
     * @return status history
     */
    public StatusHistoryDTO getProcessorStatusHistory(final String processorId) {
        final ProcessGroup root = getRootGroup();
        final ProcessorNode processor = root.findProcessor(processorId);

        // ensure the processor was found
        if (processor == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate processor with id '%s'.", processorId));
        }

        final boolean authorized = processor.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());

        final StatusHistoryDTO statusHistory = flowController.getProcessorStatusHistory(processorId, authorized);

        // if not authorized
        if (!authorized) {
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_NAME, processorId);
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_TYPE, "Processor");
        }

        return statusHistory;
    }

    /**
     * Returns the status history for the specified connection.
     *
     * @param connectionId connection id
     * @return status history
     */
    public StatusHistoryDTO getConnectionStatusHistory(final String connectionId) {
        final ProcessGroup root = getRootGroup();
        final Connection connection = root.findConnection(connectionId);

        // ensure the connection was found
        if (connection == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate connection with id '%s'.", connectionId));
        }

        final StatusHistoryDTO statusHistory = flowController.getConnectionStatusHistory(connectionId);

        // if not authorized
        if (!connection.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser())) {
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_NAME, connectionId);
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_SOURCE_NAME, connection.getSource().getIdentifier());
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_DESTINATION_NAME, connection.getDestination().getIdentifier());
        }

        return statusHistory;
    }

    /**
     * Returns the status history for the specified process group.
     *
     * @param groupId group id
     * @return status history
     */
    public StatusHistoryDTO getProcessGroupStatusHistory(final String groupId) {
        final FlowManager flowManager = flowController.getFlowManager();

        final String searchId = groupId.equals(FlowManager.ROOT_GROUP_ID_ALIAS) ? flowManager.getRootGroupId() : groupId;
        final ProcessGroup root = flowManager.getRootGroup();
        final ProcessGroup group = root.findProcessGroup(searchId);

        // ensure the processor was found
        if (group == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate process group with id '%s'.", groupId));
        }

        final StatusHistoryDTO statusHistory = flowController.getProcessGroupStatusHistory(groupId);

        // if not authorized
        if (!group.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser())) {
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_NAME, groupId);
        }

        return statusHistory;
    }

    /**
     * Returns the status history for the specified remote process group.
     *
     * @param remoteProcessGroupId remote process group id
     * @return status history
     */
    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(final String remoteProcessGroupId) {
        final ProcessGroup root = getRootGroup();
        final RemoteProcessGroup remoteProcessGroup = root.findRemoteProcessGroup(remoteProcessGroupId);

        // ensure the output port was found
        if (remoteProcessGroup == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate remote process group with id '%s'.", remoteProcessGroupId));
        }

        final StatusHistoryDTO statusHistory = flowController.getRemoteProcessGroupStatusHistory(remoteProcessGroupId);

        // if not authorized
        if (!remoteProcessGroup.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser())) {
            statusHistory.getComponentDetails().put(StatusHistoryRepository.COMPONENT_DETAIL_NAME, remoteProcessGroupId);
            statusHistory.getComponentDetails().remove(StatusHistoryRepository.COMPONENT_DETAIL_URI);
        }

        return statusHistory;
    }

    /**
     * Get the node id of this controller.
     *
     * @return node identifier
     */
    public NodeIdentifier getNodeId() {
        return flowController.getNodeId();
    }

    /**
     * @return true if is clustered
     */
    public boolean isClustered() {
        return flowController.isClustered();
    }

    /**
     * Gets the name of this controller.
     *
     * @return name
     */
    public String getName() {
        return getRootGroup().getName();
    }

    public String getInstanceId() {
        return flowController.getInstanceId();
    }

    /**
     * Gets the comments of this controller.
     *
     * @return comments
     */
    public String getComments() {
        return getRootGroup().getComments();
    }

    /**
     * Gets the max timer driven thread count of this controller.
     *
     * @return count
     */
    public int getMaxTimerDrivenThreadCount() {
        return flowController.getMaxTimerDrivenThreadCount();
    }

    /**
     * Gets the FlowFileProcessor types that this controller supports.
     *
     * @param bundleGroupFilter    if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter           if specified, type must match
     * @return types
     */
    public Set<DocumentedTypeDTO> getFlowFileProcessorTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(Processor.class), bundleGroupFilter, bundleArtifactFilter, typeFilter);
    }

    /**
     * Gets the FlowFileComparator types that this controller supports.
     *
     * @return the FlowFileComparator types that this controller supports
     */
    public Set<DocumentedTypeDTO> getFlowFileComparatorTypes() {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(FlowFilePrioritizer.class), null, null, null);
    }

    /**
     * Returns whether the specified type implements the specified serviceType.
     *
     * @param serviceType type
     * @param type        type
     * @return whether the specified type implements the specified serviceType
     */
    private boolean implementsServiceType(final Class serviceType, final Class type) {
        final List<Class<?>> interfaces = ClassUtils.getAllInterfaces(type);
        for (final Class i : interfaces) {
            if (ControllerService.class.isAssignableFrom(i) && serviceType.isAssignableFrom(i)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Gets the ControllerService types that this controller supports.
     *
     * @param serviceType           type
     * @param serviceBundleGroup    if serviceType specified, the bundle group of the serviceType
     * @param serviceBundleArtifact if serviceType specified, the bundle artifact of the serviceType
     * @param serviceBundleVersion  if serviceType specified, the bundle version of the serviceType
     * @param bundleGroupFilter     if specified, must be member of bundle group
     * @param bundleArtifactFilter  if specified, must be member of bundle artifact
     * @param typeFilter            if specified, type must match
     * @return the ControllerService types that this controller supports
     */
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType, final String serviceBundleGroup, final String serviceBundleArtifact, final String serviceBundleVersion,
                                                            final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {

        final Set<ExtensionDefinition> extensionDefinitions = getExtensionManager().getExtensions(ControllerService.class);

        // identify the controller services that implement the specified serviceType if applicable
        if (serviceType != null) {
            final BundleCoordinate bundleCoordinate = new BundleCoordinate(serviceBundleGroup, serviceBundleArtifact, serviceBundleVersion);
            final Bundle csBundle = getExtensionManager().getBundle(bundleCoordinate);
            if (csBundle == null) {
                throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
            }

            Class serviceClass = null;
            final ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(csBundle.getClassLoader());
                serviceClass = Class.forName(serviceType, false, csBundle.getClassLoader());
            } catch (final Exception e) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
                throw new IllegalArgumentException(String.format("Unable to load %s from bundle %s: %s", serviceType, bundleCoordinate, e), e);
            }

            final Map<Class, Bundle> matchingServiceImplementations = new HashMap<>();

            // check each type and remove those that aren't in the specified ancestry
            for (final ExtensionDefinition extensionDefinition : extensionDefinitions) {
                final Class csClass = getExtensionManager().getClass(extensionDefinition);
                if (implementsServiceType(serviceClass, csClass)) {
                    // We need to protect against null here because after retrieving the set of extension definitions from the ExtensionManager, a custom NAR that is part
                    // of the NAR Manager may be deleted/replaced which causes its bundle to be removed from the ExtensionManager before reaching this getBundle call
                    final Bundle csImplBundle = getExtensionManager().getBundle(csClass.getClassLoader());
                    if (csImplBundle != null) {
                        matchingServiceImplementations.put(csClass, csImplBundle);
                    }
                }
            }

            return dtoFactory.fromDocumentedTypes(matchingServiceImplementations, bundleGroupFilter, bundleArtifactFilter, typeFilter);
        } else {
            return dtoFactory.fromDocumentedTypes(extensionDefinitions, bundleGroupFilter, bundleArtifactFilter, typeFilter);
        }
    }

    /**
     * Gets the ReportingTask types that this controller supports.
     *
     * @param bundleGroupFilter    if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter           if specified, type must match
     * @return the ReportingTask types that this controller supports
     */
    public Set<DocumentedTypeDTO> getReportingTaskTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(ReportingTask.class), bundleGroupFilter, bundleArtifactFilter, typeFilter);
    }

    /**
     * Gets the FlowAnalysisRule types that this controller supports.
     *
     * @param bundleGroupFilter    if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter           if specified, type must match
     * @return the FlowAnalysisRule types that this controller supports
     */
    public Set<DocumentedTypeDTO> getFlowAnalysisRuleTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(FlowAnalysisRule.class), bundleGroupFilter, bundleArtifactFilter, typeFilter);
    }

    /**
     * Gets the FlowRegistryClient types that this controller supports.
     *
     * @return the FlowRegistryClient types that this controller supports
     */
    public Set<DocumentedTypeDTO> getFlowRegistryTypes() {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(FlowRegistryClient.class), null, null, null);
    }

    /**
     * Gets the RuntimeManifest for this overall NiFi instance.
     *
     * @return the runtime manifest
     */
    public RuntimeManifest getRuntimeManifest() {
        return runtimeManifestService.getManifest();
    }

    private ComponentManifest getComponentManifest(String group, String artifact, String version) {
        final RuntimeManifest manifest = runtimeManifestService.getManifestForBundle(group, artifact, version);
        final List<org.apache.nifi.c2.protocol.component.api.Bundle> manifestBundles = manifest.getBundles();

        if (manifestBundles.isEmpty()) {
            throw new ResourceNotFoundException("Unable to find bundle [%s:%s:%s]".formatted(group, artifact, version));
        }

        org.apache.nifi.c2.protocol.component.api.Bundle manifestBundle = manifestBundles.getFirst();
        return manifestBundle.getComponentManifest();
    }

    public ProcessorDefinition getProcessorDefinition(String group, String artifact, String version, String type) {
        final ComponentManifest componentManifest = getComponentManifest(group, artifact, version);
        return componentManifest.getProcessors().stream().filter(processorDefinition -> type.equals(processorDefinition.getType())).findFirst().orElse(null);
    }

    public ControllerServiceDefinition getControllerServiceDefinition(String group, String artifact, String version, String type) {
        final ComponentManifest componentManifest = getComponentManifest(group, artifact, version);
        return componentManifest.getControllerServices().stream().filter(controllerServiceDefinition -> type.equals(controllerServiceDefinition.getType())).findFirst().orElse(null);
    }

    public ReportingTaskDefinition getReportingTaskDefinition(String group, String artifact, String version, String type) {
        final ComponentManifest componentManifest = getComponentManifest(group, artifact, version);
        return componentManifest.getReportingTasks().stream().filter(reportingTaskDefinition -> type.equals(reportingTaskDefinition.getType())).findFirst().orElse(null);
    }

    public ParameterProviderDefinition getParameterProviderDefinition(String group, String artifact, String version, String type) {
        final ComponentManifest componentManifest = getComponentManifest(group, artifact, version);
        return componentManifest.getParameterProviders().stream().filter(parameterProviderDefinition -> type.equals(parameterProviderDefinition.getType())).findFirst().orElse(null);
    }

    public FlowAnalysisRuleDefinition getFlowAnalysisRuleDefinition(String group, String artifact, String version, String type) {
        final ComponentManifest componentManifest = getComponentManifest(group, artifact, version);
        return componentManifest.getFlowAnalysisRules().stream().filter(flowAnalysisRuleDefinition -> type.equals(flowAnalysisRuleDefinition.getType())).findFirst().orElse(null);
    }

    public String getAdditionalDetails(String group, String artifact, String version, String type) {
        final Map<String, File> additionalDetailsMap = runtimeManifestService.discoverAdditionalDetails(group, artifact, version);
        final File additionalDetailsFile = additionalDetailsMap.get(type);

        if (additionalDetailsFile == null) {
            throw new ResourceNotFoundException("Unable to find additional details for [%s]".formatted(type));
        }

        try (final Stream<String> additionalDetailsLines = Files.lines(additionalDetailsFile.toPath())) {
            return additionalDetailsLines.collect(Collectors.joining("\n"));
        } catch (final IOException e) {
            throw new RuntimeException("Unable to load additional details content for "
                    + additionalDetailsFile.getAbsolutePath() + " due to: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the ParameterProvider types that this controller supports.
     *
     * @param bundleGroupFilter    if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter           if specified, type must match
     * @return the ParameterProvider types that this controller supports
     */
    public Set<DocumentedTypeDTO> getParameterProviderTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        return dtoFactory.fromDocumentedTypes(getExtensionManager().getExtensions(ParameterProvider.class), bundleGroupFilter, bundleArtifactFilter, typeFilter);
    }

    /**
     * Gets the counters for this controller.
     *
     * @return the counters for this controller
     */
    public List<Counter> getCounters() {
        return flowController.getCounters();
    }

    /**
     * Resets the counter with the specified id.
     *
     * @param id id
     * @return the counter with the specified id
     */
    public Counter resetCounter(final String id) {
        final Counter counter = flowController.resetCounter(id);

        if (counter == null) {
            throw new ResourceNotFoundException(String.format("Unable to find Counter with id '%s'.", id));
        }

        return counter;
    }

    /**
     * Resets all counters atomically.
     *
     * @return the list of reset counters
     */
    public List<Counter> resetAllCounters() {
        return flowController.resetAllCounters();
    }

    /**
     * Gets the status of this controller.
     *
     * @return the status of this controller
     */
    public ControllerStatusDTO getControllerStatus() {
        final ProcessGroup rootGroup = getRootGroup();
        final GroupStatusCounts groupStatusCounts = flowController.getGroupStatusCounts(rootGroup);

        final ControllerStatusDTO controllerStatus = new ControllerStatusDTO();
        controllerStatus.setActiveThreadCount(groupStatusCounts.getActiveThreadCount());
        controllerStatus.setTerminatedThreadCount(groupStatusCounts.getTerminatedThreadCount());
        controllerStatus.setQueued(FormatUtils.formatCount(groupStatusCounts.getQueuedCount()) + " / " + FormatUtils.formatDataSize(groupStatusCounts.getQueuedContentSize()));
        controllerStatus.setBytesQueued(groupStatusCounts.getQueuedContentSize());
        controllerStatus.setFlowFilesQueued(groupStatusCounts.getQueuedCount());

        final ProcessGroupCounts counts = rootGroup.getCounts();
        controllerStatus.setRunningCount(counts.getRunningCount());
        controllerStatus.setStoppedCount(counts.getStoppedCount());
        controllerStatus.setInvalidCount(counts.getInvalidCount());
        controllerStatus.setDisabledCount(counts.getDisabledCount());
        controllerStatus.setActiveRemotePortCount(counts.getActiveRemotePortCount());
        controllerStatus.setInactiveRemotePortCount(counts.getInactiveRemotePortCount());
        controllerStatus.setUpToDateCount(counts.getUpToDateCount());
        controllerStatus.setLocallyModifiedCount(counts.getLocallyModifiedCount());
        controllerStatus.setStaleCount(counts.getStaleCount());
        controllerStatus.setLocallyModifiedAndStaleCount(counts.getLocallyModifiedAndStaleCount());
        controllerStatus.setSyncFailureCount(counts.getSyncFailureCount());

        return controllerStatus;
    }

    /**
     * Gets the status for the specified process group.
     *
     * @param groupId group id
     * @return the status for the specified process group
     */
    public ProcessGroupStatus getProcessGroupStatus(final String groupId) {
        return getProcessGroupStatus(groupId, Integer.MAX_VALUE);
    }

    /**
     * Gets the status for the specified process group.
     *
     * @param groupId              group id
     * @param recursiveStatusDepth the number of levels deep that we want to go before deciding to stop including component statuses
     * @return the status for the specified process group
     */
    public ProcessGroupStatus getProcessGroupStatus(final String groupId, final int recursiveStatusDepth) {
        final ProcessGroupStatus processGroupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), recursiveStatusDepth);
        if (processGroupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        return processGroupStatus;
    }

    /**
     * Gets the status for the specified processor.
     *
     * @param processorId processor id
     * @return the status for the specified processor
     */
    public ProcessorStatus getProcessorStatus(final String processorId) {
        final ProcessorStatus status = flowController.getEventAccess().getProcessorStatus(processorId, NiFiUserUtils.getNiFiUser());
        if (status == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate processor with id '%s'.", processorId));
        }

        return status;
    }

    /**
     * Gets the status for the specified connection.
     *
     * @param connectionId connection id
     * @return the status for the specified connection
     */
    public ConnectionStatus getConnectionStatus(final String connectionId) {
        final ProcessGroup root = getRootGroup();
        final Connection connection = root.findConnection(connectionId);

        // ensure the connection was found
        if (connection == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate connection with id '%s'.", connectionId));
        }

        // calculate the process group status
        final String groupId = connection.getProcessGroup().getIdentifier();
        final ProcessGroupStatus processGroupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), 1);
        if (processGroupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        final ConnectionStatus status = processGroupStatus.getConnectionStatus().stream().filter(connectionStatus -> connectionId.equals(connectionStatus.getId())).findFirst().orElse(null);
        if (status == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate connection with id '%s'.", connectionId));
        }

        return status;
    }

    /**
     * Gets status analytics for the specified connection.
     *
     * @param connectionId connection id
     * @return the statistics for the specified connection
     */
    public StatusAnalytics getConnectionStatusAnalytics(final String connectionId) {
        final ProcessGroup root = getRootGroup();
        final Connection connection = root.findConnection(connectionId);

        // ensure the connection was found
        if (connection == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate connection with id '%s'.", connectionId));
        }

        // calculate the process group status
        final String groupId = connection.getProcessGroup().getIdentifier();
        final ProcessGroupStatus processGroupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), 1);
        if (processGroupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        // get from flow controller
        final StatusAnalyticsEngine statusAnalyticsEngine = flowController.getStatusAnalyticsEngine();
        if (statusAnalyticsEngine == null) {
            throw new ResourceNotFoundException(String.format("Unable to provide analytics for connection with id '%s'. Analytics may not be enabled", connectionId));
        }

        return statusAnalyticsEngine.getStatusAnalytics(connectionId);
    }

    /**
     * Gets the status for the specified input port.
     *
     * @param portId input port id
     * @return the status for the specified input port
     */
    public PortStatus getInputPortStatus(final String portId) {
        final ProcessGroup root = getRootGroup();
        final Port port = root.findInputPort(portId);

        // ensure the input port was found
        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate input port with id '%s'.", portId));
        }

        final String groupId = port.getProcessGroup().getIdentifier();
        final ProcessGroupStatus processGroupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), 1);
        if (processGroupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        final PortStatus status = processGroupStatus.getInputPortStatus().stream().filter(portStatus -> portId.equals(portStatus.getId())).findFirst().orElse(null);
        if (status == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate input port with id '%s'.", portId));
        }

        return status;
    }

    /**
     * Gets the status for the specified output port.
     *
     * @param portId output port id
     * @return the status for the specified output port
     */
    public PortStatus getOutputPortStatus(final String portId) {
        final ProcessGroup root = getRootGroup();
        final Port port = root.findOutputPort(portId);

        // ensure the output port was found
        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate output port with id '%s'.", portId));
        }

        final String groupId = port.getProcessGroup().getIdentifier();
        final ProcessGroupStatus processGroupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), 1);
        if (processGroupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        final PortStatus status = processGroupStatus.getOutputPortStatus().stream().filter(portStatus -> portId.equals(portStatus.getId())).findFirst().orElse(null);
        if (status == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate output port with id '%s'.", portId));
        }

        return status;
    }

    /**
     * Gets the status for the specified remote process group.
     *
     * @param remoteProcessGroupId remote process group id
     * @return the status for the specified remote process group
     */
    public RemoteProcessGroupStatus getRemoteProcessGroupStatus(final String remoteProcessGroupId) {
        final ProcessGroup root = getRootGroup();
        final RemoteProcessGroup remoteProcessGroup = root.findRemoteProcessGroup(remoteProcessGroupId);

        // ensure the output port was found
        if (remoteProcessGroup == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate remote process group with id '%s'.", remoteProcessGroupId));
        }

        final String groupId = remoteProcessGroup.getProcessGroup().getIdentifier();
        final ProcessGroupStatus groupStatus = flowController.getEventAccess().getGroupStatus(groupId, NiFiUserUtils.getNiFiUser(), 1);
        if (groupStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        final RemoteProcessGroupStatus status = groupStatus.getRemoteProcessGroupStatus().stream().filter(rpgStatus -> remoteProcessGroupId.equals(rpgStatus.getId())).findFirst().orElse(null);
        if (status == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate remote process group with id '%s'.", remoteProcessGroupId));
        }

        return status;
    }

    /**
     * Saves the state of the flow controller.
     *
     * @throws NiFiCoreException ex
     */
    public void save() throws NiFiCoreException {
        // save the flow controller
        final long writeDelaySeconds = FormatUtils.getTimeDuration(properties.getFlowServiceWriteDelay(), TimeUnit.SECONDS);
        flowService.saveFlowChanges(TimeUnit.SECONDS, writeDelaySeconds);
    }

    /**
     * Returns the socket port that the local instance is listening on for
     * Site-to-Site communications
     *
     * @return the socket port that the local instance is listening on for
     * Site-to-Site communications
     */
    public Integer getRemoteSiteListeningPort() {
        return flowController.getRemoteSiteListeningPort();
    }

    /**
     * Returns the http(s) port that the local instance is listening on for
     * Site-to-Site communications
     *
     * @return the socket port that the local instance is listening on for
     * Site-to-Site communications
     */
    public Integer getRemoteSiteListeningHttpPort() {
        return flowController.getRemoteSiteListeningHttpPort();
    }

    /**
     * Indicates whether or not Site-to-Site communications with the local
     * instance are secure
     *
     * @return whether or not Site-to-Site communications with the local
     * instance are secure
     */
    public Boolean isRemoteSiteCommsSecure() {
        return flowController.isRemoteSiteCommsSecure();
    }

    /**
     * Returns a SystemDiagnostics that describes the current state of the node
     *
     * @return a SystemDiagnostics that describes the current state of the node
     */
    public SystemDiagnostics getSystemDiagnostics() {
        return flowController.getSystemDiagnostics();
    }

    public List<Resource> getResources() {
        final List<Resource> resources = new ArrayList<>();
        resources.add(ResourceFactory.getFlowResource());
        resources.add(ResourceFactory.getSystemResource());
        resources.add(ResourceFactory.getControllerResource());
        resources.add(ResourceFactory.getCountersResource());
        resources.add(ResourceFactory.getProvenanceResource());
        resources.add(ResourceFactory.getPoliciesResource());
        resources.add(ResourceFactory.getTenantResource());
        resources.add(ResourceFactory.getProxyResource());
        resources.add(ResourceFactory.getResourceResource());
        resources.add(ResourceFactory.getSiteToSiteResource());
        resources.add(ResourceFactory.getParameterContextsResource());

        // add each parameter context
        flowController.getFlowManager().getParameterContextManager().getParameterContexts().forEach(parameterContext -> resources.add(parameterContext.getResource()));

        // restricted components
        resources.add(ResourceFactory.getRestrictedComponentsResource());
        Arrays.stream(RequiredPermission.values()).forEach(requiredPermission -> resources.add(ResourceFactory.getRestrictedComponentsResource(requiredPermission)));

        final ProcessGroup root = getRootGroup();

        // include the root group
        final Resource rootResource = root.getResource();
        resources.add(rootResource);
        resources.add(ResourceFactory.getDataResource(rootResource));
        resources.add(ResourceFactory.getProvenanceDataResource(rootResource));
        resources.add(ResourceFactory.getPolicyResource(rootResource));

        // add each processor
        for (final ProcessorNode processor : root.findAllProcessors()) {
            final Resource processorResource = processor.getResource();
            resources.add(processorResource);
            resources.add(ResourceFactory.getDataResource(processorResource));
            resources.add(ResourceFactory.getProvenanceDataResource(processorResource));
            resources.add(ResourceFactory.getPolicyResource(processorResource));
            resources.add(ResourceFactory.getOperationResource(processorResource));
        }

        // add each label
        for (final Label label : root.findAllLabels()) {
            final Resource labelResource = label.getResource();
            resources.add(labelResource);
            resources.add(ResourceFactory.getPolicyResource(labelResource));
        }

        // add each process group
        for (final ProcessGroup processGroup : root.findAllProcessGroups()) {
            final Resource processGroupResource = processGroup.getResource();
            resources.add(processGroupResource);
            resources.add(ResourceFactory.getDataResource(processGroupResource));
            resources.add(ResourceFactory.getProvenanceDataResource(processGroupResource));
            resources.add(ResourceFactory.getPolicyResource(processGroupResource));
            resources.add(ResourceFactory.getOperationResource(processGroupResource));
        }

        // add each remote process group
        for (final RemoteProcessGroup remoteProcessGroup : root.findAllRemoteProcessGroups()) {
            final Resource remoteProcessGroupResource = remoteProcessGroup.getResource();
            resources.add(remoteProcessGroupResource);
            resources.add(ResourceFactory.getDataResource(remoteProcessGroupResource));
            resources.add(ResourceFactory.getProvenanceDataResource(remoteProcessGroupResource));
            resources.add(ResourceFactory.getPolicyResource(remoteProcessGroupResource));
            resources.add(ResourceFactory.getOperationResource(remoteProcessGroupResource));
        }

        // add each input port
        for (final Port inputPort : root.findAllInputPorts()) {
            final Resource inputPortResource = inputPort.getResource();
            resources.add(inputPortResource);
            resources.add(ResourceFactory.getDataResource(inputPortResource));
            resources.add(ResourceFactory.getProvenanceDataResource(inputPortResource));
            resources.add(ResourceFactory.getPolicyResource(inputPortResource));
            resources.add(ResourceFactory.getOperationResource(inputPortResource));
            if (inputPort instanceof PublicPort) {
                resources.add(ResourceFactory.getDataTransferResource(inputPortResource));
            }
        }

        // add each output port
        for (final Port outputPort : root.findAllOutputPorts()) {
            final Resource outputPortResource = outputPort.getResource();
            resources.add(outputPortResource);
            resources.add(ResourceFactory.getDataResource(outputPortResource));
            resources.add(ResourceFactory.getProvenanceDataResource(outputPortResource));
            resources.add(ResourceFactory.getPolicyResource(outputPortResource));
            resources.add(ResourceFactory.getOperationResource(outputPortResource));
            if (outputPort instanceof PublicPort) {
                resources.add(ResourceFactory.getDataTransferResource(outputPortResource));
            }
        }

        // add each controller service
        final Consumer<ControllerServiceNode> csConsumer = controllerService -> {
            final Resource controllerServiceResource = controllerService.getResource();
            resources.add(controllerServiceResource);
            resources.add(ResourceFactory.getPolicyResource(controllerServiceResource));
            resources.add(ResourceFactory.getOperationResource(controllerServiceResource));
        };

        flowController.getFlowManager().getAllControllerServices().forEach(csConsumer);
        root.findAllControllerServices().forEach(csConsumer);


        // add each reporting task
        for (final ReportingTaskNode reportingTask : flowController.getAllReportingTasks()) {
            final Resource reportingTaskResource = reportingTask.getResource();
            resources.add(reportingTaskResource);
            resources.add(ResourceFactory.getPolicyResource(reportingTaskResource));
            resources.add(ResourceFactory.getOperationResource(reportingTaskResource));
        }

        // add each parameter provider
        for (final ParameterProviderNode parameterProvider : flowController.getFlowManager().getAllParameterProviders()) {
            final Resource parameterProviderResource = parameterProvider.getResource();
            resources.add(parameterProviderResource);
            resources.add(ResourceFactory.getPolicyResource(parameterProviderResource));
            resources.add(ResourceFactory.getOperationResource(parameterProviderResource));
        }

        // add each flow registry client
        for (final FlowRegistryClientNode flowRegistryClient : flowController.getFlowManager().getAllFlowRegistryClients()) {
            final Resource flowRegistryResource = flowRegistryClient.getResource();
            resources.add(flowRegistryResource);
            resources.add(ResourceFactory.getPolicyResource(flowRegistryResource));
            resources.add(ResourceFactory.getOperationResource(flowRegistryResource));
        }

        return resources;
    }

    /**
     * Gets the available options for searching provenance.
     *
     * @return the available options for searching provenance
     */
    public ProvenanceOptionsDTO getProvenanceSearchOptions() {
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();

        // create the search options dto
        final ProvenanceOptionsDTO searchOptions = new ProvenanceOptionsDTO();
        final List<ProvenanceSearchableFieldDTO> searchableFieldNames = new ArrayList<>();
        final List<SearchableField> fields = provenanceRepository.getSearchableFields();
        for (final SearchableField field : fields) {
            // we exclude the Event Time because it is always searchable but don't want support querying it this way...
            // we prefer the user queries using startDate and endDate
            if (SearchableFields.EventTime.equals(field)) {
                continue;
            }

            final ProvenanceSearchableFieldDTO searchableField = new ProvenanceSearchableFieldDTO();
            searchableField.setId(field.getIdentifier());
            searchableField.setField(field.getSearchableFieldName());
            searchableField.setLabel(field.getFriendlyName());
            searchableField.setType(field.getFieldType().name());
            searchableFieldNames.add(searchableField);
        }
        final List<SearchableField> searchableAttributes = provenanceRepository.getSearchableAttributes();
        for (final SearchableField searchableAttr : searchableAttributes) {
            final ProvenanceSearchableFieldDTO searchableAttribute = new ProvenanceSearchableFieldDTO();
            searchableAttribute.setId(searchableAttr.getIdentifier());
            searchableAttribute.setField(searchableAttr.getSearchableFieldName());
            searchableAttribute.setLabel(searchableAttr.getFriendlyName());
            searchableAttribute.setType(searchableAttr.getFieldType().name());
            searchableFieldNames.add(searchableAttribute);
        }
        searchOptions.setSearchableFields(searchableFieldNames);
        return searchOptions;
    }

    /**
     * Submits a provenance query.
     *
     * @param provenanceDto dto
     * @return provenance info
     */
    public ProvenanceDTO submitProvenance(ProvenanceDTO provenanceDto) {
        final ProvenanceRequestDTO requestDto = provenanceDto.getRequest();

        // create the query
        final Query query = new Query(provenanceDto.getId());

        // if the request was specified
        if (requestDto != null) {
            // add each search term specified
            final Map<String, ProvenanceSearchValueDTO> searchTerms = requestDto.getSearchTerms();
            if (searchTerms != null) {
                for (final Map.Entry<String, ProvenanceSearchValueDTO> searchTerm : searchTerms.entrySet()) {
                    SearchableField field;

                    field = SearchableFields.getSearchableField(searchTerm.getKey());
                    if (field == null) {
                        field = SearchableFields.newSearchableAttribute(searchTerm.getKey());
                    }
                    query.addSearchTerm(SearchTerms.newSearchTerm(field, searchTerm.getValue().getValue(), searchTerm.getValue().getInverse()));
                }
            }

            // specify the start date if specified
            if (requestDto.getStartDate() != null) {
                query.setStartDate(requestDto.getStartDate());
            }

            // ensure an end date is populated
            if (requestDto.getEndDate() != null) {
                query.setEndDate(requestDto.getEndDate());
            }

            // set the min/max file size
            query.setMinFileSize(requestDto.getMinimumFileSize());
            query.setMaxFileSize(requestDto.getMaximumFileSize());

            // set the max results desired
            query.setMaxResults(requestDto.getMaxResults());
        }

        // submit the query to the provenance repository
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
        final QuerySubmission querySubmission = provenanceRepository.submitQuery(query, NiFiUserUtils.getNiFiUser());

        // return the query with the results populated at this point
        return getProvenanceQuery(querySubmission.getQueryIdentifier(), requestDto.getSummarize(), requestDto.getIncrementalResults());
    }

    /**
     * Retrieves the results of a provenance query.
     *
     * @param provenanceId id
     * @return the results of a provenance query
     */
    public ProvenanceDTO getProvenanceQuery(String provenanceId, Boolean summarize, Boolean incrementalResults) {
        try {
            // get the query to the provenance repository
            final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
            final QuerySubmission querySubmission = provenanceRepository.retrieveQuerySubmission(provenanceId, NiFiUserUtils.getNiFiUser());

            // ensure the query results could be found
            if (querySubmission == null) {
                throw new ResourceNotFoundException("Cannot find the results for the specified provenance requests. Results may have been purged.");
            }

            // get the original query and the results
            final Query query = querySubmission.getQuery();
            final QueryResult queryResult = querySubmission.getResult();

            // build the response
            final ProvenanceDTO provenanceDto = new ProvenanceDTO();
            final ProvenanceRequestDTO requestDto = new ProvenanceRequestDTO();
            final ProvenanceResultsDTO resultsDto = new ProvenanceResultsDTO();

            // include the original request and results
            provenanceDto.setRequest(requestDto);
            provenanceDto.setResults(resultsDto);

            // convert the original request
            requestDto.setStartDate(query.getStartDate());
            requestDto.setEndDate(query.getEndDate());
            requestDto.setMinimumFileSize(query.getMinFileSize());
            requestDto.setMaximumFileSize(query.getMaxFileSize());
            requestDto.setMaxResults(query.getMaxResults());
            if (query.getSearchTerms() != null) {
                final Map<String, ProvenanceSearchValueDTO> searchTerms = new HashMap<>();
                for (final SearchTerm searchTerm : query.getSearchTerms()) {
                    final ProvenanceSearchValueDTO searchValueDTO = new ProvenanceSearchValueDTO();
                    searchValueDTO.setValue(searchTerm.getValue());
                    searchValueDTO.setInverse(searchTerm.isInverted());
                    searchTerms.put(searchTerm.getSearchableField().getFriendlyName(), searchValueDTO);
                }
                requestDto.setSearchTerms(searchTerms);
            }

            // convert the provenance
            provenanceDto.setId(query.getIdentifier());
            provenanceDto.setSubmissionTime(querySubmission.getSubmissionTime());
            provenanceDto.setExpiration(queryResult.getExpiration());
            provenanceDto.setFinished(queryResult.isFinished());
            provenanceDto.setPercentCompleted(queryResult.getPercentComplete());

            // convert each event
            final boolean includeResults = incrementalResults == null || Boolean.TRUE.equals(incrementalResults);
            if (includeResults || queryResult.isFinished()) {
                final List<ProvenanceEventDTO> events = new ArrayList<>();
                for (final ProvenanceEventRecord record : queryResult.getMatchingEvents()) {
                    events.add(createProvenanceEventDto(record, Boolean.TRUE.equals(summarize)));
                }
                resultsDto.setProvenanceEvents(events);
            }

            if (requestDto.getMaxResults() != null && queryResult.getTotalHitCount() >= requestDto.getMaxResults()) {
                resultsDto.setTotalCount(requestDto.getMaxResults().longValue());
                resultsDto.setTotal(FormatUtils.formatCount(requestDto.getMaxResults().longValue()) + "+");
            } else {
                resultsDto.setTotalCount(queryResult.getTotalHitCount());
                resultsDto.setTotal(FormatUtils.formatCount(queryResult.getTotalHitCount()));
            }

            // include any errors
            if (queryResult.getError() != null) {
                final Set<String> errors = new HashSet<>();
                errors.add(queryResult.getError());
                resultsDto.setErrors(errors);
            }

            // set the generated timestamp
            final Date now = new Date();
            resultsDto.setGenerated(now);
            resultsDto.setTimeOffset(TimeZone.getDefault().getOffset(now.getTime()));

            // get the oldest available event time
            final List<ProvenanceEventRecord> firstEvent = provenanceRepository.getEvents(0, 1);
            if (!firstEvent.isEmpty()) {
                resultsDto.setOldestEvent(new Date(firstEvent.get(0).getEventTime()));
            }

            provenanceDto.setResults(resultsDto);
            return provenanceDto;
        } catch (final IOException ioe) {
            throw new NiFiCoreException("An error occurred while searching the provenance events.", ioe);
        }
    }

    /**
     * Submits the specified lineage request.
     *
     * @param lineageDto dto
     * @return updated lineage
     */
    public LineageDTO submitLineage(LineageDTO lineageDto) {
        final LineageRequestDTO requestDto = lineageDto.getRequest();

        // get the provenance repo
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
        final ComputeLineageSubmission result;

        if (LineageRequestType.FLOWFILE.equals(requestDto.getLineageRequestType())) {
            if (requestDto.getUuid() != null) {
                // submit uuid if it is specified
                result = provenanceRepository.submitLineageComputation(requestDto.getUuid(), NiFiUserUtils.getNiFiUser());
            } else {
                // submit the event if the flowfile uuid needs to be looked up
                result = provenanceRepository.submitLineageComputation(requestDto.getEventId(), NiFiUserUtils.getNiFiUser());
            }
        } else {
            // submit event... (parents or children)
            if (LineageRequestType.PARENTS.equals(requestDto.getLineageRequestType())) {
                result = provenanceRepository.submitExpandParents(requestDto.getEventId(), NiFiUserUtils.getNiFiUser());
            } else {
                result = provenanceRepository.submitExpandChildren(requestDto.getEventId(), NiFiUserUtils.getNiFiUser());
            }
        }

        return getLineage(result.getLineageIdentifier());
    }

    /**
     * Gets the lineage with the specified id.
     *
     * @param lineageId id
     * @return the lineage with the specified id
     */
    public LineageDTO getLineage(final String lineageId) {
        // get the query to the provenance repository
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
        final ComputeLineageSubmission computeLineageSubmission = provenanceRepository.retrieveLineageSubmission(lineageId, NiFiUserUtils.getNiFiUser());

        // ensure the submission was found
        if (computeLineageSubmission == null) {
            throw new ResourceNotFoundException("Cannot find the results for the specified lineage request. Results may have been purged.");
        }

        return dtoFactory.createLineageDto(computeLineageSubmission);
    }

    /**
     * Deletes the query with the specified id.
     *
     * @param provenanceId id
     */
    public void deleteProvenanceQuery(final String provenanceId) {
        // get the query to the provenance repository
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
        final QuerySubmission querySubmission = provenanceRepository.retrieveQuerySubmission(provenanceId, NiFiUserUtils.getNiFiUser());
        if (querySubmission != null) {
            querySubmission.cancel();
        }
    }

    /**
     * Deletes the lineage with the specified id.
     *
     * @param lineageId id
     */
    public void deleteLineage(final String lineageId) {
        // get the query to the provenance repository
        final ProvenanceRepository provenanceRepository = flowController.getProvenanceRepository();
        final ComputeLineageSubmission computeLineageSubmission = provenanceRepository.retrieveLineageSubmission(lineageId, NiFiUserUtils.getNiFiUser());
        if (computeLineageSubmission != null) {
            computeLineageSubmission.cancel();
        }
    }

    /**
     * Gets the content for the specified claim.
     *
     * @param eventId          event id
     * @param uri              uri
     * @param contentDirection direction
     * @return the content for the specified claim
     */
    public DownloadableContent getContent(final Long eventId, final String uri, final ContentDirection contentDirection) {
        try {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            // get the event in order to get the filename
            final ProvenanceEventRecord event = flowController.getProvenanceRepository().getEvent(eventId, user);
            if (event == null) {
                throw new ResourceNotFoundException("Unable to find the specified event.");
            }

            // get the flowfile attributes
            final Map<String, String> attributes;
            if (ContentDirection.INPUT.equals(contentDirection)) {
                attributes = event.getPreviousAttributes();
            } else {
                attributes = event.getAttributes();
            }

            // authorize the event
            final Authorizable dataAuthorizable = getDataAuthorizable(event);
            dataAuthorizable.authorize(authorizer, RequestAction.READ, user, attributes);

            // get the filename and fall back to the identifier (should never happen)
            String filename = attributes.get(CoreAttributes.FILENAME.key());
            if (filename == null) {
                filename = event.getFlowFileUuid();
            }

            // get the mime-type
            final String type = attributes.get(CoreAttributes.MIME_TYPE.key());

            // get the content
            final InputStream content = flowController.getContent(event, contentDirection, user.getIdentity(), uri);
            return new DownloadableContent(filename, type, content);
        } catch (final ContentNotFoundException cnfe) {
            throw new ResourceNotFoundException("Unable to find the specified content.");
        } catch (final IOException ioe) {
            logger.error("Unable to get the content for event ({}) at this time.", eventId, ioe);
            throw new IllegalStateException("Unable to get the content at this time.");
        }
    }

    private Authorizable getDataAuthorizable(final ProvenanceEventRecord event) {
        if (event.isRemotePortType()) {
            return flowController.getProvenanceAuthorizableFactory().createRemoteDataAuthorizable(event.getComponentId());
        } else {
            return flowController.getProvenanceAuthorizableFactory().createLocalDataAuthorizable(event.getComponentId());
        }
    }

    /**
     * Submits a replay request for the specified event id.
     *
     * @param eventId event id
     * @return provenance event
     */
    public ProvenanceEventDTO submitReplay(final Long eventId) {
        try {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            if (user == null) {
                throw new WebApplicationException(new Throwable("Unable to access details for current user."));
            }

            // lookup the original event
            final ProvenanceEventRecord originalEvent = flowController.getProvenanceRepository().getEvent(eventId, user);
            if (originalEvent == null) {
                throw new ResourceNotFoundException("Unable to find the specified event.");
            }

            // authorize the replay
            authorizeReplay(originalEvent);

            // replay the flow file
            final ProvenanceEventRecord event = flowController.replayFlowFile(originalEvent, user);

            // convert the event record
            return createProvenanceEventDto(event, false);
        } catch (final IOException ioe) {
            throw new NiFiCoreException("An error occurred while getting the specified event.", ioe);
        }
    }

    /**
     * Submits for replay the latest provenance event that is cached for the component with the given ID
     *
     * @param componentId the ID of the component
     * @return the ProvenanceEventDTO representing the event that was replayed, or <code>null</code> if the no event was available
     * @throws AccessDeniedException if an event is available but the current user is not permitted to replay the event
     */
    public ProvenanceEventDTO submitReplayLastEvent(String componentId) {
        try {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();
            if (user == null) {
                throw new WebApplicationException(new Throwable("Unable to access details for current user."));
            }

            // lookup the original event
            final List<ProvenanceEventRecord> latestEvents = flowController.getProvenanceRepository().getLatestCachedEvents(componentId, MAX_REPLAY_EVENT_COUNT);
            if (latestEvents.isEmpty()) {
                return null;
            }

            final Iterator<ProvenanceEventRecord> itr = latestEvents.iterator();
            while (itr.hasNext()) {
                final ProvenanceEventRecord event = itr.next();

                try {
                    // Authorize the replay
                    authorizeReplay(event);

                    // Replay the FlowFile
                    flowController.replayFlowFile(event, user);

                    // convert the event record
                    return createProvenanceEventDto(event, false);
                } catch (final IOException e) {
                    throw e;
                } catch (final Exception e) {
                    if (itr.hasNext()) {
                        logger.debug("Failed to replay Provenance Event {} but will continue to try remaining events", event, e);
                    } else {
                        throw e;
                    }
                }
            }

            // Won't happen, because we will have either thrown an Exception or returned the result of createProvenanceEventDto, but necessary for compiler
            return null;
        } catch (final IOException ioe) {
            throw new NiFiCoreException("An error occurred while getting the specified event.", ioe);
        }
    }

    /**
     * Authorizes access to replay a specified provenance event. Whether to check read data permission can be specified. The context this
     * method is invoked may have already verified these permissions. Using a flag here as it forces the caller to acknowledge this fact
     * limiting the possibility of overlooking it.
     *
     * @param event                    event
     * @param checkReadDataPermissions whether to verify read data permissions
     */
    private AuthorizationResult checkAuthorizationForReplay(final ProvenanceEventRecord event, final boolean checkReadDataPermissions) {
        // if the connection id isn't specified, then the replay wouldn't be available anyways and we have nothing to authorize against so deny it`
        if (event.getSourceQueueIdentifier() == null) {
            return AuthorizationResult.denied("The connection id in the provenance event is unknown.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Authorizable dataAuthorizable = getDataAuthorizable(event);

        final Map<String, String> eventAttributes = event.getAttributes();

        if (checkReadDataPermissions) {
            // ensure we can read the data
            final AuthorizationResult result = dataAuthorizable.checkAuthorization(authorizer, RequestAction.READ, user, eventAttributes);
            if (!Result.Approved.equals(result.getResult())) {
                return result;
            }
        }

        // ensure we can write the data; read the data should have been checked already
        return dataAuthorizable.checkAuthorization(authorizer, RequestAction.WRITE, user, eventAttributes);
    }

    /**
     * Authorizes access to replay for a specified provenance event.
     *
     * @param event event
     */
    private void authorizeReplay(final ProvenanceEventRecord event) {
        // if the connection id isn't specified, then the replay wouldn't be available anyways and we have nothing to authorize against so deny it`
        if (event.getSourceQueueIdentifier() == null) {
            throw new AccessDeniedException("The connection id in the provenance event is unknown.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Authorizable dataAuthorizable = getDataAuthorizable(event);

        // ensure we can read and write the data
        final Map<String, String> eventAttributes = event.getAttributes();
        dataAuthorizable.authorize(authorizer, RequestAction.READ, user, eventAttributes);
        dataAuthorizable.authorize(authorizer, RequestAction.WRITE, user, eventAttributes);
    }

    /**
     * Authorizes access to data for a specified provenance event.
     *
     * @param event event
     */
    private AuthorizationResult checkAuthorizationForData(ProvenanceEventRecord event) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Authorizable dataAuthorizable = getDataAuthorizable(event);

        final Map<String, String> eventAttributes = event.getAttributes();

        // ensure we can read the data
        return dataAuthorizable.checkAuthorization(authorizer, RequestAction.READ, user, eventAttributes);
    }

    /**
     * Get the provenance event with the specified event id.
     *
     * @param eventId event id
     * @return the provenance event with the specified event id
     */
    public ProvenanceEventDTO getProvenanceEvent(final Long eventId) {
        try {
            final ProvenanceEventRecord event = flowController.getProvenanceRepository().getEvent(eventId, NiFiUserUtils.getNiFiUser());
            if (event == null) {
                throw new ResourceNotFoundException("Unable to find the specified event.");
            }

            // convert the event
            return createProvenanceEventDto(event, false);
        } catch (final IOException ioe) {
            throw new NiFiCoreException("An error occurred while getting the specified event.", ioe);
        }
    }

    public LatestProvenanceEventsDTO getLatestProvenanceEvents(final String componentId, final int eventLimit) {
        final Authorizable authorizable = flowController.getProvenanceAuthorizableFactory().createProvenanceDataAuthorizable(componentId);
        final Authorizer authorizer = flowController.getAuthorizer();
        if (!authorizable.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser())) {
            throw new AccessDeniedException("User does not have permission to view the latest events for the specified component.");
        }

        try {
            final List<ProvenanceEventRecord> events = flowController.getProvenanceRepository().getLatestCachedEvents(componentId, eventLimit);
            final List<ProvenanceEventDTO> eventDtos = new ArrayList<>();
            for (final ProvenanceEventRecord event : events) {
                eventDtos.add(createProvenanceEventDto(event, false));
            }

            final LatestProvenanceEventsDTO dto = new LatestProvenanceEventsDTO();
            dto.setComponentId(componentId);
            dto.setProvenanceEvents(eventDtos);

            return dto;
        } catch (final IOException ioe) {
            throw new NiFiCoreException("An error occurred while getting the latest events for the specified component.", ioe);
        }
    }

    /**
     * Creates a ProvenanceEventDTO for the specified ProvenanceEventRecord. This should only be invoked once the
     * current user has been authorized for access to this provenance event.
     *
     * @param event event
     * @return event
     */
    private ProvenanceEventDTO createProvenanceEventDto(final ProvenanceEventRecord event, final boolean summarize) {
        final ProvenanceEventDTO dto = new ProvenanceEventDTO();
        dto.setId(String.valueOf(event.getEventId()));
        dto.setEventId(event.getEventId());
        dto.setEventTime(new Date(event.getEventTime()));
        dto.setEventType(event.getEventType().name());
        dto.setFlowFileUuid(event.getFlowFileUuid());
        dto.setFileSize(FormatUtils.formatDataSize(event.getFileSize()));
        dto.setFileSizeBytes(event.getFileSize());
        dto.setComponentId(event.getComponentId());
        dto.setComponentType(event.getComponentType());

        // only include all details if not summarizing
        if (!summarize) {
            // convert the attributes
            final Comparator<AttributeDTO> attributeComparator = (a1, a2) -> Collator.getInstance(Locale.US).compare(a1.getName(), a2.getName());

            final SortedSet<AttributeDTO> attributes = new TreeSet<>(attributeComparator);

            final Map<String, String> updatedAttrs = event.getUpdatedAttributes();
            final Map<String, String> previousAttrs = event.getPreviousAttributes();

            // add previous attributes that haven't been modified.
            for (final Map.Entry<String, String> entry : previousAttrs.entrySet()) {
                // don't add any attributes that have been updated; we will do that next
                if (updatedAttrs.containsKey(entry.getKey())) {
                    continue;
                }

                final AttributeDTO attribute = new AttributeDTO();
                attribute.setName(entry.getKey());
                attribute.setValue(entry.getValue());
                attribute.setPreviousValue(entry.getValue());
                attributes.add(attribute);
            }

            // Add all of the update attributes
            for (final Map.Entry<String, String> entry : updatedAttrs.entrySet()) {
                final AttributeDTO attribute = new AttributeDTO();
                attribute.setName(entry.getKey());
                attribute.setValue(entry.getValue());
                attribute.setPreviousValue(previousAttrs.get(entry.getKey()));
                attributes.add(attribute);
            }

            // additional event details
            dto.setAlternateIdentifierUri(event.getAlternateIdentifierUri());
            dto.setTransitUri(event.getTransitUri());
            dto.setSourceSystemFlowFileId(event.getSourceSystemFlowFileIdentifier());
            dto.setRelationship(event.getRelationship());
            dto.setDetails(event.getDetails());

            // set flowfile attributes and content only if approved for view the data
            final AuthorizationResult dataResult = checkAuthorizationForData(event);
            if (Result.Approved.equals(dataResult.getResult())) {
                final ContentAvailability contentAvailability = flowController.getContentAvailability(event);

                // attributes
                dto.setAttributes(attributes);

                // content
                dto.setContentEqual(contentAvailability.isContentSame());
                dto.setInputContentAvailable(contentAvailability.isInputAvailable());
                dto.setInputContentClaimSection(event.getPreviousContentClaimSection());
                dto.setInputContentClaimContainer(event.getPreviousContentClaimContainer());
                dto.setInputContentClaimIdentifier(event.getPreviousContentClaimIdentifier());
                dto.setInputContentClaimOffset(event.getPreviousContentClaimOffset());
                dto.setInputContentClaimFileSizeBytes(event.getPreviousFileSize());
                dto.setOutputContentAvailable(contentAvailability.isOutputAvailable());
                dto.setOutputContentClaimSection(event.getContentClaimSection());
                dto.setOutputContentClaimContainer(event.getContentClaimContainer());
                dto.setOutputContentClaimIdentifier(event.getContentClaimIdentifier());
                dto.setOutputContentClaimOffset(event.getContentClaimOffset());
                dto.setOutputContentClaimFileSize(FormatUtils.formatDataSize(event.getFileSize()));
                dto.setOutputContentClaimFileSizeBytes(event.getFileSize());

                // format the previous file sizes if possible
                if (event.getPreviousFileSize() != null) {
                    dto.setInputContentClaimFileSize(FormatUtils.formatDataSize(event.getPreviousFileSize()));
                }

                // determine if authorized for event replay - only need to check write as read was verified above
                final AuthorizationResult replayAuthorized = checkAuthorizationForReplay(event, false);

                // replay
                dto.setReplayAvailable(contentAvailability.isReplayable() && Result.Approved.equals(replayAuthorized.getResult()));
                dto.setReplayExplanation(contentAvailability.isReplayable()
                        && !Result.Approved.equals(replayAuthorized.getResult()) ? replayAuthorized.getExplanation() : contentAvailability.getReasonNotReplayable());
                dto.setSourceConnectionIdentifier(event.getSourceQueueIdentifier());
            } else {
                dto.setReplayAvailable(false);
                dto.setReplayExplanation(dataResult.getExplanation());
            }

            // event duration
            if (event.getEventDuration() >= 0) {
                dto.setEventDuration(event.getEventDuration());
            }

            // lineage duration
            if (event.getLineageStartDate() > 0) {
                final long lineageDuration = event.getEventTime() - event.getLineageStartDate();
                dto.setLineageDuration(lineageDuration);
            }

            // parent uuids
            final List<String> parentUuids = new ArrayList<>(event.getParentUuids());
            parentUuids.sort(Collator.getInstance(Locale.US));
            dto.setParentUuids(parentUuids);

            // child uuids
            final List<String> childUuids = new ArrayList<>(event.getChildUuids());
            childUuids.sort(Collator.getInstance(Locale.US));
            dto.setChildUuids(childUuids);
        }

        // sets the component details if it can find the component still in the flow
        setComponentDetails(dto);

        return dto;
    }

    private void setComponentDetails(final ProvenanceEventDTO dto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ProcessGroup root = getRootGroup();

        final Connectable connectable = findLocalConnectable(dto.getComponentId());
        if (connectable != null) {
            dto.setGroupId(connectable.getProcessGroup().getIdentifier());

            // if the user is approved for this component policy, provide additional details, otherwise override/redact as necessary
            if (Result.Approved.equals(connectable.checkAuthorization(authorizer, RequestAction.READ, user).getResult())) {
                dto.setComponentName(connectable.getName());
            } else {
                dto.setComponentType(connectable.getConnectableType().toString());
                dto.setComponentName(dto.getComponentId());
                dto.setRelationship(null);
            }

            return;
        }

        final RemoteGroupPort remoteGroupPort = root.findRemoteGroupPort(dto.getComponentId());
        if (remoteGroupPort != null) {
            dto.setGroupId(remoteGroupPort.getProcessGroupIdentifier());

            // if the user is approved for this component policy, provide additional details, otherwise override/redact as necessary
            if (Result.Approved.equals(remoteGroupPort.checkAuthorization(authorizer, RequestAction.READ, user).getResult())) {
                dto.setComponentName(remoteGroupPort.getName());
            } else {
                dto.setComponentName(dto.getComponentId());
                dto.setRelationship(null);
            }

            return;
        }

        final Connection connection = root.findConnection(dto.getComponentId());
        if (connection != null) {
            dto.setGroupId(connection.getProcessGroup().getIdentifier());

            // if the user is approved for this component policy, provide additional details, otherwise override/redact as necessary
            if (Result.Approved.equals(connection.checkAuthorization(authorizer, RequestAction.READ, user).getResult())) {
                String name = connection.getName();
                final Collection<Relationship> relationships = connection.getRelationships();
                if (StringUtils.isBlank(name) && CollectionUtils.isNotEmpty(relationships)) {
                    name = StringUtils.join(relationships.stream().map(Relationship::getName).collect(Collectors.toSet()), ", ");
                }
                dto.setComponentName(name);
            } else {
                dto.setComponentName(dto.getComponentId());
                dto.setRelationship(null);
            }
        }
    }

    /**
     * Searches this controller for the specified term.
     *
     * @param searchLiteral search string specified by the user
     * @param activeGroupId the identifier of the currently visited group
     * @return result
     */
    public SearchResultsDTO search(final String searchLiteral, final String activeGroupId) {
        final ProcessGroup rootGroup = getRootGroup();
        final ProcessGroup activeGroup = (activeGroupId == null)
                ? rootGroup
                : flowController.getFlowManager().getGroup(activeGroupId);
        final SearchResultsDTO results = new SearchResultsDTO();
        final SearchQuery searchQuery = searchQueryParser.parse(searchLiteral, NiFiUserUtils.getNiFiUser(), rootGroup, activeGroup);

        if (!StringUtils.isEmpty(searchQuery.getTerm())) {
            controllerSearchService.search(searchQuery, results);
            controllerSearchService.searchParameters(searchQuery, results);
        }

        return results;
    }

    public void verifyComponentTypes(VersionedProcessGroup versionedFlow) {
        flowController.verifyComponentTypesInSnippet(versionedFlow);
    }

    public ProcessorDiagnosticsDTO getProcessorDiagnostics(final ProcessorNode processor, final ProcessorStatus processorStatus, final BulletinRepository bulletinRepository,
                                                           final Function<String, ControllerServiceEntity> serviceEntityFactory) {
        return dtoFactory.createProcessorDiagnosticsDto(processor, processorStatus, bulletinRepository, flowController, serviceEntityFactory);
    }

    public FlowFileEventRepository getFlowFileEventRepository() {
        return flowController.getFlowFileEventRepository();
    }

    /**
     * Returns the storage usage of all provenance repositories
     *
     * @return the map of all the storage usage
     */
    public Map<String, StorageUsage> getProvenanceRepositoryStorageUsage() {
        return flowController.getEventAccess().getProvenanceRepositoryStorageUsage();
    }

    /**
     * Returns the storage usage of all content repositories
     *
     * @return the map of all the storage usage
     */
    public Map<String, StorageUsage> getContentRepositoryStorageUsage() {
        return flowController.getEventAccess().getContentRepositoryStorageUsage();
    }

    /**
     * Returns the storage usage of the flow file repository
     *
     * @return the storage usage
     */
    public StorageUsage getFlowFileRepositoryStorageUsage() {
        return flowController.getEventAccess().getFlowFileRepositoryStorageUsage();
    }

    public VersionedReportingTaskImporter createReportingTaskImporter() {
        return new StandardVersionedReportingTaskImporter(flowController);
    }

    /*
     * setters
     */

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setFlowService(FlowService flowService) {
        this.flowService = flowService;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setDtoFactory(DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setSearchQueryParser(SearchQueryParser searchQueryParser) {
        this.searchQueryParser = searchQueryParser;
    }

    public void setControllerSearchService(ControllerSearchService controllerSearchService) {
        this.controllerSearchService = controllerSearchService;
    }

    public void setRuntimeManifestService(RuntimeManifestService runtimeManifestService) {
        this.runtimeManifestService = runtimeManifestService;
    }
}
