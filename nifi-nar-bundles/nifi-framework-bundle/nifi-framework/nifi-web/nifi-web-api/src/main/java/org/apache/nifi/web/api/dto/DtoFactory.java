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
package org.apache.nifi.web.api.dto;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.component.details.ComponentDetails;
import org.apache.nifi.action.component.details.ExtensionDetails;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.component.details.FlowChangeRemoteProcessGroupDetails;
import org.apache.nifi.action.component.details.RemoteProcessGroupDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeConnectDetails;
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileSummary;
import org.apache.nifi.controller.queue.ListFlowFileState;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.RemoteQueuePartitionDiagnostics;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.state.SortedStateUtils;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.GarbageCollectionHistory;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.diagnostics.GarbageCollection;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupCounts;
import org.apache.nifi.history.History;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.ProvenanceEventLineageNode;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedComponent;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedConnection;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedControllerService;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedFunnel;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedLabel;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedPort;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessor;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedRemoteProcessGroup;
import org.apache.nifi.registry.variable.VariableRegistryUpdateRequest;
import org.apache.nifi.registry.variable.VariableRegistryUpdateStep;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.FlowModification;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.component.details.ComponentDetailsDTO;
import org.apache.nifi.web.api.dto.action.component.details.ExtensionDetailsDTO;
import org.apache.nifi.web.api.dto.action.component.details.RemoteProcessGroupDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.ActionDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.ConfigureDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.ConnectDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.MoveDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.PurgeDetailsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ClassLoaderDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ConnectionDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ConnectionDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.ControllerServiceDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.GCDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.GarbageCollectionDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMControllerDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMFlowDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMSystemDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.LocalQueuePartitionDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.RemoteQueuePartitionDTO;
import org.apache.nifi.web.api.dto.diagnostics.RepositoryUsageDTO;
import org.apache.nifi.web.api.dto.diagnostics.ThreadDumpDTO;
import org.apache.nifi.web.api.dto.flow.FlowBreadcrumbDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO.LineageRequestType;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageResultsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.ProvenanceLinkDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.ProvenanceNodeDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.revision.RevisionManager;

import javax.ws.rs.WebApplicationException;
import java.text.Collator;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class DtoFactory {

    @SuppressWarnings("rawtypes")
    private final static Comparator<Class> CLASS_NAME_COMPARATOR = new Comparator<Class>() {
        @Override
        public int compare(final Class class1, final Class class2) {
            return Collator.getInstance(Locale.US).compare(class1.getSimpleName(), class2.getSimpleName());
        }
    };
    public static final String SENSITIVE_VALUE_MASK = "********";

    private BulletinRepository bulletinRepository;
    private ControllerServiceProvider controllerServiceProvider;
    private EntityFactory entityFactory;
    private Authorizer authorizer;
    private ExtensionManager extensionManager;

    public ControllerConfigurationDTO createControllerConfigurationDto(final ControllerFacade controllerFacade) {
        final ControllerConfigurationDTO dto = new ControllerConfigurationDTO();
        dto.setMaxTimerDrivenThreadCount(controllerFacade.getMaxTimerDrivenThreadCount());
        dto.setMaxEventDrivenThreadCount(controllerFacade.getMaxEventDrivenThreadCount());
        return dto;
    }

    public FlowConfigurationDTO createFlowConfigurationDto(final String autoRefreshInterval,
                                                           final Long defaultBackPressureObjectThreshold,
                                                           final String defaultBackPressureDataSizeThreshold) {
        final FlowConfigurationDTO dto = new FlowConfigurationDTO();

        // get the refresh interval
        final long refreshInterval = FormatUtils.getTimeDuration(autoRefreshInterval, TimeUnit.SECONDS);
        dto.setAutoRefreshIntervalSeconds(refreshInterval);
        dto.setSupportsManagedAuthorizer(AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer));
        dto.setSupportsConfigurableUsersAndGroups(AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer));
        dto.setSupportsConfigurableAuthorizer(AuthorizerCapabilityDetection.isConfigurableAccessPolicyProvider(authorizer));

        final Date now = new Date();
        dto.setTimeOffset(TimeZone.getDefault().getOffset(now.getTime()));
        dto.setCurrentTime(now);

        dto.setDefaultBackPressureDataSizeThreshold(defaultBackPressureDataSizeThreshold);
        dto.setDefaultBackPressureObjectThreshold(defaultBackPressureObjectThreshold);

        return dto;
    }

    /**
     * Creates an ActionDTO for the specified Action.
     *
     * @param action action
     * @return dto
     */
    public ActionDTO createActionDto(final Action action) {
        final ActionDTO actionDto = new ActionDTO();
        actionDto.setId(action.getId());
        actionDto.setSourceId(action.getSourceId());
        actionDto.setSourceName(action.getSourceName());
        actionDto.setSourceType(action.getSourceType().toString());
        actionDto.setTimestamp(action.getTimestamp());
        actionDto.setUserIdentity(action.getUserIdentity());
        actionDto.setOperation(action.getOperation().toString());
        actionDto.setActionDetails(createActionDetailsDto(action.getActionDetails()));
        actionDto.setComponentDetails(createComponentDetailsDto(action.getComponentDetails()));

        return actionDto;
    }

    /**
     * Creates an ActionDetailsDTO for the specified ActionDetails.
     *
     * @param actionDetails details
     * @return dto
     */
    private ActionDetailsDTO createActionDetailsDto(final ActionDetails actionDetails) {
        if (actionDetails == null) {
            return null;
        }

        if (actionDetails instanceof FlowChangeConfigureDetails) {
            final ConfigureDetailsDTO configureDetails = new ConfigureDetailsDTO();
            configureDetails.setName(((ConfigureDetails) actionDetails).getName());
            configureDetails.setPreviousValue(((ConfigureDetails) actionDetails).getPreviousValue());
            configureDetails.setValue(((ConfigureDetails) actionDetails).getValue());
            return configureDetails;
        } else if (actionDetails instanceof FlowChangeConnectDetails) {
            final ConnectDetailsDTO connectDetails = new ConnectDetailsDTO();
            connectDetails.setSourceId(((ConnectDetails) actionDetails).getSourceId());
            connectDetails.setSourceName(((ConnectDetails) actionDetails).getSourceName());
            connectDetails.setSourceType(((ConnectDetails) actionDetails).getSourceType().toString());
            connectDetails.setRelationship(((ConnectDetails) actionDetails).getRelationship());
            connectDetails.setDestinationId(((ConnectDetails) actionDetails).getDestinationId());
            connectDetails.setDestinationName(((ConnectDetails) actionDetails).getDestinationName());
            connectDetails.setDestinationType(((ConnectDetails) actionDetails).getDestinationType().toString());
            return connectDetails;
        } else if (actionDetails instanceof FlowChangeMoveDetails) {
            final MoveDetailsDTO moveDetails = new MoveDetailsDTO();
            moveDetails.setPreviousGroup(((MoveDetails) actionDetails).getPreviousGroup());
            moveDetails.setPreviousGroupId(((MoveDetails) actionDetails).getPreviousGroupId());
            moveDetails.setGroup(((MoveDetails) actionDetails).getGroup());
            moveDetails.setGroupId(((MoveDetails) actionDetails).getGroupId());
            return moveDetails;
        } else if (actionDetails instanceof FlowChangePurgeDetails) {
            final PurgeDetailsDTO purgeDetails = new PurgeDetailsDTO();
            purgeDetails.setEndDate(((PurgeDetails) actionDetails).getEndDate());
            return purgeDetails;
        } else {
            throw new WebApplicationException(new IllegalArgumentException(String.format("Unrecognized type of action details encountered %s during serialization.", actionDetails.toString())));
        }
    }

    /**
     * Creates a ComponentDetailsDTO for the specified ComponentDetails.
     *
     * @param componentDetails details
     * @return dto
     */
    private ComponentDetailsDTO createComponentDetailsDto(final ComponentDetails componentDetails) {
        if (componentDetails == null) {
            return null;
        }

        if (componentDetails instanceof FlowChangeExtensionDetails) {
            final ExtensionDetailsDTO processorDetails = new ExtensionDetailsDTO();
            processorDetails.setType(((ExtensionDetails) componentDetails).getType());
            return processorDetails;
        } else if (componentDetails instanceof FlowChangeRemoteProcessGroupDetails) {
            final RemoteProcessGroupDetailsDTO remoteProcessGroupDetails = new RemoteProcessGroupDetailsDTO();
            remoteProcessGroupDetails.setUri(((RemoteProcessGroupDetails) componentDetails).getUri());
            return remoteProcessGroupDetails;
        } else {
            throw new WebApplicationException(new IllegalArgumentException(String.format("Unrecognized type of component details encountered %s during serialization. ", componentDetails.toString())));
        }
    }

    /**
     * Creates a HistoryDTO from the specified History.
     *
     * @param history history
     * @return dto
     */
    public HistoryDTO createHistoryDto(final History history) {
        final HistoryDTO historyDto = new HistoryDTO();
        historyDto.setTotal(history.getTotal());
        historyDto.setLastRefreshed(history.getLastRefreshed());
        return historyDto;
    }

    /**
     * Creates a ComponentStateDTO for the given component and state's.
     *
     * @param componentId component id
     * @param localState local state
     * @param clusterState cluster state
     * @return dto
     */
    public ComponentStateDTO createComponentStateDTO(final String componentId, final Class<?> componentClass, final StateMap localState, final StateMap clusterState) {
        final ComponentStateDTO dto = new ComponentStateDTO();
        dto.setComponentId(componentId);
        dto.setStateDescription(getStateDescription(componentClass));
        dto.setLocalState(createStateMapDTO(Scope.LOCAL, localState));
        dto.setClusterState(createStateMapDTO(Scope.CLUSTER, clusterState));
        return dto;
    }

    /**
     * Gets the description of the state this component persists.
     *
     * @param componentClass the component class
     * @return state description
     */
    private String getStateDescription(final Class<?> componentClass) {
        final Stateful capabilityDesc = componentClass.getAnnotation(Stateful.class);
        if (capabilityDesc != null) {
            return capabilityDesc.description();
        } else {
            return null;
        }
    }

    /**
     * Creates a StateMapDTO for the given scope and state map.
     *
     * @param scope the scope
     * @param stateMap the state map
     * @return dto
     */
    public StateMapDTO createStateMapDTO(final Scope scope, final StateMap stateMap) {
        if (stateMap == null) {
            return null;
        }

        final StateMapDTO dto = new StateMapDTO();
        dto.setScope(scope.toString());

        final TreeMap<String, String> sortedState = new TreeMap<>(SortedStateUtils.getKeyComparator());
        final Map<String, String> state = stateMap.toMap();
        sortedState.putAll(state);

        int count = 0;
        final List<StateEntryDTO> stateEntries = new ArrayList<>();
        final Set<Map.Entry<String, String>> entrySet = sortedState.entrySet();
        for (final Iterator<Entry<String, String>> iter = entrySet.iterator(); iter.hasNext() && count++ < SortedStateUtils.MAX_COMPONENT_STATE_ENTRIES;) {
            final Map.Entry<String, String> entry = iter.next();
            final StateEntryDTO entryDTO = new StateEntryDTO();
            entryDTO.setKey(entry.getKey());
            entryDTO.setValue(entry.getValue());
            stateEntries.add(entryDTO);
        }
        dto.setTotalEntryCount(state.size());
        dto.setState(stateEntries);

        return dto;
    }

    /**
     * Creates CounterDTOs for each Counter specified.
     *
     * @param counterDtos dtos
     * @return dto
     */
    public CountersSnapshotDTO createCountersDto(final Collection<CounterDTO> counterDtos) {
        final CountersSnapshotDTO dto = new CountersSnapshotDTO();
        dto.setCounters(counterDtos);
        dto.setGenerated(new Date());
        return dto;
    }

    /**
     * Creates a CounterDTO from the specified Counter.
     *
     * @param counter counter
     * @return dto
     */
    public CounterDTO createCounterDto(final Counter counter) {
        final CounterDTO dto = new CounterDTO();
        dto.setId(counter.getIdentifier());
        dto.setContext(counter.getContext());
        dto.setName(counter.getName());
        dto.setValueCount(counter.getValue());
        dto.setValue(FormatUtils.formatCount(counter.getValue()));
        return dto;
    }

    /**
     * Creates a PositionDTO from the specified position
     *
     * @param position position
     * @return dto
     */
    public PositionDTO createPositionDto(final Position position) {
        return new PositionDTO(position.getX(), position.getY());
    }

    private boolean isDropRequestComplete(final DropFlowFileState state) {
        return DropFlowFileState.COMPLETE.equals(state) || DropFlowFileState.CANCELED.equals(state) || DropFlowFileState.FAILURE.equals(state);
    }

    /**
     * Creates a DropRequestDTO from the specified flow file status.
     *
     * @param dropRequest dropRequest
     * @return dto
     */
    public DropRequestDTO createDropRequestDTO(final DropFlowFileStatus dropRequest) {
        final DropRequestDTO dto = new DropRequestDTO();
        dto.setId(dropRequest.getRequestIdentifier());
        dto.setSubmissionTime(new Date(dropRequest.getRequestSubmissionTime()));
        dto.setLastUpdated(new Date(dropRequest.getLastUpdated()));
        dto.setState(dropRequest.getState().toString());
        dto.setFailureReason(dropRequest.getFailureReason());
        dto.setFinished(isDropRequestComplete(dropRequest.getState()));

        final QueueSize dropped = dropRequest.getDroppedSize();
        dto.setDroppedCount(dropped.getObjectCount());
        dto.setDroppedSize(dropped.getByteCount());
        dto.setDropped(FormatUtils.formatCount(dropped.getObjectCount()) + " / " + FormatUtils.formatDataSize(dropped.getByteCount()));

        final QueueSize current = dropRequest.getCurrentSize();
        dto.setCurrentCount(current.getObjectCount());
        dto.setCurrentSize(current.getByteCount());
        dto.setCurrent(FormatUtils.formatCount(current.getObjectCount()) + " / " + FormatUtils.formatDataSize(current.getByteCount()));

        final QueueSize original = dropRequest.getOriginalSize();
        dto.setOriginalCount(original.getObjectCount());
        dto.setOriginalSize(original.getByteCount());
        dto.setOriginal(FormatUtils.formatCount(original.getObjectCount()) + " / " + FormatUtils.formatDataSize(original.getByteCount()));

        if (isDropRequestComplete(dropRequest.getState())) {
            dto.setPercentCompleted(100);
        } else {
            dto.setPercentCompleted((dropped.getObjectCount() * 100) / original.getObjectCount());
        }

        return dto;
    }

    private boolean isListingRequestComplete(final ListFlowFileState state) {
        return ListFlowFileState.COMPLETE.equals(state) || ListFlowFileState.CANCELED.equals(state) || ListFlowFileState.FAILURE.equals(state);
    }

    private QueueSizeDTO createQueueSizeDTO(final QueueSize queueSize) {
        final QueueSizeDTO dto = new QueueSizeDTO();
        dto.setByteCount(queueSize.getByteCount());
        dto.setObjectCount(queueSize.getObjectCount());
        return dto;
    }

    /**
     * Creates a ListingRequestDTO from the specified ListFlowFileStatus.
     *
     * @param listingRequest listingRequest
     * @return dto
     */
    public ListingRequestDTO createListingRequestDTO(final ListFlowFileStatus listingRequest) {
        final ListingRequestDTO dto = new ListingRequestDTO();
        dto.setId(listingRequest.getRequestIdentifier());
        dto.setSubmissionTime(new Date(listingRequest.getRequestSubmissionTime()));
        dto.setLastUpdated(new Date(listingRequest.getLastUpdated()));
        dto.setState(listingRequest.getState().toString());
        dto.setFailureReason(listingRequest.getFailureReason());
        dto.setFinished(isListingRequestComplete(listingRequest.getState()));
        dto.setMaxResults(listingRequest.getMaxResults());
        dto.setPercentCompleted(listingRequest.getCompletionPercentage());

        dto.setQueueSize(createQueueSizeDTO(listingRequest.getQueueSize()));

        if (isListingRequestComplete(listingRequest.getState())) {
            final List<FlowFileSummary> flowFileSummaries = listingRequest.getFlowFileSummaries();
            if (flowFileSummaries != null) {
                final Date now = new Date();
                final List<FlowFileSummaryDTO> summaryDtos = new ArrayList<>(flowFileSummaries.size());
                for (final FlowFileSummary summary : flowFileSummaries) {
                    summaryDtos.add(createFlowFileSummaryDTO(summary, now));
                }
                dto.setFlowFileSummaries(summaryDtos);
            }
        }

        return dto;
    }

    /**
     * Creates a FlowFileSummaryDTO from the specified FlowFileSummary.
     *
     * @param summary summary
     * @return dto
     */
    public FlowFileSummaryDTO createFlowFileSummaryDTO(final FlowFileSummary summary, final Date now) {
        final FlowFileSummaryDTO dto = new FlowFileSummaryDTO();
        dto.setUuid(summary.getUuid());
        dto.setFilename(summary.getFilename());
        dto.setPenalized(summary.isPenalized());
        dto.setPosition(summary.getPosition());
        dto.setSize(summary.getSize());

        final long queuedDuration = now.getTime() - summary.getLastQueuedTime();
        dto.setQueuedDuration(queuedDuration);

        final long age = now.getTime() - summary.getLineageStartDate();
        dto.setLineageDuration(age);

        return dto;
    }

    /**
     * Creates a FlowFileDTO from the specified FlowFileRecord.
     *
     * @param record record
     * @return dto
     */
    public FlowFileDTO createFlowFileDTO(final FlowFileRecord record) {
        final Date now = new Date();
        final FlowFileDTO dto = new FlowFileDTO();
        dto.setUuid(record.getAttribute(CoreAttributes.UUID.key()));
        dto.setFilename(record.getAttribute(CoreAttributes.FILENAME.key()));
        dto.setPenalized(record.isPenalized());
        dto.setSize(record.getSize());
        dto.setAttributes(record.getAttributes());

        final long queuedDuration = now.getTime() - record.getLastQueueDate();
        dto.setQueuedDuration(queuedDuration);

        final long age = now.getTime() - record.getLineageStartDate();
        dto.setLineageDuration(age);

        final ContentClaim contentClaim = record.getContentClaim();
        if (contentClaim != null) {
            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            dto.setContentClaimSection(resourceClaim.getSection());
            dto.setContentClaimContainer(resourceClaim.getContainer());
            dto.setContentClaimIdentifier(resourceClaim.getId());
            dto.setContentClaimOffset(contentClaim.getOffset() + record.getContentClaimOffset());
            dto.setContentClaimFileSizeBytes(record.getSize());
            dto.setContentClaimFileSize(FormatUtils.formatDataSize(record.getSize()));
        }

        return dto;
    }

    /**
     * Creates a ConnectionDTO from the specified Connection.
     *
     * @param connection connection
     * @return dto
     */
    public ConnectionDTO createConnectionDto(final Connection connection) {
        if (connection == null) {
            return null;
        }

        final ConnectionDTO dto = new ConnectionDTO();

        dto.setId(connection.getIdentifier());
        dto.setParentGroupId(connection.getProcessGroup().getIdentifier());

        final List<PositionDTO> bendPoints = new ArrayList<>();
        for (final Position bendPoint : connection.getBendPoints()) {
            bendPoints.add(createPositionDto(bendPoint));
        }
        dto.setBends(bendPoints);
        dto.setName(connection.getName());
        dto.setLabelIndex(connection.getLabelIndex());
        dto.setzIndex(connection.getZIndex());
        dto.setSource(createConnectableDto(connection.getSource()));
        dto.setDestination(createConnectableDto(connection.getDestination()));
        dto.setVersionedComponentId(connection.getVersionedComponentId().orElse(null));

        final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();

        dto.setBackPressureObjectThreshold(flowFileQueue.getBackPressureObjectThreshold());
        dto.setBackPressureDataSizeThreshold(flowFileQueue.getBackPressureDataSizeThreshold());
        dto.setFlowFileExpiration(flowFileQueue.getFlowFileExpiration());
        dto.setPrioritizers(new ArrayList<String>());
        for (final FlowFilePrioritizer comparator : flowFileQueue.getPriorities()) {
            dto.getPrioritizers().add(comparator.getClass().getCanonicalName());
        }

        // For ports, we do not want to populate the relationships.
        for (final Relationship selectedRelationship : connection.getRelationships()) {
            if (!Relationship.ANONYMOUS.equals(selectedRelationship)) {
                if (dto.getSelectedRelationships() == null) {
                    dto.setSelectedRelationships(new TreeSet<String>(Collator.getInstance(Locale.US)));
                }

                dto.getSelectedRelationships().add(selectedRelationship.getName());
            }
        }

        // For ports, we do not want to populate the relationships.
        for (final Relationship availableRelationship : connection.getSource().getRelationships()) {
            if (!Relationship.ANONYMOUS.equals(availableRelationship)) {
                if (dto.getAvailableRelationships() == null) {
                    dto.setAvailableRelationships(new TreeSet<String>(Collator.getInstance(Locale.US)));
                }

                dto.getAvailableRelationships().add(availableRelationship.getName());
            }
        }

        final LoadBalanceStrategy loadBalanceStrategy = flowFileQueue.getLoadBalanceStrategy();
        dto.setLoadBalancePartitionAttribute(flowFileQueue.getPartitioningAttribute());
        dto.setLoadBalanceStrategy(loadBalanceStrategy.name());
        dto.setLoadBalanceCompression(flowFileQueue.getLoadBalanceCompression().name());

        if (loadBalanceStrategy == LoadBalanceStrategy.DO_NOT_LOAD_BALANCE) {
            dto.setLoadBalanceStatus(ConnectionDTO.LOAD_BALANCE_NOT_CONFIGURED);
        } else if (flowFileQueue.isActivelyLoadBalancing()) {
            dto.setLoadBalanceStatus(ConnectionDTO.LOAD_BALANCE_ACTIVE);
        } else {
            dto.setLoadBalanceStatus(ConnectionDTO.LOAD_BALANCE_INACTIVE);
        }

        return dto;
    }

    /**
     * Creates a ConnectableDTO from the specified Connectable.
     *
     * @param connectable connectable
     * @return dto
     */
    public ConnectableDTO createConnectableDto(final Connectable connectable) {
        if (connectable == null) {
            return null;
        }

        boolean isAuthorized = connectable.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());

        final ConnectableDTO dto = new ConnectableDTO();
        dto.setId(connectable.getIdentifier());
        dto.setName(isAuthorized ? connectable.getName() : connectable.getIdentifier());
        dto.setType(connectable.getConnectableType().name());
        dto.setVersionedComponentId(connectable.getVersionedComponentId().orElse(null));

        if (connectable instanceof RemoteGroupPort) {
            final RemoteGroupPort remoteGroupPort = (RemoteGroupPort) connectable;
            final RemoteProcessGroup remoteGroup = remoteGroupPort.getRemoteProcessGroup();
            dto.setGroupId(remoteGroup.getIdentifier());
            dto.setRunning(remoteGroupPort.isTargetRunning());
            dto.setTransmitting(remoteGroupPort.isRunning());
            dto.setExists(remoteGroupPort.getTargetExists());
            if (isAuthorized) {
                dto.setComments(remoteGroup.getComments());
            }
        } else {
            dto.setGroupId(connectable.getProcessGroup().getIdentifier());
            dto.setRunning(connectable.isRunning());
            if (isAuthorized) {
                dto.setComments(connectable.getComments());
            }
        }

        return dto;
    }

    /**
     * Creates a LabelDTO from the specified Label.
     *
     * @param label label
     * @return dto
     */
    public LabelDTO createLabelDto(final Label label) {
        if (label == null) {
            return null;
        }

        final LabelDTO dto = new LabelDTO();
        dto.setId(label.getIdentifier());
        dto.setPosition(createPositionDto(label.getPosition()));
        dto.setStyle(label.getStyle());
        dto.setHeight(label.getSize().getHeight());
        dto.setWidth(label.getSize().getWidth());
        dto.setLabel(label.getValue());
        dto.setParentGroupId(label.getProcessGroup().getIdentifier());
        dto.setVersionedComponentId(label.getVersionedComponentId().orElse(null));

        return dto;
    }

    /**
     * Creates a {@link UserDTO} from the specified {@link User}.
     *
     * @param user user
     * @return dto
     */
    public UserDTO createUserDto(final User user, final Set<TenantEntity> groups, final Set<AccessPolicySummaryEntity> accessPolicies) {
        if (user == null) {
            return null;
        }

        final UserDTO dto = new UserDTO();
        dto.setId(user.getIdentifier());
        dto.setUserGroups(groups);
        dto.setIdentity(user.getIdentity());
        dto.setConfigurable(AuthorizerCapabilityDetection.isUserConfigurable(authorizer, user));
        dto.setAccessPolicies(accessPolicies);

        return dto;
    }

    /**
     * Creates a {@link TenantDTO} from the specified {@link User}.
     *
     * @param user user
     * @return dto
     */
    public TenantDTO createTenantDTO(User user) {
        if (user == null) {
            return null;
        }

        final TenantDTO dto = new TenantDTO();
        dto.setId(user.getIdentifier());
        dto.setIdentity(user.getIdentity());
        dto.setConfigurable(AuthorizerCapabilityDetection.isUserConfigurable(authorizer, user));

        return dto;
    }

    /**
     * Creates a {@link UserGroupDTO} from the specified {@link Group}.
     *
     * @param userGroup user group
     * @return dto
     */
    public UserGroupDTO createUserGroupDto(final Group userGroup, Set<TenantEntity> users, final Set<AccessPolicySummaryEntity> accessPolicies) {
        if (userGroup == null) {
            return null;
        }

        // convert to access policies to handle backward compatibility due to incorrect
        // type in the UserGroupDTO
        final Set<AccessPolicyEntity> policies = accessPolicies.stream().map(summaryEntity -> {
            final AccessPolicyDTO policy = new AccessPolicyDTO();
            policy.setId(summaryEntity.getId());

            if (summaryEntity.getPermissions().getCanRead()) {
                final AccessPolicySummaryDTO summary = summaryEntity.getComponent();
                policy.setResource(summary.getResource());
                policy.setAction(summary.getAction());
                policy.setConfigurable(summary.getConfigurable());
                policy.setComponentReference(summary.getComponentReference());
            }

            return entityFactory.createAccessPolicyEntity(policy, summaryEntity.getRevision(), summaryEntity.getPermissions());
        }).collect(Collectors.toSet());

        final UserGroupDTO dto = new UserGroupDTO();
        dto.setId(userGroup.getIdentifier());
        dto.setUsers(users);
        dto.setIdentity(userGroup.getName());
        dto.setConfigurable(AuthorizerCapabilityDetection.isGroupConfigurable(authorizer, userGroup));
        dto.setAccessPolicies(policies);

        return dto;
    }

    /**
     * Creates a {@link TenantDTO} from the specified {@link User}.
     *
     * @param userGroup user
     * @return dto
     */
    public TenantDTO createTenantDTO(Group userGroup) {
        if (userGroup == null) {
            return null;
        }

        final TenantDTO dto = new TenantDTO();
        dto.setId(userGroup.getIdentifier());
        dto.setIdentity(userGroup.getName());
        dto.setConfigurable(AuthorizerCapabilityDetection.isGroupConfigurable(authorizer, userGroup));

        return dto;
    }

    /**
     * Creates a FunnelDTO from the specified Funnel.
     *
     * @param funnel funnel
     * @return dto
     */
    public FunnelDTO createFunnelDto(final Funnel funnel) {
        if (funnel == null) {
            return null;
        }

        final FunnelDTO dto = new FunnelDTO();
        dto.setId(funnel.getIdentifier());
        dto.setPosition(createPositionDto(funnel.getPosition()));
        dto.setParentGroupId(funnel.getProcessGroup().getIdentifier());
        dto.setVersionedComponentId(funnel.getVersionedComponentId().orElse(null));

        return dto;
    }

    /**
     * Creates a SnippetDTO from the specified Snippet.
     *
     * @param snippet snippet
     * @return dto
     */
    public SnippetDTO createSnippetDto(final Snippet snippet) {
        final SnippetDTO dto = new SnippetDTO();
        dto.setId(snippet.getId());
        dto.setParentGroupId(snippet.getParentGroupId());

        // populate the snippet contents ids
        dto.setConnections(mapRevisionToDto(snippet.getConnections()));
        dto.setFunnels(mapRevisionToDto(snippet.getFunnels()));
        dto.setInputPorts(mapRevisionToDto(snippet.getInputPorts()));
        dto.setLabels(mapRevisionToDto(snippet.getLabels()));
        dto.setOutputPorts(mapRevisionToDto(snippet.getOutputPorts()));
        dto.setProcessGroups(mapRevisionToDto(snippet.getProcessGroups()));
        dto.setProcessors(mapRevisionToDto(snippet.getProcessors()));
        dto.setRemoteProcessGroups(mapRevisionToDto(snippet.getRemoteProcessGroups()));

        return dto;
    }

    private Map<String, RevisionDTO> mapRevisionToDto(final Map<String, Revision> revisionMap) {
        final Map<String, RevisionDTO> dtos = new HashMap<>(revisionMap.size());
        for (final Map.Entry<String, Revision> entry : revisionMap.entrySet()) {
            final Revision revision = entry.getValue();
            final RevisionDTO revisionDto = new RevisionDTO();
            revisionDto.setClientId(revision.getClientId());
            revisionDto.setVersion(revision.getVersion());

            dtos.put(entry.getKey(), revisionDto);
        }
        return dtos;
    }

    /**
     * Creates a TemplateDTO from the specified template.
     *
     * @param template template
     * @return dto
     */
    public TemplateDTO createTemplateDTO(final Template template) {
        if (template == null) {
            return null;
        }

        final TemplateDTO original = template.getDetails();

        final TemplateDTO copy = new TemplateDTO();
        copy.setId(original.getId());
        copy.setGroupId(template.getProcessGroup().getIdentifier());
        copy.setName(original.getName());
        copy.setDescription(original.getDescription());
        copy.setTimestamp(original.getTimestamp());
        copy.setUri(original.getUri());
        copy.setEncodingVersion(original.getEncodingVersion());

        return copy;
    }


    public RemoteProcessGroupStatusDTO createRemoteProcessGroupStatusDto(final RemoteProcessGroup remoteProcessGroup, final RemoteProcessGroupStatus remoteProcessGroupStatus) {
        final RemoteProcessGroupStatusDTO dto = new RemoteProcessGroupStatusDTO();
        dto.setId(remoteProcessGroupStatus.getId());
        dto.setGroupId(remoteProcessGroupStatus.getGroupId());
        dto.setTargetUri(remoteProcessGroupStatus.getTargetUri());
        dto.setName(remoteProcessGroupStatus.getName());
        dto.setTransmissionStatus(remoteProcessGroupStatus.getTransmissionStatus().toString());
        dto.setStatsLastRefreshed(new Date());
        dto.setValidationStatus(getRemoteProcessGroupValidationStatus(remoteProcessGroup).name());

        final RemoteProcessGroupStatusSnapshotDTO snapshot = new RemoteProcessGroupStatusSnapshotDTO();
        dto.setAggregateSnapshot(snapshot);

        snapshot.setId(remoteProcessGroupStatus.getId());
        snapshot.setGroupId(remoteProcessGroupStatus.getGroupId());
        snapshot.setName(remoteProcessGroupStatus.getName());
        snapshot.setTargetUri(remoteProcessGroupStatus.getTargetUri());
        snapshot.setTransmissionStatus(remoteProcessGroupStatus.getTransmissionStatus().toString());

        snapshot.setActiveThreadCount(remoteProcessGroupStatus.getActiveThreadCount());
        snapshot.setFlowFilesSent(remoteProcessGroupStatus.getSentCount());
        snapshot.setBytesSent(remoteProcessGroupStatus.getSentContentSize());
        snapshot.setFlowFilesReceived(remoteProcessGroupStatus.getReceivedCount());
        snapshot.setBytesReceived(remoteProcessGroupStatus.getReceivedContentSize());

        StatusMerger.updatePrettyPrintedFields(snapshot);
        return dto;
    }

    private ValidationStatus getRemoteProcessGroupValidationStatus(RemoteProcessGroup remoteProcessGroup) {
        final boolean hasAuthIssue = remoteProcessGroup.getAuthorizationIssue() != null && !remoteProcessGroup.getAuthorizationIssue().isEmpty();
        final Collection<ValidationResult> validationResults = remoteProcessGroup.validate();
        final boolean hasValidationIssue = validationResults != null && !validationResults.isEmpty();
        return hasAuthIssue || hasValidationIssue ? ValidationStatus.INVALID : ValidationStatus.VALID;
    }

    public ProcessGroupStatusDTO createConciseProcessGroupStatusDto(final ProcessGroupStatus processGroupStatus) {
        final ProcessGroupStatusDTO processGroupStatusDto = new ProcessGroupStatusDTO();
        processGroupStatusDto.setId(processGroupStatus.getId());
        processGroupStatusDto.setName(processGroupStatus.getName());
        processGroupStatusDto.setStatsLastRefreshed(new Date());

        final ProcessGroupStatusSnapshotDTO snapshot = new ProcessGroupStatusSnapshotDTO();
        processGroupStatusDto.setAggregateSnapshot(snapshot);

        snapshot.setId(processGroupStatus.getId());
        snapshot.setName(processGroupStatus.getName());

        if (processGroupStatus.getVersionedFlowState() != null) {
            snapshot.setVersionedFlowState(processGroupStatus.getVersionedFlowState().name());
        }

        snapshot.setFlowFilesQueued(processGroupStatus.getQueuedCount());
        snapshot.setBytesQueued(processGroupStatus.getQueuedContentSize());
        snapshot.setBytesRead(processGroupStatus.getBytesRead());
        snapshot.setBytesWritten(processGroupStatus.getBytesWritten());
        snapshot.setFlowFilesIn(processGroupStatus.getInputCount());
        snapshot.setBytesIn(processGroupStatus.getInputContentSize());
        snapshot.setFlowFilesOut(processGroupStatus.getOutputCount());
        snapshot.setBytesOut(processGroupStatus.getOutputContentSize());
        snapshot.setFlowFilesTransferred(processGroupStatus.getFlowFilesTransferred());
        snapshot.setBytesTransferred(processGroupStatus.getBytesTransferred());
        snapshot.setFlowFilesSent(processGroupStatus.getFlowFilesSent());
        snapshot.setBytesSent(processGroupStatus.getBytesSent());
        snapshot.setFlowFilesReceived(processGroupStatus.getFlowFilesReceived());
        snapshot.setBytesReceived(processGroupStatus.getBytesReceived());

        snapshot.setActiveThreadCount(processGroupStatus.getActiveThreadCount());
        snapshot.setTerminatedThreadCount(processGroupStatus.getTerminatedThreadCount());

        StatusMerger.updatePrettyPrintedFields(snapshot);
        return processGroupStatusDto;
    }

    public ProcessGroupStatusDTO createProcessGroupStatusDto(final ProcessGroup processGroup, final ProcessGroupStatus processGroupStatus) {
        final ProcessGroupStatusDTO processGroupStatusDto = createConciseProcessGroupStatusDto(processGroupStatus);
        final ProcessGroupStatusSnapshotDTO snapshot = processGroupStatusDto.getAggregateSnapshot();

        // processor status
        final Collection<ProcessorStatusSnapshotEntity> processorStatusSnapshotEntities = new ArrayList<>();
        snapshot.setProcessorStatusSnapshots(processorStatusSnapshotEntities);
        final Collection<ProcessorStatus> processorStatusCollection = processGroupStatus.getProcessorStatus();
        if (processorStatusCollection != null) {
            for (final ProcessorStatus processorStatus : processorStatusCollection) {
                final ProcessorStatusDTO processorStatusDto = createProcessorStatusDto(processorStatus);
                final ProcessorNode processor = processGroup.findProcessor(processorStatusDto.getId());
                final PermissionsDTO processorPermissions = createPermissionsDto(processor);
                processorStatusSnapshotEntities.add(entityFactory.createProcessorStatusSnapshotEntity(processorStatusDto.getAggregateSnapshot(), processorPermissions));
            }
        }

        // connection status
        final Collection<ConnectionStatusSnapshotEntity> connectionStatusDtoCollection = new ArrayList<>();
        snapshot.setConnectionStatusSnapshots(connectionStatusDtoCollection);
        final Collection<ConnectionStatus> connectionStatusCollection = processGroupStatus.getConnectionStatus();
        if (connectionStatusCollection != null) {
            for (final ConnectionStatus connectionStatus : connectionStatusCollection) {
                final ConnectionStatusDTO connectionStatusDto = createConnectionStatusDto(connectionStatus);
                final Connection connection = processGroup.findConnection(connectionStatusDto.getId());
                final PermissionsDTO connectionPermissions = createPermissionsDto(connection);
                connectionStatusDtoCollection.add(entityFactory.createConnectionStatusSnapshotEntity(connectionStatusDto.getAggregateSnapshot(), connectionPermissions));
            }
        }

        // local child process groups
        final Collection<ProcessGroupStatusSnapshotEntity> childProcessGroupStatusDtoCollection = new ArrayList<>();
        snapshot.setProcessGroupStatusSnapshots(childProcessGroupStatusDtoCollection);
        final Collection<ProcessGroupStatus> childProcessGroupStatusCollection = processGroupStatus.getProcessGroupStatus();
        if (childProcessGroupStatusCollection != null) {
            for (final ProcessGroupStatus childProcessGroupStatus : childProcessGroupStatusCollection) {
                final ProcessGroupStatusDTO childProcessGroupStatusDto = createProcessGroupStatusDto(processGroup, childProcessGroupStatus);
                final ProcessGroup childProcessGroup = processGroup.findProcessGroup(childProcessGroupStatusDto.getId());
                final PermissionsDTO childProcessGroupPermissions = createPermissionsDto(childProcessGroup);
                childProcessGroupStatusDtoCollection.add(entityFactory.createProcessGroupStatusSnapshotEntity(childProcessGroupStatusDto.getAggregateSnapshot(), childProcessGroupPermissions));
            }
        }

        // remote child process groups
        final Collection<RemoteProcessGroupStatusSnapshotEntity> childRemoteProcessGroupStatusDtoCollection = new ArrayList<>();
        snapshot.setRemoteProcessGroupStatusSnapshots(childRemoteProcessGroupStatusDtoCollection);
        final Collection<RemoteProcessGroupStatus> childRemoteProcessGroupStatusCollection = processGroupStatus.getRemoteProcessGroupStatus();
        if (childRemoteProcessGroupStatusCollection != null) {
            for (final RemoteProcessGroupStatus childRemoteProcessGroupStatus : childRemoteProcessGroupStatusCollection) {
                final RemoteProcessGroup remoteProcessGroup = processGroup.findRemoteProcessGroup(childRemoteProcessGroupStatus.getId());
                final RemoteProcessGroupStatusDTO childRemoteProcessGroupStatusDto = createRemoteProcessGroupStatusDto(remoteProcessGroup, childRemoteProcessGroupStatus);
                final PermissionsDTO remoteProcessGroupPermissions = createPermissionsDto(remoteProcessGroup);
                childRemoteProcessGroupStatusDtoCollection.add(entityFactory.createRemoteProcessGroupStatusSnapshotEntity(childRemoteProcessGroupStatusDto.getAggregateSnapshot(),
                        remoteProcessGroupPermissions));
            }
        }

        // input ports
        final Collection<PortStatusSnapshotEntity> inputPortStatusDtoCollection = new ArrayList<>();
        snapshot.setInputPortStatusSnapshots(inputPortStatusDtoCollection);
        final Collection<PortStatus> inputPortStatusCollection = processGroupStatus.getInputPortStatus();
        if (inputPortStatusCollection != null) {
            for (final PortStatus portStatus : inputPortStatusCollection) {
                final PortStatusDTO portStatusDto = createPortStatusDto(portStatus);
                final Port inputPort = processGroup.findInputPort(portStatus.getId());
                final PermissionsDTO inputPortPermissions = createPermissionsDto(inputPort);
                inputPortStatusDtoCollection.add(entityFactory.createPortStatusSnapshotEntity(portStatusDto.getAggregateSnapshot(), inputPortPermissions));
            }
        }

        // output ports
        final Collection<PortStatusSnapshotEntity> outputPortStatusDtoCollection = new ArrayList<>();
        snapshot.setOutputPortStatusSnapshots(outputPortStatusDtoCollection);
        final Collection<PortStatus> outputPortStatusCollection = processGroupStatus.getOutputPortStatus();
        if (outputPortStatusCollection != null) {
            for (final PortStatus portStatus : outputPortStatusCollection) {
                final PortStatusDTO portStatusDto = createPortStatusDto(portStatus);
                final Port outputPort = processGroup.findOutputPort(portStatus.getId());
                final PermissionsDTO outputPortPermissions = createPermissionsDto(outputPort);
                outputPortStatusDtoCollection.add(entityFactory.createPortStatusSnapshotEntity(portStatusDto.getAggregateSnapshot(), outputPortPermissions));
            }
        }

        return processGroupStatusDto;
    }

    public ConnectionStatusDTO createConnectionStatusDto(final ConnectionStatus connectionStatus) {
        final ConnectionStatusDTO connectionStatusDto = new ConnectionStatusDTO();
        connectionStatusDto.setGroupId(connectionStatus.getGroupId());
        connectionStatusDto.setId(connectionStatus.getId());
        connectionStatusDto.setName(connectionStatus.getName());
        connectionStatusDto.setSourceId(connectionStatus.getSourceId());
        connectionStatusDto.setSourceName(connectionStatus.getSourceName());
        connectionStatusDto.setDestinationId(connectionStatus.getDestinationId());
        connectionStatusDto.setDestinationName(connectionStatus.getDestinationName());
        connectionStatusDto.setStatsLastRefreshed(new Date());

        final ConnectionStatusSnapshotDTO snapshot = new ConnectionStatusSnapshotDTO();
        connectionStatusDto.setAggregateSnapshot(snapshot);

        snapshot.setId(connectionStatus.getId());
        snapshot.setGroupId(connectionStatus.getGroupId());
        snapshot.setName(connectionStatus.getName());
        snapshot.setSourceName(connectionStatus.getSourceName());
        snapshot.setDestinationName(connectionStatus.getDestinationName());

        snapshot.setFlowFilesQueued(connectionStatus.getQueuedCount());
        snapshot.setBytesQueued(connectionStatus.getQueuedBytes());

        snapshot.setFlowFilesIn(connectionStatus.getInputCount());
        snapshot.setBytesIn(connectionStatus.getInputBytes());

        snapshot.setFlowFilesOut(connectionStatus.getOutputCount());
        snapshot.setBytesOut(connectionStatus.getOutputBytes());

        if (connectionStatus.getBackPressureObjectThreshold() > 0) {
            snapshot.setPercentUseCount(Math.min(100, StatusMerger.getUtilization(connectionStatus.getQueuedCount(), connectionStatus.getBackPressureObjectThreshold())));
        }
        if (connectionStatus.getBackPressureBytesThreshold() > 0) {
            snapshot.setPercentUseBytes(Math.min(100, StatusMerger.getUtilization(connectionStatus.getQueuedBytes(), connectionStatus.getBackPressureBytesThreshold())));
        }

        StatusMerger.updatePrettyPrintedFields(snapshot);

        return connectionStatusDto;
    }

    public ProcessorStatusDTO createProcessorStatusDto(final ProcessorStatus procStatus) {
        final ProcessorStatusDTO dto = new ProcessorStatusDTO();
        dto.setId(procStatus.getId());
        dto.setGroupId(procStatus.getGroupId());
        dto.setName(procStatus.getName());
        dto.setStatsLastRefreshed(new Date());
        dto.setRunStatus(procStatus.getRunStatus().toString());

        final ProcessorStatusSnapshotDTO snapshot = new ProcessorStatusSnapshotDTO();
        dto.setAggregateSnapshot(snapshot);

        snapshot.setId(procStatus.getId());
        snapshot.setGroupId(procStatus.getGroupId());
        snapshot.setName(procStatus.getName());

        snapshot.setFlowFilesOut(procStatus.getOutputCount());
        snapshot.setBytesOut(procStatus.getOutputBytes());

        snapshot.setFlowFilesIn(procStatus.getInputCount());
        snapshot.setBytesIn(procStatus.getInputBytes());

        snapshot.setBytesRead(procStatus.getBytesRead());
        snapshot.setBytesWritten(procStatus.getBytesWritten());

        snapshot.setTaskCount(procStatus.getInvocations());
        snapshot.setTasksDurationNanos(procStatus.getProcessingNanos());
        snapshot.setTasksDuration(FormatUtils.formatHoursMinutesSeconds(procStatus.getProcessingNanos(), TimeUnit.NANOSECONDS));

        // determine the run status
        snapshot.setRunStatus(procStatus.getRunStatus().toString());
        snapshot.setExecutionNode(procStatus.getExecutionNode().toString());

        snapshot.setActiveThreadCount(procStatus.getActiveThreadCount());
        snapshot.setTerminatedThreadCount(procStatus.getTerminatedThreadCount());
        snapshot.setType(procStatus.getType());

        StatusMerger.updatePrettyPrintedFields(snapshot);
        return dto;
    }

    /**
     * Creates a PortStatusDTO for the specified PortStatus.
     *
     * @param portStatus status
     * @return dto
     */
    public PortStatusDTO createPortStatusDto(final PortStatus portStatus) {
        final PortStatusDTO dto = new PortStatusDTO();
        dto.setId(portStatus.getId());
        dto.setGroupId(portStatus.getGroupId());
        dto.setName(portStatus.getName());
        dto.setRunStatus(portStatus.getRunStatus().toString());
        dto.setTransmitting(portStatus.isTransmitting());
        dto.setStatsLastRefreshed(new Date());

        final PortStatusSnapshotDTO snapshot = new PortStatusSnapshotDTO();
        dto.setAggregateSnapshot(snapshot);

        snapshot.setId(portStatus.getId());
        snapshot.setGroupId(portStatus.getGroupId());
        snapshot.setName(portStatus.getName());
        snapshot.setRunStatus(portStatus.getRunStatus().toString());

        snapshot.setActiveThreadCount(portStatus.getActiveThreadCount());
        snapshot.setFlowFilesOut(portStatus.getOutputCount());
        snapshot.setBytesOut(portStatus.getOutputBytes());

        snapshot.setFlowFilesIn(portStatus.getInputCount());
        snapshot.setBytesIn(portStatus.getInputBytes());
        StatusMerger.updatePrettyPrintedFields(snapshot);

        return dto;
    }

    /**
     * Copies the specified snippet.
     *
     * @param originalSnippet snippet
     * @return dto
     */
    public FlowSnippetDTO copySnippetContents(final FlowSnippetDTO originalSnippet) {
        final FlowSnippetDTO copySnippet = new FlowSnippetDTO();

        if (originalSnippet.getConnections() != null) {
            for (final ConnectionDTO connection : originalSnippet.getConnections()) {
                copySnippet.getConnections().add(copy(connection));
            }
        }
        if (originalSnippet.getInputPorts() != null) {
            for (final PortDTO port : originalSnippet.getInputPorts()) {
                copySnippet.getInputPorts().add(copy(port));
            }
        }
        if (originalSnippet.getOutputPorts() != null) {
            for (final PortDTO port : originalSnippet.getOutputPorts()) {
                copySnippet.getOutputPorts().add(copy(port));
            }
        }
        if (originalSnippet.getProcessGroups() != null) {
            for (final ProcessGroupDTO processGroup : originalSnippet.getProcessGroups()) {
                copySnippet.getProcessGroups().add(copy(processGroup, true));
            }
        }
        if (originalSnippet.getProcessors() != null) {
            for (final ProcessorDTO processor : originalSnippet.getProcessors()) {
                copySnippet.getProcessors().add(copy(processor));
            }
        }
        if (originalSnippet.getLabels() != null) {
            for (final LabelDTO label : originalSnippet.getLabels()) {
                copySnippet.getLabels().add(copy(label));
            }
        }
        if (originalSnippet.getFunnels() != null) {
            for (final FunnelDTO funnel : originalSnippet.getFunnels()) {
                copySnippet.getFunnels().add(copy(funnel));
            }
        }
        if (originalSnippet.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteGroup : originalSnippet.getRemoteProcessGroups()) {
                copySnippet.getRemoteProcessGroups().add(copy(remoteGroup));
            }
        }
        if (originalSnippet.getControllerServices() != null) {
            for (final ControllerServiceDTO controllerService : originalSnippet.getControllerServices()) {
                copySnippet.getControllerServices().add(copy(controllerService));
            }
        }

        return copySnippet;
    }

    /**
     * Creates a PortDTO from the specified Port.
     *
     * @param port port
     * @return dto
     */
    public PortDTO createPortDto(final Port port) {
        if (port == null) {
            return null;
        }

        final PortDTO dto = new PortDTO();
        dto.setId(port.getIdentifier());
        dto.setPosition(createPositionDto(port.getPosition()));
        dto.setName(port.getName());
        dto.setComments(port.getComments());
        dto.setConcurrentlySchedulableTaskCount(port.getMaxConcurrentTasks());
        dto.setParentGroupId(port.getProcessGroup().getIdentifier());
        dto.setState(port.getScheduledState().toString());
        dto.setType(port.getConnectableType().name());
        dto.setVersionedComponentId(port.getVersionedComponentId().orElse(null));

        // if this port is on the root group, determine if its actually connected to another nifi
        if (port instanceof RootGroupPort) {
            final RootGroupPort rootGroupPort = (RootGroupPort) port;
            dto.setTransmitting(rootGroupPort.isTransmitting());
            dto.setGroupAccessControl(rootGroupPort.getGroupAccessControl());
            dto.setUserAccessControl(rootGroupPort.getUserAccessControl());
        }

        final Collection<ValidationResult> validationErrors = port.getValidationErrors();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    public ReportingTaskDTO createReportingTaskDto(final ReportingTaskNode reportingTaskNode) {
        final BundleCoordinate bundleCoordinate = reportingTaskNode.getBundleCoordinate();
        final List<Bundle> compatibleBundles = extensionManager.getBundles(reportingTaskNode.getCanonicalClassName()).stream().filter(bundle -> {
            final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
            return bundleCoordinate.getGroup().equals(coordinate.getGroup()) && bundleCoordinate.getId().equals(coordinate.getId());
        }).collect(Collectors.toList());

        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setId(reportingTaskNode.getIdentifier());
        dto.setName(reportingTaskNode.getName());
        dto.setType(reportingTaskNode.getCanonicalClassName());
        dto.setBundle(createBundleDto(bundleCoordinate));
        dto.setSchedulingStrategy(reportingTaskNode.getSchedulingStrategy().name());
        dto.setSchedulingPeriod(reportingTaskNode.getSchedulingPeriod());
        dto.setState(reportingTaskNode.getScheduledState().name());
        dto.setActiveThreadCount(reportingTaskNode.getActiveThreadCount());
        dto.setAnnotationData(reportingTaskNode.getAnnotationData());
        dto.setComments(reportingTaskNode.getComments());
        dto.setPersistsState(reportingTaskNode.getReportingTask().getClass().isAnnotationPresent(Stateful.class));
        dto.setRestricted(reportingTaskNode.isRestricted());
        dto.setDeprecated(reportingTaskNode.isDeprecated());
        dto.setExtensionMissing(reportingTaskNode.isExtensionMissing());
        dto.setMultipleVersionsAvailable(compatibleBundles.size() > 1);

        final Map<String, String> defaultSchedulingPeriod = new HashMap<>();
        defaultSchedulingPeriod.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod());
        defaultSchedulingPeriod.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod());
        dto.setDefaultSchedulingPeriod(defaultSchedulingPeriod);

        // sort a copy of the properties
        final Map<PropertyDescriptor, String> sortedProperties = new TreeMap<>(new Comparator<PropertyDescriptor>() {
            @Override
            public int compare(final PropertyDescriptor o1, final PropertyDescriptor o2) {
                return Collator.getInstance(Locale.US).compare(o1.getName(), o2.getName());
            }
        });
        sortedProperties.putAll(reportingTaskNode.getProperties());

        // get the property order from the reporting task
        final ReportingTask reportingTask = reportingTaskNode.getReportingTask();
        final Map<PropertyDescriptor, String> orderedProperties = new LinkedHashMap<>();
        final List<PropertyDescriptor> descriptors = reportingTask.getPropertyDescriptors();
        if (descriptors != null && !descriptors.isEmpty()) {
            for (final PropertyDescriptor descriptor : descriptors) {
                orderedProperties.put(descriptor, null);
            }
        }
        orderedProperties.putAll(sortedProperties);

        // build the descriptor and property dtos
        dto.setDescriptors(new LinkedHashMap<String, PropertyDescriptorDTO>());
        dto.setProperties(new LinkedHashMap<String, String>());
        for (final Map.Entry<PropertyDescriptor, String> entry : orderedProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();

            // store the property descriptor
            dto.getDescriptors().put(descriptor.getName(), createPropertyDescriptorDto(descriptor, null));

            // determine the property value - don't include sensitive properties
            String propertyValue = entry.getValue();
            if (propertyValue != null && descriptor.isSensitive()) {
                propertyValue = SENSITIVE_VALUE_MASK;
            }

            // set the property value
            dto.getProperties().put(descriptor.getName(), propertyValue);
        }

        final ValidationStatus validationStatus = reportingTaskNode.getValidationStatus(1, TimeUnit.MILLISECONDS);
        dto.setValidationStatus(validationStatus.name());

        // add the validation errors
        final Collection<ValidationResult> validationErrors = reportingTaskNode.getValidationErrors();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    public ControllerServiceDTO createControllerServiceDto(final ControllerServiceNode controllerServiceNode) {
        final BundleCoordinate bundleCoordinate = controllerServiceNode.getBundleCoordinate();
        final List<Bundle> compatibleBundles = extensionManager.getBundles(controllerServiceNode.getCanonicalClassName()).stream().filter(bundle -> {
            final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
            return bundleCoordinate.getGroup().equals(coordinate.getGroup()) && bundleCoordinate.getId().equals(coordinate.getId());
        }).collect(Collectors.toList());

        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setId(controllerServiceNode.getIdentifier());
        dto.setParentGroupId(controllerServiceNode.getProcessGroup() == null ? null : controllerServiceNode.getProcessGroup().getIdentifier());
        dto.setName(controllerServiceNode.getName());
        dto.setType(controllerServiceNode.getCanonicalClassName());
        dto.setBundle(createBundleDto(bundleCoordinate));
        dto.setControllerServiceApis(createControllerServiceApiDto(controllerServiceNode.getControllerServiceImplementation().getClass()));
        dto.setState(controllerServiceNode.getState().name());
        dto.setAnnotationData(controllerServiceNode.getAnnotationData());
        dto.setComments(controllerServiceNode.getComments());
        dto.setPersistsState(controllerServiceNode.getControllerServiceImplementation().getClass().isAnnotationPresent(Stateful.class));
        dto.setRestricted(controllerServiceNode.isRestricted());
        dto.setDeprecated(controllerServiceNode.isDeprecated());
        dto.setExtensionMissing(controllerServiceNode.isExtensionMissing());
        dto.setMultipleVersionsAvailable(compatibleBundles.size() > 1);
        dto.setVersionedComponentId(controllerServiceNode.getVersionedComponentId().orElse(null));

        // sort a copy of the properties
        final Map<PropertyDescriptor, String> sortedProperties = new TreeMap<>(new Comparator<PropertyDescriptor>() {
            @Override
            public int compare(final PropertyDescriptor o1, final PropertyDescriptor o2) {
                return Collator.getInstance(Locale.US).compare(o1.getName(), o2.getName());
            }
        });
        sortedProperties.putAll(controllerServiceNode.getProperties());

        // get the property order from the controller service
        final ControllerService controllerService = controllerServiceNode.getControllerServiceImplementation();
        final Map<PropertyDescriptor, String> orderedProperties = new LinkedHashMap<>();
        final List<PropertyDescriptor> descriptors = controllerService.getPropertyDescriptors();
        if (descriptors != null && !descriptors.isEmpty()) {
            for (final PropertyDescriptor descriptor : descriptors) {
                orderedProperties.put(descriptor, null);
            }
        }
        orderedProperties.putAll(sortedProperties);

        // build the descriptor and property dtos
        dto.setDescriptors(new LinkedHashMap<String, PropertyDescriptorDTO>());
        dto.setProperties(new LinkedHashMap<String, String>());
        for (final Map.Entry<PropertyDescriptor, String> entry : orderedProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();

            // store the property descriptor
            final String groupId = controllerServiceNode.getProcessGroup() == null ? null : controllerServiceNode.getProcessGroup().getIdentifier();
            dto.getDescriptors().put(descriptor.getName(), createPropertyDescriptorDto(descriptor, groupId));

            // determine the property value - don't include sensitive properties
            String propertyValue = entry.getValue();
            if (propertyValue != null && descriptor.isSensitive()) {
                propertyValue = SENSITIVE_VALUE_MASK;
            }

            // set the property value
            dto.getProperties().put(descriptor.getName(), propertyValue);
        }

        dto.setValidationStatus(controllerServiceNode.getValidationStatus(1, TimeUnit.MILLISECONDS).name());

        // add the validation errors
        final Collection<ValidationResult> validationErrors = controllerServiceNode.getValidationErrors();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    public ControllerServiceReferencingComponentDTO createControllerServiceReferencingComponentDTO(final ComponentNode component) {
        final ControllerServiceReferencingComponentDTO dto = new ControllerServiceReferencingComponentDTO();
        dto.setId(component.getIdentifier());
        dto.setName(component.getName());

        String processGroupId = null;
        List<PropertyDescriptor> propertyDescriptors = null;
        Collection<ValidationResult> validationErrors = null;
        if (component instanceof ProcessorNode) {
            final ProcessorNode node = ((ProcessorNode) component);
            dto.setGroupId(node.getProcessGroup().getIdentifier());
            dto.setState(node.getScheduledState().name());
            dto.setActiveThreadCount(node.getActiveThreadCount());
            dto.setType(node.getComponentType());
            dto.setReferenceType(Processor.class.getSimpleName());

            propertyDescriptors = node.getProcessor().getPropertyDescriptors();
            validationErrors = node.getValidationErrors();
            processGroupId = node.getProcessGroup().getIdentifier();
        } else if (component instanceof ControllerServiceNode) {
            final ControllerServiceNode node = ((ControllerServiceNode) component);
            dto.setState(node.getState().name());
            dto.setType(node.getComponentType());
            dto.setReferenceType(ControllerService.class.getSimpleName());

            propertyDescriptors = node.getControllerServiceImplementation().getPropertyDescriptors();
            validationErrors = node.getValidationErrors();
            processGroupId = node.getProcessGroup() == null ? null : node.getProcessGroup().getIdentifier();
        } else if (component instanceof ReportingTaskNode) {
            final ReportingTaskNode node = ((ReportingTaskNode) component);
            dto.setState(node.getScheduledState().name());
            dto.setActiveThreadCount(node.getActiveThreadCount());
            dto.setType(node.getComponentType());
            dto.setReferenceType(ReportingTask.class.getSimpleName());

            propertyDescriptors = node.getReportingTask().getPropertyDescriptors();
            validationErrors = node.getValidationErrors();
            processGroupId = null;
        }

        // ensure descriptors is non null
        if (propertyDescriptors == null) {
            propertyDescriptors = new ArrayList<>();
        }

        // process properties unconditionally since dynamic properties are available here and not in getPropertyDescriptors
        final Map<PropertyDescriptor, String> sortedProperties = new TreeMap<>(new Comparator<PropertyDescriptor>() {
            @Override
            public int compare(final PropertyDescriptor o1, final PropertyDescriptor o2) {
                return Collator.getInstance(Locale.US).compare(o1.getName(), o2.getName());
            }
        });
        sortedProperties.putAll(component.getProperties());

        final Map<PropertyDescriptor, String> orderedProperties = new LinkedHashMap<>();
        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            orderedProperties.put(descriptor, null);
        }
        orderedProperties.putAll(sortedProperties);

        // build the descriptor and property dtos
        dto.setDescriptors(new LinkedHashMap<String, PropertyDescriptorDTO>());
        dto.setProperties(new LinkedHashMap<String, String>());
        for (final Map.Entry<PropertyDescriptor, String> entry : orderedProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();

            // store the property descriptor
            dto.getDescriptors().put(descriptor.getName(), createPropertyDescriptorDto(descriptor, processGroupId));

            // determine the property value - don't include sensitive properties
            String propertyValue = entry.getValue();
            if (propertyValue != null && descriptor.isSensitive()) {
                propertyValue = SENSITIVE_VALUE_MASK;
            }

            // set the property value
            dto.getProperties().put(descriptor.getName(), propertyValue);
        }

        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    public RemoteProcessGroupPortDTO createRemoteProcessGroupPortDto(final RemoteGroupPort port) {
        if (port == null) {
            return null;
        }

        final RemoteProcessGroupPortDTO dto = new RemoteProcessGroupPortDTO();
        dto.setId(port.getIdentifier());
        dto.setGroupId(port.getRemoteProcessGroup().getIdentifier());
        dto.setTargetId(port.getTargetIdentifier());
        dto.setName(port.getName());
        dto.setComments(port.getComments());
        dto.setTransmitting(port.isRunning());
        dto.setTargetRunning(port.isTargetRunning());
        dto.setConcurrentlySchedulableTaskCount(port.getMaxConcurrentTasks());
        dto.setUseCompression(port.isUseCompression());
        dto.setExists(port.getTargetExists());
        dto.setVersionedComponentId(port.getVersionedComponentId().orElse(null));

        final BatchSettingsDTO batchDTO = new BatchSettingsDTO();
        batchDTO.setCount(port.getBatchCount());
        batchDTO.setSize(port.getBatchSize());
        batchDTO.setDuration(port.getBatchDuration());
        dto.setBatchSettings(batchDTO);

        // determine if this port is currently connected to another component locally
        if (ConnectableType.REMOTE_OUTPUT_PORT.equals(port.getConnectableType())) {
            dto.setConnected(!port.getConnections().isEmpty());
        } else {
            dto.setConnected(port.hasIncomingConnection());
        }

        return dto;
    }

    /**
     * Creates a RemoteProcessGroupDTO from the specified RemoteProcessGroup.
     *
     * @param group group
     * @return dto
     */
    public RemoteProcessGroupDTO createRemoteProcessGroupDto(final RemoteProcessGroup group) {
        if (group == null) {
            return null;
        }

        final Set<RemoteProcessGroupPortDTO> inputPorts = new HashSet<>();
        final Set<RemoteProcessGroupPortDTO> outputPorts = new HashSet<>();

        int activeRemoteInputPortCount = 0;
        int inactiveRemoteInputPortCount = 0;
        for (final Port port : group.getInputPorts()) {
            inputPorts.add(createRemoteProcessGroupPortDto((RemoteGroupPort) port));

            if (port.hasIncomingConnection()) {
                if (port.isRunning()) {
                    activeRemoteInputPortCount++;
                } else {
                    inactiveRemoteInputPortCount++;
                }
            }
        }

        int activeRemoteOutputPortCount = 0;
        int inactiveRemoteOutputPortCount = 0;
        for (final Port port : group.getOutputPorts()) {
            outputPorts.add(createRemoteProcessGroupPortDto((RemoteGroupPort) port));

            if (!port.getConnections().isEmpty()) {
                if (port.isRunning()) {
                    activeRemoteOutputPortCount++;
                } else {
                    inactiveRemoteOutputPortCount++;
                }
            }
        }

        final RemoteProcessGroupContentsDTO contents = new RemoteProcessGroupContentsDTO();
        contents.setInputPorts(inputPorts);
        contents.setOutputPorts(outputPorts);

        final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
        dto.setId(group.getIdentifier());
        dto.setName(group.getName());
        dto.setPosition(createPositionDto(group.getPosition()));
        dto.setComments(group.getComments());
        dto.setTransmitting(group.isTransmitting());
        dto.setCommunicationsTimeout(group.getCommunicationsTimeout());
        dto.setYieldDuration(group.getYieldDuration());
        dto.setParentGroupId(group.getProcessGroup().getIdentifier());
        dto.setTargetUris(group.getTargetUris());
        dto.setFlowRefreshed(group.getLastRefreshTime());
        dto.setContents(contents);
        dto.setTransportProtocol(group.getTransportProtocol().name());
        dto.setProxyHost(group.getProxyHost());
        dto.setProxyPort(group.getProxyPort());
        dto.setProxyUser(group.getProxyUser());
        if (!StringUtils.isEmpty(group.getProxyPassword())) {
            dto.setProxyPassword(SENSITIVE_VALUE_MASK);
        }

        // only specify the secure flag if we know the target system has site to site enabled
        if (group.isSiteToSiteEnabled()) {
            dto.setTargetSecure(group.getSecureFlag());
        }

        if (group.getAuthorizationIssue() != null) {
            dto.setAuthorizationIssues(Arrays.asList(group.getAuthorizationIssue()));
        }

        final Collection<ValidationResult> validationErrors = group.validate();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        dto.setLocalNetworkInterface(group.getNetworkInterface());

        dto.setActiveRemoteInputPortCount(activeRemoteInputPortCount);
        dto.setInactiveRemoteInputPortCount(inactiveRemoteInputPortCount);
        dto.setActiveRemoteOutputPortCount(activeRemoteOutputPortCount);
        dto.setInactiveRemoteOutputPortCount(inactiveRemoteOutputPortCount);
        dto.setVersionedComponentId(group.getVersionedComponentId().orElse(null));

        final RemoteProcessGroupCounts counts = group.getCounts();
        if (counts != null) {
            dto.setInputPortCount(counts.getInputPortCount());
            dto.setOutputPortCount(counts.getOutputPortCount());
        }

        return dto;
    }

    /**
     * Creates a FlowBreadcrumbEntity from the specified parent ProcessGroup.
     *
     * @param group group
     * @return dto
     */
    private FlowBreadcrumbEntity createBreadcrumbEntity(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final FlowBreadcrumbDTO dto = createBreadcrumbDto(group);
        final PermissionsDTO permissions = createPermissionsDto(group);
        final FlowBreadcrumbEntity entity = entityFactory.createFlowBreadcrumbEntity(dto, permissions);

        if (group.getParent() != null) {
            entity.setParentBreadcrumb(createBreadcrumbEntity(group.getParent()));
        }

        return entity;
    }

    /**
     * Creates a FlowBreadcrumbDTO from the specified parent ProcessGroup.
     *
     * @param group group
     * @return dto
     */
    private FlowBreadcrumbDTO createBreadcrumbDto(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final FlowBreadcrumbDTO dto = new FlowBreadcrumbDTO();
        dto.setId(group.getIdentifier());
        dto.setName(group.getName());

        final VersionControlInformationDTO versionControlInformation = createVersionControlInformationDto(group);
        dto.setVersionControlInformation(versionControlInformation);

        return dto;
    }

    public ComponentReferenceDTO createComponentReferenceDto(final Authorizable authorizable) {
        if (authorizable == null || !(authorizable instanceof ComponentAuthorizable)) {
            return null;
        }

        final ComponentAuthorizable componentAuthorizable = (ComponentAuthorizable) authorizable;
        final ComponentReferenceDTO dto = new ComponentReferenceDTO();
        dto.setId(componentAuthorizable.getIdentifier());
        dto.setParentGroupId(componentAuthorizable.getProcessGroupIdentifier());
        dto.setName(authorizable.getResource().getName());

        return dto;
    }

    public AccessPolicySummaryDTO createAccessPolicySummaryDto(final AccessPolicy accessPolicy, final ComponentReferenceEntity componentReference) {
        if (accessPolicy == null) {
            return null;
        }

        final AccessPolicySummaryDTO dto = new AccessPolicySummaryDTO();
        dto.setId(accessPolicy.getIdentifier());
        dto.setResource(accessPolicy.getResource());
        dto.setAction(accessPolicy.getAction().toString());
        dto.setConfigurable(AuthorizerCapabilityDetection.isAccessPolicyConfigurable(authorizer, accessPolicy));
        dto.setComponentReference(componentReference);
        return dto;
    }

    public AccessPolicyDTO createAccessPolicyDto(final AccessPolicy accessPolicy, final Set<TenantEntity> userGroups,
                                                 final Set<TenantEntity> users, final ComponentReferenceEntity componentReference) {

        if (accessPolicy == null) {
            return null;
        }

        final AccessPolicyDTO dto = new AccessPolicyDTO();
        dto.setUserGroups(userGroups);
        dto.setUsers(users);
        dto.setId(accessPolicy.getIdentifier());
        dto.setResource(accessPolicy.getResource());
        dto.setAction(accessPolicy.getAction().toString());
        dto.setConfigurable(AuthorizerCapabilityDetection.isAccessPolicyConfigurable(authorizer, accessPolicy));
        dto.setComponentReference(componentReference);
        return dto;
    }

    /**
     * Creates the PermissionsDTO based on the specified Authorizable.
     *
     * @param authorizable authorizable
     * @return dto
     */
    public PermissionsDTO createPermissionsDto(final Authorizable authorizable) {
        return createPermissionsDto(authorizable, NiFiUserUtils.getNiFiUser());
    }

    /**
     * Creates the PermissionsDTO based on the specified Authorizable for the given user
     *
     * @param authorizable authorizable
     * @param user the NiFi User for which the Permissions are being created
     * @return dto
     */
    public PermissionsDTO createPermissionsDto(final Authorizable authorizable, final NiFiUser user) {
        final PermissionsDTO dto = new PermissionsDTO();
        dto.setCanRead(authorizable.isAuthorized(authorizer, RequestAction.READ, user));
        dto.setCanWrite(authorizable.isAuthorized(authorizer, RequestAction.WRITE, user));
        return dto;
    }

    public AffectedComponentEntity createAffectedComponentEntity(final ProcessorEntity processorEntity) {
        if (processorEntity == null) {
            return null;
        }

        final AffectedComponentEntity component = new AffectedComponentEntity();
        component.setBulletins(processorEntity.getBulletins());
        component.setId(processorEntity.getId());
        component.setPermissions(processorEntity.getPermissions());
        component.setPosition(processorEntity.getPosition());
        component.setRevision(processorEntity.getRevision());
        component.setUri(processorEntity.getUri());

        final ProcessorDTO processorDto = processorEntity.getComponent();
        final AffectedComponentDTO componentDto = new AffectedComponentDTO();
        componentDto.setId(processorDto.getId());
        componentDto.setName(processorDto.getName());
        componentDto.setProcessGroupId(processorDto.getParentGroupId());
        componentDto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        componentDto.setState(processorDto.getState());
        componentDto.setValidationErrors(processorDto.getValidationErrors());
        component.setComponent(componentDto);

        return component;
    }

    public AffectedComponentEntity createAffectedComponentEntity(final PortEntity portEntity, final String referenceType) {
        if (portEntity == null) {
            return null;
        }

        final AffectedComponentEntity component = new AffectedComponentEntity();
        component.setBulletins(portEntity.getBulletins());
        component.setId(portEntity.getId());
        component.setPermissions(portEntity.getPermissions());
        component.setPosition(portEntity.getPosition());
        component.setRevision(portEntity.getRevision());
        component.setUri(portEntity.getUri());

        final PortDTO portDto = portEntity.getComponent();
        final AffectedComponentDTO componentDto = new AffectedComponentDTO();
        componentDto.setId(portDto.getId());
        componentDto.setName(portDto.getName());
        componentDto.setProcessGroupId(portDto.getParentGroupId());
        componentDto.setReferenceType(referenceType);
        componentDto.setState(portDto.getState());
        componentDto.setValidationErrors(portDto.getValidationErrors());
        component.setComponent(componentDto);

        return component;
    }

    public AffectedComponentEntity createAffectedComponentEntity(final ControllerServiceEntity serviceEntity) {
        if (serviceEntity == null) {
            return null;
        }

        final AffectedComponentEntity component = new AffectedComponentEntity();
        component.setBulletins(serviceEntity.getBulletins());
        component.setId(serviceEntity.getId());
        component.setPermissions(serviceEntity.getPermissions());
        component.setPosition(serviceEntity.getPosition());
        component.setRevision(serviceEntity.getRevision());
        component.setUri(serviceEntity.getUri());

        final ControllerServiceDTO serviceDto = serviceEntity.getComponent();
        final AffectedComponentDTO componentDto = new AffectedComponentDTO();
        componentDto.setId(serviceDto.getId());
        componentDto.setName(serviceDto.getName());
        componentDto.setProcessGroupId(serviceDto.getParentGroupId());
        componentDto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
        componentDto.setState(serviceDto.getState());
        componentDto.setValidationErrors(serviceDto.getValidationErrors());
        component.setComponent(componentDto);

        return component;
    }

    public AffectedComponentEntity createAffectedComponentEntity(final RemoteProcessGroupPortDTO remotePortDto, final String referenceType, final RemoteProcessGroupEntity rpgEntity) {
        if (remotePortDto == null) {
            return null;
        }

        final AffectedComponentEntity component = new AffectedComponentEntity();
        component.setId(remotePortDto.getId());
        component.setPermissions(rpgEntity.getPermissions());
        component.setRevision(rpgEntity.getRevision());
        component.setUri(rpgEntity.getUri());

        final AffectedComponentDTO componentDto = new AffectedComponentDTO();
        componentDto.setId(remotePortDto.getId());
        componentDto.setName(remotePortDto.getName());
        componentDto.setProcessGroupId(remotePortDto.getGroupId());
        componentDto.setReferenceType(referenceType);
        componentDto.setState(remotePortDto.isTransmitting() ? "Running" : "Stopped");
        component.setComponent(componentDto);

        return component;
    }


    public AffectedComponentDTO createAffectedComponentDto(final ComponentNode component) {
        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(component.getIdentifier());
        dto.setName(component.getName());
        dto.setProcessGroupId(component.getProcessGroupIdentifier());

        if (component instanceof ProcessorNode) {
            final ProcessorNode node = ((ProcessorNode) component);
            dto.setState(node.getScheduledState().name());
            dto.setActiveThreadCount(node.getActiveThreadCount());
            dto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR);
        } else if (component instanceof ControllerServiceNode) {
            final ControllerServiceNode node = ((ControllerServiceNode) component);
            dto.setState(node.getState().name());
            dto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
        }

        final Collection<ValidationResult> validationErrors = component.getValidationErrors();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    /**
     * Creates a ProcessGroupDTO from the specified ProcessGroup.
     *
     * @param group group
     * @return dto
     */
    public ProcessGroupDTO createProcessGroupDto(final ProcessGroup group) {
        return createProcessGroupDto(group, false);
    }

    public ProcessGroupFlowDTO createProcessGroupFlowDto(final ProcessGroup group, final ProcessGroupStatus groupStatus, final RevisionManager revisionManager,
                                                         final Function<ProcessGroup, List<BulletinEntity>> getProcessGroupBulletins) {

        final ProcessGroupFlowDTO dto = new ProcessGroupFlowDTO();
        dto.setId(group.getIdentifier());
        dto.setLastRefreshed(new Date());
        dto.setBreadcrumb(createBreadcrumbEntity(group));
        dto.setFlow(createFlowDto(group, groupStatus, revisionManager, getProcessGroupBulletins));

        final ProcessGroup parent = group.getParent();
        if (parent != null) {
            dto.setParentGroupId(parent.getIdentifier());
        }

        return dto;
    }

    public FlowDTO createFlowDto(final ProcessGroup group, final ProcessGroupStatus groupStatus, final FlowSnippetDTO snippet, final RevisionManager revisionManager,
                                 final Function<ProcessGroup, List<BulletinEntity>> getProcessGroupBulletins) {
        if (snippet == null) {
            return null;
        }

        final FlowDTO flow = new FlowDTO();

        for (final ConnectionDTO snippetConnection : snippet.getConnections()) {
            final Connection connection = group.getConnection(snippetConnection.getId());

            // marshal the actual connection as the snippet is pruned
            final ConnectionDTO dto = createConnectionDto(connection);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(connection.getIdentifier()));
            final PermissionsDTO accessPolicy = createPermissionsDto(connection);
            final ConnectionStatusDTO status = getComponentStatus(
                () -> groupStatus.getConnectionStatus().stream().filter(connectionStatus -> connection.getIdentifier().equals(connectionStatus.getId())).findFirst().orElse(null),
                connectionStatus -> createConnectionStatusDto(connectionStatus)
            );
            flow.getConnections().add(entityFactory.createConnectionEntity(dto, revision, accessPolicy, status));
        }

        for (final FunnelDTO snippetFunnel : snippet.getFunnels()) {
            final Funnel funnel = group.getFunnel(snippetFunnel.getId());

            // marshal the actual funnel as the snippet is pruned
            final FunnelDTO dto = createFunnelDto(funnel);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
            final PermissionsDTO accessPolicy = createPermissionsDto(funnel);
            flow.getFunnels().add(entityFactory.createFunnelEntity(dto, revision, accessPolicy));
        }

        for (final PortDTO snippetInputPort : snippet.getInputPorts()) {
            final Port inputPort = group.getInputPort(snippetInputPort.getId());

            // marshal the actual port as the snippet is pruned
            final PortDTO dto = createPortDto(inputPort);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(inputPort.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(inputPort);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(inputPort));
            final PortStatusDTO status = getComponentStatus(
                () -> groupStatus.getInputPortStatus().stream().filter(inputPortStatus -> inputPort.getIdentifier().equals(inputPortStatus.getId())).findFirst().orElse(null),
                inputPortStatus -> createPortStatusDto(inputPortStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPort.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            flow.getInputPorts().add(entityFactory.createPortEntity(dto, revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final PortDTO snippetOutputPort : snippet.getOutputPorts()) {
            final Port outputPort = group.getOutputPort(snippetOutputPort.getId());

            // marshal the actual port as the snippet is pruned
            final PortDTO dto = createPortDto(outputPort);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(outputPort.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(outputPort);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(outputPort));
            final PortStatusDTO status = getComponentStatus(
                () -> groupStatus.getOutputPortStatus().stream().filter(outputPortStatus -> outputPort.getIdentifier().equals(outputPortStatus.getId())).findFirst().orElse(null),
                outputPortStatus -> createPortStatusDto(outputPortStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPort.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            flow.getOutputPorts().add(entityFactory.createPortEntity(dto, revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final LabelDTO snippetLabel : snippet.getLabels()) {
            final Label label = group.getLabel(snippetLabel.getId());

            // marshal the actual label as the snippet is pruned
            final LabelDTO dto = createLabelDto(label);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
            final PermissionsDTO accessPolicy = createPermissionsDto(label);
            flow.getLabels().add(entityFactory.createLabelEntity(dto, revision, accessPolicy));
        }

        for (final ProcessGroupDTO snippetProcessGroup : snippet.getProcessGroups()) {
            final ProcessGroup processGroup = group.getProcessGroup(snippetProcessGroup.getId());

            // marshal the actual group as the snippet is pruned
            final ProcessGroupDTO dto = createProcessGroupDto(processGroup);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(processGroup.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(processGroup);
            final ProcessGroupStatusDTO status = getComponentStatus(
                () -> groupStatus.getProcessGroupStatus().stream().filter(processGroupStatus -> processGroup.getIdentifier().equals(processGroupStatus.getId())).findFirst().orElse(null),
                processGroupStatus -> createConciseProcessGroupStatusDto(processGroupStatus)
            );
            final List<BulletinEntity> bulletins = getProcessGroupBulletins.apply(processGroup);
            flow.getProcessGroups().add(entityFactory.createProcessGroupEntity(dto, revision, permissions, status, bulletins));
        }

        for (final ProcessorDTO snippetProcessor : snippet.getProcessors()) {
            final ProcessorNode processor = group.getProcessor(snippetProcessor.getId());

            // marshal the actual processor as the snippet is pruned
            final ProcessorDTO dto = createProcessorDto(processor);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(processor);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(processor));
            final ProcessorStatusDTO status = getComponentStatus(
                () -> groupStatus.getProcessorStatus().stream().filter(processorStatus -> processor.getIdentifier().equals(processorStatus.getId())).findFirst().orElse(null),
                processorStatus -> createProcessorStatusDto(processorStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(processor.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            flow.getProcessors().add(entityFactory.createProcessorEntity(dto, revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final RemoteProcessGroupDTO snippetRemoteProcessGroup : snippet.getRemoteProcessGroups()) {
            final RemoteProcessGroup remoteProcessGroup = group.getRemoteProcessGroup(snippetRemoteProcessGroup.getId());

            // marshal the actual rpm as the snippet is pruned
            final RemoteProcessGroupDTO dto = createRemoteProcessGroupDto(remoteProcessGroup);
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(remoteProcessGroup.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(remoteProcessGroup);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(remoteProcessGroup));
            final RemoteProcessGroupStatusDTO status = getComponentStatus(
                () -> groupStatus.getRemoteProcessGroupStatus().stream().filter(rpgStatus -> remoteProcessGroup.getIdentifier().equals(rpgStatus.getId())).findFirst().orElse(null),
                remoteProcessGroupStatus -> createRemoteProcessGroupStatusDto(remoteProcessGroup, remoteProcessGroupStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroup.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            flow.getRemoteProcessGroups().add(entityFactory.createRemoteProcessGroupEntity(dto, revision, permissions, operatePermissions, status, bulletinEntities));
        }

        return flow;
    }

    private <T, S> T getComponentStatus(final Supplier<S> getComponentStatus, final Function<S, T> convertToDto) {
        final T statusDTO;
        final S status = getComponentStatus.get();
        if (status != null) {
            statusDTO = convertToDto.apply(status);
        } else {
            statusDTO = null;
        }
        return statusDTO;
    }

    public FlowDTO createFlowDto(final ProcessGroup group, final ProcessGroupStatus groupStatus, final RevisionManager revisionManager,
                                 final Function<ProcessGroup, List<BulletinEntity>> getProcessGroupBulletins) {
        final FlowDTO dto = new FlowDTO();

        for (final ProcessorNode procNode : group.getProcessors()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(procNode.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(procNode);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(procNode));
            final ProcessorStatusDTO status = getComponentStatus(
                () -> groupStatus.getProcessorStatus().stream().filter(processorStatus -> procNode.getIdentifier().equals(processorStatus.getId())).findFirst().orElse(null),
                processorStatus -> createProcessorStatusDto(processorStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(procNode.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            dto.getProcessors().add(entityFactory.createProcessorEntity(createProcessorDto(procNode), revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final Connection connNode : group.getConnections()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(connNode.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(connNode);
            final ConnectionStatusDTO status = getComponentStatus(
                () -> groupStatus.getConnectionStatus().stream().filter(connectionStatus -> connNode.getIdentifier().equals(connectionStatus.getId())).findFirst().orElse(null),
                connectionStatus -> createConnectionStatusDto(connectionStatus)
            );
            dto.getConnections().add(entityFactory.createConnectionEntity(createConnectionDto(connNode), revision, permissions, status));
        }

        for (final Label label : group.getLabels()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(label);
            dto.getLabels().add(entityFactory.createLabelEntity(createLabelDto(label), revision, permissions));
        }

        for (final Funnel funnel : group.getFunnels()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(funnel);
            dto.getFunnels().add(entityFactory.createFunnelEntity(createFunnelDto(funnel), revision, permissions));
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(childGroup.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(childGroup);
            final ProcessGroupStatusDTO status = getComponentStatus(
                () -> groupStatus.getProcessGroupStatus().stream().filter(processGroupStatus -> childGroup.getIdentifier().equals(processGroupStatus.getId())).findFirst().orElse(null),
                processGroupStatus -> createConciseProcessGroupStatusDto(processGroupStatus)
            );
            final List<BulletinEntity> bulletins = getProcessGroupBulletins.apply(childGroup);
            dto.getProcessGroups().add(entityFactory.createProcessGroupEntity(createProcessGroupDto(childGroup), revision, permissions, status, bulletins));
        }

        for (final RemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(rpg.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(rpg);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(rpg));
            final RemoteProcessGroupStatusDTO status = getComponentStatus(
                () -> groupStatus.getRemoteProcessGroupStatus().stream().filter(remoteProcessGroupStatus -> rpg.getIdentifier().equals(remoteProcessGroupStatus.getId())).findFirst().orElse(null),
                remoteProcessGroupStatus -> createRemoteProcessGroupStatusDto(rpg, remoteProcessGroupStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(rpg.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            dto.getRemoteProcessGroups().add(entityFactory.createRemoteProcessGroupEntity(createRemoteProcessGroupDto(rpg), revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final Port inputPort : group.getInputPorts()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(inputPort.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(inputPort);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(inputPort));
            final PortStatusDTO status = getComponentStatus(
                () -> groupStatus.getInputPortStatus().stream().filter(inputPortStatus -> inputPort.getIdentifier().equals(inputPortStatus.getId())).findFirst().orElse(null),
                inputPortStatus -> createPortStatusDto(inputPortStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPort.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            dto.getInputPorts().add(entityFactory.createPortEntity(createPortDto(inputPort), revision, permissions, operatePermissions, status, bulletinEntities));
        }

        for (final Port outputPort : group.getOutputPorts()) {
            final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(outputPort.getIdentifier()));
            final PermissionsDTO permissions = createPermissionsDto(outputPort);
            final PermissionsDTO operatePermissions = createPermissionsDto(new OperationAuthorizable(outputPort));
            final PortStatusDTO status = getComponentStatus(
                () -> groupStatus.getOutputPortStatus().stream().filter(outputPortStatus -> outputPort.getIdentifier().equals(outputPortStatus.getId())).findFirst().orElse(null),
                outputPortStatus -> createPortStatusDto(outputPortStatus)
            );
            final List<BulletinDTO> bulletins = createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPort.getIdentifier()));
            final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
            dto.getOutputPorts().add(entityFactory.createPortEntity(createPortDto(outputPort), revision, permissions, operatePermissions, status, bulletinEntities));
        }

        return dto;
    }

    /**
     * Creates a ProcessGroupDTO from the specified ProcessGroup.
     *
     * @param group group
     * @param recurse recurse
     * @return dto
     */
    public ProcessGroupDTO createProcessGroupDto(final ProcessGroup group, final boolean recurse) {
        final ProcessGroupDTO dto = createConciseProcessGroupDto(group);
        dto.setContents(createProcessGroupContentsDto(group, recurse));
        return dto;
    }

    /**
     * Creates a ProcessGroupDTO from the specified ProcessGroup.
     *
     * @param group group
     * @return dto
     */
    private ProcessGroupDTO createConciseProcessGroupDto(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final ProcessGroupDTO dto = new ProcessGroupDTO();
        dto.setId(group.getIdentifier());
        dto.setPosition(createPositionDto(group.getPosition()));
        dto.setComments(group.getComments());
        dto.setName(group.getName());
        dto.setVersionedComponentId(group.getVersionedComponentId().orElse(null));
        dto.setVersionControlInformation(createVersionControlInformationDto(group));

        final Map<String, String> variables = group.getVariableRegistry().getVariableMap().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
        dto.setVariables(variables);

        final ProcessGroup parentGroup = group.getParent();
        if (parentGroup != null) {
            dto.setParentGroupId(parentGroup.getIdentifier());
        }

        final ProcessGroupCounts counts = group.getCounts();
        dto.setRunningCount(counts.getRunningCount());
        dto.setStoppedCount(counts.getStoppedCount());
        dto.setInvalidCount(counts.getInvalidCount());
        dto.setDisabledCount(counts.getDisabledCount());
        dto.setInputPortCount(counts.getInputPortCount());
        dto.setOutputPortCount(counts.getOutputPortCount());
        dto.setActiveRemotePortCount(counts.getActiveRemotePortCount());
        dto.setInactiveRemotePortCount(counts.getInactiveRemotePortCount());
        dto.setUpToDateCount(counts.getUpToDateCount());
        dto.setLocallyModifiedCount(counts.getLocallyModifiedCount());
        dto.setStaleCount(counts.getStaleCount());
        dto.setLocallyModifiedAndStaleCount(counts.getLocallyModifiedAndStaleCount());
        dto.setSyncFailureCount(counts.getSyncFailureCount());

        return dto;
    }


    public Set<ComponentDifferenceDTO> createComponentDifferenceDtos(final FlowComparison comparison) {
        final Map<ComponentDifferenceDTO, List<DifferenceDTO>> differencesByComponent = new HashMap<>();

        for (final FlowDifference difference : comparison.getDifferences()) {
            // Ignore these as local differences for now because we can't do anything with it
            if (difference.getDifferenceType() == DifferenceType.BUNDLE_CHANGED) {
                continue;
            }

            // Ignore differences for adding remote ports
            if (FlowDifferenceFilters.isAddedOrRemovedRemotePort(difference)) {
                continue;
            }

            if (FlowDifferenceFilters.isIgnorableVersionedFlowCoordinateChange(difference)) {
                continue;
            }

            final ComponentDifferenceDTO componentDiff = createComponentDifference(difference);
            final List<DifferenceDTO> differences = differencesByComponent.computeIfAbsent(componentDiff, key -> new ArrayList<>());

            final DifferenceDTO dto = new DifferenceDTO();
            dto.setDifferenceType(difference.getDifferenceType().getDescription());
            dto.setDifference(difference.getDescription());

            differences.add(dto);
        }

        for (final Map.Entry<ComponentDifferenceDTO, List<DifferenceDTO>> entry : differencesByComponent.entrySet()) {
            entry.getKey().setDifferences(entry.getValue());
        }

        return differencesByComponent.keySet();
    }

    private ComponentDifferenceDTO createComponentDifference(final FlowDifference difference) {
        VersionedComponent component = difference.getComponentA();
        if (component == null || difference.getComponentB() instanceof InstantiatedVersionedComponent) {
            component = difference.getComponentB();
        }

        final ComponentDifferenceDTO dto = new ComponentDifferenceDTO();
        dto.setComponentName(component.getName());
        dto.setComponentType(component.getComponentType().toString());

        if (component instanceof InstantiatedVersionedComponent) {
            final InstantiatedVersionedComponent instantiatedComponent = (InstantiatedVersionedComponent) component;
            dto.setComponentId(instantiatedComponent.getInstanceId());
            dto.setProcessGroupId(instantiatedComponent.getInstanceGroupId());
        } else {
            dto.setComponentId(component.getIdentifier());
            dto.setProcessGroupId(dto.getProcessGroupId());
        }

        return dto;
    }


    public VersionControlInformationDTO createVersionControlInformationDto(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final VersionControlInformation versionControlInfo = group.getVersionControlInformation();
        if (versionControlInfo == null) {
            return null;
        }

        final VersionControlInformationDTO dto = new VersionControlInformationDTO();
        dto.setGroupId(group.getIdentifier());
        dto.setRegistryId(versionControlInfo.getRegistryIdentifier());
        dto.setRegistryName(versionControlInfo.getRegistryName());
        dto.setBucketId(versionControlInfo.getBucketIdentifier());
        dto.setBucketName(versionControlInfo.getBucketName());
        dto.setFlowId(versionControlInfo.getFlowIdentifier());
        dto.setFlowName(versionControlInfo.getFlowName());
        dto.setFlowDescription(versionControlInfo.getFlowDescription());
        dto.setVersion(versionControlInfo.getVersion());

        final VersionedFlowStatus status = versionControlInfo.getStatus();
        final VersionedFlowState state = status.getState();
        dto.setState(state == null ? null : state.name());
        dto.setStateExplanation(status.getStateExplanation());

        return dto;
    }

    public Map<String, String> createVersionControlComponentMappingDto(final InstantiatedVersionedProcessGroup group) {
        final Map<String, String> mapping = new HashMap<>();

        mapping.put(group.getInstanceId(), group.getIdentifier());
        group.getProcessors().stream()
            .map(proc -> (InstantiatedVersionedProcessor) proc)
            .forEach(proc -> mapping.put(proc.getInstanceId(), proc.getIdentifier()));
        group.getFunnels().stream()
            .map(funnel -> (InstantiatedVersionedFunnel) funnel)
            .forEach(funnel -> mapping.put(funnel.getInstanceId(), funnel.getIdentifier()));
        group.getInputPorts().stream()
            .map(port -> (InstantiatedVersionedPort) port)
            .forEach(port -> mapping.put(port.getInstanceId(), port.getIdentifier()));
        group.getOutputPorts().stream()
            .map(port -> (InstantiatedVersionedPort) port)
            .forEach(port -> mapping.put(port.getInstanceId(), port.getIdentifier()));
        group.getControllerServices().stream()
            .map(service -> (InstantiatedVersionedControllerService) service)
            .forEach(service -> mapping.put(service.getInstanceId(), service.getIdentifier()));
        group.getLabels().stream()
            .map(label -> (InstantiatedVersionedLabel) label)
            .forEach(label -> mapping.put(label.getInstanceId(), label.getIdentifier()));
        group.getConnections().stream()
            .map(conn -> (InstantiatedVersionedConnection) conn)
            .forEach(conn -> mapping.put(conn.getInstanceId(), conn.getIdentifier()));
        group.getRemoteProcessGroups().stream()
            .map(rpg -> (InstantiatedVersionedRemoteProcessGroup) rpg)
            .forEach(rpg -> {
                mapping.put(rpg.getInstanceId(), rpg.getIdentifier());

                if (rpg.getInputPorts() != null) {
                    rpg.getInputPorts().stream()
                        .map(port -> (InstantiatedVersionedRemoteGroupPort) port)
                        .forEach(port -> mapping.put(port.getInstanceId(), port.getIdentifier()));
                }

                if (rpg.getOutputPorts() != null) {
                    rpg.getOutputPorts().stream()
                        .map(port -> (InstantiatedVersionedRemoteGroupPort) port)
                        .forEach(port -> mapping.put(port.getInstanceId(), port.getIdentifier()));
                }
            });

        group.getProcessGroups().stream()
            .map(child -> (InstantiatedVersionedProcessGroup) child)
            .forEach(child -> {
                final Map<String, String> childMapping = createVersionControlComponentMappingDto(child);
                mapping.putAll(childMapping);
            });

        return mapping;
    }


    /**
     * Creates a ProcessGroupContentDTO from the specified ProcessGroup.
     *
     * @param group group
     * @param recurse recurse
     * @return dto
     */
    private FlowSnippetDTO createProcessGroupContentsDto(final ProcessGroup group, final boolean recurse) {
        if (group == null) {
            return null;
        }

        final FlowSnippetDTO dto = new FlowSnippetDTO();

        for (final ProcessorNode procNode : group.getProcessors()) {
            dto.getProcessors().add(createProcessorDto(procNode));
        }

        for (final Connection connNode : group.getConnections()) {
            dto.getConnections().add(createConnectionDto(connNode));
        }

        for (final Label label : group.getLabels()) {
            dto.getLabels().add(createLabelDto(label));
        }

        for (final Funnel funnel : group.getFunnels()) {
            dto.getFunnels().add(createFunnelDto(funnel));
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            if (recurse) {
                dto.getProcessGroups().add(createProcessGroupDto(childGroup, recurse));
            } else {
                dto.getProcessGroups().add(createConciseProcessGroupDto(childGroup));
            }
        }

        for (final RemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
            dto.getRemoteProcessGroups().add(createRemoteProcessGroupDto(remoteProcessGroup));
        }

        for (final Port inputPort : group.getInputPorts()) {
            dto.getInputPorts().add(createPortDto(inputPort));
        }

        for (final Port outputPort : group.getOutputPorts()) {
            dto.getOutputPorts().add(createPortDto(outputPort));
        }

        return dto;
    }

    private boolean isRestricted(final Class<?> cls) {
        return cls.isAnnotationPresent(Restricted.class);
    }

    private String getUsageRestriction(final Class<?> cls) {
        final Restricted restricted = cls.getAnnotation(Restricted.class);

        if (restricted == null) {
            return null;
        }

        if (StringUtils.isBlank(restricted.value())) {
            return null;
        }

        return restricted.value();
    }

    private Set<ExplicitRestrictionDTO> getExplicitRestrictions(final Class<?> cls) {
        final Restricted restricted = cls.getAnnotation(Restricted.class);

        if (restricted == null) {
            return null;
        }

        final Restriction[] restrictions = restricted.restrictions();

        if (restrictions == null || restrictions.length == 0) {
            return null;
        }

        return Arrays.stream(restrictions).map(restriction -> {
            final RequiredPermissionDTO requiredPermission = new RequiredPermissionDTO();
            requiredPermission.setId(restriction.requiredPermission().getPermissionIdentifier());
            requiredPermission.setLabel(restriction.requiredPermission().getPermissionLabel());

            final ExplicitRestrictionDTO usageRestriction = new ExplicitRestrictionDTO();
            usageRestriction.setRequiredPermission(requiredPermission);
            usageRestriction.setExplanation(restriction.explanation());
            return usageRestriction;
        }).collect(Collectors.toSet());
    }

    private String getDeprecationReason(final Class<?> cls) {
        final DeprecationNotice deprecationNotice = cls.getAnnotation(DeprecationNotice.class);
        return deprecationNotice == null ? null : deprecationNotice.reason();
    }

    public Set<AffectedComponentEntity> createAffectedComponentEntities(final Set<ComponentNode> affectedComponents, final RevisionManager revisionManager) {
        return affectedComponents.stream()
                .map(component -> {
                    final AffectedComponentDTO affectedComponent = createAffectedComponentDto(component);
                    final PermissionsDTO permissions = createPermissionsDto(component);
                    final RevisionDTO revision = createRevisionDTO(revisionManager.getRevision(component.getIdentifier()));
                    return entityFactory.createAffectedComponentEntity(affectedComponent, revision, permissions);
                })
                .collect(Collectors.toSet());
    }

    public VariableRegistryDTO createVariableRegistryDto(final ProcessGroup processGroup, final RevisionManager revisionManager) {
        final ComponentVariableRegistry variableRegistry = processGroup.getVariableRegistry();

        final List<String> variableNames = variableRegistry.getVariableMap().keySet().stream()
            .map(descriptor -> descriptor.getName())
            .collect(Collectors.toList());

        final Set<VariableEntity> variableEntities = new LinkedHashSet<>();

        for (final String variableName : variableNames) {
            final VariableDTO variableDto = new VariableDTO();
            variableDto.setName(variableName);
            variableDto.setValue(variableRegistry.getVariableValue(variableName));
            variableDto.setProcessGroupId(processGroup.getIdentifier());

            final Set<AffectedComponentEntity> affectedComponentEntities = createAffectedComponentEntities(processGroup.getComponentsAffectedByVariable(variableName), revisionManager);

            boolean canWrite = true;
            for (final AffectedComponentEntity affectedComponent : affectedComponentEntities) {
                final PermissionsDTO permissions = affectedComponent.getPermissions();
                if (!permissions.getCanRead() || !permissions.getCanWrite()) {
                    canWrite = false;
                    break;
                }
            }

            variableDto.setAffectedComponents(affectedComponentEntities);

            final VariableEntity variableEntity = new VariableEntity();
            variableEntity.setVariable(variableDto);
            variableEntity.setCanWrite(canWrite);

            variableEntities.add(variableEntity);
        }

        final VariableRegistryDTO registryDto = new VariableRegistryDTO();
        registryDto.setProcessGroupId(processGroup.getIdentifier());
        registryDto.setVariables(variableEntities);

        return registryDto;
    }

    public VariableRegistryUpdateRequestDTO createVariableRegistryUpdateRequestDto(final VariableRegistryUpdateRequest request) {
        final VariableRegistryUpdateRequestDTO dto = new VariableRegistryUpdateRequestDTO();
        dto.setComplete(request.isComplete());
        dto.setFailureReason(request.getFailureReason());
        dto.setLastUpdated(request.getLastUpdated());
        dto.setProcessGroupId(request.getProcessGroupId());
        dto.setRequestId(request.getRequestId());
        dto.setSubmissionTime(request.getSubmissionTime());

        final List<VariableRegistryUpdateStepDTO> updateSteps = new ArrayList<>();
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getIdentifyRelevantComponentsStep()));
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getStopProcessorsStep()));
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getDisableServicesStep()));
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getApplyUpdatesStep()));
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getEnableServicesStep()));
        updateSteps.add(createVariableRegistryUpdateStepDto(request.getStartProcessorsStep()));
        dto.setUpdateSteps(updateSteps);

        dto.setAffectedComponents(new HashSet<>(request.getAffectedComponents().values()));

        return dto;
    }

    public VariableRegistryUpdateStepDTO createVariableRegistryUpdateStepDto(final VariableRegistryUpdateStep step) {
        final VariableRegistryUpdateStepDTO dto = new VariableRegistryUpdateStepDTO();
        dto.setComplete(step.isComplete());
        dto.setDescription(step.getDescription());
        dto.setFailureReason(step.getFailureReason());
        return dto;
    }


    public VariableRegistryDTO populateAffectedComponents(final VariableRegistryDTO variableRegistry, final ProcessGroup group, final RevisionManager revisionManager) {
        if (!group.getIdentifier().equals(variableRegistry.getProcessGroupId())) {
            throw new IllegalArgumentException("Variable Registry does not have the same Group ID as the given Process Group");
        }

        final Set<VariableEntity> variableEntities = new LinkedHashSet<>();

        if (variableRegistry.getVariables() != null) {
            for (final VariableEntity inputEntity : variableRegistry.getVariables()) {
                final VariableEntity entity = new VariableEntity();

                final VariableDTO inputDto = inputEntity.getVariable();
                final VariableDTO variableDto = new VariableDTO();
                variableDto.setName(inputDto.getName());
                variableDto.setValue(inputDto.getValue());
                variableDto.setProcessGroupId(group.getIdentifier());

                final Set<AffectedComponentEntity> affectedComponentEntities = createAffectedComponentEntities(group.getComponentsAffectedByVariable(variableDto.getName()), revisionManager);

                boolean canWrite = true;
                for (final AffectedComponentEntity affectedComponent : affectedComponentEntities) {
                    final PermissionsDTO permissions = affectedComponent.getPermissions();
                    if (!permissions.getCanRead() || !permissions.getCanWrite()) {
                        canWrite = false;
                        break;
                    }
                }

                variableDto.setAffectedComponents(affectedComponentEntities);

                entity.setCanWrite(canWrite);
                entity.setVariable(inputDto);

                variableEntities.add(entity);
            }
        }

        final VariableRegistryDTO registryDto = new VariableRegistryDTO();
        registryDto.setProcessGroupId(group.getIdentifier());
        registryDto.setVariables(variableEntities);

        return registryDto;
    }


    /**
     * Gets the capability description from the specified class.
     */
    private String getCapabilityDescription(final Class<?> cls) {
        final CapabilityDescription capabilityDesc = cls.getAnnotation(CapabilityDescription.class);
        return capabilityDesc == null ? null : capabilityDesc.value();
    }

    /**
     * Gets the tags from the specified class.
     */
    private Set<String> getTags(final Class<?> cls) {
        final Set<String> tags = new HashSet<>();
        final Tags tagsAnnotation = cls.getAnnotation(Tags.class);
        if (tagsAnnotation != null) {
            for (final String tag : tagsAnnotation.value()) {
                tags.add(tag);
            }
        }

        if (cls.isAnnotationPresent(Restricted.class)) {
            tags.add("restricted");
        }

        return tags;
    }

    /**
     * Creates a bundle DTO from the specified class.
     *
     * @param coordinate bundle coordinates
     * @return dto
     */
    public BundleDTO createBundleDto(final BundleCoordinate coordinate) {
        final BundleDTO dto = new BundleDTO();
        dto.setGroup(coordinate.getGroup());
        dto.setArtifact(coordinate.getId());
        dto.setVersion(coordinate.getVersion());
        return dto;
    }

    private List<ControllerServiceApiDTO> createControllerServiceApiDto(final Class cls) {
        final Set<Class> serviceApis = new HashSet<>();

        // if this is a controller service
        if (ControllerService.class.isAssignableFrom(cls)) {
            // get all of it's interfaces to determine the controller service api's it implements
            final List<Class<?>> interfaces = ClassUtils.getAllInterfaces(cls);
            for (final Class i : interfaces) {
                // add all controller services that's not ControllerService itself
                if (ControllerService.class.isAssignableFrom(i) && !ControllerService.class.equals(i)) {
                    serviceApis.add(i);
                }
            }

            final List<ControllerServiceApiDTO> dtos = new ArrayList<>();
            for (final Class serviceApi : serviceApis) {
                final Bundle bundle = extensionManager.getBundle(serviceApi.getClassLoader());
                final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();

                final ControllerServiceApiDTO dto = new ControllerServiceApiDTO();
                dto.setType(serviceApi.getName());
                dto.setBundle(createBundleDto(bundleCoordinate));
                dtos.add(dto);
            }
            return dtos;
        } else {
            return null;
        }
    }

    /**
     * Gets the DocumentedTypeDTOs from the specified classes.
     *
     * @param classes classes
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return dtos
     */
    public Set<DocumentedTypeDTO> fromDocumentedTypes(final Map<Class, Bundle> classes, final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        final Set<DocumentedTypeDTO> types = new LinkedHashSet<>();
        final List<Class> sortedClasses = new ArrayList<>(classes.keySet());
        Collections.sort(sortedClasses, CLASS_NAME_COMPARATOR);

        for (final Class cls : sortedClasses) {
            final Bundle bundle = classes.get(cls);
            final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();

            // only include classes that meet the criteria if specified
            if (bundleGroupFilter != null && !bundleGroupFilter.equals(coordinate.getGroup())) {
                continue;
            }
            if (bundleArtifactFilter != null && !bundleArtifactFilter.equals(coordinate.getId())) {
                continue;
            }
            if (typeFilter != null && !typeFilter.equals(cls.getName())) {
                continue;
            }

            final DocumentedTypeDTO dto = new DocumentedTypeDTO();
            dto.setType(cls.getName());
            dto.setBundle(createBundleDto(coordinate));
            dto.setControllerServiceApis(createControllerServiceApiDto(cls));
            dto.setDescription(getCapabilityDescription(cls));
            dto.setRestricted(isRestricted(cls));
            dto.setUsageRestriction(getUsageRestriction(cls));
            dto.setExplicitRestrictions(getExplicitRestrictions(cls));
            dto.setDeprecationReason(getDeprecationReason(cls));
            dto.setTags(getTags(cls));
            types.add(dto);
        }

        return types;
    }

    /**
     * Gets the DocumentedTypeDTOs from the specified classes.
     *
     * @param classes classes
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return dtos
     */
    public Set<DocumentedTypeDTO> fromDocumentedTypes(final Set<Class> classes, final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter) {
        final Map<Class, Bundle> classBundles = new HashMap<>();
        for (final Class cls : classes) {
            classBundles.put(cls, extensionManager.getBundle(cls.getClassLoader()));
        }
        return fromDocumentedTypes(classBundles, bundleGroupFilter, bundleArtifactFilter, typeFilter);
    }

    /**
     * Creates a ProcessorDTO from the specified ProcessorNode.
     *
     * @param node node
     * @return dto
     */
    public ProcessorDTO createProcessorDto(final ProcessorNode node) {
        if (node == null) {
            return null;
        }

        final BundleCoordinate bundleCoordinate = node.getBundleCoordinate();
        final List<Bundle> compatibleBundles = extensionManager.getBundles(node.getCanonicalClassName()).stream().filter(bundle -> {
            final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
            return bundleCoordinate.getGroup().equals(coordinate.getGroup()) && bundleCoordinate.getId().equals(coordinate.getId());
        }).collect(Collectors.toList());

        final ProcessorDTO dto = new ProcessorDTO();
        dto.setId(node.getIdentifier());
        dto.setPosition(createPositionDto(node.getPosition()));
        dto.setStyle(node.getStyle());
        dto.setParentGroupId(node.getProcessGroup().getIdentifier());
        dto.setInputRequirement(node.getInputRequirement().name());
        dto.setPersistsState(node.getProcessor().getClass().isAnnotationPresent(Stateful.class));
        dto.setRestricted(node.isRestricted());
        dto.setDeprecated(node.isDeprecated());
        dto.setExecutionNodeRestricted(node.isExecutionNodeRestricted());
        dto.setExtensionMissing(node.isExtensionMissing());
        dto.setMultipleVersionsAvailable(compatibleBundles.size() > 1);
        dto.setVersionedComponentId(node.getVersionedComponentId().orElse(null));

        dto.setType(node.getCanonicalClassName());
        dto.setBundle(createBundleDto(bundleCoordinate));
        dto.setName(node.getName());
        dto.setState(node.getScheduledState().toString());

        // build the relationship dtos
        final List<RelationshipDTO> relationships = new ArrayList<>();
        for (final Relationship rel : node.getRelationships()) {
            final RelationshipDTO relationshipDTO = new RelationshipDTO();
            relationshipDTO.setDescription(rel.getDescription());
            relationshipDTO.setName(rel.getName());
            relationshipDTO.setAutoTerminate(node.isAutoTerminated(rel));
            relationships.add(relationshipDTO);
        }

        // sort the relationships
        Collections.sort(relationships, new Comparator<RelationshipDTO>() {
            @Override
            public int compare(final RelationshipDTO r1, final RelationshipDTO r2) {
                return Collator.getInstance(Locale.US).compare(r1.getName(), r2.getName());
            }
        });

        // set the relationships
        dto.setRelationships(relationships);

        dto.setDescription(getCapabilityDescription(node.getClass()));
        dto.setSupportsParallelProcessing(!node.isTriggeredSerially());
        dto.setSupportsEventDriven(node.isEventDrivenSupported());
        dto.setSupportsBatching(node.isSessionBatchingSupported());
        dto.setConfig(createProcessorConfigDto(node));

        final ValidationStatus validationStatus = node.getValidationStatus(1, TimeUnit.MILLISECONDS);
        dto.setValidationStatus(validationStatus.name());

        final Collection<ValidationResult> validationErrors = node.getValidationErrors();
        if (validationErrors != null && !validationErrors.isEmpty()) {
            final List<String> errors = new ArrayList<>();
            for (final ValidationResult validationResult : validationErrors) {
                errors.add(validationResult.toString());
            }

            dto.setValidationErrors(errors);
        }

        return dto;
    }

    /**
     * Creates a BulletinBoardDTO for the specified bulletins.
     *
     * @param bulletins bulletins
     * @return dto
     */
    public BulletinBoardDTO createBulletinBoardDto(final List<BulletinEntity> bulletins) {
        // sort the bulletins
        Collections.sort(bulletins, new Comparator<BulletinEntity>() {
            @Override
            public int compare(final BulletinEntity bulletin1, final BulletinEntity bulletin2) {
                if (bulletin1 == null && bulletin2 == null) {
                    return 0;
                } else if (bulletin1 == null) {
                    return 1;
                } else if (bulletin2 == null) {
                    return -1;
                }

                final Date timestamp1 = bulletin1.getTimestamp();
                final Date timestamp2 = bulletin2.getTimestamp();
                if (timestamp1 == null && timestamp2 == null) {
                    return 0;
                } else if (timestamp1 == null) {
                    return 1;
                } else if (timestamp2 == null) {
                    return -1;
                } else {
                    return timestamp1.compareTo(timestamp2);
                }
            }
        });

        // create the bulletin board
        final BulletinBoardDTO bulletinBoard = new BulletinBoardDTO();
        bulletinBoard.setBulletins(bulletins);
        bulletinBoard.setGenerated(new Date());
        return bulletinBoard;
    }

    /**
     * Creates BulletinDTOs for the specified Bulletins.
     *
     * @param bulletins bulletin
     * @return dto
     */
    public List<BulletinDTO> createBulletinDtos(final List<Bulletin> bulletins) {
        final List<BulletinDTO> bulletinDtos = new ArrayList<>(bulletins.size());
        for (final Bulletin bulletin : bulletins) {
            bulletinDtos.add(createBulletinDto(bulletin));
        }
        return bulletinDtos;
    }

    /**
     * Creates a BulletinDTO for the specified Bulletin.
     *
     * @param bulletin bulletin
     * @return dto
     */
    public BulletinDTO createBulletinDto(final Bulletin bulletin) {
        final BulletinDTO dto = new BulletinDTO();
        dto.setId(bulletin.getId());
        dto.setNodeAddress(bulletin.getNodeAddress());
        dto.setTimestamp(bulletin.getTimestamp());
        dto.setGroupId(bulletin.getGroupId());
        dto.setSourceId(bulletin.getSourceId());
        dto.setSourceName(bulletin.getSourceName());
        dto.setCategory(bulletin.getCategory());
        dto.setLevel(bulletin.getLevel());
        dto.setMessage(bulletin.getMessage());
        return dto;
    }

    /**
     * Creates a ProvenanceEventNodeDTO for the specified ProvenanceEventLineageNode.
     *
     * @param node node
     * @return dto
     */
    public ProvenanceNodeDTO createProvenanceEventNodeDTO(final ProvenanceEventLineageNode node) {
        final ProvenanceNodeDTO dto = new ProvenanceNodeDTO();
        dto.setId(node.getIdentifier());
        dto.setType("EVENT");
        dto.setEventType(node.getEventType().toString());
        dto.setTimestamp(new Date(node.getTimestamp()));
        dto.setMillis(node.getTimestamp());
        dto.setFlowFileUuid(node.getFlowFileUuid());
        dto.setParentUuids(node.getParentUuids());
        dto.setChildUuids(node.getChildUuids());
        return dto;
    }

    /**
     * Creates a FlowFileNodeDTO for the specified LineageNode.
     *
     * @param node node
     * @return dto
     */
    public ProvenanceNodeDTO createFlowFileNodeDTO(final LineageNode node) {
        final ProvenanceNodeDTO dto = new ProvenanceNodeDTO();
        dto.setId(node.getIdentifier());
        dto.setType("FLOWFILE");
        dto.setTimestamp(new Date(node.getTimestamp()));
        dto.setMillis(node.getTimestamp());
        dto.setFlowFileUuid(node.getFlowFileUuid());
        return dto;
    }

    /**
     * Creates a ProvenanceLinkDTO for the specified LineageEdge.
     *
     * @param edge edge
     * @return dto
     */
    public ProvenanceLinkDTO createProvenanceLinkDTO(final LineageEdge edge) {
        final LineageNode source = edge.getSource();
        final LineageNode target = edge.getDestination();

        final ProvenanceLinkDTO dto = new ProvenanceLinkDTO();
        dto.setTimestamp(new Date(target.getTimestamp()));
        dto.setMillis(target.getTimestamp());
        dto.setFlowFileUuid(edge.getUuid());
        dto.setSourceId(source.getIdentifier());
        dto.setTargetId(target.getIdentifier());
        return dto;
    }

    /**
     * Creates a LineageDTO for the specified Lineage.
     *
     * @param computeLineageSubmission submission
     * @return dto
     */
    public LineageDTO createLineageDto(final ComputeLineageSubmission computeLineageSubmission) {
        // build the lineage dto
        final LineageDTO dto = new LineageDTO();
        final LineageRequestDTO requestDto = new LineageRequestDTO();
        final LineageResultsDTO resultsDto = new LineageResultsDTO();

        // include the original request and results
        dto.setRequest(requestDto);
        dto.setResults(resultsDto);

        // rebuild the request from the submission object
        switch (computeLineageSubmission.getLineageComputationType()) {
            case EXPAND_CHILDREN:
                requestDto.setEventId(computeLineageSubmission.getExpandedEventId());
                requestDto.setLineageRequestType(LineageRequestType.CHILDREN);
                break;
            case EXPAND_PARENTS:
                requestDto.setEventId(computeLineageSubmission.getExpandedEventId());
                requestDto.setLineageRequestType(LineageRequestType.PARENTS);
                break;
            case FLOWFILE_LINEAGE:
                final Collection<String> uuids = computeLineageSubmission.getLineageFlowFileUuids();
                if (uuids.size() == 1) {
                    requestDto.setUuid(uuids.iterator().next());
                }
                requestDto.setEventId(computeLineageSubmission.getExpandedEventId());
                requestDto.setLineageRequestType(LineageRequestType.FLOWFILE);
                break;
        }

        // include lineage details
        dto.setId(computeLineageSubmission.getLineageIdentifier());
        dto.setSubmissionTime(computeLineageSubmission.getSubmissionTime());

        // create the results dto
        final ComputeLineageResult results = computeLineageSubmission.getResult();
        dto.setFinished(results.isFinished());
        dto.setPercentCompleted(results.getPercentComplete());
        dto.setExpiration(results.getExpiration());

        final List<LineageNode> nodes = results.getNodes();
        final List<LineageEdge> edges = results.getEdges();

        final List<ProvenanceNodeDTO> nodeDtos = new ArrayList<>();
        if (results.isFinished()) {
            // create the node dto's
            for (final LineageNode node : nodes) {
                switch (node.getNodeType()) {
                    case FLOWFILE_NODE:
                        nodeDtos.add(createFlowFileNodeDTO(node));
                        break;
                    case PROVENANCE_EVENT_NODE:
                        nodeDtos.add(createProvenanceEventNodeDTO((ProvenanceEventLineageNode) node));
                        break;
                }
            }
        }
        resultsDto.setNodes(nodeDtos);

        // include any errors
        if (results.getError() != null) {
            final Set<String> errors = new HashSet<>();
            errors.add(results.getError());
            resultsDto.setErrors(errors);
        }

        // create the link dto's
        final List<ProvenanceLinkDTO> linkDtos = new ArrayList<>();
        for (final LineageEdge edge : edges) {
            linkDtos.add(createProvenanceLinkDTO(edge));
        }
        resultsDto.setLinks(linkDtos);

        return dto;
    }

    /**
     * Creates a SystemDiagnosticsDTO for the specified SystemDiagnostics.
     *
     * @param sysDiagnostics diags
     * @return dto
     */
    public SystemDiagnosticsDTO createSystemDiagnosticsDto(final SystemDiagnostics sysDiagnostics) {

        final SystemDiagnosticsDTO dto = new SystemDiagnosticsDTO();
        final SystemDiagnosticsSnapshotDTO snapshot = new SystemDiagnosticsSnapshotDTO();
        dto.setAggregateSnapshot(snapshot);

        snapshot.setStatsLastRefreshed(new Date(sysDiagnostics.getCreationTimestamp()));

        // processors
        snapshot.setAvailableProcessors(sysDiagnostics.getAvailableProcessors());
        snapshot.setProcessorLoadAverage(sysDiagnostics.getProcessorLoadAverage());

        // threads
        snapshot.setDaemonThreads(sysDiagnostics.getDaemonThreads());
        snapshot.setTotalThreads(sysDiagnostics.getTotalThreads());

        // heap
        snapshot.setMaxHeap(FormatUtils.formatDataSize(sysDiagnostics.getMaxHeap()));
        snapshot.setMaxHeapBytes(sysDiagnostics.getMaxHeap());
        snapshot.setTotalHeap(FormatUtils.formatDataSize(sysDiagnostics.getTotalHeap()));
        snapshot.setTotalHeapBytes(sysDiagnostics.getTotalHeap());
        snapshot.setUsedHeap(FormatUtils.formatDataSize(sysDiagnostics.getUsedHeap()));
        snapshot.setUsedHeapBytes(sysDiagnostics.getUsedHeap());
        snapshot.setFreeHeap(FormatUtils.formatDataSize(sysDiagnostics.getFreeHeap()));
        snapshot.setFreeHeapBytes(sysDiagnostics.getFreeHeap());
        if (sysDiagnostics.getHeapUtilization() != -1) {
            snapshot.setHeapUtilization(FormatUtils.formatUtilization(sysDiagnostics.getHeapUtilization()));
        }

        // non heap
        snapshot.setMaxNonHeap(FormatUtils.formatDataSize(sysDiagnostics.getMaxNonHeap()));
        snapshot.setMaxNonHeapBytes(sysDiagnostics.getMaxNonHeap());
        snapshot.setTotalNonHeap(FormatUtils.formatDataSize(sysDiagnostics.getTotalNonHeap()));
        snapshot.setTotalNonHeapBytes(sysDiagnostics.getTotalNonHeap());
        snapshot.setUsedNonHeap(FormatUtils.formatDataSize(sysDiagnostics.getUsedNonHeap()));
        snapshot.setUsedNonHeapBytes(sysDiagnostics.getUsedNonHeap());
        snapshot.setFreeNonHeap(FormatUtils.formatDataSize(sysDiagnostics.getFreeNonHeap()));
        snapshot.setFreeNonHeapBytes(sysDiagnostics.getFreeNonHeap());
        if (sysDiagnostics.getNonHeapUtilization() != -1) {
            snapshot.setNonHeapUtilization(FormatUtils.formatUtilization(sysDiagnostics.getNonHeapUtilization()));
        }

        // flow file disk usage
        final SystemDiagnosticsSnapshotDTO.StorageUsageDTO flowFileRepositoryStorageUsageDto = createStorageUsageDTO(null, sysDiagnostics.getFlowFileRepositoryStorageUsage());
        snapshot.setFlowFileRepositoryStorageUsage(flowFileRepositoryStorageUsageDto);

        // content disk usage
        final Set<SystemDiagnosticsSnapshotDTO.StorageUsageDTO> contentRepositoryStorageUsageDtos = new LinkedHashSet<>();
        snapshot.setContentRepositoryStorageUsage(contentRepositoryStorageUsageDtos);
        for (final Map.Entry<String, StorageUsage> entry : sysDiagnostics.getContentRepositoryStorageUsage().entrySet()) {
            contentRepositoryStorageUsageDtos.add(createStorageUsageDTO(entry.getKey(), entry.getValue()));
        }

        // provenance disk usage
        final Set<SystemDiagnosticsSnapshotDTO.StorageUsageDTO> provenanceRepositoryStorageUsageDtos = new LinkedHashSet<>();
        snapshot.setProvenanceRepositoryStorageUsage(provenanceRepositoryStorageUsageDtos);
        for (final Map.Entry<String, StorageUsage> entry : sysDiagnostics.getProvenanceRepositoryStorageUsage().entrySet()) {
            provenanceRepositoryStorageUsageDtos.add(createStorageUsageDTO(entry.getKey(), entry.getValue()));
        }

        // garbage collection
        final Set<SystemDiagnosticsSnapshotDTO.GarbageCollectionDTO> garbageCollectionDtos = new LinkedHashSet<>();
        snapshot.setGarbageCollection(garbageCollectionDtos);
        for (final Map.Entry<String, GarbageCollection> entry : sysDiagnostics.getGarbageCollection().entrySet()) {
            garbageCollectionDtos.add(createGarbageCollectionDTO(entry.getKey(), entry.getValue()));
        }

        // version info
        final SystemDiagnosticsSnapshotDTO.VersionInfoDTO versionInfoDto = createVersionInfoDTO();
        snapshot.setVersionInfo(versionInfoDto);

        // uptime
        snapshot.setUptime(FormatUtils.formatHoursMinutesSeconds(sysDiagnostics.getUptime(), TimeUnit.MILLISECONDS));

        return dto;
    }

    /**
     * Creates a StorageUsageDTO from the specified StorageUsage.
     *
     * @param identifier id
     * @param storageUsage usage
     * @return dto
     */
    public SystemDiagnosticsSnapshotDTO.StorageUsageDTO createStorageUsageDTO(final String identifier, final StorageUsage storageUsage) {
        final SystemDiagnosticsSnapshotDTO.StorageUsageDTO dto = new SystemDiagnosticsSnapshotDTO.StorageUsageDTO();
        dto.setIdentifier(identifier);
        dto.setFreeSpace(FormatUtils.formatDataSize(storageUsage.getFreeSpace()));
        dto.setTotalSpace(FormatUtils.formatDataSize(storageUsage.getTotalSpace()));
        dto.setUsedSpace(FormatUtils.formatDataSize(storageUsage.getUsedSpace()));
        dto.setFreeSpaceBytes(storageUsage.getFreeSpace());
        dto.setTotalSpaceBytes(storageUsage.getTotalSpace());
        dto.setUsedSpaceBytes(storageUsage.getUsedSpace());
        dto.setUtilization(FormatUtils.formatUtilization(storageUsage.getDiskUtilization()));
        return dto;
    }

    /**
     * Creates a GarbageCollectionDTO from the specified GarbageCollection.
     *
     * @param name name
     * @param garbageCollection gc
     * @return dto
     */
    public SystemDiagnosticsSnapshotDTO.GarbageCollectionDTO createGarbageCollectionDTO(final String name, final GarbageCollection garbageCollection) {
        final SystemDiagnosticsSnapshotDTO.GarbageCollectionDTO dto = new SystemDiagnosticsSnapshotDTO.GarbageCollectionDTO();
        dto.setName(name);
        dto.setCollectionCount(garbageCollection.getCollectionCount());
        dto.setCollectionTime(FormatUtils.formatHoursMinutesSeconds(garbageCollection.getCollectionTime(), TimeUnit.MILLISECONDS));
        dto.setCollectionMillis(garbageCollection.getCollectionTime());
        return dto;
    }

    public SystemDiagnosticsSnapshotDTO.VersionInfoDTO createVersionInfoDTO() {
        final SystemDiagnosticsSnapshotDTO.VersionInfoDTO dto = new SystemDiagnosticsSnapshotDTO.VersionInfoDTO();
        dto.setJavaVendor(System.getProperty("java.vendor"));
        dto.setJavaVersion(System.getProperty("java.version"));
        dto.setOsName(System.getProperty("os.name"));
        dto.setOsVersion(System.getProperty("os.version"));
        dto.setOsArchitecture(System.getProperty("os.arch"));

        final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
        if (frameworkBundle != null) {
            final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();

            dto.setNiFiVersion(frameworkDetails.getCoordinate().getVersion());

            // Get build info
            dto.setBuildTag(frameworkDetails.getBuildTag());
            dto.setBuildRevision(frameworkDetails.getBuildRevision());
            dto.setBuildBranch(frameworkDetails.getBuildBranch());
            dto.setBuildTimestamp(frameworkDetails.getBuildTimestampDate());
        }

        return dto;
    }

    /**
     * Creates a ResourceDTO from the specified Resource.
     *
     * @param resource resource
     * @return dto
     */
    public ResourceDTO createResourceDto(final Resource resource) {
        final ResourceDTO dto = new ResourceDTO();
        dto.setIdentifier(resource.getIdentifier());
        dto.setName(resource.getName());
        return dto;
    }

    /**
     * Creates a ProcessorDiagnosticsDTO from the given Processor and status information with some additional supporting information
     *
     * @param procNode the processor to create diagnostics for
     * @param procStatus the status of given processor
     * @param bulletinRepo the bulletin repository
     * @param flowController flowController
     * @param serviceEntityFactory function for creating a ControllerServiceEntity from a given ID
     * @return ProcessorDiagnosticsDTO for the given Processor
     */
    public ProcessorDiagnosticsDTO createProcessorDiagnosticsDto(final ProcessorNode procNode, final ProcessorStatus procStatus, final BulletinRepository bulletinRepo,
            final FlowController flowController, final Function<String, ControllerServiceEntity> serviceEntityFactory) {

        final ProcessorDiagnosticsDTO procDiagnostics = new ProcessorDiagnosticsDTO();

        procDiagnostics.setClassLoaderDiagnostics(createClassLoaderDiagnosticsDto(procNode));
        procDiagnostics.setIncomingConnections(procNode.getIncomingConnections().stream()
            .map(this::createConnectionDiagnosticsDto)
            .collect(Collectors.toSet()));
        procDiagnostics.setOutgoingConnections(procNode.getConnections().stream()
            .map(this::createConnectionDiagnosticsDto)
            .collect(Collectors.toSet()));
        procDiagnostics.setJvmDiagnostics(createJvmDiagnosticsDto(flowController));
        procDiagnostics.setProcessor(createProcessorDto(procNode));
        procDiagnostics.setProcessorStatus(createProcessorStatusDto(procStatus));
        procDiagnostics.setThreadDumps(createThreadDumpDtos(procNode));

        final Set<ControllerServiceDiagnosticsDTO> referencedServiceDiagnostics = createReferencedServiceDiagnostics(procNode.getProperties(),
            flowController.getControllerServiceProvider(), serviceEntityFactory);
        procDiagnostics.setReferencedControllerServices(referencedServiceDiagnostics);

        return procDiagnostics;
    }

    private Set<ControllerServiceDiagnosticsDTO> createReferencedServiceDiagnostics(final Map<PropertyDescriptor, String> properties, final ControllerServiceProvider serviceProvider,
        final Function<String, ControllerServiceEntity> serviceEntityFactory) {

        final Set<ControllerServiceDiagnosticsDTO> referencedServiceDiagnostics = new HashSet<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() == null) {
                continue;
            }

            final String serviceId = entry.getValue();
            if (serviceId == null) {
                continue;
            }

            final ControllerServiceNode serviceNode = serviceProvider.getControllerServiceNode(serviceId);
            if (serviceNode == null) {
                continue;
            }

            final ControllerServiceDiagnosticsDTO serviceDiagnostics = createControllerServiceDiagnosticsDto(serviceNode, serviceEntityFactory, serviceProvider);
            if (serviceDiagnostics != null) {
                referencedServiceDiagnostics.add(serviceDiagnostics);
            }
        }

        return referencedServiceDiagnostics;
    }

    /**
     * Creates a ControllerServiceDiagnosticsDTO from the given Controller Service with some additional supporting information
     *
     * @param serviceNode the controller service to create diagnostics for
     * @param serviceEntityFactory a function to convert a controller service id to a controller service entity
     * @param serviceProvider the controller service provider
     * @return ControllerServiceDiagnosticsDTO for the given Controller Service
     */
    public ControllerServiceDiagnosticsDTO createControllerServiceDiagnosticsDto(final ControllerServiceNode serviceNode, final Function<String, ControllerServiceEntity> serviceEntityFactory,
            final ControllerServiceProvider serviceProvider) {

        final ControllerServiceDiagnosticsDTO serviceDiagnostics = new ControllerServiceDiagnosticsDTO();
        final ControllerServiceEntity serviceEntity = serviceEntityFactory.apply(serviceNode.getIdentifier());
        serviceDiagnostics.setControllerService(serviceEntity);

        serviceDiagnostics.setClassLoaderDiagnostics(createClassLoaderDiagnosticsDto(serviceNode));
        return serviceDiagnostics;
    }


    private ClassLoaderDiagnosticsDTO createClassLoaderDiagnosticsDto(final ControllerServiceNode serviceNode) {
        ClassLoader componentClassLoader = extensionManager.getInstanceClassLoader(serviceNode.getIdentifier());
        if (componentClassLoader == null) {
            componentClassLoader = serviceNode.getControllerServiceImplementation().getClass().getClassLoader();
        }

        return createClassLoaderDiagnosticsDto(componentClassLoader);
    }


    private ClassLoaderDiagnosticsDTO createClassLoaderDiagnosticsDto(final ProcessorNode procNode) {
        ClassLoader componentClassLoader = extensionManager.getInstanceClassLoader(procNode.getIdentifier());
        if (componentClassLoader == null) {
            componentClassLoader = procNode.getProcessor().getClass().getClassLoader();
        }

        return createClassLoaderDiagnosticsDto(componentClassLoader);
    }

    private ClassLoaderDiagnosticsDTO createClassLoaderDiagnosticsDto(final ClassLoader classLoader) {
        final ClassLoaderDiagnosticsDTO dto = new ClassLoaderDiagnosticsDTO();

        final Bundle bundle = extensionManager.getBundle(classLoader);
        if (bundle != null) {
            dto.setBundle(createBundleDto(bundle.getBundleDetails().getCoordinate()));
        }

        final ClassLoader parentClassLoader = classLoader.getParent();
        if (parentClassLoader != null) {
            dto.setParentClassLoader(createClassLoaderDiagnosticsDto(parentClassLoader));
        }

        return dto;
    }


    private ConnectionDiagnosticsDTO createConnectionDiagnosticsDto(final Connection connection) {
        final ConnectionDiagnosticsDTO dto = new ConnectionDiagnosticsDTO();
        dto.setConnection(createConnectionDto(connection));
        dto.setAggregateSnapshot(createConnectionDiagnosticsSnapshotDto(connection));
        return dto;
    }

    private ConnectionDiagnosticsSnapshotDTO createConnectionDiagnosticsSnapshotDto(final Connection connection) {
        final ConnectionDiagnosticsSnapshotDTO dto = new ConnectionDiagnosticsSnapshotDTO();

        final QueueDiagnostics queueDiagnostics = connection.getFlowFileQueue().getQueueDiagnostics();

        final FlowFileQueue queue = connection.getFlowFileQueue();
        final QueueSize totalSize = queue.size();
        dto.setTotalByteCount(totalSize.getByteCount());
        dto.setTotalFlowFileCount(totalSize.getObjectCount());

        final LocalQueuePartitionDiagnostics localDiagnostics = queueDiagnostics.getLocalQueuePartitionDiagnostics();
        dto.setLocalQueuePartition(createLocalQueuePartitionDto(localDiagnostics));

        final List<RemoteQueuePartitionDiagnostics> remoteDiagnostics = queueDiagnostics.getRemoteQueuePartitionDiagnostics();
        if (remoteDiagnostics != null) {
            final List<RemoteQueuePartitionDTO> remoteDiagnosticsDtos = remoteDiagnostics.stream()
                .map(this::createRemoteQueuePartitionDto)
                .collect(Collectors.toList());

            dto.setRemoteQueuePartitions(remoteDiagnosticsDtos);
        }

        return dto;
    }

    private LocalQueuePartitionDTO createLocalQueuePartitionDto(final LocalQueuePartitionDiagnostics queueDiagnostics) {
        final LocalQueuePartitionDTO dto = new LocalQueuePartitionDTO();

        final QueueSize activeSize = queueDiagnostics.getActiveQueueSize();
        dto.setActiveQueueByteCount(activeSize.getByteCount());
        dto.setActiveQueueFlowFileCount(activeSize.getObjectCount());

        final QueueSize inFlightSize = queueDiagnostics.getUnacknowledgedQueueSize();
        dto.setInFlightByteCount(inFlightSize.getByteCount());
        dto.setInFlightFlowFileCount(inFlightSize.getObjectCount());

        final QueueSize swapSize = queueDiagnostics.getSwapQueueSize();
        dto.setSwapByteCount(swapSize.getByteCount());
        dto.setSwapFlowFileCount(swapSize.getObjectCount());
        dto.setSwapFiles(queueDiagnostics.getSwapFileCount());

        dto.setTotalByteCount(activeSize.getByteCount() + inFlightSize.getByteCount() + swapSize.getByteCount());
        dto.setTotalFlowFileCount(activeSize.getObjectCount() + inFlightSize.getObjectCount() + swapSize.getObjectCount());

        dto.setAllActiveQueueFlowFilesPenalized(queueDiagnostics.isAllActiveFlowFilesPenalized());
        dto.setAnyActiveQueueFlowFilesPenalized(queueDiagnostics.isAnyActiveFlowFilePenalized());

        return dto;
    }

    private RemoteQueuePartitionDTO createRemoteQueuePartitionDto(final RemoteQueuePartitionDiagnostics queueDiagnostics) {
        final RemoteQueuePartitionDTO dto = new RemoteQueuePartitionDTO();

        dto.setNodeIdentifier(queueDiagnostics.getNodeIdentifier());

        final QueueSize activeSize = queueDiagnostics.getActiveQueueSize();
        dto.setActiveQueueByteCount(activeSize.getByteCount());
        dto.setActiveQueueFlowFileCount(activeSize.getObjectCount());

        final QueueSize inFlightSize = queueDiagnostics.getUnacknowledgedQueueSize();
        dto.setInFlightByteCount(inFlightSize.getByteCount());
        dto.setInFlightFlowFileCount(inFlightSize.getObjectCount());

        final QueueSize swapSize = queueDiagnostics.getSwapQueueSize();
        dto.setSwapByteCount(swapSize.getByteCount());
        dto.setSwapFlowFileCount(swapSize.getObjectCount());
        dto.setSwapFiles(queueDiagnostics.getSwapFileCount());

        dto.setTotalByteCount(activeSize.getByteCount() + inFlightSize.getByteCount() + swapSize.getByteCount());
        dto.setTotalFlowFileCount(activeSize.getObjectCount() + inFlightSize.getObjectCount() + swapSize.getObjectCount());

        return dto;
    }

    private JVMDiagnosticsDTO createJvmDiagnosticsDto(final FlowController flowController) {
        final JVMDiagnosticsDTO dto = new JVMDiagnosticsDTO();
        dto.setAggregateSnapshot(createJvmDiagnosticsSnapshotDto(flowController));
        dto.setClustered(flowController.isClustered());
        dto.setConnected(flowController.isConnected());
        return dto;
    }

    private JVMDiagnosticsSnapshotDTO createJvmDiagnosticsSnapshotDto(final FlowController flowController) {
        final JVMDiagnosticsSnapshotDTO dto = new JVMDiagnosticsSnapshotDTO();

        final JVMControllerDiagnosticsSnapshotDTO controllerDiagnosticsDto = new JVMControllerDiagnosticsSnapshotDTO();
        final JVMFlowDiagnosticsSnapshotDTO flowDiagnosticsDto = new JVMFlowDiagnosticsSnapshotDTO();
        final JVMSystemDiagnosticsSnapshotDTO systemDiagnosticsDto = new JVMSystemDiagnosticsSnapshotDTO();

        dto.setControllerDiagnostics(controllerDiagnosticsDto);
        dto.setFlowDiagnosticsDto(flowDiagnosticsDto);
        dto.setSystemDiagnosticsDto(systemDiagnosticsDto);

        final SystemDiagnostics systemDiagnostics = flowController.getSystemDiagnostics();

        // flow-related information
        final Set<BundleDTO> bundlesLoaded = extensionManager.getAllBundles().stream()
            .map(bundle -> bundle.getBundleDetails().getCoordinate())
            .sorted((a, b) -> a.getCoordinate().compareTo(b.getCoordinate()))
            .map(this::createBundleDto)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        flowDiagnosticsDto.setActiveEventDrivenThreads(flowController.getActiveEventDrivenThreadCount());
        flowDiagnosticsDto.setActiveTimerDrivenThreads(flowController.getActiveTimerDrivenThreadCount());
        flowDiagnosticsDto.setBundlesLoaded(bundlesLoaded);
        flowDiagnosticsDto.setTimeZone(System.getProperty("user.timezone"));
        flowDiagnosticsDto.setUptime(FormatUtils.formatHoursMinutesSeconds(systemDiagnostics.getUptime(), TimeUnit.MILLISECONDS));

        // controller-related information
        controllerDiagnosticsDto.setClusterCoordinator(flowController.isClusterCoordinator());
        controllerDiagnosticsDto.setPrimaryNode(flowController.isPrimary());
        controllerDiagnosticsDto.setMaxEventDrivenThreads(flowController.getMaxEventDrivenThreadCount());
        controllerDiagnosticsDto.setMaxTimerDrivenThreads(flowController.getMaxTimerDrivenThreadCount());

        // system-related information
        systemDiagnosticsDto.setMaxOpenFileDescriptors(systemDiagnostics.getMaxOpenFileHandles());
        systemDiagnosticsDto.setOpenFileDescriptors(systemDiagnostics.getOpenFileHandles());
        systemDiagnosticsDto.setPhysicalMemoryBytes(systemDiagnostics.getTotalPhysicalMemory());
        systemDiagnosticsDto.setPhysicalMemory(FormatUtils.formatDataSize(systemDiagnostics.getTotalPhysicalMemory()));

        final NumberFormat percentageFormat = NumberFormat.getPercentInstance();
        percentageFormat.setMaximumFractionDigits(2);

        final Set<RepositoryUsageDTO> contentRepoUsage = new HashSet<>();
        for (final Map.Entry<String, StorageUsage> entry : systemDiagnostics.getContentRepositoryStorageUsage().entrySet()) {
            final String repoName = entry.getKey();
            final StorageUsage usage = entry.getValue();

            final RepositoryUsageDTO usageDto = new RepositoryUsageDTO();
            usageDto.setName(repoName);

            usageDto.setFileStoreHash(DigestUtils.sha256Hex(flowController.getContentRepoFileStoreName(repoName)));
            usageDto.setFreeSpace(FormatUtils.formatDataSize(usage.getFreeSpace()));
            usageDto.setFreeSpaceBytes(usage.getFreeSpace());
            usageDto.setTotalSpace(FormatUtils.formatDataSize(usage.getTotalSpace()));
            usageDto.setTotalSpaceBytes(usage.getTotalSpace());

            final double usedPercentage = (usage.getTotalSpace() - usage.getFreeSpace()) / (double) usage.getTotalSpace();
            final String utilization = percentageFormat.format(usedPercentage);
            usageDto.setUtilization(utilization);
            contentRepoUsage.add(usageDto);
        }

        final Set<RepositoryUsageDTO> provRepoUsage = new HashSet<>();
        for (final Map.Entry<String, StorageUsage> entry : systemDiagnostics.getProvenanceRepositoryStorageUsage().entrySet()) {
            final String repoName = entry.getKey();
            final StorageUsage usage = entry.getValue();

            final RepositoryUsageDTO usageDto = new RepositoryUsageDTO();
            usageDto.setName(repoName);

            usageDto.setFileStoreHash(DigestUtils.sha256Hex(flowController.getProvenanceRepoFileStoreName(repoName)));
            usageDto.setFreeSpace(FormatUtils.formatDataSize(usage.getFreeSpace()));
            usageDto.setFreeSpaceBytes(usage.getFreeSpace());
            usageDto.setTotalSpace(FormatUtils.formatDataSize(usage.getTotalSpace()));
            usageDto.setTotalSpaceBytes(usage.getTotalSpace());

            final double usedPercentage = (usage.getTotalSpace() - usage.getFreeSpace()) / (double) usage.getTotalSpace();
            final String utilization = percentageFormat.format(usedPercentage);
            usageDto.setUtilization(utilization);
            provRepoUsage.add(usageDto);
        }

        final RepositoryUsageDTO flowFileRepoUsage = new RepositoryUsageDTO();
        for (final Map.Entry<String, StorageUsage> entry : systemDiagnostics.getProvenanceRepositoryStorageUsage().entrySet()) {
            final String repoName = entry.getKey();
            final StorageUsage usage = entry.getValue();

            flowFileRepoUsage.setName(repoName);

            flowFileRepoUsage.setFileStoreHash(DigestUtils.sha256Hex(flowController.getFlowRepoFileStoreName()));
            flowFileRepoUsage.setFreeSpace(FormatUtils.formatDataSize(usage.getFreeSpace()));
            flowFileRepoUsage.setFreeSpaceBytes(usage.getFreeSpace());
            flowFileRepoUsage.setTotalSpace(FormatUtils.formatDataSize(usage.getTotalSpace()));
            flowFileRepoUsage.setTotalSpaceBytes(usage.getTotalSpace());

            final double usedPercentage = (usage.getTotalSpace() - usage.getFreeSpace()) / (double) usage.getTotalSpace();
            final String utilization = percentageFormat.format(usedPercentage);
            flowFileRepoUsage.setUtilization(utilization);
        }

        systemDiagnosticsDto.setContentRepositoryStorageUsage(contentRepoUsage);
        systemDiagnosticsDto.setCpuCores(systemDiagnostics.getAvailableProcessors());
        systemDiagnosticsDto.setCpuLoadAverage(systemDiagnostics.getProcessorLoadAverage());
        systemDiagnosticsDto.setFlowFileRepositoryStorageUsage(flowFileRepoUsage);
        systemDiagnosticsDto.setMaxHeapBytes(systemDiagnostics.getMaxHeap());
        systemDiagnosticsDto.setMaxHeap(FormatUtils.formatDataSize(systemDiagnostics.getMaxHeap()));
        systemDiagnosticsDto.setProvenanceRepositoryStorageUsage(provRepoUsage);

        // Create the Garbage Collection History info
        final GarbageCollectionHistory gcHistory = flowController.getGarbageCollectionHistory();
        final List<GarbageCollectionDiagnosticsDTO> gcDiagnostics = new ArrayList<>();
        for (final String memoryManager : gcHistory.getMemoryManagerNames()) {
            final List<GarbageCollectionStatus> statuses = gcHistory.getGarbageCollectionStatuses(memoryManager);

            final List<GCDiagnosticsSnapshotDTO> gcSnapshots = new ArrayList<>();
            for (final GarbageCollectionStatus status : statuses) {
                final GCDiagnosticsSnapshotDTO snapshotDto = new GCDiagnosticsSnapshotDTO();
                snapshotDto.setTimestamp(status.getTimestamp());
                snapshotDto.setCollectionCount(status.getCollectionCount());
                snapshotDto.setCollectionMillis(status.getCollectionMillis());
                gcSnapshots.add(snapshotDto);
            }

            gcSnapshots.sort(Comparator.comparing(GCDiagnosticsSnapshotDTO::getTimestamp).reversed());

            final GarbageCollectionDiagnosticsDTO gcDto = new GarbageCollectionDiagnosticsDTO();
            gcDto.setMemoryManagerName(memoryManager);
            gcDto.setSnapshots(gcSnapshots);
            gcDiagnostics.add(gcDto);
        }

        systemDiagnosticsDto.setGarbageCollectionDiagnostics(gcDiagnostics);

        return dto;
    }

    private List<ThreadDumpDTO> createThreadDumpDtos(final ProcessorNode procNode) {
        final List<ThreadDumpDTO> threadDumps = new ArrayList<>();

        final List<ActiveThreadInfo> activeThreads = procNode.getActiveThreads();
        for (final ActiveThreadInfo threadInfo : activeThreads) {
            final ThreadDumpDTO dto = new ThreadDumpDTO();
            dto.setStackTrace(threadInfo.getStackTrace());
            dto.setThreadActiveMillis(threadInfo.getActiveMillis());
            dto.setThreadName(threadInfo.getThreadName());
            dto.setTaskTerminated(threadInfo.isTerminated());
            threadDumps.add(dto);
        }

        return threadDumps;
    }

    /**
     * Creates a ProcessorConfigDTO from the specified ProcessorNode.
     *
     * @param procNode node
     * @return dto
     */
    public ProcessorConfigDTO createProcessorConfigDto(final ProcessorNode procNode) {
        if (procNode == null) {
            return null;
        }

        final ProcessorConfigDTO dto = new ProcessorConfigDTO();

        // sort a copy of the properties
        final Map<PropertyDescriptor, String> sortedProperties = new TreeMap<>(new Comparator<PropertyDescriptor>() {
            @Override
            public int compare(final PropertyDescriptor o1, final PropertyDescriptor o2) {
                return Collator.getInstance(Locale.US).compare(o1.getName(), o2.getName());
            }
        });
        sortedProperties.putAll(procNode.getProperties());

        // get the property order from the processor
        final Processor processor = procNode.getProcessor();
        final Map<PropertyDescriptor, String> orderedProperties = new LinkedHashMap<>();
        final List<PropertyDescriptor> descriptors = processor.getPropertyDescriptors();
        if (descriptors != null && !descriptors.isEmpty()) {
            for (final PropertyDescriptor descriptor : descriptors) {
                orderedProperties.put(descriptor, null);
            }
        }
        orderedProperties.putAll(sortedProperties);

        // build the descriptor and property dtos
        dto.setDescriptors(new LinkedHashMap<String, PropertyDescriptorDTO>());
        dto.setProperties(new LinkedHashMap<String, String>());
        for (final Map.Entry<PropertyDescriptor, String> entry : orderedProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();

            // store the property descriptor
            dto.getDescriptors().put(descriptor.getName(), createPropertyDescriptorDto(descriptor, procNode.getProcessGroup().getIdentifier()));

            // determine the property value - don't include sensitive properties
            String propertyValue = entry.getValue();
            if (propertyValue != null && descriptor.isSensitive()) {
                propertyValue = SENSITIVE_VALUE_MASK;
            } else if (propertyValue == null && descriptor.getDefaultValue() != null) {
                propertyValue = descriptor.getDefaultValue();
            }

            // set the property value
            dto.getProperties().put(descriptor.getName(), propertyValue);
        }

        dto.setSchedulingPeriod(procNode.getSchedulingPeriod());
        dto.setPenaltyDuration(procNode.getPenalizationPeriod());
        dto.setYieldDuration(procNode.getYieldPeriod());
        dto.setRunDurationMillis(procNode.getRunDuration(TimeUnit.MILLISECONDS));
        dto.setConcurrentlySchedulableTaskCount(procNode.getMaxConcurrentTasks());
        dto.setLossTolerant(procNode.isLossTolerant());
        dto.setComments(procNode.getComments());
        dto.setBulletinLevel(procNode.getBulletinLevel().name());
        dto.setSchedulingStrategy(procNode.getSchedulingStrategy().name());
        dto.setExecutionNode(procNode.getExecutionNode().name());
        dto.setAnnotationData(procNode.getAnnotationData());

        // set up the default values for concurrent tasks and scheduling period
        final Map<String, String> defaultConcurrentTasks = new HashMap<>();
        defaultConcurrentTasks.put(SchedulingStrategy.TIMER_DRIVEN.name(), String.valueOf(SchedulingStrategy.TIMER_DRIVEN.getDefaultConcurrentTasks()));
        defaultConcurrentTasks.put(SchedulingStrategy.EVENT_DRIVEN.name(), String.valueOf(SchedulingStrategy.EVENT_DRIVEN.getDefaultConcurrentTasks()));
        defaultConcurrentTasks.put(SchedulingStrategy.CRON_DRIVEN.name(), String.valueOf(SchedulingStrategy.CRON_DRIVEN.getDefaultConcurrentTasks()));
        dto.setDefaultConcurrentTasks(defaultConcurrentTasks);

        final Map<String, String> defaultSchedulingPeriod = new HashMap<>();
        defaultSchedulingPeriod.put(SchedulingStrategy.TIMER_DRIVEN.name(), SchedulingStrategy.TIMER_DRIVEN.getDefaultSchedulingPeriod());
        defaultSchedulingPeriod.put(SchedulingStrategy.CRON_DRIVEN.name(), SchedulingStrategy.CRON_DRIVEN.getDefaultSchedulingPeriod());
        dto.setDefaultSchedulingPeriod(defaultSchedulingPeriod);

        return dto;
    }

    /**
     * Creates a PropertyDesriptorDTO from the specified PropertyDesriptor.
     *
     * @param propertyDescriptor descriptor
     * @param groupId the Identifier of the Process Group that the component belongs to
     * @return dto
     */
    public PropertyDescriptorDTO createPropertyDescriptorDto(final PropertyDescriptor propertyDescriptor, final String groupId) {
        if (propertyDescriptor == null) {
            return null;
        }

        final PropertyDescriptorDTO dto = new PropertyDescriptorDTO();

        dto.setName(propertyDescriptor.getName());
        dto.setDisplayName(propertyDescriptor.getDisplayName());
        dto.setRequired(propertyDescriptor.isRequired());
        dto.setSensitive(propertyDescriptor.isSensitive());
        dto.setDynamic(propertyDescriptor.isDynamic());
        dto.setDescription(propertyDescriptor.getDescription());
        dto.setDefaultValue(propertyDescriptor.getDefaultValue());
        dto.setSupportsEl(propertyDescriptor.isExpressionLanguageSupported());

        // to support legacy/deprecated method .expressionLanguageSupported(true)
        String description = propertyDescriptor.isExpressionLanguageSupported()
                && propertyDescriptor.getExpressionLanguageScope().equals(ExpressionLanguageScope.NONE)
                ? "true (undefined scope)" : propertyDescriptor.getExpressionLanguageScope().getDescription();
        dto.setExpressionLanguageScope(description);

        // set the identifies controller service is applicable
        if (propertyDescriptor.getControllerServiceDefinition() != null) {
            final Class serviceClass = propertyDescriptor.getControllerServiceDefinition();
            final Bundle serviceBundle = extensionManager.getBundle(serviceClass.getClassLoader());

            dto.setIdentifiesControllerService(serviceClass.getName());
            dto.setIdentifiesControllerServiceBundle(createBundleDto(serviceBundle.getBundleDetails().getCoordinate()));
        }

        final Class<? extends ControllerService> serviceDefinition = propertyDescriptor.getControllerServiceDefinition();
        if (propertyDescriptor.getAllowableValues() == null) {
            if (serviceDefinition == null) {
                dto.setAllowableValues(null);
            } else {
                final List<AllowableValueEntity> allowableValues = new ArrayList<>();
                final List<String> controllerServiceIdentifiers = new ArrayList<>(controllerServiceProvider.getControllerServiceIdentifiers(serviceDefinition, groupId));
                Collections.sort(controllerServiceIdentifiers, Collator.getInstance(Locale.US));
                for (final String serviceIdentifier : controllerServiceIdentifiers) {
                    final ControllerServiceNode service = controllerServiceProvider.getControllerServiceNode(serviceIdentifier);
                    final boolean isServiceAuthorized = service.isAuthorized(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                    final String displayName = isServiceAuthorized ? service.getName() : serviceIdentifier;

                    final AllowableValueDTO allowableValue = new AllowableValueDTO();
                    allowableValue.setDisplayName(displayName);
                    allowableValue.setValue(serviceIdentifier);
                    allowableValues.add(entityFactory.createAllowableValueEntity(allowableValue, isServiceAuthorized));
                }
                dto.setAllowableValues(allowableValues);
            }
        } else {
            final List<AllowableValueEntity> allowableValues = new ArrayList<>();
            for (final AllowableValue allowableValue : propertyDescriptor.getAllowableValues()) {
                final AllowableValueDTO allowableValueDto = new AllowableValueDTO();
                allowableValueDto.setDisplayName(allowableValue.getDisplayName());
                allowableValueDto.setValue(allowableValue.getValue());
                allowableValueDto.setDescription(allowableValue.getDescription());
                allowableValues.add(entityFactory.createAllowableValueEntity(allowableValueDto, true));
            }

            dto.setAllowableValues(allowableValues);
        }

        return dto;
    }

    // Copy methods
    public LabelDTO copy(final LabelDTO original) {
        final LabelDTO copy = new LabelDTO();
        copy.setId(original.getId());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setLabel(original.getLabel());
        copy.setStyle(copy(original.getStyle()));
        copy.setPosition(original.getPosition());
        copy.setWidth(original.getWidth());
        copy.setHeight(original.getHeight());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        return copy;
    }

    public ControllerServiceDTO copy(final ControllerServiceDTO original) {
        final ControllerServiceDTO copy = new ControllerServiceDTO();
        copy.setAnnotationData(original.getAnnotationData());
        copy.setControllerServiceApis(original.getControllerServiceApis());
        copy.setComments(original.getComments());
        copy.setCustomUiUrl(original.getCustomUiUrl());
        copy.setDescriptors(copy(original.getDescriptors()));
        copy.setId(original.getId());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setName(original.getName());
        copy.setProperties(copy(original.getProperties()));
        copy.setReferencingComponents(copy(original.getReferencingComponents()));
        copy.setState(original.getState());
        copy.setType(original.getType());
        copy.setBundle(copy(original.getBundle()));
        copy.setExtensionMissing(original.getExtensionMissing());
        copy.setMultipleVersionsAvailable(original.getMultipleVersionsAvailable());
        copy.setPersistsState(original.getPersistsState());
        copy.setValidationErrors(copy(original.getValidationErrors()));
        copy.setValidationStatus(original.getValidationStatus());
        copy.setVersionedComponentId(original.getVersionedComponentId());
        return copy;
    }

    public FunnelDTO copy(final FunnelDTO original) {
        final FunnelDTO copy = new FunnelDTO();
        copy.setId(original.getId());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setPosition(original.getPosition());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        return copy;
    }

    private <T> List<T> copy(final List<T> original) {
        if (original == null) {
            return null;
        } else {
            return new ArrayList<>(original);
        }
    }

    private <T> List<T> copy(final Collection<T> original) {
        if (original == null) {
            return null;
        } else {
            return new ArrayList<>(original);
        }
    }

    private <T> Set<T> copy(final Set<T> original) {
        if (original == null) {
            return null;
        } else {
            return new LinkedHashSet<>(original);
        }
    }

    private <S, T> Map<S, T> copy(final Map<S, T> original) {
        if (original == null) {
            return null;
        } else {
            return new LinkedHashMap<>(original);
        }
    }

    public BundleDTO copy(final BundleDTO original) {
        if (original == null) {
            return null;
        }

        final BundleDTO copy = new BundleDTO();
        copy.setGroup(original.getGroup());
        copy.setArtifact(original.getArtifact());
        copy.setVersion(original.getVersion());
        return copy;
    }

    public ProcessorDTO copy(final ProcessorDTO original) {
        final ProcessorDTO copy = new ProcessorDTO();
        copy.setConfig(copy(original.getConfig()));
        copy.setPosition(original.getPosition());
        copy.setId(original.getId());
        copy.setName(original.getName());
        copy.setDescription(original.getDescription());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setRelationships(copy(original.getRelationships()));
        copy.setState(original.getState());
        copy.setStyle(copy(original.getStyle()));
        copy.setType(original.getType());
        copy.setBundle(copy(original.getBundle()));
        copy.setSupportsParallelProcessing(original.getSupportsParallelProcessing());
        copy.setSupportsEventDriven(original.getSupportsEventDriven());
        copy.setSupportsBatching(original.getSupportsBatching());
        copy.setPersistsState(original.getPersistsState());
        copy.setExecutionNodeRestricted(original.isExecutionNodeRestricted());
        copy.setExtensionMissing(original.getExtensionMissing());
        copy.setMultipleVersionsAvailable(original.getMultipleVersionsAvailable());
        copy.setValidationErrors(copy(original.getValidationErrors()));
        copy.setValidationStatus(original.getValidationStatus());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        return copy;
    }

    private ProcessorConfigDTO copy(final ProcessorConfigDTO original) {
        final ProcessorConfigDTO copy = new ProcessorConfigDTO();
        copy.setAnnotationData(original.getAnnotationData());
        copy.setAutoTerminatedRelationships(copy(original.getAutoTerminatedRelationships()));
        copy.setComments(original.getComments());
        copy.setSchedulingStrategy(original.getSchedulingStrategy());
        copy.setExecutionNode(original.getExecutionNode());
        copy.setConcurrentlySchedulableTaskCount(original.getConcurrentlySchedulableTaskCount());
        copy.setCustomUiUrl(original.getCustomUiUrl());
        copy.setDescriptors(copy(original.getDescriptors()));
        copy.setProperties(copy(original.getProperties()));
        copy.setSchedulingPeriod(original.getSchedulingPeriod());
        copy.setPenaltyDuration(original.getPenaltyDuration());
        copy.setYieldDuration(original.getYieldDuration());
        copy.setRunDurationMillis(original.getRunDurationMillis());
        copy.setBulletinLevel(original.getBulletinLevel());
        copy.setDefaultConcurrentTasks(original.getDefaultConcurrentTasks());
        copy.setDefaultSchedulingPeriod(original.getDefaultSchedulingPeriod());
        copy.setLossTolerant(original.isLossTolerant());

        return copy;
    }

    public ConnectionDTO copy(final ConnectionDTO original) {
        final ConnectionDTO copy = new ConnectionDTO();
        copy.setAvailableRelationships(copy(original.getAvailableRelationships()));
        copy.setDestination(original.getDestination());
        copy.setPosition(original.getPosition());
        copy.setId(original.getId());
        copy.setName(original.getName());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setSelectedRelationships(copy(original.getSelectedRelationships()));
        copy.setFlowFileExpiration(original.getFlowFileExpiration());
        copy.setBackPressureObjectThreshold(original.getBackPressureObjectThreshold());
        copy.setBackPressureDataSizeThreshold(original.getBackPressureDataSizeThreshold());
        copy.setPrioritizers(copy(original.getPrioritizers()));
        copy.setSource(original.getSource());
        copy.setzIndex(original.getzIndex());
        copy.setLabelIndex(original.getLabelIndex());
        copy.setBends(copy(original.getBends()));
        copy.setLoadBalancePartitionAttribute(original.getLoadBalancePartitionAttribute());
        copy.setLoadBalanceStrategy(original.getLoadBalanceStrategy());
        copy.setLoadBalanceCompression(original.getLoadBalanceCompression());
        copy.setLoadBalanceStatus(original.getLoadBalanceStatus());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        return copy;
    }

    public BulletinDTO copy(final BulletinDTO original) {
        final BulletinDTO copy = new BulletinDTO();
        copy.setId(original.getId());
        copy.setTimestamp(original.getTimestamp());
        copy.setGroupId(original.getGroupId());
        copy.setSourceId(original.getSourceId());
        copy.setSourceName(original.getSourceName());
        copy.setCategory(original.getCategory());
        copy.setLevel(original.getLevel());
        copy.setMessage(original.getMessage());
        copy.setNodeAddress(original.getNodeAddress());
        return copy;
    }

    public PortDTO copy(final PortDTO original) {
        final PortDTO copy = new PortDTO();
        copy.setPosition(original.getPosition());
        copy.setId(original.getId());
        copy.setName(original.getName());
        copy.setComments(original.getComments());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setState(original.getState());
        copy.setType(original.getType());
        copy.setTransmitting(original.isTransmitting());
        copy.setConcurrentlySchedulableTaskCount(original.getConcurrentlySchedulableTaskCount());
        copy.setUserAccessControl(copy(original.getUserAccessControl()));
        copy.setGroupAccessControl(copy(original.getGroupAccessControl()));
        copy.setValidationErrors(copy(original.getValidationErrors()));
        copy.setVersionedComponentId(original.getVersionedComponentId());
        return copy;
    }

    public RemoteProcessGroupPortDTO copy(final RemoteProcessGroupPortDTO original) {
        final RemoteProcessGroupPortDTO copy = new RemoteProcessGroupPortDTO();
        copy.setId(original.getId());
        copy.setTargetId(original.getTargetId());
        copy.setGroupId(original.getGroupId());
        copy.setName(original.getName());
        copy.setComments(original.getComments());
        copy.setConnected(original.isConnected());
        copy.setTargetRunning(original.isTargetRunning());
        copy.setTransmitting(original.isTransmitting());
        copy.setConcurrentlySchedulableTaskCount(original.getConcurrentlySchedulableTaskCount());
        copy.setUseCompression(original.getUseCompression());
        copy.setExists(original.getExists());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        final BatchSettingsDTO batchOrg = original.getBatchSettings();
        if (batchOrg != null) {
            final BatchSettingsDTO batchCopy = new BatchSettingsDTO();
            batchCopy.setCount(batchOrg.getCount());
            batchCopy.setSize(batchOrg.getSize());
            batchCopy.setDuration(batchOrg.getDuration());
            copy.setBatchSettings(batchCopy);
        }
        return copy;
    }

    public ProcessGroupDTO copy(final ProcessGroupDTO original, final boolean deep) {
        final ProcessGroupDTO copy = new ProcessGroupDTO();
        copy.setComments(original.getComments());
        copy.setContents(copy(original.getContents(), deep));
        copy.setPosition(original.getPosition());
        copy.setId(original.getId());
        copy.setInputPortCount(original.getInputPortCount());
        copy.setInvalidCount(original.getInvalidCount());
        copy.setName(original.getName());
        copy.setVersionControlInformation(copy(original.getVersionControlInformation()));
        copy.setOutputPortCount(original.getOutputPortCount());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        copy.setRunningCount(original.getRunningCount());
        copy.setStoppedCount(original.getStoppedCount());
        copy.setDisabledCount(original.getDisabledCount());
        copy.setActiveRemotePortCount(original.getActiveRemotePortCount());
        copy.setInactiveRemotePortCount(original.getInactiveRemotePortCount());

        copy.setUpToDateCount(original.getUpToDateCount());
        copy.setLocallyModifiedCount(original.getLocallyModifiedCount());
        copy.setStaleCount(original.getStaleCount());
        copy.setLocallyModifiedAndStaleCount(original.getLocallyModifiedAndStaleCount());
        copy.setSyncFailureCount(original.getSyncFailureCount());

        if (original.getVariables() != null) {
            copy.setVariables(new HashMap<>(original.getVariables()));
        }

        return copy;
    }

    public VersionControlInformationDTO copy(final VersionControlInformationDTO original) {
        if (original == null) {
            return null;
        }

        final VersionControlInformationDTO copy = new VersionControlInformationDTO();
        copy.setRegistryId(original.getRegistryId());
        copy.setRegistryName(original.getRegistryName());
        copy.setBucketId(original.getBucketId());
        copy.setBucketName(original.getBucketName());
        copy.setFlowId(original.getFlowId());
        copy.setFlowName(original.getFlowName());
        copy.setFlowDescription(original.getFlowDescription());
        copy.setVersion(original.getVersion());
        copy.setState(original.getState());
        copy.setStateExplanation(original.getStateExplanation());
        return copy;
    }

    public RemoteProcessGroupDTO copy(final RemoteProcessGroupDTO original) {
        final RemoteProcessGroupContentsDTO originalContents = original.getContents();
        final RemoteProcessGroupContentsDTO copyContents = new RemoteProcessGroupContentsDTO();

        if (originalContents.getInputPorts() != null) {
            final Set<RemoteProcessGroupPortDTO> inputPorts = new HashSet<>();
            for (final RemoteProcessGroupPortDTO port : originalContents.getInputPorts()) {
                inputPorts.add(copy(port));
            }
            copyContents.setInputPorts(inputPorts);
        }

        if (originalContents.getOutputPorts() != null) {
            final Set<RemoteProcessGroupPortDTO> outputPorts = new HashSet<>();
            for (final RemoteProcessGroupPortDTO port : originalContents.getOutputPorts()) {
                outputPorts.add(copy(port));
            }
            copyContents.setOutputPorts(outputPorts);
        }

        final RemoteProcessGroupDTO copy = new RemoteProcessGroupDTO();
        copy.setComments(original.getComments());
        copy.setPosition(original.getPosition());
        copy.setId(original.getId());
        copy.setCommunicationsTimeout(original.getCommunicationsTimeout());
        copy.setYieldDuration(original.getYieldDuration());
        copy.setName(original.getName());
        copy.setInputPortCount(original.getInputPortCount());
        copy.setOutputPortCount(original.getOutputPortCount());
        copy.setActiveRemoteInputPortCount(original.getActiveRemoteInputPortCount());
        copy.setInactiveRemoteInputPortCount(original.getInactiveRemoteInputPortCount());
        copy.setActiveRemoteOutputPortCount(original.getActiveRemoteOutputPortCount());
        copy.setInactiveRemoteOutputPortCount(original.getInactiveRemoteOutputPortCount());
        copy.setParentGroupId(original.getParentGroupId());
        copy.setTargetUris(original.getTargetUris());
        copy.setTransportProtocol(original.getTransportProtocol());
        copy.setProxyHost(original.getProxyHost());
        copy.setProxyPort(original.getProxyPort());
        copy.setProxyUser(original.getProxyUser());
        copy.setProxyPassword(original.getProxyPassword());
        copy.setLocalNetworkInterface(original.getLocalNetworkInterface());
        copy.setVersionedComponentId(original.getVersionedComponentId());

        copy.setContents(copyContents);

        return copy;
    }

    public ConnectableDTO createConnectableDto(final PortDTO port, final ConnectableType type) {
        final ConnectableDTO connectable = new ConnectableDTO();
        connectable.setGroupId(port.getParentGroupId());
        connectable.setId(port.getId());
        connectable.setName(port.getName());
        connectable.setType(type.name());
        connectable.setVersionedComponentId(port.getVersionedComponentId());
        return connectable;
    }

    public ConnectableDTO createConnectableDto(final ProcessorDTO processor) {
        final ConnectableDTO connectable = new ConnectableDTO();
        connectable.setGroupId(processor.getParentGroupId());
        connectable.setId(processor.getId());
        connectable.setName(processor.getName());
        connectable.setType(ConnectableType.PROCESSOR.name());
        connectable.setVersionedComponentId(processor.getVersionedComponentId());
        return connectable;
    }

    public ConnectableDTO createConnectableDto(final FunnelDTO funnel) {
        final ConnectableDTO connectable = new ConnectableDTO();
        connectable.setGroupId(funnel.getParentGroupId());
        connectable.setId(funnel.getId());
        connectable.setType(ConnectableType.FUNNEL.name());
        connectable.setVersionedComponentId(funnel.getVersionedComponentId());
        return connectable;
    }

    public ConnectableDTO createConnectableDto(final RemoteProcessGroupPortDTO remoteGroupPort, final ConnectableType type) {
        final ConnectableDTO connectable = new ConnectableDTO();
        connectable.setGroupId(remoteGroupPort.getGroupId());
        connectable.setId(remoteGroupPort.getId());
        connectable.setName(remoteGroupPort.getName());
        connectable.setType(type.name());
        connectable.setVersionedComponentId(connectable.getVersionedComponentId());
        return connectable;
    }

    /**
     *
     * @param original orig
     * @param deep if <code>true</code>, all Connections, ProcessGroups, Ports, Processors, etc. will be copied. If <code>false</code>, the copy will have links to the same objects referenced by
     * <code>original</code>.
     *
     * @return dto
     */
    private FlowSnippetDTO copy(final FlowSnippetDTO original, final boolean deep) {
        final FlowSnippetDTO copy = new FlowSnippetDTO();

        final Set<ConnectionDTO> connections = new LinkedHashSet<>();
        final Set<ProcessGroupDTO> groups = new LinkedHashSet<>();
        final Set<PortDTO> inputPorts = new LinkedHashSet<>();
        final Set<PortDTO> outputPorts = new LinkedHashSet<>();
        final Set<LabelDTO> labels = new LinkedHashSet<>();
        final Set<ProcessorDTO> processors = new LinkedHashSet<>();
        final Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
        final Set<FunnelDTO> funnels = new LinkedHashSet<>();
        final Set<ControllerServiceDTO> controllerServices = new LinkedHashSet<>();

        if (deep) {
            for (final ProcessGroupDTO group : original.getProcessGroups()) {
                groups.add(copy(group, deep));
            }

            for (final PortDTO port : original.getInputPorts()) {
                inputPorts.add(copy(port));
            }

            for (final PortDTO port : original.getOutputPorts()) {
                outputPorts.add(copy(port));
            }

            for (final LabelDTO label : original.getLabels()) {
                labels.add(copy(label));
            }

            for (final ProcessorDTO processor : original.getProcessors()) {
                processors.add(copy(processor));
            }

            for (final RemoteProcessGroupDTO remoteGroup : original.getRemoteProcessGroups()) {
                remoteProcessGroups.add(copy(remoteGroup));
            }

            for (final FunnelDTO funnel : original.getFunnels()) {
                funnels.add(copy(funnel));
            }

            for (final ConnectionDTO connection : original.getConnections()) {
                connections.add(copy(connection));
            }

            for (final ControllerServiceDTO controllerService : original.getControllerServices()) {
                controllerServices.add(copy(controllerService));
            }
        } else {
            if (original.getConnections() != null) {
                connections.addAll(copy(original.getConnections()));
            }
            if (original.getProcessGroups() != null) {
                groups.addAll(copy(original.getProcessGroups()));
            }
            if (original.getInputPorts() != null) {
                inputPorts.addAll(copy(original.getInputPorts()));
            }
            if (original.getOutputPorts() != null) {
                outputPorts.addAll(copy(original.getOutputPorts()));
            }
            if (original.getLabels() != null) {
                labels.addAll(copy(original.getLabels()));
            }
            if (original.getProcessors() != null) {
                processors.addAll(copy(original.getProcessors()));
            }
            if (original.getRemoteProcessGroups() != null) {
                remoteProcessGroups.addAll(copy(original.getRemoteProcessGroups()));
            }
            if (original.getFunnels() != null) {
                funnels.addAll(copy(original.getFunnels()));
            }
            if (original.getControllerServices() != null) {
                controllerServices.addAll(copy(original.getControllerServices()));
            }
        }

        copy.setConnections(connections);
        copy.setProcessGroups(groups);
        copy.setInputPorts(inputPorts);
        copy.setLabels(labels);
        copy.setOutputPorts(outputPorts);
        copy.setProcessors(processors);
        copy.setRemoteProcessGroups(remoteProcessGroups);
        copy.setFunnels(funnels);
        copy.setControllerServices(controllerServices);

        return copy;
    }

    /**
     * Factory method for creating a new RevisionDTO based on this controller.
     *
     * @param lastMod mod
     * @return dto
     */
    public RevisionDTO createRevisionDTO(final FlowModification lastMod) {
        final Revision revision = lastMod.getRevision();

        // create the dto
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setVersion(revision.getVersion());
        revisionDTO.setClientId(revision.getClientId());
        revisionDTO.setLastModifier(lastMod.getLastModifier());

        return revisionDTO;
    }

    public RevisionDTO createRevisionDTO(final Revision revision) {
        final RevisionDTO dto = new RevisionDTO();
        dto.setVersion(revision.getVersion());
        dto.setClientId(revision.getClientId());
        return dto;
    }

    public NodeDTO createNodeDTO(final NodeIdentifier nodeId, final NodeConnectionStatus status, final NodeHeartbeat nodeHeartbeat, final List<NodeEvent> events, final Set<String> roles) {
        final NodeDTO nodeDto = new NodeDTO();

        // populate node dto
        nodeDto.setNodeId(nodeId.getId());
        nodeDto.setAddress(nodeId.getApiAddress());
        nodeDto.setApiPort(nodeId.getApiPort());
        nodeDto.setStatus(status.getState().name());
        nodeDto.setRoles(roles);
        if (status.getConnectionRequestTime() != null) {
            final Date connectionRequested = new Date(status.getConnectionRequestTime());
            nodeDto.setConnectionRequested(connectionRequested);
        }

        // only connected nodes have heartbeats
        if (nodeHeartbeat != null) {
            final Date heartbeat = new Date(nodeHeartbeat.getTimestamp());
            nodeDto.setHeartbeat(heartbeat);
            nodeDto.setNodeStartTime(new Date(nodeHeartbeat.getSystemStartTime()));
            nodeDto.setActiveThreadCount(nodeHeartbeat.getActiveThreadCount());
            nodeDto.setQueued(FormatUtils.formatCount(nodeHeartbeat.getFlowFileCount()) + " / " + FormatUtils.formatDataSize(nodeHeartbeat.getFlowFileBytes()));
        }

        // populate node events
        final List<NodeEvent> nodeEvents = new ArrayList<>(events);
        Collections.sort(nodeEvents, new Comparator<NodeEvent>() {
            @Override
            public int compare(final NodeEvent event1, final NodeEvent event2) {
                return new Date(event2.getTimestamp()).compareTo(new Date(event1.getTimestamp()));
            }
        });

        // create the node event dtos
        final List<NodeEventDTO> nodeEventDtos = new ArrayList<>();
        for (final NodeEvent event : nodeEvents) {
            // create node event dto
            final NodeEventDTO nodeEventDto = new NodeEventDTO();
            nodeEventDtos.add(nodeEventDto);

            // populate node event dto
            nodeEventDto.setMessage(event.getMessage());
            nodeEventDto.setCategory(event.getSeverity().name());
            nodeEventDto.setTimestamp(new Date(event.getTimestamp()));
        }
        nodeDto.setEvents(nodeEventDtos);

        return nodeDto;
    }

    public RegistryDTO createRegistryDto(FlowRegistry registry) {
        final RegistryDTO dto = new RegistryDTO();
        dto.setDescription(registry.getDescription());
        dto.setId(registry.getIdentifier());
        dto.setName(registry.getName());
        dto.setUri(registry.getURL());
        return dto;
    }


    /* setters */
    public void setControllerServiceProvider(final ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setEntityFactory(final EntityFactory entityFactory) {
        this.entityFactory = entityFactory;
    }

    public void setBulletinRepository(BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }
}
