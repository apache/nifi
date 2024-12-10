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

package org.apache.nifi.flow.synchronization;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.encrypt.EncryptionException;
import org.apache.nifi.flow.BatchSize;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.FlowSynchronizationOptions.ComponentStopTimeoutAction;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.PropertyDecryptor;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.groups.StandardVersionedFlowStatus;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardControllerServiceFactory;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.ParameterReferencedControllerServiceData;
import org.apache.nifi.parameter.StandardParameterProviderConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.diff.StaticDifferenceDescriptor;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardVersionedComponentSynchronizer implements VersionedComponentSynchronizer {
    private static final Logger LOG = LoggerFactory.getLogger(StandardVersionedComponentSynchronizer.class);
    private static final String TEMP_FUNNEL_ID_SUFFIX = "-temp-funnel";
    public static final String ENC_PREFIX = "enc{";
    public static final String ENC_SUFFIX = "}";

    private final VersionedFlowSynchronizationContext context;
    private final Set<String> updatedVersionedComponentIds = new HashSet<>();
    private final List<CreatedOrModifiedExtension> createdAndModifiedExtensions = new ArrayList<>();

    private FlowSynchronizationOptions syncOptions;
    private final ConnectableAdditionTracker connectableAdditionTracker = new ConnectableAdditionTracker();

    public StandardVersionedComponentSynchronizer(final VersionedFlowSynchronizationContext context) {
        this.context = context;
    }

    public void setSynchronizationOptions(final FlowSynchronizationOptions syncOptions) {
        this.syncOptions = syncOptions;
    }

    @Override
    public ComponentAdditions addVersionedComponentsToProcessGroup(final ProcessGroup group, final VersionedComponentAdditions additions, final FlowSynchronizationOptions options) {
        updatedVersionedComponentIds.clear();
        createdAndModifiedExtensions.clear();
        setSynchronizationOptions(options);

        final ComponentAdditions.Builder additionsBuilder = new ComponentAdditions.Builder();

        // add any controller services first since they may be referenced by components to follow
        final Map<VersionedControllerService, ControllerServiceNode> instanceMapping = new HashMap<>();
        additions.getControllerServices().forEach(controllerService -> {
            final ControllerServiceNode newService = addControllerService(group, controllerService, options.getComponentIdGenerator(), group);
            instanceMapping.put(controllerService, newService);
            additionsBuilder.addControllerService(newService);
        });

        // go through the controller services again and update each to update any service references
        // to their new identifiers
        additions.getControllerServices().forEach(controllerService -> {
            final ControllerServiceNode newService = instanceMapping.get(controllerService);
            if (newService != null) {
                updateControllerService(newService, controllerService, group);
            }
        });

        // add any processors
        additions.getProcessors().forEach(processor -> {
            try {
                final ProcessorNode newProcessor = addProcessor(group, processor, options.getComponentIdGenerator(), group);
                additionsBuilder.addProcessor(newProcessor);
            } catch (final ProcessorInstantiationException pie) {
                throw new RuntimeException(pie);
            }
        });

        // track the proposed port names so they can be updated after adding with guaranteed unique names
        final Map<Port, String> proposedPortFinalNames = new HashMap<>();
        final Set<String> existingInputPorts = group.getInputPorts().stream().map(Port::getName).collect(Collectors.toSet());
        final Set<String> existingOutputPorts = group.getOutputPorts().stream().map(Port::getName).collect(Collectors.toSet());

        // add any input ports
        additions.getInputPorts().forEach(inputPort -> {
            // if we're adding to the root group than ports must allow remote access
            if (group.isRootGroup()) {
                inputPort.setAllowRemoteAccess(true);
            }

            final String temporaryName = generateTemporaryPortName(inputPort);
            final Port newInputPort = addInputPort(group, inputPort, options.getComponentIdGenerator(), temporaryName);

            // if the proposed port name does not conflict with any existing ports include the proposed name for updating later
            if (!existingInputPorts.contains(inputPort.getName())) {
                proposedPortFinalNames.put(newInputPort, inputPort.getName());
            }

            additionsBuilder.addInputPort(newInputPort);
        });

        // add any output ports
        additions.getOutputPorts().forEach(outputPort -> {
            // if we're adding to the root group than ports must allow remote access
            if (group.isRootGroup()) {
                outputPort.setAllowRemoteAccess(true);
            }

            final String temporaryName = generateTemporaryPortName(outputPort);
            final Port newOutputPort = addOutputPort(group, outputPort, options.getComponentIdGenerator(), temporaryName);

            // if the proposed port name does not conflict with any existing ports include the proposed name for updating later
            if (!existingOutputPorts.contains(outputPort.getName())) {
                proposedPortFinalNames.put(newOutputPort, outputPort.getName());
            }

            additionsBuilder.addOutputPort(newOutputPort);
        });

        // add any labels
        additions.getLabels().forEach(label -> {
            final Label newLabel = addLabel(group, label, options.getComponentIdGenerator());
            additionsBuilder.addLabel(newLabel);
        });

        // add any funnels
        additions.getFunnels().forEach(funnel -> {
            final Funnel newFunnel = addFunnel(group, funnel, options.getComponentIdGenerator());
            additionsBuilder.addFunnel(newFunnel);
        });

        // add any remote process groups
        additions.getRemoteProcessGroups().forEach(remoteProcessGroup -> {
            final RemoteProcessGroup newRemoteProcessGroup = addRemoteProcessGroup(group, remoteProcessGroup, options.getComponentIdGenerator());
            additionsBuilder.addRemoteProcessGroup(newRemoteProcessGroup);
        });

        // add any process groups
        additions.getProcessGroups().forEach(processGroup -> {
            try {
                final ProcessGroup newProcessGroup = addProcessGroup(group, processGroup, options.getComponentIdGenerator(),
                        additions.getParameterContexts(), additions.getParameterProviders(), group);
                additionsBuilder.addProcessGroup(newProcessGroup);
            } catch (final ProcessorInstantiationException pie) {
                throw new RuntimeException(pie);
            }
        });

        // lastly add any connections with all source/destinations already added
        additions.getConnections().forEach(connection -> {
            // null out any instance id's in the connections source/destination since that would be favored
            // when attaching the connection to the appropriate components
            if (connection.getSource() != null) {
                connection.getSource().setInstanceIdentifier(null);
            }
            if (connection.getDestination() != null) {
                connection.getDestination().setInstanceIdentifier(null);
            }

            final Connection newConnection = addConnection(group, connection, options.getComponentIdGenerator());
            additionsBuilder.addConnection(newConnection);
        });

        // update ports to final names
        updatePortsToFinalNames(proposedPortFinalNames);

        for (final CreatedOrModifiedExtension createdOrModifiedExtension : createdAndModifiedExtensions) {
            final ComponentNode extension = createdOrModifiedExtension.extension();
            final Map<String, String> originalPropertyValues = createdOrModifiedExtension.propertyValues();

            final ControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(context.getExtensionManager(), context.getFlowManager(),
                    context.getControllerServiceProvider(), extension);

            if (extension instanceof final ProcessorNode processor) {
                processor.migrateConfiguration(originalPropertyValues, serviceFactory);
            } else if (extension instanceof final ControllerServiceNode service) {
                service.migrateConfiguration(originalPropertyValues, serviceFactory);
            }
        }

        return additionsBuilder.build();
    }

    @Override
    public void synchronize(final ProcessGroup group, final VersionedExternalFlow versionedExternalFlow, final FlowSynchronizationOptions options) {
        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(context.getExtensionManager(), context.getFlowMappingOptions());
        final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(group, context.getControllerServiceProvider(), context.getFlowManager(), true);

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Currently Loaded Flow", versionedGroup);
        final ComparableDataFlow proposedFlow = new StandardComparableDataFlow("Proposed Flow", versionedExternalFlow.getFlowContents());

        final PropertyDecryptor decryptor = options.getPropertyDecryptor();
        final FlowComparator flowComparator = new StandardFlowComparator(localFlow, proposedFlow, group.getAncestorServiceIds(),
            new StaticDifferenceDescriptor(), decryptor::decrypt, options.getComponentComparisonIdLookup(), FlowComparatorVersionedStrategy.DEEP);
        final FlowComparison flowComparison = flowComparator.compare();

        updatedVersionedComponentIds.clear();
        createdAndModifiedExtensions.clear();
        setSynchronizationOptions(options);

        for (final FlowDifference diff : flowComparison.getDifferences()) {
            if (FlowDifferenceFilters.isPropertyMissingFromGhostComponent(diff, context.getFlowManager())) {
                continue;
            }
            if (FlowDifferenceFilters.isScheduledStateNew(diff)) {
                continue;
            }

            // If this update adds a new Controller Service, then we need to check if the service already exists at a higher level
            // and if so compare our VersionedControllerService to the existing service.
            if (diff.getDifferenceType() == DifferenceType.COMPONENT_ADDED) {
                final VersionedComponent component = diff.getComponentA() == null ? diff.getComponentB() : diff.getComponentA();
                if (ComponentType.CONTROLLER_SERVICE == component.getComponentType()) {
                    final ControllerServiceNode serviceNode = getVersionedControllerService(group, component.getIdentifier());
                    if (serviceNode != null) {
                        final VersionedControllerService versionedService = mapper.mapControllerService(serviceNode, context.getControllerServiceProvider(),
                            Collections.singleton(serviceNode.getProcessGroupIdentifier()), new HashMap<>());
                        final Set<FlowDifference> differences = flowComparator.compareControllerServices(versionedService, (VersionedControllerService) component);

                        if (!differences.isEmpty()) {
                            updatedVersionedComponentIds.add(component.getIdentifier());
                        }

                        continue;
                    }
                }
            }

            if (diff.getDifferenceType() == DifferenceType.POSITION_CHANGED) {
                continue;
            }

            final VersionedComponent component = diff.getComponentA() == null ? diff.getComponentB() : diff.getComponentA();
            updatedVersionedComponentIds.add(component.getIdentifier());

            if (component.getComponentType() == ComponentType.REMOTE_INPUT_PORT || component.getComponentType() == ComponentType.REMOTE_OUTPUT_PORT) {
                final String remoteGroupId = ((VersionedRemoteGroupPort) component).getRemoteGroupId();
                updatedVersionedComponentIds.add(remoteGroupId);
            }
        }

        if (LOG.isInfoEnabled()) {
            final Set<FlowDifference> differences = flowComparison.getDifferences();
            if (differences.isEmpty()) {
                LOG.info("No differences between current flow and proposed flow for {}", group);
            } else {
                final String differencesByLine = differences.stream()
                    .map(FlowDifference::toString)
                    .collect(Collectors.joining("\n"));

                LOG.info("Updating {} to {}; there are {} differences to take into account:\n{}", group, versionedExternalFlow,
                    differences.size(), differencesByLine);
            }
        }

        // Pause component scheduling until after all properties have been migrated. This will ensure that we are able to migrate them
        // before enabling any Controller Services or starting any properties.
        context.getComponentScheduler().pause();
        try {
            context.getFlowManager().withParameterContextResolution(() -> {
                try {
                    final Map<String, ParameterProviderReference> parameterProviderReferences = versionedExternalFlow.getParameterProviders() == null
                        ? new HashMap<>() : versionedExternalFlow.getParameterProviders();
                    final ProcessGroup topLevelGroup = syncOptions.getTopLevelGroupId() == null ? group : context.getFlowManager().getGroup(syncOptions.getTopLevelGroupId());
                    synchronize(group, versionedExternalFlow.getFlowContents(), versionedExternalFlow.getParameterContexts(),
                        parameterProviderReferences, topLevelGroup, syncOptions.isUpdateSettings());
                } catch (final ProcessorInstantiationException pie) {
                    throw new RuntimeException(pie);
                }
            });

            for (final CreatedOrModifiedExtension createdOrModifiedExtension : createdAndModifiedExtensions) {
                final ComponentNode extension = createdOrModifiedExtension.extension();
                final Map<String, String> originalPropertyValues = createdOrModifiedExtension.propertyValues();

                final ControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(context.getExtensionManager(), context.getFlowManager(),
                    context.getControllerServiceProvider(), extension);

                if (extension instanceof final ProcessorNode processor) {
                    processor.migrateConfiguration(originalPropertyValues, serviceFactory);
                } else if (extension instanceof final ControllerServiceNode service) {
                    service.migrateConfiguration(originalPropertyValues, serviceFactory);
                } else if (extension instanceof final ReportingTaskNode task) {
                    task.migrateConfiguration(originalPropertyValues, serviceFactory);
                }
            }
        } finally {
            // Resume component scheduler, now that properties have been migrated, so that any components that are intended to be scheduled are.
            context.getComponentScheduler().resume();
        }

        group.onComponentModified();
    }

    private void synchronize(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                             final Map<String, ParameterProviderReference> parameterProviderReferences, final ProcessGroup topLevelGroup, final boolean updateGroupSettings)
        throws ProcessorInstantiationException {

        // Some components, such as Processors, may have a Scheduled State of RUNNING in the proposed flow. However, if we
        // transition the service into the RUNNING state, and then we need to update a Connection that is connected to it,
        // updating the Connection will fail because the Connection's source & destination must both be stopped in order to
        // update it. To avoid that, we simply pause the scheduler. Once all updates have been made, we will resume the scheduler.
        context.getComponentScheduler().pause();

        group.setComments(proposed.getComments());

        if (updateGroupSettings) {
            if (proposed.getName() != null) {
                group.setName(proposed.getName());
            }

            if (proposed.getPosition() != null) {
                group.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
            }
        }

        boolean proposedParameterContextExistsBeforeSynchronize = getParameterContextByName(proposed.getParameterContextName()) != null;

        // Ensure that we create all Parameter Contexts before updating them. This is necessary in case the proposed incoming dataflow has
        // parameter contexts that inherit from one another and neither the inheriting nor inherited parameter context exists.
        if (versionedParameterContexts != null) {
            versionedParameterContexts.values().forEach(this::createParameterContextWithoutReferences);
        }

        updateParameterContext(group, proposed, versionedParameterContexts, parameterProviderReferences, context.getComponentIdGenerator());

        final FlowFileConcurrency flowFileConcurrency = proposed.getFlowFileConcurrency() == null ? FlowFileConcurrency.UNBOUNDED :
            FlowFileConcurrency.valueOf(proposed.getFlowFileConcurrency());
        group.setFlowFileConcurrency(flowFileConcurrency);

        final FlowFileOutboundPolicy outboundPolicy = proposed.getFlowFileOutboundPolicy() == null ? FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE :
            FlowFileOutboundPolicy.valueOf(proposed.getFlowFileOutboundPolicy());
        group.setFlowFileOutboundPolicy(outboundPolicy);

        group.setDefaultFlowFileExpiration(proposed.getDefaultFlowFileExpiration());
        group.setDefaultBackPressureObjectThreshold(proposed.getDefaultBackPressureObjectThreshold());
        group.setDefaultBackPressureDataSizeThreshold(proposed.getDefaultBackPressureDataSizeThreshold());

        if (group.getLogFileSuffix() == null || group.getLogFileSuffix().isEmpty()) {
            group.setLogFileSuffix(proposed.getLogFileSuffix());
        }

        final ExecutionEngine proposedExecutionEngine = proposed.getExecutionEngine();
        if (proposedExecutionEngine != null) {
            group.setExecutionEngine(proposedExecutionEngine);
        }
        final Integer maxConcurrentTasks = proposed.getMaxConcurrentTasks();
        if (maxConcurrentTasks != null) {
            group.setMaxConcurrentTasks(maxConcurrentTasks);
        }
        final String statelessTimeout = proposed.getStatelessFlowTimeout();
        if (statelessTimeout != null) {
            group.setStatelessFlowTimeout(statelessTimeout);
        }
        if (proposed.getScheduledState() != null && ScheduledState.RUNNING.name().equals(proposed.getScheduledState().name()) ) {
            context.getComponentScheduler().startStatelessGroup(group);
        }

        final VersionedFlowCoordinates remoteCoordinates = proposed.getVersionedFlowCoordinates();
        if (remoteCoordinates == null) {
            group.disconnectVersionControl(false);
        } else {
            final String registryId = determineRegistryId(remoteCoordinates);
            final String branch = remoteCoordinates.getBranch();
            final String bucketId = remoteCoordinates.getBucketId();
            final String flowId = remoteCoordinates.getFlowId();
            final String version = remoteCoordinates.getVersion();
            final String storageLocation = remoteCoordinates.getStorageLocation();

            final FlowRegistryClientNode flowRegistry = context.getFlowManager().getFlowRegistryClient(registryId);
            final String registryName = flowRegistry == null ? registryId : flowRegistry.getName();

            final VersionedFlowState flowState;
            if (remoteCoordinates.getLatest() == null) {
                flowState = VersionedFlowState.SYNC_FAILURE;
            } else {
                flowState = remoteCoordinates.getLatest() ? VersionedFlowState.UP_TO_DATE : VersionedFlowState.STALE;
            }

            // only attempt to set the version control information when an applicable registry client could be discovered
            if (registryId != null) {
                final VersionControlInformation vci = new StandardVersionControlInformation.Builder()
                    .registryId(registryId)
                    .registryName(registryName)
                    .branch(branch)
                    .bucketId(bucketId)
                    .bucketName(bucketId)
                    .flowId(flowId)
                    .storageLocation(storageLocation)
                    .flowName(flowId)
                    .version(version)
                    .flowSnapshot(syncOptions.isUpdateGroupVersionControlSnapshot() ? proposed : null)
                    .status(new StandardVersionedFlowStatus(flowState, flowState.getDescription()))
                    .build();

                group.setVersionControlInformation(vci, Collections.emptyMap());
            }
        }

        // In order to properly update all of the components, we have to follow a specific order of operations, in order to ensure that
        // we don't try to perform illegal operations like removing a Processor that has an incoming connection (which would throw an
        // IllegalStateException and fail).
        //
        // The sequence of steps / order of operations are as follows:
        //
        // 1. Remove any Controller Services that do not exist in the proposed group
        // 2. Add any Controller Services that are in the proposed group that are not in the current flow
        // 3. Update Controller Services to match those in the proposed group
        // 4. Remove any connections that do not exist in the proposed group
        // 5. For any connection that does exist, if the proposed group has a different destination for the connection, update the destination.
        //    If the new destination does not yet exist in the flow, set the destination as some temporary component.
        // 6. Remove any other components that do not exist in the proposed group.
        // 7. Add any components, other than Connections, that exist in the proposed group but not in the current flow
        // 8. Update components, other than Connections, to match those in the proposed group
        // 9. Add connections that exist in the proposed group that are not in the current flow
        // 10. Update connections to match those in the proposed group
        // 11. Delete the temporary destination that was created above


        // During the flow update, we will use temporary names for process group ports. This is because port names must be
        // unique within a process group, but during an update we might temporarily be in a state where two ports have the same name.
        // For example, if a process group update involves removing/renaming port A, and then adding/updating port B where B is given
        // A's former name. This is a valid state by the end of the flow update, but for a brief moment there may be two ports with the
        // same name. To avoid this conflict, we keep the final names in a map indexed by port id, use a temporary name for each port
        // during the update, and after all ports have been added/updated/removed, we set the final names on all ports.
        final Map<Port, String> proposedPortFinalNames = new HashMap<>();

        // Controller Services
        final Map<String, ControllerServiceNode> controllerServicesByVersionedId = componentsById(group, grp -> grp.getControllerServices(false),
            ControllerServiceNode::getIdentifier, ControllerServiceNode::getVersionedComponentId);
        removeMissingControllerServices(group, proposed, controllerServicesByVersionedId);
        synchronizeControllerServices(group, proposed, controllerServicesByVersionedId, topLevelGroup);

        // Remove any connections that are not in the Proposed Process Group
        // Connections must be the first thing to remove, not the last. Otherwise, we will fail
        // to remove a component if it has a connection going to it!
        final Map<String, Connection> connectionsByVersionedId = componentsById(group, ProcessGroup::getConnections, Connection::getIdentifier, Connection::getVersionedComponentId);
        removeMissingConnections(group, proposed, connectionsByVersionedId);

        // Before we remove other components, we have to ensure that the Connections have the appropriate destinations. Otherwise, we could have a situation
        // where Connection A used to have a destination of B but now has a destination of C, which doesn't exist yet. And B doesn't exist in the new flow.
        // This is a problem because we cannot remove B, since it has an incoming Connection. And we can't change the destination to C because C hasn't been
        // added yet. As a result, we need a temporary location to set as the Connection's destination. So we create a Funnel for this and then we can update
        // all Connections to have the appropriate destinations.
        final Set<String> connectionsWithTempDestination = updateConnectionDestinations(group, proposed, connectionsByVersionedId);

        try {
            try {
                final Map<String, Funnel> funnelsByVersionedId = componentsById(group, ProcessGroup::getFunnels);
                final Map<String, ProcessorNode> processorsByVersionedId = componentsById(group, ProcessGroup::getProcessors);
                final Map<String, Port> inputPortsByVersionedId = componentsById(group, ProcessGroup::getInputPorts);
                final Map<String, Port> outputPortsByVersionedId = componentsById(group, ProcessGroup::getOutputPorts);
                final Map<String, Label> labelsByVersionedId = componentsById(group, ProcessGroup::getLabels, Label::getIdentifier, Label::getVersionedComponentId);
                final Map<String, RemoteProcessGroup> rpgsByVersionedId = componentsById(group, ProcessGroup::getRemoteProcessGroups,
                    RemoteProcessGroup::getIdentifier, RemoteProcessGroup::getVersionedComponentId);
                final Map<String, ProcessGroup> childGroupsByVersionedId = componentsById(group, ProcessGroup::getProcessGroups, ProcessGroup::getIdentifier, ProcessGroup::getVersionedComponentId);

                removeMissingProcessors(group, proposed, processorsByVersionedId);
                removeMissingFunnels(group, proposed, funnelsByVersionedId);
                removeMissingInputPorts(group, proposed, inputPortsByVersionedId);
                removeMissingOutputPorts(group, proposed, outputPortsByVersionedId);
                removeMissingLabels(group, proposed, labelsByVersionedId);
                removeMissingRpg(group, proposed, rpgsByVersionedId);
                removeMissingChildGroups(group, proposed, childGroupsByVersionedId);

                // Synchronize Child Process Groups
                synchronizeChildGroups(group, proposed, versionedParameterContexts, childGroupsByVersionedId, parameterProviderReferences, topLevelGroup);

                synchronizeFunnels(group, proposed, funnelsByVersionedId);
                synchronizeInputPorts(group, proposed, proposedPortFinalNames, inputPortsByVersionedId);
                synchronizeOutputPorts(group, proposed, proposedPortFinalNames, outputPortsByVersionedId);
                synchronizeLabels(group, proposed, labelsByVersionedId);
                synchronizeProcessors(group, proposed, processorsByVersionedId, topLevelGroup);
                synchronizeRemoteGroups(group, proposed, rpgsByVersionedId);
            } finally {
                // Make sure that we reset the connections
                restoreConnectionDestinations(group, proposed, connectionsByVersionedId, connectionsWithTempDestination);
            }

            final Map<String, Parameter> newParameters = new HashMap<>();
            if (!proposedParameterContextExistsBeforeSynchronize && this.context.getFlowMappingOptions().isMapControllerServiceReferencesToVersionedId()) {
                final Map<String, String> controllerServiceVersionedIdToId = group.getControllerServices(false)
                    .stream()
                    .filter(controllerServiceNode -> controllerServiceNode.getVersionedComponentId().isPresent())
                    .collect(Collectors.toMap(
                        controllerServiceNode -> controllerServiceNode.getVersionedComponentId().get(),
                        ComponentNode::getIdentifier
                    ));

                final ParameterContext parameterContext = group.getParameterContext();

                if (parameterContext != null) {
                    parameterContext.getParameters().forEach((descriptor, parameter) -> {
                        final List<ParameterReferencedControllerServiceData> referencedControllerServiceData = parameterContext
                            .getParameterReferenceManager()
                            .getReferencedControllerServiceData(parameterContext, descriptor.getName());

                        if (referencedControllerServiceData.isEmpty()) {
                            newParameters.put(descriptor.getName(), parameter);
                        } else {
                            final Parameter adjustedParameter = new Parameter.Builder()
                                .fromParameter(parameter)
                                .value(controllerServiceVersionedIdToId.get(parameter.getValue()))
                                .build();

                            newParameters.put(descriptor.getName(), adjustedParameter);
                        }
                    });

                    parameterContext.setParameters(newParameters);
                }
            }

            // We can now add in any necessary connections, since all connectable components have now been created.
            synchronizeConnections(group, proposed, connectionsByVersionedId);

            // All ports have now been added/removed as necessary. We can now resolve the port names.
            updatePortsToFinalNames(proposedPortFinalNames);

            // Start all components that are queued up to be started now
            context.getComponentScheduler().resume();
        } finally {
            // If we created a temporary funnel, remove it if there's no longer anything pointing to it.
            removeTemporaryFunnel(group);
        }
    }

    private String determineRegistryId(final VersionedFlowCoordinates coordinates) {
        final String explicitRegistryId = coordinates.getRegistryId();
        if (explicitRegistryId != null) {
            final FlowRegistryClientNode clientNode = context.getFlowManager().getFlowRegistryClient(explicitRegistryId);
            if (clientNode == null) {
                LOG.debug("Encountered Versioned Flow Coordinates with a Client Registry ID of {} but that Registry ID does not exist. Will check for an applicable Registry Client",
                    explicitRegistryId);
            } else {
                return explicitRegistryId;
            }
        }

        final String location = coordinates.getStorageLocation();
        for (final FlowRegistryClientNode flowRegistryClientNode : context.getFlowManager().getAllFlowRegistryClients()) {
            final boolean locationApplicable;
            try {
                locationApplicable = flowRegistryClientNode.isStorageLocationApplicable(location);
            } catch (final Exception e) {
                LOG.error("Unable to determine if {} is an applicable Flow Registry Client for storage location {}", flowRegistryClientNode, location, e);
                continue;
            }

            if (locationApplicable) {
                LOG.debug("Found Flow Registry Client {} that is applicable for storage location {}", flowRegistryClientNode, location);
                return flowRegistryClientNode.getIdentifier();
            }
        }

        LOG.debug("Found no Flow Registry Client that is applicable for storage location {}; will return explicitly specified Registry ID {}", location, explicitRegistryId);
        return explicitRegistryId;
    }

    private void synchronizeChildGroups(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                        final Map<String, ProcessGroup> childGroupsByVersionedId, final Map<String, ParameterProviderReference> parameterProviderReferences,
                                        final ProcessGroup topLevelGroup) throws ProcessorInstantiationException {

        for (final VersionedProcessGroup proposedChildGroup : proposed.getProcessGroups()) {
            final ProcessGroup childGroup = childGroupsByVersionedId.get(proposedChildGroup.getIdentifier());
            final VersionedFlowCoordinates childCoordinates = proposedChildGroup.getVersionedFlowCoordinates();

            if (childGroup == null) {
                final ProcessGroup added = addProcessGroup(group, proposedChildGroup, context.getComponentIdGenerator(),
                        versionedParameterContexts, parameterProviderReferences, topLevelGroup);
                context.getFlowManager().onProcessGroupAdded(added);
                added.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
                LOG.info("Added {} to {}", added, group);
            } else if (childCoordinates == null || syncOptions.isUpdateDescendantVersionedFlows()) {
                synchronize(childGroup, proposedChildGroup, versionedParameterContexts, parameterProviderReferences, topLevelGroup, true);
                LOG.info("Updated {}", childGroup);
            }
        }
    }

    private void synchronizeControllerServices(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ControllerServiceNode> servicesByVersionedId,
                                               final ProcessGroup topLevelGroup) {
        // Controller Services have to be handled a bit differently than other components. This is because Processors and Controller
        // Services may reference other Controller Services. Since we may be adding Service A, which depends on Service B, before adding
        // Service B, we need to ensure that we create all Controller Services first and then call updateControllerService for each
        // Controller Service. This way, we ensure that all services have been created before setting the properties. This allows us to
        // properly obtain the correct mapping of Controller Service VersionedComponentID to Controller Service instance id.
        final Map<ControllerServiceNode, VersionedControllerService> services = new HashMap<>();

        // Add any Controller Service that does not yet exist.
        final Map<String, ControllerServiceNode> servicesAdded = new HashMap<>();
        for (final VersionedControllerService proposedService : proposed.getControllerServices()) {
            ControllerServiceNode service = servicesByVersionedId.get(proposedService.getIdentifier());
            if (service == null) {
                service = addControllerService(group, proposedService, context.getComponentIdGenerator(), topLevelGroup);

                LOG.info("Added {} to {}", service, group);
                servicesAdded.put(proposedService.getIdentifier(), service);
            }

            services.put(service, proposedService);
        }

        // Because we don't know what order to instantiate the Controller Services, it's possible that we have two services such that Service A references Service B.
        // If Service A happens to get created before Service B, the identifiers won't get matched up. As a result, we now iterate over all created Controller Services
        // and update them again now that all Controller Services have been created at this level, so that the linkage can now be properly established.
        for (final VersionedControllerService proposedService : proposed.getControllerServices()) {
            final ControllerServiceNode addedService = servicesAdded.get(proposedService.getIdentifier());
            if (addedService == null) {
                continue;
            }

            updateControllerService(addedService, proposedService, topLevelGroup);
        }

        // Update all of the Controller Services to match the VersionedControllerService
        for (final Map.Entry<ControllerServiceNode, VersionedControllerService> entry : services.entrySet()) {
            final ControllerServiceNode service = entry.getKey();
            final VersionedControllerService proposedService = entry.getValue();

            if (updatedVersionedComponentIds.contains(proposedService.getIdentifier())) {
                updateControllerService(service, proposedService, topLevelGroup);
                // Any existing component that is modified during synchronization may have its properties reverted to a pre-migration state,
                // so we then add it to the set to allow migrateProperties to be called again to get it back to the migrated state
                createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(service, getPropertyValues(service)));
                LOG.info("Updated {}", service);
            }
        }

        // Determine all Controller Services whose scheduled state indicate they should be enabled.
        final Set<ControllerServiceNode> toEnable = new HashSet<>();
        for (final Map.Entry<ControllerServiceNode, VersionedControllerService> entry : services.entrySet()) {
            if (entry.getValue().getScheduledState() == org.apache.nifi.flow.ScheduledState.ENABLED) {
                toEnable.add(entry.getKey());
            }
        }

        // Perform Validation so we can enable controller services and then enable them
        toEnable.forEach(ComponentNode::performValidation);

        // Enable the services. We have to do this at the end, after creating all of them, in case one service depends on another and
        // therefore is not valid until all have been created.
        toEnable.forEach(service -> {
            if (service.getState() == ControllerServiceState.DISABLED) {
                context.getComponentScheduler().enableControllerServicesAsync(Collections.singleton(service));
            }
        });
    }

    private void removeMissingConnections(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Connection> connectionsByVersionedId) {
        final Set<String> connectionsRemoved = new HashSet<>(connectionsByVersionedId.keySet());
        final Set<String> connectionsRemovedDueToChangingSourceId = new HashSet<>();

        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            connectionsRemoved.remove(proposedConnection.getIdentifier());
        }

        // Check for any case where there's an existing connection whose ID matches the proposed connection, but whose source doesn't match
        // the proposed source ID. The source of a Connection should never change from one component to another. However, there are cases
        // in which the Versioned Component ID might change, in order to avoid conflicts with sibling Process Groups. In such a case, we must remove
        // the connection and create a new one, since we cannot simply change the source in the same way that we can change the destination.
        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            final Connection existingConnection = connectionsByVersionedId.get(proposedConnection.getIdentifier());

            if (existingConnection != null) {
                final String proposedSourceId = proposedConnection.getSource().getId();
                final String existingSourceId = existingConnection.getSource().getVersionedComponentId().orElse(null);

                if (existingSourceId != null && !Objects.equals(proposedSourceId, existingSourceId)) {
                    connectionsRemovedDueToChangingSourceId.add(proposedConnection.getIdentifier());
                    connectionsRemoved.add(proposedConnection.getIdentifier());
                }
            }
        }

        for (final String removedVersionedId : connectionsRemoved) {
            final Connection connection = connectionsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", connection, group);
            group.removeConnection(connection);
        }

        for (final String removedVersionedId : connectionsRemovedDueToChangingSourceId) {
            connectionsByVersionedId.remove(removedVersionedId);
        }
    }

    private void synchronizeConnections(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Connection> connectionsByVersionedId) {
        // Add and update Connections
        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            final Connection connection = connectionsByVersionedId.get(proposedConnection.getIdentifier());
            if (connection == null) {
                final Connection added = addConnection(group, proposedConnection, context.getComponentIdGenerator());
                context.getFlowManager().onConnectionAdded(added);
                LOG.info("Added {} to {}", added, group);
            } else if (isUpdateable(connection)) {
                // If the connection needs to be updated, then the source and destination will already have
                // been stopped (else, the validation above would fail). So if the source or the destination is running,
                // then we know that we don't need to update the connection.
                updateConnection(connection, proposedConnection);
                LOG.info("Updated {}", connection);
            }
        }
    }

    private Set<String> updateConnectionDestinations(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Connection> connectionsByVersionedId) {

        final Set<String> connectionsWithTempDestination = new HashSet<>();
        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            final Connection connection = connectionsByVersionedId.get(proposedConnection.getIdentifier());
            if (connection == null) {
                continue;
            }

            // If the Connection's destination didn't change, and the new is still reachable, nothing to do
            final String destinationVersionId = connection.getDestination().getVersionedComponentId().orElse(null);
            final String proposedDestinationId = proposedConnection.getDestination().getId();

            Connectable newDestination = getConnectable(group, proposedConnection.getDestination());
            final boolean newDestinationReachableFromSource = isConnectionDestinationReachable(connection.getSource(), newDestination);
            if (Objects.equals(destinationVersionId, proposedDestinationId) && newDestinationReachableFromSource) {
                continue;
            }

            // Find the destination of the connection. If the destination doesn't yet exist (because it's part of the proposed Process Group but not yet added),
            // we will set the destination to a temporary destination. Then, after adding components, we will update the destinations again.
            final boolean useTempDestination = isTempDestinationNecessary(connection, proposedConnection, newDestination);
            if (useTempDestination) {
                final Funnel temporaryDestination = getTemporaryFunnel(connection.getProcessGroup());
                LOG.debug("Updated Connection {} to have a temporary destination of {}", connection, temporaryDestination);
                newDestination = temporaryDestination;
                connectionsWithTempDestination.add(proposedConnection.getIdentifier());
            }

            LOG.debug("Changing destination of Connection {} from {} to {}", connection, connection.getDestination(), newDestination);
            connection.setDestination(newDestination);
        }

        return connectionsWithTempDestination;
    }

    /**
     * Checks if a Connection can be made from the given source component to the given destination component
     * @param source the source component
     * @param destination the destination component
     * @return true if the connection is allowable, <code>false</code> if the connection cannot be made due to the Process Group hierarchies
     */
    private boolean isConnectionDestinationReachable(final Connectable source, final Connectable destination) {
        if (source == null || destination == null) {
            return false;
        }

        // If the source is an Output Port, the destination must be in the parent group, unless the destination is the Input Port of another group
        if (source.getConnectableType() == ConnectableType.OUTPUT_PORT) {
            if (destination.getConnectableType() == ConnectableType.INPUT_PORT) {
                return Objects.equals(source.getProcessGroup().getParent(), destination.getProcessGroup().getParent());
            }

            return Objects.equals(source.getProcessGroup().getParent(), destination.getProcessGroup());
        }

        return Objects.equals(source.getProcessGroup(), destination.getProcessGroup());
    }

    private boolean isTempDestinationNecessary(final Connection existingConnection, final VersionedConnection proposedConnection, final Connectable newDestination) {
        if (newDestination == null) {
            LOG.debug("Will use a temporary destination for {} because its destination doesn't yet exist", existingConnection);
            return true;
        }

        // If the destination is an Input Port or an Output Port and the group changed, use a temp destination
        final ConnectableType connectableType = newDestination.getConnectableType();
        final boolean port = connectableType == ConnectableType.OUTPUT_PORT || connectableType == ConnectableType.INPUT_PORT;
        final boolean groupChanged = !newDestination.getProcessGroup().equals(existingConnection.getDestination().getProcessGroup());
        if (port && groupChanged) {
            LOG.debug("Will use a temporary destination for {} because its destination is a port whose group has changed", existingConnection);
            return true;
        }

        // If the proposed destination has a different group than the existing group, use a temp destination.
        final String proposedDestinationGroupId = proposedConnection.getDestination().getGroupId();
        final String destinationGroupVersionedComponentId = getVersionedId(existingConnection.getDestination().getProcessGroup());
        if (!Objects.equals(proposedDestinationGroupId, destinationGroupVersionedComponentId)) {
            LOG.debug("Will use a temporary destination for {} because its destination has a different group than the existing group. " +
                      "Existing group ID is [{}] (instance ID of [{}]); proposed is [{}]",
                existingConnection, destinationGroupVersionedComponentId, existingConnection.getProcessGroup().getIdentifier(), proposedDestinationGroupId);
            return true;
        }

        // If the proposed connection exists in a different group than the existing group, use a temp destination.
        final String connectionGroupVersionedComponentId = getVersionedId(existingConnection.getProcessGroup());
        final String proposedGroupId = proposedConnection.getGroupIdentifier();
        if (!Objects.equals(proposedGroupId, connectionGroupVersionedComponentId)) {
            LOG.debug("Will use a temporary destination for {} because it has a different group than the existing group. Existing group ID is [{}]; proposed is [{}]",
                existingConnection, connectionGroupVersionedComponentId, proposedGroupId);
            return true;
        }

        return false;
    }

    private String getVersionedId(final ProcessGroup processGroup) {
        return getVersionedId(processGroup.getIdentifier(), processGroup.getVersionedComponentId().orElse(null));
    }

    /**
     * Determines the Versioned Component ID to use for a component by first using the Versioned ID if it is already available. Otherwise,
     * use the Component ID Lookup to determine the Versioned ID based on the Instance ID. This allows us to ensure that when we sync the dataflow,
     * we use the same approach to mapping as we will use when we write out the dataflow.
     *
     * @return the Versioned Component ID to use for the component
     */
    private String getVersionedId(final String instanceId, final String versionedId) {
        if (versionedId != null) {
            return versionedId;
        }

        return this.context.getFlowMappingOptions().getComponentIdLookup().getComponentId(Optional.empty(), instanceId);
    }

    private Funnel getTemporaryFunnel(final ProcessGroup group) {
        final String tempFunnelId = group.getIdentifier() + TEMP_FUNNEL_ID_SUFFIX;
        Funnel temporaryFunnel = context.getFlowManager().getFunnel(tempFunnelId);
        if (temporaryFunnel == null) {
            temporaryFunnel = context.getFlowManager().createFunnel(tempFunnelId);
            temporaryFunnel.setPosition(new Position(0, 0));
            group.addFunnel(temporaryFunnel, false);
        }

        return temporaryFunnel;
    }

    private void restoreConnectionDestinations(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Connection> connectionsByVersionedId,
                                               final Set<String> connectionsWithTempDestination) {
        if (connectionsWithTempDestination.isEmpty()) {
            LOG.debug("No connections with temporary destinations for {}", group);
            return;
        }

        final Map<String, VersionedConnection> versionedConnectionsById = proposed.getConnections().stream()
            .collect(Collectors.toMap(VersionedConnection::getIdentifier, Function.identity()));

        for (final String connectionId : connectionsWithTempDestination) {
            final Connection connection = connectionsByVersionedId.get(connectionId);
            final VersionedConnection versionedConnection = versionedConnectionsById.get(connectionId);

            final Connectable newDestination = getConnectable(group, versionedConnection.getDestination());
            if (newDestination != null) {
                LOG.debug("Updated Connection {} from its temporary destination to its correct destination of {}", connection, newDestination);
                connection.setDestination(newDestination);
            }
        }
    }

    private void removeTemporaryFunnel(final ProcessGroup group) {
        final String tempFunnelId = group.getIdentifier() + TEMP_FUNNEL_ID_SUFFIX;
        final Funnel temporaryFunnel = context.getFlowManager().getFunnel(tempFunnelId);
        if (temporaryFunnel == null) {
            LOG.debug("No temporary funnel to remove for {}", group);
            return;
        }

        if (temporaryFunnel.getIncomingConnections().isEmpty()) {
            LOG.debug("Updated all temporary connections for {}. Removing Temporary funnel from flow", group);
            group.removeFunnel(temporaryFunnel);
        } else {
            LOG.warn("The temporary funnel {} for {} still has {} connections. It cannot be removed.", temporaryFunnel, group, temporaryFunnel.getIncomingConnections().size());
        }
    }

    private <T extends Connectable> Map<String, T> componentsById(final ProcessGroup group, final Function<ProcessGroup, Collection<T>> retrieveComponents) {
        return retrieveComponents.apply(group).stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
    }

    private <T> Map<String, T> componentsById(final ProcessGroup group, final Function<ProcessGroup, Collection<T>> retrieveComponents,
                                              final Function<T, String> retrieveId, final Function<T, Optional<String>> retrieveVersionedComponentId) {

        return retrieveComponents.apply(group).stream()
            .collect(Collectors.toMap(component -> retrieveVersionedComponentId.apply(component).orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(retrieveId.apply(component))), Function.identity()));
    }

    private void synchronizeFunnels(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Funnel> funnelsByVersionedId) {
        for (final VersionedFunnel proposedFunnel : proposed.getFunnels()) {
            final Funnel funnel = funnelsByVersionedId.get(proposedFunnel.getIdentifier());
            if (funnel == null) {
                final Funnel added = addFunnel(group, proposedFunnel, context.getComponentIdGenerator());
                context.getFlowManager().onFunnelAdded(added);
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedFunnel.getIdentifier())) {
                updateFunnel(funnel, proposedFunnel);
                LOG.info("Updated {}", funnel);
            } else {
                funnel.setPosition(new Position(proposedFunnel.getPosition().getX(), proposedFunnel.getPosition().getY()));
            }
        }
    }

    private void synchronizeInputPorts(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<Port, String> proposedPortFinalNames,
                                       final Map<String, Port> inputPortsByVersionedId) {
        for (final VersionedPort proposedPort : proposed.getInputPorts()) {
            final Port port = inputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                final Port added = addInputPort(group, proposedPort, context.getComponentIdGenerator(), temporaryName);
                proposedPortFinalNames.put(added, proposedPort.getName());
                context.getFlowManager().onInputPortAdded(added);
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                proposedPortFinalNames.put(port, proposedPort.getName());
                updatePort(port, proposedPort, temporaryName);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }
        }
    }

    private void synchronizeOutputPorts(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<Port, String> proposedPortFinalNames,
                                        final Map<String, Port> outputPortsByVersionedId) {

        for (final VersionedPort proposedPort : proposed.getOutputPorts()) {
            final Port port = outputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                final Port added = addOutputPort(group, proposedPort, context.getComponentIdGenerator(), temporaryName);
                proposedPortFinalNames.put(added, proposedPort.getName());
                context.getFlowManager().onOutputPortAdded(added);
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                proposedPortFinalNames.put(port, proposedPort.getName());
                updatePort(port, proposedPort, temporaryName);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }
        }
    }

    private void updatePortsToFinalNames(final Map<Port, String> proposedPortFinalNames) {
        // Now that all input/output ports have been removed, we should be able to update
        // all ports to the final name that was proposed in the new flow version.
        for (final Map.Entry<Port, String> portAndFinalName : proposedPortFinalNames.entrySet()) {
            final Port port = portAndFinalName.getKey();
            final String finalName = portAndFinalName.getValue();
            LOG.info("Updating {} to replace temporary name with final name", port);

            // For public ports we need to consider if another public port exists somewhere else in the flow with the
            // same name, and if so then rename the incoming port so the flow can still be imported
            if (port instanceof PublicPort) {
                final PublicPort publicPort = (PublicPort) port;
                final String publicPortFinalName = getPublicPortFinalName(publicPort, finalName);
                updatePortToSetFinalName(publicPort, publicPortFinalName);
            } else {
                updatePortToSetFinalName(port, finalName);
            }
        }
    }

    private void synchronizeLabels(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Label> labelsByVersionedId) {
        for (final VersionedLabel proposedLabel : proposed.getLabels()) {
            final Label label = labelsByVersionedId.get(proposedLabel.getIdentifier());
            if (label == null) {
                final Label added = addLabel(group, proposedLabel, context.getComponentIdGenerator());
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedLabel.getIdentifier())) {
                updateLabel(label, proposedLabel);
                LOG.info("Updated {}", label);
            } else {
                label.setPosition(new Position(proposedLabel.getPosition().getX(), proposedLabel.getPosition().getY()));
            }
        }
    }

    private void removeMissingProcessors(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ProcessorNode> processorsByVersionedId) {
        removeMissingComponents(group, proposed, processorsByVersionedId, VersionedProcessGroup::getProcessors, ProcessGroup::removeProcessor);
    }

    private void removeMissingInputPorts(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Port> portsByVersionedId) {
        removeMissingComponents(group, proposed, portsByVersionedId, VersionedProcessGroup::getInputPorts, ProcessGroup::removeInputPort);
    }

    private void removeMissingOutputPorts(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Port> portsByVersionedId) {
        removeMissingComponents(group, proposed, portsByVersionedId, VersionedProcessGroup::getOutputPorts, ProcessGroup::removeOutputPort);
    }

    private void removeMissingLabels(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Label> labelsByVersionedId) {
        removeMissingComponents(group, proposed, labelsByVersionedId, VersionedProcessGroup::getLabels, ProcessGroup::removeLabel);
    }

    private void removeMissingFunnels(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, Funnel> funnelsByVersionedId) {
        removeMissingComponents(group, proposed, funnelsByVersionedId, VersionedProcessGroup::getFunnels, (removalGroup, funnelToRemove) -> {
            // Skip our temporary funnel
            if (funnelToRemove.getIdentifier().equals(removalGroup.getIdentifier() + TEMP_FUNNEL_ID_SUFFIX)) {
                return;
            }

            removalGroup.removeFunnel(funnelToRemove);
        });
    }

    private void removeMissingRpg(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, RemoteProcessGroup> rpgsByVersionedId) {
        removeMissingComponents(group, proposed, rpgsByVersionedId, VersionedProcessGroup::getRemoteProcessGroups, ProcessGroup::removeRemoteProcessGroup);
    }

    private void removeMissingControllerServices(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ControllerServiceNode> servicesByVersionedId) {
        final BiConsumer<ProcessGroup, ControllerServiceNode> componentRemoval = (grp, service) -> context.getControllerServiceProvider().removeControllerService(service);
        removeMissingComponents(group, proposed, servicesByVersionedId, VersionedProcessGroup::getControllerServices, componentRemoval);
    }

    private void removeMissingChildGroups(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ProcessGroup> groupsByVersionedId) {
        removeMissingComponents(group, proposed, groupsByVersionedId, VersionedProcessGroup::getProcessGroups,
            (procGroup, childGroup) -> {
                // We cannot remove a Process Group unless it is empty. At this point, we've already removed
                // all Processors, Input Ports, etc. that are no longer needed. However, we have not removed all
                // Process Groups. We may have a situation where we have nested Process Groups, each one consisting
                // now of only other Process Groups that can be removed, such as A -> B -> C -> D.
                // Each of these is a Process Group that contains only other (otherwise empty) process groups.
                // To accomplish this, we need to use a depth-first approach, removing the inner-most group (D),
                // then C, then B, and finally A.
                if (!childGroup.isEmpty()) {
                    purgeChildGroupOfEmptyChildren(childGroup);
                }

                procGroup.removeProcessGroup(childGroup);
            });
    }

    private void purgeChildGroupOfEmptyChildren(final ProcessGroup group) {
        for (final ProcessGroup child : group.getProcessGroups()) {
            purgeChildGroupOfEmptyChildren(child);

            if (child.isEmpty()) {
                group.removeProcessGroup(child);
            }
        }
    }

    private <C, V extends VersionedComponent> void removeMissingComponents(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, C> componentsById,
                                                                           final Function<VersionedProcessGroup, Collection<V>> getVersionedComponents,
                                                                           final BiConsumer<ProcessGroup, C> removeComponent) {

        // Determine the ID's of the components to remove. To do this, we get the ID's of all components in the Process Group,
        // and then remove from that the ID's of the components in the proposed group. That leaves us with the ID's of components
        // that exist currently that are not in the proposed flow.
        final Set<String> idsOfComponentsToRemove = new HashSet<>(componentsById.keySet());
        for (final V versionedComponent : getVersionedComponents.apply(proposed)) {
            idsOfComponentsToRemove.remove(versionedComponent.getIdentifier());
        }

        // Remove any of those components
        for (final String idToRemove : idsOfComponentsToRemove) {
            final C toRemove = componentsById.get(idToRemove);
            LOG.info("Removing {} from {}", toRemove, group);
            removeComponent.accept(group, toRemove);
        }
    }

    private void synchronizeProcessors(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ProcessorNode> processorsByVersionedId,
                                       final ProcessGroup topLevelGroup)
                throws ProcessorInstantiationException {

        for (final VersionedProcessor proposedProcessor : proposed.getProcessors()) {
            final ProcessorNode processor = processorsByVersionedId.get(proposedProcessor.getIdentifier());
            if (processor == null) {
                final ProcessorNode added = addProcessor(group, proposedProcessor, context.getComponentIdGenerator(), topLevelGroup);
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedProcessor.getIdentifier())) {
                updateProcessor(processor, proposedProcessor, topLevelGroup);
                // Any existing component that is modified during synchronization may have its properties reverted to a pre-migration state,
                // so we then add it to the set to allow migrateProperties to be called again to get it back to the migrated state
                createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(processor, getPropertyValues(processor)));
                LOG.info("Updated {}", processor);
            } else {
                processor.setPosition(new Position(proposedProcessor.getPosition().getX(), proposedProcessor.getPosition().getY()));
            }
        }
    }

    private void synchronizeRemoteGroups(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, RemoteProcessGroup> rpgsByVersionedId) {
        for (final VersionedRemoteProcessGroup proposedRpg : proposed.getRemoteProcessGroups()) {
            final RemoteProcessGroup rpg = rpgsByVersionedId.get(proposedRpg.getIdentifier());
            if (rpg == null) {
                final RemoteProcessGroup added = addRemoteProcessGroup(group, proposedRpg, context.getComponentIdGenerator());
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedRpg.getIdentifier())) {
                updateRemoteProcessGroup(rpg, proposedRpg, context.getComponentIdGenerator());
                LOG.info("Updated {}", rpg);
            } else {
                rpg.setPosition(new Position(proposedRpg.getPosition().getX(), proposedRpg.getPosition().getY()));
            }
        }
    }

    @Override
    public void verifyCanAddVersionedComponents(final ProcessGroup group, final VersionedComponentAdditions additions) {
        verifyCanInstantiateProcessors(group, additions.getProcessors(), additions.getProcessGroups());
        verifyCanInstantiateControllerServices(group, additions.getControllerServices(), additions.getProcessGroups());
        verifyCanInstantiateConnections(group, additions.getConnections(), additions.getProcessGroups());
    }

    @Override
    public void verifyCanSynchronize(final ProcessGroup group, final VersionedProcessGroup flowContents, final boolean verifyConnectionRemoval) {
        // Optionally check that no deleted connections contain data in their queue.
        // Note that this check enforces ancestry among the group components to avoid a scenario where
        // a component is matched by id, but it does not exist in the same hierarchy and thus will be removed and
        // re-added when the update is performed
        verifyCanRemoveMissingComponents(group, flowContents, verifyConnectionRemoval);

        // Determine which input ports were removed from this process group
        final Map<String, Port> removedInputPortsByVersionId = new HashMap<>();
        group.getInputPorts()
            .forEach(port -> removedInputPortsByVersionId.put(port.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(port.getIdentifier())), port));

        flowContents.getInputPorts().stream()
            .map(VersionedPort::getIdentifier)
            .forEach(removedInputPortsByVersionId::remove);

        // Ensure that there are no incoming connections for any Input Port that was removed.
        for (final Port inputPort : removedInputPortsByVersionId.values()) {
            final List<Connection> incomingConnections = inputPort.getIncomingConnections();
            if (!incomingConnections.isEmpty()) {
                throw new IllegalStateException(group + " cannot be updated to the proposed flow because the proposed flow "
                    + "does not contain the Input Port " + inputPort + " and the Input Port currently has an incoming connection");
            }
        }

        // Determine which output ports were removed from this process group
        final Map<String, Port> removedOutputPortsByVersionId = new HashMap<>();
        group.getOutputPorts()
            .forEach(port -> removedOutputPortsByVersionId.put(port.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(port.getIdentifier())), port));

        flowContents.getOutputPorts().stream()
            .map(VersionedPort::getIdentifier)
            .forEach(removedOutputPortsByVersionId::remove);

        // Ensure that there are no outgoing connections for any Output Port that was removed.
        for (final Port outputPort : removedOutputPortsByVersionId.values()) {
            final Set<Connection> outgoingConnections = outputPort.getConnections();
            if (!outgoingConnections.isEmpty()) {
                throw new IllegalStateException(group + " cannot be updated to the proposed flow because the proposed flow "
                    + "does not contain the Output Port " + outputPort + " and the Output Port currently has an outgoing connection");
            }
        }

        verifyCanInstantiateProcessors(group, flowContents.getProcessors(), flowContents.getProcessGroups());
        verifyCanInstantiateControllerServices(group, flowContents.getControllerServices(), flowContents.getProcessGroups());
        verifyCanInstantiateConnections(group, flowContents.getConnections(), flowContents.getProcessGroups());
    }

    private void verifyCanInstantiateProcessors(final ProcessGroup group, final Set<VersionedProcessor> processors, final Set<VersionedProcessGroup> childGroups) {
        // Ensure that all Processors are instantiable
        final Map<String, VersionedProcessor> proposedProcessors = new HashMap<>();
        findAllProcessors(processors, childGroups, proposedProcessors);

        group.findAllProcessors()
                .forEach(proc -> proposedProcessors.remove(proc.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(proc.getIdentifier()))));

        for (final VersionedProcessor processorToAdd : proposedProcessors.values()) {
            final String processorToAddClass = processorToAdd.getType();
            final BundleCoordinate processorToAddCoordinate = toCoordinate(processorToAdd.getBundle());

            // Get the exact bundle requested, if it exists.
            final Bundle bundle = processorToAdd.getBundle();
            final BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            final org.apache.nifi.bundle.Bundle resolved = context.getExtensionManager().getBundle(coordinate);

            if (resolved == null) {
                // Could not resolve the bundle explicitly. Check for possible bundles.
                final List<org.apache.nifi.bundle.Bundle> possibleBundles = context.getExtensionManager().getBundles(processorToAddClass);
                final boolean bundleExists = possibleBundles.stream()
                        .anyMatch(b -> processorToAddCoordinate.equals(b.getBundleDetails().getCoordinate()));

                if (!bundleExists && possibleBundles.size() != 1) {
                    LOG.warn("Unknown bundle {} for processor type {} - will use Ghosted component instead", processorToAddCoordinate, processorToAddClass);
                }
            }
        }
    }

    private void verifyCanInstantiateControllerServices(final ProcessGroup group, final Set<VersionedControllerService> controllerServices, final Set<VersionedProcessGroup> childGroups) {
        // Ensure that all Controller Services are instantiable
        final Map<String, VersionedControllerService> proposedServices = new HashMap<>();
        findAllControllerServices(controllerServices, childGroups, proposedServices);

        group.findAllControllerServices()
                .forEach(service -> proposedServices.remove(service.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(service.getIdentifier()))));

        for (final VersionedControllerService serviceToAdd : proposedServices.values()) {
            final String serviceToAddClass = serviceToAdd.getType();
            final BundleCoordinate serviceToAddCoordinate = toCoordinate(serviceToAdd.getBundle());

            final org.apache.nifi.bundle.Bundle resolved = context.getExtensionManager().getBundle(serviceToAddCoordinate);
            if (resolved == null) {
                final List<org.apache.nifi.bundle.Bundle> possibleBundles = context.getExtensionManager().getBundles(serviceToAddClass);
                final boolean bundleExists = possibleBundles.stream()
                        .anyMatch(b -> serviceToAddCoordinate.equals(b.getBundleDetails().getCoordinate()));

                if (!bundleExists && possibleBundles.size() != 1) {
                    LOG.warn("Unknown bundle {} for processor type {} - will use Ghosted component instead", serviceToAddCoordinate, serviceToAddClass);
                }
            }
        }
    }

    private void verifyCanInstantiateConnections(final ProcessGroup group, final Set<VersionedConnection> connections, final Set<VersionedProcessGroup> childGroups) {
        // Ensure that all Prioritizers are instantiable and that any load balancing configuration is correct
        // Enforcing ancestry on connection matching here is not important because all we're interested in is locating
        // new prioritizers and load balance strategy types so if a matching connection existed anywhere in the current
        // flow, then its prioritizer and load balance strategy are already validated
        final Map<String, VersionedConnection> proposedConnections = new HashMap<>();
        findAllConnections(connections, childGroups, proposedConnections);

        group.findAllConnections()
                .forEach(conn -> proposedConnections.remove(conn.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(conn.getIdentifier()))));

        for (final VersionedConnection connectionToAdd : proposedConnections.values()) {
            if (connectionToAdd.getPrioritizers() != null) {
                for (final String prioritizerType : connectionToAdd.getPrioritizers()) {
                    try {
                        context.getFlowManager().createPrioritizer(prioritizerType);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unable to create Prioritizer of type " + prioritizerType, e);
                    }
                }
            }

            final String loadBalanceStrategyName = connectionToAdd.getLoadBalanceStrategy();
            if (loadBalanceStrategyName != null) {
                try {
                    LoadBalanceStrategy.valueOf(loadBalanceStrategyName);
                } catch (final IllegalArgumentException iae) {
                    throw new IllegalArgumentException("Unable to create Connection with Load Balance Strategy of '" + loadBalanceStrategyName
                            + "' because this is not a known Load Balance Strategy");
                }
            }
        }
    }

    private ProcessGroup addProcessGroup(final ProcessGroup destination, final VersionedProcessGroup proposed, final ComponentIdGenerator componentIdGenerator,
                                         final Map<String, VersionedParameterContext> versionedParameterContexts,
                                         final Map<String, ParameterProviderReference> parameterProviderReferences, ProcessGroup topLevelGroup) throws ProcessorInstantiationException {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final ProcessGroup group = context.getFlowManager().createProcessGroup(id);
        group.setVersionedComponentId(proposed.getIdentifier());
        group.setParent(destination);
        group.setName(proposed.getName());

        destination.addProcessGroup(group);

        synchronize(group, proposed, versionedParameterContexts, parameterProviderReferences, topLevelGroup, true);

        return group;
    }

    private ControllerServiceNode addControllerService(final ProcessGroup destination, final VersionedControllerService proposed, final ComponentIdGenerator componentIdGenerator,
                                                       final ProcessGroup topLevelGroup) {
        final String destinationId = destination == null ? "Controller" : destination.getIdentifier();
        final String identifier = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destinationId);
        LOG.debug("Adding Controller Service with ID {} of type {}", identifier, proposed.getType());

        final BundleCoordinate coordinate = toCoordinate(proposed.getBundle());
        final Set<URL> additionalUrls = Collections.emptySet();
        final ControllerServiceNode newService = context.getFlowManager().createControllerService(proposed.getType(), identifier, coordinate, additionalUrls, true, true, null);
        newService.setVersionedComponentId(proposed.getIdentifier());

        if (destination == null) {
            context.getFlowManager().addRootControllerService(newService);
        } else {
            destination.addControllerService(newService);
        }

        final Map<String, String> decryptedProperties = getDecryptedProperties(proposed.getProperties());
        createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(newService, decryptedProperties));

        updateControllerService(newService, proposed, topLevelGroup);

        return newService;
    }

    private void verifyCanSynchronize(final ControllerServiceNode controllerService, final VersionedControllerService proposed) {
        // If service is null, we can always synchronize by creating the proposed service.
        if (controllerService == null) {
            return;
        }

        // Ensure that service is in a state that it can be removed.
        if (proposed == null) {
            controllerService.verifyCanDelete();
            return;
        }

        // Verify service can be updated
        controllerService.verifyCanUpdate();
    }

    @Override
    public void synchronize(final ControllerServiceNode controllerService, final VersionedControllerService proposed, final ProcessGroup group,
                            final FlowSynchronizationOptions synchronizationOptions) throws FlowSynchronizationException, TimeoutException, InterruptedException {
        if (controllerService == null && proposed == null) {
            return;
        }

        setSynchronizationOptions(synchronizationOptions);

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();
        final ControllerServiceProvider serviceProvider = context.getControllerServiceProvider();

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // Disable the controller service, if necessary, in order to update it.
            final Set<ComponentNode> referencesToRestart = new HashSet<>();
            final Set<ControllerServiceNode> servicesToRestart = new HashSet<>();

            try {
                stopControllerService(controllerService, proposed, timeout, synchronizationOptions.getComponentStopTimeoutAction(),
                        referencesToRestart, servicesToRestart, synchronizationOptions);
                verifyCanSynchronize(controllerService, proposed);

                try {
                    final ProcessGroup topLevelGroup = synchronizationOptions.getTopLevelGroupId() != null ? context.getFlowManager().getGroup(synchronizationOptions.getTopLevelGroupId()) : group;

                    if (proposed == null) {
                        serviceProvider.removeControllerService(controllerService);
                        LOG.info("Successfully synchronized {} by removing it from the flow", controllerService);
                    } else if (controllerService == null) {
                        final ControllerServiceNode added = addControllerService(group, proposed, synchronizationOptions.getComponentIdGenerator(), topLevelGroup);

                        if (proposed.getScheduledState() == org.apache.nifi.flow.ScheduledState.ENABLED) {
                            servicesToRestart.add(added);
                        }

                        LOG.info("Successfully synchronized {} by adding it to the flow", added);
                    } else {
                        updateControllerService(controllerService, proposed, topLevelGroup);

                        if (proposed.getScheduledState() == org.apache.nifi.flow.ScheduledState.ENABLED) {
                            servicesToRestart.add(controllerService);
                        }

                        LOG.info("Successfully synchronized {} by updating it to match proposed version", controllerService);
                    }
                } catch (final Exception e) {
                    throw new FlowSynchronizationException("Failed to synchronize Controller Service " + controllerService + " with proposed version", e);
                }
            } finally {
                // If the intent was to remove the Controller Service, or to disable it, then anything that was previously referencing it should remain stopped.
                // However, if the intended state for the Controller Service is to be ENABLED, go ahead and re-enable/restart what we've stopped/disabled.
                if (proposed != null && proposed.getScheduledState() != org.apache.nifi.flow.ScheduledState.DISABLED) {
                    // Re-enable the controller service if necessary
                    serviceProvider.enableControllerServicesAsync(servicesToRestart);
                    notifyScheduledStateChange(servicesToRestart, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);

                    // Restart any components that need to be restarted.
                    if (controllerService != null) {
                        serviceProvider.scheduleReferencingComponents(controllerService, referencesToRestart, context.getComponentScheduler());
                        referencesToRestart.forEach(componentNode -> notifyScheduledStateChange(componentNode, synchronizationOptions, org.apache.nifi.flow.ScheduledState.RUNNING));
                    }
                }
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private void waitForStopCompletion(final Future<?> future, final Object component, final long timeout, final FlowSynchronizationOptions.ComponentStopTimeoutAction timeoutAction)
        throws InterruptedException, FlowSynchronizationException, TimeoutException {
        try {
            future.get(timeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedException("Interrupted while waiting for " + component + " to stop/disable");
        } catch (final ExecutionException ee) {
            throw new FlowSynchronizationException("Failed to stop/disable " + component, ee.getCause());
        } catch (final TimeoutException e) {
            // On timeout, if action is to terminate and the component is a processor, terminate it.
            if (component instanceof ProcessorNode) {
                switch (timeoutAction) {
                    case THROW_TIMEOUT_EXCEPTION:
                        throw e;
                    case TERMINATE:
                    default:
                        ((ProcessorNode) component).terminate();
                        return;
                }
            }

            throw new TimeoutException("Timed out waiting for " + component + " to stop/disable");
        }
    }

    private void updateControllerService(final ControllerServiceNode service, final VersionedControllerService proposed, final ProcessGroup topLevelGroup) {
        LOG.debug("Updating {}", service);

        service.pauseValidationTrigger();
        try {
            service.setAnnotationData(proposed.getAnnotationData());
            service.setComments(proposed.getComments());
            service.setName(proposed.getName());

            if (proposed.getBulletinLevel() != null) {
                service.setBulletinLevel(LogLevel.valueOf(proposed.getBulletinLevel()));
            } else {
                // this situation exists for backward compatibility with nifi 1.16 and earlier where controller services do not have bulletinLevels set in flow.xml/flow.json
                // and bulletinLevels are at the WARN level by default
                service.setBulletinLevel(LogLevel.WARN);
            }

            if (!isEqual(service.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(service.getRawPropertyValues().keySet());
                final Set<URL> additionalUrls = service.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(service, proposed.getType(), newBundleCoordinate, additionalUrls);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(service, proposed.getProperties(), proposed.getPropertyDescriptors().values());
            final Map<String, String> properties = populatePropertiesMap(service, proposed.getProperties(), proposed.getPropertyDescriptors(), service.getProcessGroup(), topLevelGroup);
            service.setProperties(properties, true, sensitiveDynamicPropertyNames);

        } finally {
            service.resumeValidationTrigger();
        }
    }

    private Set<String> getSensitiveDynamicPropertyNames(
            final ComponentNode componentNode,
            final Map<String, String> proposedProperties,
            final Collection<VersionedPropertyDescriptor> proposedDescriptors
    ) {
        final Set<String> sensitiveDynamicPropertyNames = new LinkedHashSet<>();

        // Find sensitive dynamic property names using proposed Versioned Property Descriptors
        proposedDescriptors.stream()
                .filter(VersionedPropertyDescriptor::isSensitive)
                .map(VersionedPropertyDescriptor::getName)
                .map(componentNode::getPropertyDescriptor)
                .filter(PropertyDescriptor::isDynamic)
                .map(PropertyDescriptor::getName)
                .forEach(sensitiveDynamicPropertyNames::add);

        // Find Encrypted Property values and find associated dynamic Property Descriptor names
        proposedProperties.entrySet()
                .stream()
                .filter(entry -> isValueEncrypted(entry.getValue()))
                .map(Map.Entry::getKey)
                .map(componentNode::getPropertyDescriptor)
                .filter(PropertyDescriptor::isDynamic)
                .map(PropertyDescriptor::getName)
                .forEach(sensitiveDynamicPropertyNames::add);

        return sensitiveDynamicPropertyNames;
    }

    private Map<String, String> populatePropertiesMap(final ComponentNode componentNode, final Map<String, String> proposedProperties,
                                                      final Map<String, VersionedPropertyDescriptor> proposedPropertyDescriptors,
                                                      final ProcessGroup group, final ProcessGroup topLevelGroup) {

        // Explicitly set all existing properties to null, except for sensitive properties, so that if there isn't an entry in the proposedProperties
        // it will get removed from the processor. We don't do this for sensitive properties because when we retrieve the VersionedProcessGroup from registry,
        // any sensitive properties will already have been removed, and we don't want to clear those values, or else we'd always clear sensitive values.
        final Map<String, String> fullPropertyMap = new HashMap<>();
        for (final PropertyDescriptor property : componentNode.getRawPropertyValues().keySet()) {
            if (!property.isSensitive()) {
                fullPropertyMap.put(property.getName(), null);
            }
        }

        if (proposedProperties != null) {
            // Build a Set of all properties that are included in either the currently configured property values or the proposed values.
            final Set<String> updatedPropertyNames = new HashSet<>(proposedProperties.keySet());
            componentNode.getProperties().keySet().stream()
                .map(PropertyDescriptor::getName)
                .forEach(updatedPropertyNames::add);

            for (final String propertyName : updatedPropertyNames) {
                final PropertyDescriptor descriptor = componentNode.getPropertyDescriptor(propertyName);
                final VersionedPropertyDescriptor versionedDescriptor = (proposedPropertyDescriptors == null) ? null : proposedPropertyDescriptors.get(propertyName);
                final boolean referencesService = (descriptor != null && descriptor.getControllerServiceDefinition() != null)
                    || (versionedDescriptor != null && versionedDescriptor.getIdentifiesControllerService());
                final boolean sensitive = (descriptor != null && descriptor.isSensitive())
                    || (versionedDescriptor != null && versionedDescriptor.isSensitive());

                final String value;
                if (descriptor != null && referencesService && (proposedProperties.get(propertyName) != null)) {
                    // Need to determine if the component's property descriptor for this service is already set to an id
                    // of an existing service that is outside the current processor group, and if it is we want to leave
                    // the property set to that value
                    String existingExternalServiceId = null;
                    final String componentDescriptorValue = componentNode.getEffectivePropertyValue(descriptor);
                    if (componentDescriptorValue != null) {
                        final ProcessGroup parentGroup = topLevelGroup.getParent();
                        if (parentGroup != null) {
                            final ControllerServiceNode serviceNode = parentGroup.findControllerService(componentDescriptorValue, false, true);
                            if (serviceNode != null) {
                                existingExternalServiceId = componentDescriptorValue;
                            }
                        }
                    }

                    // If the component's property descriptor is not already set to an id of an existing external service,
                    // then we need to take the Versioned Component ID and resolve this to the instance ID of the service
                    if (existingExternalServiceId == null) {
                        final String serviceVersionedComponentId = proposedProperties.get(propertyName);
                        String instanceId = getServiceInstanceId(serviceVersionedComponentId, group);
                        value = (instanceId == null) ? serviceVersionedComponentId : instanceId;

                        // Find the same property descriptor in the component's CreatedExtension and replace it with the
                        // instance ID of the service
                        createdAndModifiedExtensions.stream().filter(ce -> ce.extension.equals(componentNode)).forEach(createdOrModifiedExtension -> {
                            createdOrModifiedExtension.propertyValues.replace(propertyName, value);
                        });
                    } else {
                        value = existingExternalServiceId;
                    }
                } else {
                    value = proposedProperties.get(propertyName);
                }

                // skip any sensitive properties that are not populated so we can retain whatever is currently set. We do this because sensitive properties are not stored in the registry
                // unless the value is a reference to a Parameter. If the value in the registry is null, it indicates that the sensitive value was removed, so we want to keep the currently
                // populated value. The exception to this rule is if the currently configured value is a Parameter Reference and the Versioned Flow is empty. In this case, it implies
                // that the Versioned Flow has changed from a Parameter Reference to an explicit value. In this case, we do in fact want to change the value of the Sensitive Property from
                // the current parameter reference to an unset value.
                if (sensitive && value == null) {
                    final PropertyConfiguration propertyConfiguration = componentNode.getProperty(descriptor);
                    if (propertyConfiguration == null) {
                        continue;
                    }

                    // No parameter references. Property currently is set to an explicit value. We don't want to change it.
                    if (propertyConfiguration.getParameterReferences().isEmpty()) {
                        continue;
                    }

                    // Once we reach this point, the property is configured to reference a Parameter, and the value in the Versioned Flow is an explicit value,
                    // so we want to continue on and update the value to null.
                }

                fullPropertyMap.put(propertyName, decrypt(value, syncOptions.getPropertyDecryptor()));
            }
        }

        return fullPropertyMap;
    }

    private Map<String, String> getDecryptedProperties(final Map<String, String> properties) {
        final Map<String, String> decryptedProperties = new LinkedHashMap<>();

        final PropertyDecryptor decryptor = syncOptions.getPropertyDecryptor();
        properties.forEach((propertyName, propertyValue) -> {
            final String propertyValueDecrypted = decrypt(propertyValue, decryptor);
            decryptedProperties.put(propertyName, propertyValueDecrypted);
        });

        return decryptedProperties;
    }

    private static String decrypt(final String value, final PropertyDecryptor decryptor) {
        if (isValueEncrypted(value)) {
            try {
                return decryptor.decrypt(value.substring(ENC_PREFIX.length(), value.length() - ENC_SUFFIX.length()));
            } catch (EncryptionException e) {
                final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                        "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.json.gz file";
                throw new EncryptionException(moreDescriptiveMessage, e);
            }
        } else {
            return value;
        }
    }

    private static boolean isValueEncrypted(final String value) {
        return value != null && value.startsWith(ENC_PREFIX) && value.endsWith(ENC_SUFFIX);
    }

    private void verifyCanSynchronize(final ParameterContext parameterContext, final VersionedParameterContext proposed) throws FlowSynchronizationException {
        // Make sure that we have a unique name and add the Parameter Context if none exists
        if (parameterContext == null) {
            final ParameterContext existingContext = getParameterContextByName(proposed.getName());
            if (existingContext != null) {
                throw new FlowSynchronizationException("Cannot synchronize flow with proposed Parameter Context because a Parameter Context already exists with the name " + proposed.getName());
            }
        }

        // If deleting, must ensure that no other parameter contexts inherit from this one.
        if (proposed == null) {
            verifyNotInherited(parameterContext.getIdentifier());
        }

        if (parameterContext != null && proposed != null) {
            // Check that the parameters have appropriate sensitivity flag
            for (final VersionedParameter versionedParameter : proposed.getParameters()) {
                final Optional<Parameter> optionalParameter = parameterContext.getParameter(versionedParameter.getName());
                if (optionalParameter.isPresent()) {
                    final boolean paramSensitive = optionalParameter.get().getDescriptor().isSensitive();
                    if (paramSensitive != versionedParameter.isSensitive()) {
                        throw new FlowSynchronizationException("Cannot synchronize flow with proposed Parameter Context because the Parameter [" + versionedParameter.getName() + "] in " +
                            parameterContext + " has a sensitivity flag of " + paramSensitive + " while the proposed version has a sensitivity flag of " + versionedParameter.isSensitive());
                    }
                }
            }

            // Check that parameter contexts to inherit exist
            final List<String> inheritedContexts = proposed.getInheritedParameterContexts();
            if (inheritedContexts != null) {
                for (final String contextName : inheritedContexts) {
                    final ParameterContext existing = getParameterContextByName(contextName);
                    if (existing == null) {
                        throw new FlowSynchronizationException("Cannot synchronize flow with proposed Parameter Context because proposed version inherits from Parameter Context with name " +
                            contextName + " but there is no Parameter Context with that name in the current flow");
                    }
                }
            }
        }
    }

    @Override
    public void synchronize(final ParameterContext parameterContext, final VersionedParameterContext proposed, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (parameterContext == null && proposed == null) {
            return;
        }

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();
        verifyCanSynchronize(parameterContext, proposed);

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // Make sure that we have a unique name and add the Parameter Context if none exists
            if (parameterContext == null) {
                final String contextId = synchronizationOptions.getComponentIdGenerator().generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), "Controller");
                final ParameterContext added = createParameterContext(proposed, contextId, Collections.emptyMap(), Collections.emptyMap(), synchronizationOptions.getComponentIdGenerator());
                LOG.info("Successfully synchronized {} by adding it to the flow", added);
                return;
            }

            final ParameterReferenceManager referenceManager = parameterContext.getParameterReferenceManager();
            final Set<String> updatedParameterNames = getUpdatedParameterNames(parameterContext, proposed);

            final Set<ComponentNode> componentsToRestart = new HashSet<>();
            final Set<ControllerServiceNode> servicesToRestart = new HashSet<>();
            try {
                // Stop components necessary
                for (final String paramName : updatedParameterNames) {
                    final Set<ProcessorNode> processors = referenceManager.getProcessorsReferencing(parameterContext, paramName);
                    componentsToRestart.addAll(stopOrTerminate(processors, timeout, synchronizationOptions));

                    final Set<ControllerServiceNode> referencingServices = referenceManager.getControllerServicesReferencing(parameterContext, paramName);

                    for (final ControllerServiceNode referencingService : referencingServices) {
                        final boolean isServiceActive = referencingService.isActive();
                        stopControllerService(referencingService, null, timeout, synchronizationOptions.getComponentStopTimeoutAction(), componentsToRestart, servicesToRestart,
                                synchronizationOptions);
                        if (isServiceActive) {
                            servicesToRestart.add(referencingService);
                        }
                    }
                }

                // Remove or update parameter context.
                final ParameterContextManager contextManager = context.getFlowManager().getParameterContextManager();
                if (proposed == null) {
                    for (final ProcessGroup groupBound : referenceManager.getProcessGroupsBound(parameterContext)) {
                        groupBound.setParameterContext(null);
                    }

                    contextManager.removeParameterContext(parameterContext.getIdentifier());
                    LOG.info("Successfully synchronized {} by removing it from the flow", parameterContext);
                } else {
                    final Map<String, Parameter> updatedParameters = createParameterMap(proposed.getParameters());

                    // If any parameters are removed, need to add a null value to the map in order to make sure that the parameter is removed.
                    for (final ParameterDescriptor existingParameterDescriptor : parameterContext.getParameters().keySet()) {
                        final String name = existingParameterDescriptor.getName();
                        if (!updatedParameters.containsKey(name)) {
                            updatedParameters.put(name, null);
                        }
                    }

                    final Map<String, ParameterContext> contextsByName = contextManager.getParameterContextNameMapping();
                    final List<ParameterContext> inheritedContexts = new ArrayList<>();
                    final List<String> inheritedContextNames = proposed.getInheritedParameterContexts();
                    if (inheritedContextNames != null) {
                        for (final String inheritedContextName : inheritedContextNames) {
                            final ParameterContext inheritedContext = contextsByName.get(inheritedContextName);
                            inheritedContexts.add(inheritedContext);
                        }
                    }

                    parameterContext.setParameters(updatedParameters);
                    parameterContext.setName(proposed.getName());
                    parameterContext.setDescription(proposed.getDescription());
                    parameterContext.setInheritedParameterContexts(inheritedContexts);
                    LOG.info("Successfully synchronized {} by updating it to match the proposed version", parameterContext);
                }
            } finally {
                // TODO: How will this behave if Controller Service was changed to DISABLING but then timed out waiting for it to disable?
                //       In that case, I think this will fail to enable the controller services, and as a result it will remain DISABLED.
                //       We probably want to update the logic here so that it marks a desired state of ENABLED and when the service finally transitions
                //       to DISABLED we enable it.
                context.getControllerServiceProvider().enableControllerServicesAsync(servicesToRestart);

                // We don't use ControllerServiceProvider.scheduleReferencingComponents here, as we do when dealing with a Controller Service
                // because if we timeout while waiting for a Controller Service to stop, then that Controller Service won't be in our list of Controller Services
                // to re-enable. As a result, we don't have the appropriate Controller Service to pass to the scheduleReferencingComponents.
                for (final ComponentNode stoppedComponent : componentsToRestart) {
                    if (stoppedComponent instanceof Connectable) {
                        context.getComponentScheduler().startComponent((Connectable) stoppedComponent);
                        notifyScheduledStateChange(stoppedComponent, synchronizationOptions, org.apache.nifi.flow.ScheduledState.RUNNING);
                    }
                }
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private void collectValueAndReferences(final ParameterContext parameterContext, final Map<String, ParameterValueAndReferences> valueAndRef) {
        parameterContext.getEffectiveParameters()
                .forEach((pd, param) -> valueAndRef.put(pd.getName(), getValueAndReferences(param)));
    }

    protected Set<String> getUpdatedParameterNames(final ParameterContext parameterContext, final VersionedParameterContext proposed) {
        final Map<String, ParameterValueAndReferences> originalValues = new HashMap<>();
        collectValueAndReferences(parameterContext, originalValues);
        parameterContext.getEffectiveParameters().forEach((pd, param) -> originalValues.put(pd.getName(), getValueAndReferences(param)));

        final Map<String, ParameterValueAndReferences> proposedValues = new HashMap<>();
        if (proposed != null) {
            if (proposed.getInheritedParameterContexts() != null) {
                for (int i = proposed.getInheritedParameterContexts().size() - 1; i >= 0; i--) {
                    final String name = proposed.getInheritedParameterContexts().get(i);
                    final ParameterContext inheritedContext = getParameterContextByName(name);
                    if (inheritedContext != null) {
                        collectValueAndReferences(inheritedContext, proposedValues);
                        inheritedContext.getEffectiveParameters().forEach((pd, param) -> proposedValues.put(pd.getName(), getValueAndReferences(param)));
                    }
                }
            }
            proposed.getParameters().forEach(versionedParam -> proposedValues.put(versionedParam.getName(), getValueAndReferences(versionedParam)));
        }

        final Map<String, ParameterValueAndReferences> copyOfOriginalValues = new HashMap<>(originalValues);
        proposedValues.forEach(originalValues::remove);
        copyOfOriginalValues.forEach(proposedValues::remove);

        final Set<String> updatedParameterNames = new HashSet<>(originalValues.keySet());
        updatedParameterNames.addAll(proposedValues.keySet());

        return updatedParameterNames;
    }

    private ParameterValueAndReferences getValueAndReferences(final Parameter parameter) {
        final List<Asset> assets = parameter.getReferencedAssets();
        if (assets == null || assets.isEmpty()) {
            return new ParameterValueAndReferences(parameter.getValue(), null);
        }
        final List<String> assetIds = assets.stream().map(Asset::getIdentifier).toList();
        return new ParameterValueAndReferences(null, assetIds);
    }

    private ParameterValueAndReferences getValueAndReferences(final VersionedParameter parameter) {
        final List<VersionedAsset> assets = parameter.getReferencedAssets();
        if (assets == null || assets.isEmpty()) {
            return new ParameterValueAndReferences(parameter.getValue(), null);
        }
        final List<String> assetIds = assets.stream().map(VersionedAsset::getIdentifier).toList();
        return new ParameterValueAndReferences(null, assetIds);
    }

    @Override
    public void synchronizeProcessGroupSettings(final ProcessGroup processGroup, final VersionedProcessGroup proposed, final ProcessGroup parentGroup,
                                                final FlowSynchronizationOptions synchronizationOptions)
                    throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (processGroup == null && proposed == null) {
            return;
        }

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // Check if we need to delete the Process Group
            if (proposed == null) {
                // Ensure that there are no incoming connections
                processGroup.getInputPorts().forEach(Port::verifyCanDelete);

                // Bleed out the data by stopping all input ports and source processors, then waiting
                // for all connections to become empty
                bleedOut(processGroup, timeout, synchronizationOptions);

                processGroup.stopProcessing();
                waitFor(timeout, () -> isDoneProcessing(processGroup));

                // Disable all Controller Services
                final Collection<ControllerServiceNode> controllerServices = processGroup.findAllControllerServices();
                final Future<Void> disableServicesFuture = context.getControllerServiceProvider().disableControllerServicesAsync(controllerServices);
                notifyScheduledStateChange(controllerServices, synchronizationOptions, org.apache.nifi.flow.ScheduledState.DISABLED);
                try {
                    disableServicesFuture.get(timeout, TimeUnit.MILLISECONDS);
                } catch (final ExecutionException ee) {
                    throw new FlowSynchronizationException("Could not synchronize flow with proposal due to: failed to disable Controller Services", ee.getCause());
                }

                // Remove the group
                processGroup.getParent().removeProcessGroup(processGroup);

                LOG.info("Successfully synchronized {} by removing it from the flow", processGroup);
                return;
            }

            // Create the Process Group if it doesn't exist
            final ProcessGroup groupToUpdate;
            if (processGroup == null) {
                final String groupId = synchronizationOptions.getComponentIdGenerator().generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), parentGroup.getIdentifier());
                final ProcessGroup group = context.getFlowManager().createProcessGroup(groupId);
                group.setVersionedComponentId(proposed.getIdentifier());
                group.setParent(parentGroup);
                group.setName(proposed.getName());

                parentGroup.addProcessGroup(group);
                groupToUpdate = group;
            } else {
                groupToUpdate = processGroup;
            }

            // Ensure that the referenced Parameter Context is valid
            final ParameterContext parameterContext = proposed.getParameterContextName() == null ? null :
                context.getFlowManager().getParameterContextManager().getParameterContextNameMapping().get(proposed.getParameterContextName());

            if (parameterContext == null && proposed.getParameterContextName() != null) {
                throw new FlowSynchronizationException("Cannot synchronize flow with proposed version because proposal indicates that Process Group " + groupToUpdate +
                    " should use Parameter Context with name [" + proposed.getParameterContextName() + "] but no Parameter Context exists with that name");
            }

            // Determine which components must be stopped/disabled based on Parameter Context name changing
            final Set<ProcessorNode> processorsToStop = new HashSet<>();
            final Set<ControllerServiceNode> controllerServicesToStop = new HashSet<>();
            final String currentParameterContextName = groupToUpdate.getParameterContext() == null ? null : groupToUpdate.getParameterContext().getName();
            if (!Objects.equals(currentParameterContextName, proposed.getParameterContextName())) {
                groupToUpdate.getProcessors().stream()
                    .filter(ProcessorNode::isRunning)
                    .filter(ProcessorNode::isReferencingParameter)
                    .forEach(processorsToStop::add);

                final Set<ControllerServiceNode> servicesReferencingParams = groupToUpdate.getControllerServices(false).stream()
                    .filter(ControllerServiceNode::isReferencingParameter)
                    .collect(Collectors.toSet());

                for (final ControllerServiceNode service : servicesReferencingParams) {
                    if (!service.isActive()) {
                        continue;
                    }

                    controllerServicesToStop.add(service);

                    for (final ControllerServiceNode referencingService : service.getReferences().findRecursiveReferences(ControllerServiceNode.class)) {
                        if (!referencingService.isActive()) {
                            continue;
                        }

                        controllerServicesToStop.add(referencingService);
                    }
                }

                for (final ControllerServiceNode service : controllerServicesToStop) {
                    service.getReferences().findRecursiveReferences(ProcessorNode.class).stream()
                        .filter(ProcessorNode::isRunning)
                        .forEach(processorsToStop::add);
                }
            }

            try {
                // Stop all necessary running processors
                stopOrTerminate(processorsToStop, timeout, synchronizationOptions);

                // Stop all necessary enabled/active Controller Services
                final Future<Void> serviceDisableFuture = context.getControllerServiceProvider().disableControllerServicesAsync(controllerServicesToStop);
                notifyScheduledStateChange(controllerServicesToStop, synchronizationOptions, org.apache.nifi.flow.ScheduledState.DISABLED);
                try {
                    serviceDisableFuture.get(timeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    throw new FlowSynchronizationException("Failed to disable Controller Services necessary in order to perform update of Process Group", e);
                }

                // Update the Process Group
                groupToUpdate.setDefaultBackPressureDataSizeThreshold(proposed.getDefaultBackPressureDataSizeThreshold());
                groupToUpdate.setDefaultBackPressureObjectThreshold(proposed.getDefaultBackPressureObjectThreshold());
                groupToUpdate.setDefaultFlowFileExpiration(proposed.getDefaultFlowFileExpiration());
                groupToUpdate.setFlowFileConcurrency(proposed.getFlowFileConcurrency() == null ? FlowFileConcurrency.UNBOUNDED : FlowFileConcurrency.valueOf(proposed.getFlowFileConcurrency()));
                groupToUpdate.setFlowFileOutboundPolicy(proposed.getFlowFileOutboundPolicy() == null ? FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE :
                    FlowFileOutboundPolicy.valueOf(proposed.getFlowFileOutboundPolicy()));
                groupToUpdate.setParameterContext(parameterContext);
                groupToUpdate.setComments(proposed.getComments());
                groupToUpdate.setName(proposed.getName());
                groupToUpdate.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
                groupToUpdate.setLogFileSuffix(proposed.getLogFileSuffix());

                if (processGroup == null) {
                    LOG.info("Successfully synchronized {} by adding it to the flow", groupToUpdate);
                } else {
                    LOG.info("Successfully synchronized {} by updating it to match proposed version", groupToUpdate);
                }
            } finally {
                // Re-enable all Controller Services that we disabled and restart all processors
                context.getControllerServiceProvider().enableControllerServicesAsync(controllerServicesToStop);
                notifyScheduledStateChange(controllerServicesToStop, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);

                for (final ProcessorNode processor : processorsToStop) {
                    processor.getProcessGroup().startProcessor(processor, false);
                    notifyScheduledStateChange((ComponentNode) processor, synchronizationOptions, org.apache.nifi.flow.ScheduledState.RUNNING);
                }
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private boolean isDoneProcessing(final ProcessGroup group) {
        for (final ProcessorNode processor : group.getProcessors()) {
            if (processor.isRunning()) {
                return false;
            }
        }

        for (final Port port : group.getInputPorts()) {
            if (port.isRunning()) {
                return false;
            }
        }

        for (final Port port : group.getOutputPorts()) {
            if (port.isRunning()) {
                return false;
            }
        }

        for (final RemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            for (final RemoteGroupPort port : rpg.getInputPorts()) {
                if (port.isRunning()) {
                    return false;
                }
            }

            for (final RemoteGroupPort port : rpg.getOutputPorts()) {
                if (port.isRunning()) {
                    return false;
                }
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            if (!isDoneProcessing(childGroup)) {
                return false;
            }
        }

        return true;
    }

    private void bleedOut(final ProcessGroup processGroup, final long timeout, final FlowSynchronizationOptions synchronizationOptions)
                throws FlowSynchronizationException, TimeoutException, InterruptedException {
        processGroup.getInputPorts().forEach(processGroup::stopInputPort);

        final Set<ProcessorNode> sourceProcessors = processGroup.findAllProcessors().stream()
            .filter(this::isSourceProcessor)
            .collect(Collectors.toSet());

        stopOrTerminate(sourceProcessors, timeout, synchronizationOptions);

        final List<Connection> connections = processGroup.findAllConnections();
        waitFor(timeout, () -> connectionsEmpty(connections));
    }

    private void waitFor(final long timeout, final BooleanSupplier condition) throws InterruptedException {
        while (System.currentTimeMillis() <= timeout && !condition.getAsBoolean()) {
            Thread.sleep(10L);
        }
    }

    private boolean connectionsEmpty(final Collection<Connection> connections) {
        for (final Connection connection : connections) {
            if (!connection.getFlowFileQueue().isEmpty()) {
                return false;
            }
        }

        return true;
    }

    private boolean isSourceProcessor(final ProcessorNode processor) {
        return processor.getIncomingConnections().stream()
            .anyMatch(connection -> connection.getSource() != processor);
    }


    private void verifyNotInherited(final String parameterContextId) {
        for (final ParameterContext parameterContext : context.getFlowManager().getParameterContextManager().getParameterContexts()) {
            if (parameterContext.getInheritedParameterContexts().stream().anyMatch(pc -> pc.getIdentifier().equals(parameterContextId))) {
                throw new IllegalStateException(String.format("Cannot delete Parameter Context with ID [%s] because it is referenced by at least one Parameter Context [%s]",
                    parameterContextId, parameterContext.getIdentifier()));
            }
        }
    }

    private void updateParameterContext(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                        final Map<String, ParameterProviderReference> parameterProviderReferences, final ComponentIdGenerator componentIdGenerator) {
        // Update the Parameter Context
        final ParameterContext currentParamContext = group.getParameterContext();
        final String proposedParameterContextName = proposed.getParameterContextName();
        if (proposedParameterContextName == null && currentParamContext != null) {
            group.setParameterContext(null);
        } else if (proposedParameterContextName != null) {
            final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(proposedParameterContextName);
            if (versionedParameterContext != null) {
                createMissingParameterProvider(versionedParameterContext, versionedParameterContext.getParameterProvider(), parameterProviderReferences, componentIdGenerator);
                if (currentParamContext == null) {
                    // Create a new Parameter Context based on the parameters provided
                    final ParameterContext contextByName = getParameterContextByName(versionedParameterContext.getName());
                    final ParameterContext selectedParameterContext;
                    if (contextByName == null) {
                        final String parameterContextId = componentIdGenerator.generateUuid(versionedParameterContext.getName(),
                                versionedParameterContext.getName(), versionedParameterContext.getName());
                        selectedParameterContext = createParameterContext(versionedParameterContext, parameterContextId, versionedParameterContexts,
                                parameterProviderReferences, componentIdGenerator);
                    } else {
                        selectedParameterContext = contextByName;
                        addMissingConfiguration(versionedParameterContext, selectedParameterContext, versionedParameterContexts, parameterProviderReferences, componentIdGenerator);
                    }

                    group.setParameterContext(selectedParameterContext);
                } else {
                    // Update the current Parameter Context so that it has any Parameters included in the proposed context
                    addMissingConfiguration(versionedParameterContext, currentParamContext, versionedParameterContexts, parameterProviderReferences, componentIdGenerator);
                }
            }
        }
    }

    private void createMissingParameterProvider(final VersionedParameterContext versionedParameterContext, final String parameterProviderId,
                                                final Map<String, ParameterProviderReference> parameterProviderReferences, final ComponentIdGenerator componentIdGenerator) {
        String parameterProviderIdToSet = parameterProviderId;
        if (parameterProviderId != null) {
            ParameterProviderNode parameterProviderNode = context.getFlowManager().getParameterProvider(parameterProviderId);
            if (parameterProviderNode == null) {
                final ParameterProviderReference reference = parameterProviderReferences.get(parameterProviderId);
                if (reference == null) {
                    parameterProviderIdToSet = null;
                } else {
                    parameterProviderNode = context.getFlowManager().getParameterProvider(reference.getIdentifier());
                    if (parameterProviderNode != null) {
                        parameterProviderIdToSet = reference.getIdentifier();
                    } else {
                        final String newParameterProviderId = componentIdGenerator.generateUuid(parameterProviderId, parameterProviderId, null);

                        final Bundle bundle = reference.getBundle();
                        parameterProviderNode = context.getFlowManager().createParameterProvider(reference.getType(), newParameterProviderId,
                                new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion()), true);

                        parameterProviderNode.pauseValidationTrigger(); // avoid triggering validation multiple times
                        parameterProviderNode.setName(reference.getName());
                        parameterProviderNode.resumeValidationTrigger();
                        parameterProviderIdToSet = parameterProviderNode.getIdentifier();

                        // Set the reference id to the new id so it can be picked up by other contexts referencing the same provider
                        reference.setIdentifier(parameterProviderIdToSet);
                        parameterProviderReferences.put(parameterProviderIdToSet, reference);
                    }
                }
            }
        }
        versionedParameterContext.setParameterProvider(parameterProviderIdToSet);
    }

    private String getPublicPortFinalName(final PublicPort publicPort, final String proposedFinalName) {
        final Optional<Port> existingPublicPort;
        if (TransferDirection.RECEIVE == publicPort.getDirection()) {
            existingPublicPort = context.getFlowManager().getPublicInputPort(proposedFinalName);
        } else {
            existingPublicPort = context.getFlowManager().getPublicOutputPort(proposedFinalName);
        }

        if (existingPublicPort.isPresent() && !existingPublicPort.get().getIdentifier().equals(publicPort.getIdentifier())) {
            return getPublicPortFinalName(publicPort, "Copy of " + proposedFinalName);
        } else {
            return proposedFinalName;
        }
    }

    private ParameterContext getParameterContextByName(final String contextName) {
        return context.getFlowManager().getParameterContextManager().getParameterContextNameMapping().get(contextName);
    }

    private ParameterContext createParameterContextWithoutReferences(final VersionedParameterContext versionedParameterContext) {
        final ParameterContext existing = context.getFlowManager().getParameterContextManager().getParameterContextNameMapping().get(versionedParameterContext.getName());
        if (existing != null) {
            return existing;
        }

        final ComponentIdGenerator componentIdGenerator = this.syncOptions.getComponentIdGenerator();
        final String parameterContextId = componentIdGenerator.generateUuid(versionedParameterContext.getName(), versionedParameterContext.getName(), versionedParameterContext.getName());

        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versionedParameter : versionedParameterContext.getParameters()) {
            if (versionedParameter == null) {
                continue;
            }

            final Parameter parameter = createParameter(null, versionedParameter);
            parameters.put(versionedParameter.getName(), parameter);
        }

        return context.getFlowManager().createParameterContext(parameterContextId, versionedParameterContext.getName(), versionedParameterContext.getDescription(),
                                                               parameters, Collections.emptyList(), null);
    }

    private ParameterProviderConfiguration getParameterProviderConfiguration(final VersionedParameterContext context) {
        return context.getParameterProvider() == null ? null
                : new StandardParameterProviderConfiguration(context.getParameterProvider(), context.getParameterGroupName(), context.isSynchronized());
    }

    private ParameterContext createParameterContext(final VersionedParameterContext versionedParameterContext, final String parameterContextId,
                                                    final Map<String, VersionedParameterContext> versionedParameterContexts,
                                                    final Map<String, ParameterProviderReference> parameterProviderReferences, final ComponentIdGenerator componentIdGenerator) {

        final Map<String, Parameter> parameters = createParameterMap(versionedParameterContext.getParameters());

        final List<String> parameterContextRefs = new ArrayList<>();
        if (versionedParameterContext.getInheritedParameterContexts() != null) {
            versionedParameterContext.getInheritedParameterContexts().stream()
                .map(name -> createParameterReferenceId(name, versionedParameterContexts, parameterProviderReferences, componentIdGenerator))
                .forEach(parameterContextRefs::add);
        }

        final AtomicReference<ParameterContext> contextReference = new AtomicReference<>();
        context.getFlowManager().withParameterContextResolution(() -> {
            final ParameterContext created = context.getFlowManager().createParameterContext(parameterContextId, versionedParameterContext.getName(),
                    versionedParameterContext.getDescription(), parameters, parameterContextRefs, getParameterProviderConfiguration(versionedParameterContext));

            contextReference.set(created);
        });

        return contextReference.get();
    }

    private Map<String, Parameter> createParameterMap(final Collection<VersionedParameter> versionedParameters) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versionedParameter : versionedParameters) {
            final Parameter parameter = createParameter(null, versionedParameter);
            parameters.put(versionedParameter.getName(), parameter);
        }

        return parameters;
    }

    private String createParameterReferenceId(final String parameterContextName, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                              final Map<String, ParameterProviderReference> parameterProviderReferences, final ComponentIdGenerator componentIdGenerator) {
        final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(parameterContextName);
        final ParameterContext selectedParameterContext = selectParameterContext(versionedParameterContext, versionedParameterContexts, parameterProviderReferences, componentIdGenerator);
        return selectedParameterContext.getIdentifier();
    }

    private ParameterContext selectParameterContext(final VersionedParameterContext versionedParameterContext,
                                                    final Map<String, VersionedParameterContext> versionedParameterContexts,
                                                    final Map<String, ParameterProviderReference> parameterProviderReferences,
                                                    final ComponentIdGenerator componentIdGenerator) {
        final ParameterContext contextByName = getParameterContextByName(versionedParameterContext.getName());
        final ParameterContext selectedParameterContext;
        if (contextByName == null) {
            final String parameterContextId = context.getFlowMappingOptions().getComponentIdLookup().getComponentId(Optional.ofNullable(versionedParameterContext.getIdentifier()),
                versionedParameterContext.getInstanceIdentifier());
            selectedParameterContext = createParameterContext(versionedParameterContext, parameterContextId, versionedParameterContexts, parameterProviderReferences, componentIdGenerator);
        } else {
            selectedParameterContext = contextByName;
            addMissingConfiguration(versionedParameterContext, selectedParameterContext, versionedParameterContexts, parameterProviderReferences, componentIdGenerator);
        }

        return selectedParameterContext;
    }

    private void addMissingConfiguration(final VersionedParameterContext versionedParameterContext, final ParameterContext currentParameterContext,
                                         final Map<String, VersionedParameterContext> versionedParameterContexts,
                                         final Map<String, ParameterProviderReference> parameterProviderReferences,
                                         final ComponentIdGenerator componentIdGenerator) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versionedParameter : versionedParameterContext.getParameters()) {
            final Optional<Parameter> parameterOption = currentParameterContext.getParameter(versionedParameter.getName());
            if (parameterOption.isPresent()) {
                // Skip this parameter, since it is already defined. We only want to add missing parameters
                continue;
            }

            final Parameter parameter = createParameter(currentParameterContext.getIdentifier(), versionedParameter);
            parameters.put(versionedParameter.getName(), parameter);
        }

        currentParameterContext.setParameters(parameters);

        // If the current parameter context doesn't have any inherited param contexts but the versioned one does,
        // add the versioned ones.
        if (versionedParameterContext.getInheritedParameterContexts() != null && !versionedParameterContext.getInheritedParameterContexts().isEmpty()
            && currentParameterContext.getInheritedParameterContexts().isEmpty()) {
            currentParameterContext.setInheritedParameterContexts(versionedParameterContext.getInheritedParameterContexts().stream()
                .map(name -> selectParameterContext(versionedParameterContexts.get(name), versionedParameterContexts, parameterProviderReferences, componentIdGenerator))
                .collect(Collectors.toList()));
        }

        if (versionedParameterContext.getParameterProvider() != null && currentParameterContext.getParameterProvider() == null) {
            createMissingParameterProvider(versionedParameterContext, versionedParameterContext.getParameterProvider(), parameterProviderReferences, componentIdGenerator);
            currentParameterContext.configureParameterProvider(getParameterProviderConfiguration(versionedParameterContext));
        }
    }

    private Parameter createParameter(final String contextId, final VersionedParameter versionedParameter) {
        final List<VersionedAsset> referencedAssets = versionedParameter.getReferencedAssets();

        final List<Asset> assets;
        if (referencedAssets == null || referencedAssets.isEmpty()) {
            assets = null;
        } else {
            final AssetManager assetManager = context.getAssetManager();
            assets = new ArrayList<>();
            for (final VersionedAsset reference : referencedAssets) {
                final Optional<Asset> assetOption = assetManager.getAsset(reference.getIdentifier());
                final Asset asset = assetOption.orElseGet(() -> assetManager.createMissingAsset(contextId, reference.getName()));
                assets.add(asset);
            }
        }

        return new Parameter.Builder()
            .name(versionedParameter.getName())
            .description(versionedParameter.getDescription())
            .sensitive(versionedParameter.isSensitive())
            .value(versionedParameter.getValue())
            .referencedAssets(assets)
            .provided(versionedParameter.isProvided())
            .parameterContextId(contextId)
            .build();
    }

    private boolean isEqual(final BundleCoordinate coordinate, final Bundle bundle) {
        if (!bundle.getGroup().equals(coordinate.getGroup())) {
            return false;
        }

        if (!bundle.getArtifact().equals(coordinate.getId())) {
            return false;
        }

        return bundle.getVersion().equals(coordinate.getVersion());
    }

    private BundleCoordinate toCoordinate(final Bundle bundle) {
        return new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }

    @Override
    public void synchronize(final Funnel funnel, final VersionedFunnel proposed, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (funnel == null && proposed == null) {
            return;
        }

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();

        if (proposed == null) {
            verifyCanDelete(funnel, timeout);
        } else if (funnel != null) {
            funnel.verifyCanUpdate();
        }

        final Set<Connectable> toRestart = new HashSet<>();
        try {
            if (proposed == null) {
                final Set<Connectable> stoppedDownstream = stopDownstreamComponents(funnel, timeout, synchronizationOptions);
                toRestart.addAll(stoppedDownstream);

                funnel.getProcessGroup().removeFunnel(funnel);
                LOG.info("Successfully synchronized {} by removing it from the flow", funnel);
            } else if (funnel == null) {
                final Funnel added = addFunnel(group, proposed, synchronizationOptions.getComponentIdGenerator());
                LOG.info("Successfully synchronized {} by adding it to the flow", added);
            } else {
                updateFunnel(funnel, proposed);
                LOG.info("Successfully synchronized {} by updating it to match proposed version", funnel);
            }
        } finally {
            // Restart any components that need to be restarted.
            startComponents(toRestart, synchronizationOptions);
        }
    }

    @Override
    public void synchronize(final Label label, final VersionedLabel proposed, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions) {
        if (label == null && proposed == null) {
            return;
        }

        if (proposed == null) {
            label.getProcessGroup().removeLabel(label);
        } else if (label == null) {
            addLabel(group, proposed, synchronizationOptions.getComponentIdGenerator());
        } else {
            updateLabel(label, proposed);
        }
    }

    private void updateFunnel(final Funnel funnel, final VersionedFunnel proposed) {
        funnel.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
    }

    private Funnel addFunnel(final ProcessGroup destination, final VersionedFunnel proposed, final ComponentIdGenerator componentIdGenerator) {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final Funnel funnel = context.getFlowManager().createFunnel(id);
        funnel.setVersionedComponentId(proposed.getIdentifier());
        destination.addFunnel(funnel);
        updateFunnel(funnel, proposed);
        connectableAdditionTracker.addComponent(destination.getIdentifier(), proposed.getIdentifier(), funnel);

        return funnel;
    }

    private boolean isUpdateable(final Connection connection) {
        final Optional<String> versionIdOptional = connection.getVersionedComponentId();
        if (versionIdOptional.isPresent() && !updatedVersionedComponentIds.contains(versionIdOptional.get())) {
            return false;
        }

        final Connectable source = connection.getSource();
        if (source.getConnectableType() != ConnectableType.FUNNEL && source.isRunning()) {
            return false;
        }

        final Connectable destination = connection.getDestination();
        return destination.getConnectableType() == ConnectableType.FUNNEL || !destination.isRunning();
    }

    private String generateTemporaryPortName(final VersionedPort proposedPort) {
        final String versionedPortId = proposedPort.getIdentifier();
        final String proposedPortFinalName = proposedPort.getName();
        return proposedPortFinalName + " (" + versionedPortId + ")";
    }

    private void updatePortToSetFinalName(final Port port, final String name) {
        port.setName(name);
    }

    private void verifyCanSynchronize(final Port port, final VersionedPort proposed, final long timeout) throws InterruptedException, TimeoutException, FlowSynchronizationException {
        if (proposed == null) {
            verifyCanDelete(port, timeout);
            return;
        }

        final ComponentType proposedType = proposed.getComponentType();
        if (proposedType != ComponentType.INPUT_PORT && proposedType != ComponentType.OUTPUT_PORT) {
            throw new FlowSynchronizationException("Cannot synchronize port " + port + " with the proposed Port definition because its type is "
                + proposedType + " and expected either an INPUT_PORT or an OUTPUT_PORT");
        }
    }

    @Override
    public void synchronize(final Port port, final VersionedPort proposed, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (port == null && proposed == null) {
            return;
        }

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();
        verifyCanSynchronize(port, proposed, timeout);

        synchronizationOptions.getComponentScheduler().pause();
        try {
            final Set<Connectable> toRestart = new HashSet<>();
            if (port != null) {
                final boolean stopped = stopOrTerminate(port, timeout, synchronizationOptions);
                if (stopped && proposed != null && proposed.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
                    toRestart.add(port);
                }
            }

            try {
                if (port == null) {
                    final ComponentType proposedType = proposed.getComponentType();

                    final Port newPort;
                    if (proposedType == ComponentType.INPUT_PORT) {
                        newPort = addInputPort(group, proposed, synchronizationOptions.getComponentIdGenerator(), proposed.getName());
                    } else {
                        newPort = addOutputPort(group, proposed, synchronizationOptions.getComponentIdGenerator(), proposed.getName());
                    }

                    LOG.info("Successfully synchronized {} by adding it to the flow", newPort);
                } else if (proposed == null) {
                    final Set<Connectable> stoppedDownstream = stopDownstreamComponents(port, timeout, synchronizationOptions);
                    toRestart.addAll(stoppedDownstream);

                    verifyCanDelete(port, timeout);

                    switch (port.getConnectableType()) {
                        case INPUT_PORT:
                            port.getProcessGroup().removeInputPort(port);
                            break;
                        case OUTPUT_PORT:
                            port.getProcessGroup().removeOutputPort(port);
                            break;
                    }

                    LOG.info("Successfully synchronized {} by removing it from the flow", port);
                } else {
                    updatePort(port, proposed, proposed.getName());
                    LOG.info("Successfully synchronized {} by updating it to match proposed version", port);
                }
            } finally {
                // Restart any components that need to be restarted.
                startComponents(toRestart, synchronizationOptions);
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private void startComponents(final Collection<Connectable> stoppedComponents, final FlowSynchronizationOptions synchronizationOptions) {
        for (final Connectable stoppedComponent : stoppedComponents) {
            context.getComponentScheduler().startComponent(stoppedComponent);
            notifyScheduledStateChange(stoppedComponent, synchronizationOptions, org.apache.nifi.flow.ScheduledState.RUNNING);
        }
    }

    private void updatePort(final Port port, final VersionedPort proposed, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();
        port.setComments(proposed.getComments());
        port.setName(name);
        port.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        port.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());
        if (proposed.getPortFunction() != null) {
            port.setPortFunction(proposed.getPortFunction());
        }

        context.getComponentScheduler().transitionComponentState(port, proposed.getScheduledState());
        notifyScheduledStateChange(port, syncOptions, proposed.getScheduledState());
    }

    private Port addInputPort(final ProcessGroup destination, final VersionedPort proposed, final ComponentIdGenerator componentIdGenerator, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();

        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());

        final Port port;
        if (proposed.isAllowRemoteAccess()) {
            port = context.getFlowManager().createPublicInputPort(id, name);
        } else {
            port = context.getFlowManager().createLocalInputPort(id, name);
        }

        port.setVersionedComponentId(proposed.getIdentifier());
        if (proposed.getPortFunction() != null) {
            port.setPortFunction(proposed.getPortFunction());
        }

        destination.addInputPort(port);
        updatePort(port, proposed, temporaryName);
        connectableAdditionTracker.addComponent(destination.getIdentifier(), proposed.getIdentifier(), port);

        return port;
    }

    private Port addOutputPort(final ProcessGroup destination, final VersionedPort proposed, final ComponentIdGenerator componentIdGenerator, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());

        final Port port;
        if (proposed.isAllowRemoteAccess()) {
            port = context.getFlowManager().createPublicOutputPort(id, name);
        } else {
            port = context.getFlowManager().createLocalOutputPort(id, name);
        }

        port.setVersionedComponentId(proposed.getIdentifier());
        if (proposed.getPortFunction() != null) {
            port.setPortFunction(proposed.getPortFunction());
        }
        destination.addOutputPort(port);
        updatePort(port, proposed, temporaryName);
        connectableAdditionTracker.addComponent(destination.getIdentifier(), proposed.getIdentifier(), port);

        return port;
    }

    private Label addLabel(final ProcessGroup destination, final VersionedLabel proposed, final ComponentIdGenerator componentIdGenerator) {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final Label label = context.getFlowManager().createLabel(id, proposed.getLabel());
        label.setVersionedComponentId(proposed.getIdentifier());
        destination.addLabel(label);
        updateLabel(label, proposed);

        return label;
    }

    private void updateLabel(final Label label, final VersionedLabel proposed) {
        label.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        label.setSize(new Size(proposed.getWidth(), proposed.getHeight()));
        label.setStyle(proposed.getStyle());
        label.setValue(proposed.getLabel());

        if (proposed.getzIndex() != null) {
            label.setZIndex(proposed.getzIndex());
        }
    }

    private ProcessorNode addProcessor(final ProcessGroup destination, final VersionedProcessor proposed, final ComponentIdGenerator componentIdGenerator,
                                       final ProcessGroup topLevelGroup) throws ProcessorInstantiationException {
        final String identifier = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        LOG.debug("Adding Processor with ID {} of type {}", identifier, proposed.getType());

        final BundleCoordinate coordinate = toCoordinate(proposed.getBundle());
        final ProcessorNode procNode = context.getFlowManager().createProcessor(proposed.getType(), identifier, coordinate, true);
        procNode.setVersionedComponentId(proposed.getIdentifier());

        destination.addProcessor(procNode);

        final Map<String, String> decryptedProperties = getDecryptedProperties(proposed.getProperties());
        createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(procNode, decryptedProperties));

        updateProcessor(procNode, proposed, topLevelGroup);

        // Notify the processor node that the configuration (properties, e.g.) has been restored
        final ProcessContext processContext = context.getProcessContextFactory().apply(procNode);
        procNode.onConfigurationRestored(processContext);
        connectableAdditionTracker.addComponent(destination.getIdentifier(), proposed.getIdentifier(), procNode);

        return procNode;
    }

    private void verifyCanSynchronize(final ProcessorNode processor, final VersionedProcessor proposedProcessor, final long timeout)
        throws InterruptedException, TimeoutException, FlowSynchronizationException {

        // If processor is null, we can always synchronize by creating the proposed processor.
        if (processor == null) {
            return;
        }

        // Ensure that processor is in a state that it can be removed.
        if (proposedProcessor == null) {
            verifyCanDelete(processor, timeout);
            return;
        }

        // Verify processor can be updated
        processor.verifyCanUpdate();
    }

    private void verifyCanDelete(final Connectable connectable, final long timeout) throws InterruptedException, TimeoutException, FlowSynchronizationException {
        verifyNoIncomingConnections(connectable);
        verifyCanDeleteConnections(connectable, timeout);
        connectable.verifyCanDelete(true);
    }

    private void verifyCanDeleteConnections(final Connectable connectable, final long timeout) throws InterruptedException, TimeoutException, FlowSynchronizationException {
        final Set<Connection> connections = connectable.getConnections();
        for (final Connection connection : connections) {
            verifyCanDeleteWhenQueueEmpty(connection);
        }

        for (final Connection connection : connections) {
            waitForQueueEmpty(connection, Duration.ofMillis(timeout - System.currentTimeMillis()));
        }
    }

    private void verifyNoIncomingConnections(final Connectable connectable) throws FlowSynchronizationException {
        for (final Connection incoming : connectable.getIncomingConnections()) {
            final Connectable source = incoming.getSource();
            if (source == connectable) {
                continue;
            }

            throw new FlowSynchronizationException("Cannot remove " + connectable + " because it has an incoming connection from " + incoming.getSource());
        }
    }

    @Override
    public void synchronize(final ProcessorNode processor, final VersionedProcessor proposedProcessor, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (processor == null && proposedProcessor == null) {
            return;
        }

        setSynchronizationOptions(synchronizationOptions);
        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // Stop the processor, if necessary, in order to update it.
            final Set<Connectable> toRestart = new HashSet<>();
            if (processor != null) {
                final boolean stopped = stopOrTerminate(processor, timeout, synchronizationOptions);

                if (stopped && proposedProcessor != null && proposedProcessor.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
                    toRestart.add(processor);
                }
            }

            try {
                verifyCanSynchronize(processor, proposedProcessor, timeout);

                try {
                    final ProcessGroup topLevelGroup = synchronizationOptions.getTopLevelGroupId() != null ? context.getFlowManager().getGroup(synchronizationOptions.getTopLevelGroupId()) : group;
                    if (proposedProcessor == null) {
                        final Set<Connectable> stoppedDownstream = stopDownstreamComponents(processor, timeout, synchronizationOptions);
                        toRestart.addAll(stoppedDownstream);

                        processor.getProcessGroup().removeProcessor(processor);
                        LOG.info("Successfully synchronized {} by removing it from the flow", processor);
                    } else if (processor == null) {
                        final ProcessorNode added = addProcessor(group, proposedProcessor, synchronizationOptions.getComponentIdGenerator(), topLevelGroup);
                        LOG.info("Successfully synchronized {} by adding it to the flow", added);
                    } else {
                        updateProcessor(processor, proposedProcessor, topLevelGroup);
                        LOG.info("Successfully synchronized {} by updating it to match proposed version", processor);
                    }
                } catch (final Exception e) {
                    throw new FlowSynchronizationException("Failed to synchronize processor " + processor + " with proposed version", e);
                }
            } finally {
                // Restart any components that need to be restarted.
                startComponents(toRestart, synchronizationOptions);
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private Set<Connectable> stopDownstreamComponents(final Connectable component, final long timeout, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException {

        final Set<Connectable> stoppedComponents = new HashSet<>();

        for (final Connection connection : component.getConnections()) {
            final Connectable destination = connection.getDestination();
            final boolean stopped = stopOrTerminate(destination, timeout, synchronizationOptions);

            if (stopped) {
                stoppedComponents.add(destination);
            }
        }

        return stoppedComponents;
    }

    private <T extends Connectable> Set<T> stopOrTerminate(final Set<T> components, final long timeout, final FlowSynchronizationOptions synchronizationOptions)
        throws TimeoutException, FlowSynchronizationException {

        final Set<T> stoppedComponents = new HashSet<>();

        for (final T component : components) {
            final boolean stopped = stopOrTerminate(component, timeout, synchronizationOptions);
            if (stopped) {
                stoppedComponents.add(component);
            }
        }

        return stoppedComponents;
    }

    private void notifyScheduledStateChange(final Connectable component, final FlowSynchronizationOptions synchronizationOptions, final org.apache.nifi.flow.ScheduledState intendedState) {
        try {
            if (component instanceof ProcessorNode) {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange((ProcessorNode) component, intendedState);
            } else if (component instanceof Port) {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange((Port) component, intendedState);
            }
        } catch (final Exception e) {
            LOG.debug("Failed to notify listeners of ScheduledState changes", e);
        }
    }

    private void notifyScheduledStateChange(final ComponentNode component, final FlowSynchronizationOptions synchronizationOptions, final org.apache.nifi.flow.ScheduledState intendedState) {
        if (component instanceof Triggerable && intendedState == org.apache.nifi.flow.ScheduledState.RUNNING && ((Triggerable) component).getScheduledState() == ScheduledState.DISABLED) {
            return;
        }
        try {
            if (component instanceof ProcessorNode) {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange((ProcessorNode) component, intendedState);
            } else if (component instanceof Port) {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange((Port) component, intendedState);
            } else if (component instanceof ControllerServiceNode) {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange((ControllerServiceNode) component, intendedState);
            } else if (component instanceof ReportingTaskNode) {
                final ReportingTaskNode reportingTaskNode = (ReportingTaskNode) component;
                if (intendedState == org.apache.nifi.flow.ScheduledState.RUNNING && reportingTaskNode.getScheduledState() == ScheduledState.DISABLED) {
                    return;
                }

                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange(reportingTaskNode, intendedState);
            }
        } catch (final Exception e) {
            LOG.debug("Failed to notify listeners of ScheduledState changes", e);
        }
    }

    private void notifyScheduledStateChange(final Collection<ControllerServiceNode> servicesToRestart, final FlowSynchronizationOptions synchronizationOptions,
                                            final org.apache.nifi.flow.ScheduledState intendedState) {
        try {
            servicesToRestart.forEach(service -> {
                synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange(service, intendedState);
                if (intendedState == org.apache.nifi.flow.ScheduledState.DISABLED) {
                    service.getReferences().findRecursiveReferences(ControllerServiceNode.class)
                            .forEach(reference -> synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange(reference, org.apache.nifi.flow.ScheduledState.DISABLED));
                } else if (intendedState == org.apache.nifi.flow.ScheduledState.ENABLED) {
                    service.getRequiredControllerServices().forEach(requiredService -> synchronizationOptions.getScheduledStateChangeListener()
                            .onScheduledStateChange(requiredService, org.apache.nifi.flow.ScheduledState.ENABLED));
                }
            });
        } catch (final Exception e) {
            LOG.debug("Failed to notify listeners of ScheduledState changes", e);
        }
    }

    private void notifyScheduledStateChange(final Port inputPort, final FlowSynchronizationOptions synchronizationOptions, final org.apache.nifi.flow.ScheduledState intendedState) {
        try {
            synchronizationOptions.getScheduledStateChangeListener().onScheduledStateChange(inputPort, intendedState);
        } catch (final Exception e) {
            LOG.debug("Failed to notify listeners of ScheduledState changes", e);
        }
    }

    private boolean stopOrTerminate(final Connectable component, final long timeout, final FlowSynchronizationOptions synchronizationOptions) throws TimeoutException, FlowSynchronizationException {
        if (!component.isRunning()) {
            return false;
        }

        final ConnectableType connectableType = component.getConnectableType();
        switch (connectableType) {
            case INPUT_PORT:
                final Port inputPort = (Port) component;
                component.getProcessGroup().stopInputPort(inputPort);
                notifyScheduledStateChange(inputPort, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
                return true;
            case OUTPUT_PORT:
                final Port outputPort = (Port) component;
                component.getProcessGroup().stopOutputPort(outputPort);
                notifyScheduledStateChange(outputPort, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
                return true;
            case PROCESSOR:
                final ProcessorNode processorNode = (ProcessorNode) component;
                return stopOrTerminate(processorNode, timeout, synchronizationOptions);
            default:
                return false;
        }
    }

    private boolean stopOrTerminate(final ProcessorNode processor, final long timeout, final FlowSynchronizationOptions synchronizationOptions) throws TimeoutException, FlowSynchronizationException {
        try {
            LOG.debug("Stopping {} in order to synchronize it with proposed version", processor);

            return stopProcessor(processor, timeout);
        } catch (final TimeoutException te) {
            if (synchronizationOptions.getComponentStopTimeoutAction() == ComponentStopTimeoutAction.THROW_TIMEOUT_EXCEPTION) {
                throw te;
            }

            processor.terminate();
            return true;
        } finally {
            notifyScheduledStateChange((ComponentNode) processor, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
        }
    }

    private boolean stopProcessor(final ProcessorNode processor, final long timeout) throws FlowSynchronizationException, TimeoutException {
        if (!processor.isRunning() && processor.getPhysicalScheduledState() != ScheduledState.STARTING) {
            return false;
        }

        final Future<Void> future = processor.getProcessGroup().stopProcessor(processor);
        try {
            future.get(timeout - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (final ExecutionException ee) {
            throw new FlowSynchronizationException("Failed to stop processor " + processor, ee.getCause());
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlowSynchronizationException("Interrupted while waiting for processor " + processor + " to stop", ie);
        }
    }

    private void stopControllerService(final ControllerServiceNode controllerService, final VersionedControllerService proposed, final long timeout,
                                       final FlowSynchronizationOptions.ComponentStopTimeoutAction timeoutAction, final Set<ComponentNode> referencesStopped,
                                       final Set<ControllerServiceNode> servicesDisabled, final FlowSynchronizationOptions synchronizationOptions) throws FlowSynchronizationException,
        TimeoutException, InterruptedException {
        final ControllerServiceProvider serviceProvider = context.getControllerServiceProvider();
        if (controllerService == null) {
            return;
        }

        final Map<ComponentNode, Future<Void>> futures = serviceProvider.unscheduleReferencingComponents(controllerService);
        referencesStopped.addAll(futures.keySet());

        for (final Map.Entry<ComponentNode, Future<Void>> entry : futures.entrySet()) {
            final ComponentNode component = entry.getKey();
            final Future<Void> future = entry.getValue();

            waitForStopCompletion(future, component, timeout, timeoutAction);
            notifyScheduledStateChange(component, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
        }

        if (controllerService.isActive()) {
            // If the Controller Service is active, we need to disable it. To do that, we must first disable all referencing services.
            final List<ControllerServiceNode> referencingServices = controllerService.getReferences().findRecursiveReferences(ControllerServiceNode.class);

            if (proposed != null && proposed.getScheduledState() != org.apache.nifi.flow.ScheduledState.DISABLED) {
                servicesDisabled.add(controllerService);
            }

            for (final ControllerServiceNode reference : referencingServices) {
                if (reference.isActive()) {
                    servicesDisabled.add(reference);
                }
            }

            // We want to stop all dependent services plus the controller service we are synchronizing.
            final Set<ControllerServiceNode> servicesToStop = new HashSet<>(servicesDisabled);
            servicesToStop.add(controllerService);

            // Disable the service and wait for completion, up to the timeout allowed
            final Future<Void> future = serviceProvider.disableControllerServicesAsync(servicesToStop);
            waitForStopCompletion(future, controllerService, timeout, timeoutAction);
            notifyScheduledStateChange(servicesToStop, synchronizationOptions, org.apache.nifi.flow.ScheduledState.DISABLED);
        }
    }


    private void updateProcessor(final ProcessorNode processor, final VersionedProcessor proposed, final ProcessGroup topLevelGroup) throws ProcessorInstantiationException {
        LOG.debug("Updating Processor {}", processor);

        processor.pauseValidationTrigger();
        try {
            processor.setAnnotationData(proposed.getAnnotationData());
            processor.setBulletinLevel(LogLevel.valueOf(proposed.getBulletinLevel()));
            processor.setComments(proposed.getComments());
            processor.setName(proposed.getName());
            processor.setPenalizationPeriod(proposed.getPenaltyDuration());

            if (!isEqual(processor.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(processor.getProperties().keySet());
                final Set<URL> additionalUrls = processor.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(processor, proposed.getType(), newBundleCoordinate, additionalUrls);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(processor, proposed.getProperties(), proposed.getPropertyDescriptors().values());
            final Map<String, String> properties = populatePropertiesMap(processor, proposed.getProperties(), proposed.getPropertyDescriptors(), processor.getProcessGroup(), topLevelGroup);
            processor.setProperties(properties, true, sensitiveDynamicPropertyNames);
            processor.setRunDuration(proposed.getRunDurationMillis(), TimeUnit.MILLISECONDS);
            processor.setSchedulingStrategy(SchedulingStrategy.valueOf(proposed.getSchedulingStrategy()));
            processor.setSchedulingPeriod(proposed.getSchedulingPeriod());
            processor.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());
            processor.setExecutionNode(ExecutionNode.valueOf(proposed.getExecutionNode()));
            processor.setStyle(proposed.getStyle());
            processor.setYieldPeriod(proposed.getYieldDuration());
            processor.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));

            processor.setMaxBackoffPeriod(proposed.getMaxBackoffPeriod());
            processor.setRetriedRelationships(proposed.getRetriedRelationships());

            final Set<String> proposedAutoTerminated = proposed.getAutoTerminatedRelationships();
            if (proposedAutoTerminated != null) {
                final Set<Relationship> relationshipsToAutoTerminate = proposedAutoTerminated.stream()
                    .map(processor::getRelationship)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

                processor.setAutoTerminatedRelationships(relationshipsToAutoTerminate);
            }

            if (proposed.getRetryCount() != null) {
                processor.setRetryCount(proposed.getRetryCount());
            } else {
                processor.setRetryCount(10);
            }

            if (proposed.getBackoffMechanism() != null) {
                processor.setBackoffMechanism(BackoffMechanism.valueOf(proposed.getBackoffMechanism()));
            }

            // Transition state to disabled/enabled/running
            context.getComponentScheduler().transitionComponentState(processor, proposed.getScheduledState());
            notifyScheduledStateChange((ComponentNode) processor, syncOptions, proposed.getScheduledState());
        } finally {
            processor.resumeValidationTrigger();
        }
    }

    private String getServiceInstanceId(final String serviceVersionedComponentId, final ProcessGroup group) {
        for (final ControllerServiceNode serviceNode : group.getControllerServices(false)) {
            final String versionedId = serviceNode.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(serviceNode.getIdentifier()));
            if (versionedId.equals(serviceVersionedComponentId)) {
                return serviceNode.getIdentifier();
            }
        }

        final ProcessGroup parent = group.getParent();
        if (parent == null) {
            return null;
        }

        return getServiceInstanceId(serviceVersionedComponentId, parent);
    }

    @Override
    public void synchronize(final RemoteProcessGroup rpg, final VersionedRemoteProcessGroup proposed, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException, InterruptedException {

        if (rpg == null && proposed == null) {
            return;
        }

        setSynchronizationOptions(synchronizationOptions);
        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // Stop the rpg, if necessary, in order to update it.
            final Set<Connectable> toRestart = new HashSet<>();
            if (rpg != null) {
                if (rpg.isTransmitting()) {
                    final Set<RemoteGroupPort> transmitting = getTransmittingPorts(rpg);

                    final Future<?> future = rpg.stopTransmitting();
                    try {
                        transmitting.forEach(remoteGroupPort -> synchronizationOptions.getScheduledStateChangeListener()
                                .onScheduledStateChange(remoteGroupPort, org.apache.nifi.flow.ScheduledState.ENABLED));
                    } catch (final Exception e) {
                        LOG.debug("Failed to notify listeners of ScheduledState changes", e);
                    }
                    waitForStopCompletion(future, rpg, timeout, synchronizationOptions.getComponentStopTimeoutAction());

                    final boolean proposedTransmitting = isTransmitting(proposed);
                    if (proposed != null && proposedTransmitting) {
                        toRestart.addAll(transmitting);
                    }
                }
            }

            try {
                if (proposed == null) {
                    // Stop any downstream components so that we can delete the RPG
                    for (final RemoteGroupPort outPort : rpg.getOutputPorts()) {
                        final Set<Connectable> stoppedDownstream = stopDownstreamComponents(outPort, timeout, synchronizationOptions);
                        toRestart.addAll(stoppedDownstream);
                    }

                    // Verify that we can delete the components
                    for (final RemoteGroupPort port : rpg.getInputPorts()) {
                        verifyCanDelete(port, timeout);
                    }
                    for (final RemoteGroupPort port : rpg.getOutputPorts()) {
                        verifyCanDelete(port, timeout);
                    }

                    rpg.getProcessGroup().removeRemoteProcessGroup(rpg);
                    LOG.info("Successfully synchronized {} by removing it from the flow", rpg);
                } else if (rpg == null) {
                    final RemoteProcessGroup added = addRemoteProcessGroup(group, proposed, synchronizationOptions.getComponentIdGenerator());
                    LOG.info("Successfully synchronized {} by adding it to the flow", added);
                } else {
                    updateRemoteProcessGroup(rpg, proposed, synchronizationOptions.getComponentIdGenerator());
                    LOG.info("Successfully synchronized {} by updating it to match proposed version", rpg);
                }
            } catch (final Exception e) {
                throw new FlowSynchronizationException("Failed to synchronize " + rpg + " with proposed version", e);
            } finally {
                // Restart any components that need to be restarted.
                startComponents(toRestart, synchronizationOptions);
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private boolean isTransmitting(final VersionedRemoteProcessGroup versionedRpg) {
        if (versionedRpg == null) {
            return false;
        }

        for (final VersionedRemoteGroupPort port : versionedRpg.getInputPorts()) {
            if (port.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
                return true;
            }
        }

        for (final VersionedRemoteGroupPort port : versionedRpg.getOutputPorts()) {
            if (port.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
                return true;
            }
        }

        return false;
    }

    private Set<RemoteGroupPort> getTransmittingPorts(final RemoteProcessGroup rpg) {
        if (rpg == null) {
            return Collections.emptySet();
        }

        final Set<RemoteGroupPort> transmitting = new HashSet<>();
        rpg.getInputPorts().stream()
            .filter(port -> port.getScheduledState() == ScheduledState.RUNNING)
            .forEach(transmitting::add);

        rpg.getOutputPorts().stream()
            .filter(port -> port.getScheduledState() == ScheduledState.RUNNING)
            .forEach(transmitting::add);

        return transmitting;
    }

    private RemoteProcessGroup addRemoteProcessGroup(final ProcessGroup destination, final VersionedRemoteProcessGroup proposed, final ComponentIdGenerator componentIdGenerator) {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final RemoteProcessGroup rpg = context.getFlowManager().createRemoteProcessGroup(id, proposed.getTargetUris());
        rpg.setVersionedComponentId(proposed.getIdentifier());

        destination.addRemoteProcessGroup(rpg);
        updateRemoteProcessGroup(rpg, proposed, componentIdGenerator);

        rpg.initialize();
        return rpg;
    }

    private void updateRemoteProcessGroup(final RemoteProcessGroup rpg, final VersionedRemoteProcessGroup proposed, final ComponentIdGenerator componentIdGenerator) {
        rpg.setComments(proposed.getComments());
        rpg.setCommunicationsTimeout(proposed.getCommunicationsTimeout());
        rpg.setInputPorts(proposed.getInputPorts() == null ? Collections.emptySet() : proposed.getInputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdGenerator, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);

        synchronizeRemoteGroupPorts(rpg.getInputPorts(), proposed.getInputPorts());
        synchronizeRemoteGroupPorts(rpg.getOutputPorts(), proposed.getOutputPorts());
        rpg.setName(proposed.getName());
        rpg.setNetworkInterface(proposed.getLocalNetworkInterface());
        rpg.setOutputPorts(proposed.getOutputPorts() == null ? Collections.emptySet() : proposed.getOutputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdGenerator, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);
        rpg.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        rpg.setProxyHost(proposed.getProxyHost());
        rpg.setProxyPort(proposed.getProxyPort());
        rpg.setProxyUser(proposed.getProxyUser());
        rpg.setProxyPassword(decrypt(proposed.getProxyPassword(), syncOptions.getPropertyDecryptor()));
        rpg.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(proposed.getTransportProtocol()));
        rpg.setYieldDuration(proposed.getYieldDuration());

        if (syncOptions.isUpdateRpgUrls()) {
            rpg.setTargetUris(proposed.getTargetUris());
        }

        if (proposed.getInputPorts() != null) {
            for (final VersionedRemoteGroupPort port : proposed.getInputPorts()) {
                final RemoteGroupPort remoteGroupPort = getRpgInputPort(port, rpg, componentIdGenerator);
                if (remoteGroupPort != null) {
                    synchronizeTransmissionState(port, remoteGroupPort);
                }
            }
        }

        if (proposed.getOutputPorts() != null) {
            for (final VersionedRemoteGroupPort port : proposed.getOutputPorts()) {
                final RemoteGroupPort remoteGroupPort = getRpgOutputPort(port, rpg, componentIdGenerator);
                if (remoteGroupPort != null) {
                    synchronizeTransmissionState(port, remoteGroupPort);
                }
            }
        }
    }

    private void synchronizeRemoteGroupPorts(final Set<RemoteGroupPort> remoteGroupPorts, final Set<VersionedRemoteGroupPort> proposedPorts) {
        final Map<String, VersionedRemoteGroupPort> inputPortsByTargetId = mapRemoteGroupPortsByTargetId(proposedPorts);
        remoteGroupPorts.forEach(port -> {
            final VersionedRemoteGroupPort proposedPort = inputPortsByTargetId.get(port.getTargetIdentifier());
            if (proposedPort != null) {
                if (proposedPort.getBatchSize() != null) {
                    final BatchSize batchSize = proposedPort.getBatchSize();
                    port.setBatchSize(batchSize.getSize());
                    port.setBatchCount(batchSize.getCount());
                    port.setBatchDuration(batchSize.getDuration());
                }
                if (proposedPort.isUseCompression() != null) {
                    port.setUseCompression(proposedPort.isUseCompression());
                }
                if (proposedPort.getConcurrentlySchedulableTaskCount() != null) {
                    port.setMaxConcurrentTasks(proposedPort.getConcurrentlySchedulableTaskCount());
                }
            }
        });
    }

    private Map<String, VersionedRemoteGroupPort> mapRemoteGroupPortsByTargetId(final Set<VersionedRemoteGroupPort> remoteGroupPorts) {
        return remoteGroupPorts == null ? Collections.emptyMap() : remoteGroupPorts.stream()
                .collect(Collectors.toMap(
                        VersionedRemoteGroupPort::getTargetId,
                        Function.identity()
                ));
    }

    private RemoteGroupPort getRpgInputPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator) {
        return getRpgPort(port, rpg, componentIdGenerator, rpg::getInputPort, rpg.getInputPorts());
    }

    private RemoteGroupPort getRpgOutputPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator) {
        return getRpgPort(port, rpg, componentIdGenerator, rpg::getOutputPort, rpg.getOutputPorts());
    }

    private RemoteGroupPort getRpgPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator,
                                       final Function<String, RemoteGroupPort> portLookup, final Set<RemoteGroupPort> ports) {
        final String instanceId = port.getInstanceIdentifier();
        if (instanceId != null) {
            final RemoteGroupPort remoteGroupPort = portLookup.apply(instanceId);
            if (remoteGroupPort != null) {
                return remoteGroupPort;
            }
        }

        final Optional<RemoteGroupPort> portByName = ports.stream()
                .filter(p -> p.getName().equals(port.getName()))
                .findFirst();
        if (portByName.isPresent()) {
            return portByName.get();
        }


        final String portId = componentIdGenerator.generateUuid(port.getIdentifier(), port.getInstanceIdentifier(), rpg.getIdentifier());
        final RemoteGroupPort remoteGroupPort = portLookup.apply(portId);
        return remoteGroupPort;
    }

    private void synchronizeTransmissionState(final VersionedRemoteGroupPort versionedPort, final RemoteGroupPort remoteGroupPort) {
        final ScheduledState portState = remoteGroupPort.getScheduledState();

        if (versionedPort.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
            if (portState != ScheduledState.RUNNING) {
                context.getComponentScheduler().startComponent(remoteGroupPort);
                notifyScheduledStateChange(remoteGroupPort, syncOptions, org.apache.nifi.flow.ScheduledState.RUNNING);
            }
        } else {
            if (portState == ScheduledState.RUNNING) {
                context.getComponentScheduler().stopComponent(remoteGroupPort);
                notifyScheduledStateChange(remoteGroupPort, syncOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
            }
        }
    }

    private RemoteProcessGroupPortDescriptor createPortDescriptor(final VersionedRemoteGroupPort proposed, final ComponentIdGenerator componentIdGenerator, final String rpgId) {
        final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
        descriptor.setVersionedComponentId(proposed.getIdentifier());

        final BatchSize batchSize = proposed.getBatchSize();
        if (batchSize != null) {
            descriptor.setBatchCount(batchSize.getCount());
            descriptor.setBatchDuration(batchSize.getDuration());
            descriptor.setBatchSize(batchSize.getSize());
        }

        descriptor.setComments(proposed.getComments());
        descriptor.setConcurrentlySchedulableTaskCount(proposed.getConcurrentlySchedulableTaskCount());
        descriptor.setGroupId(proposed.getRemoteGroupId());
        descriptor.setTargetId(proposed.getTargetId());

        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), rpgId);
        descriptor.setId(id);
        descriptor.setName(proposed.getName());
        descriptor.setUseCompression(proposed.isUseCompression());
        return descriptor;
    }

    private void verifyCanSynchronize(final Connection connection, final VersionedConnection proposedConnection) throws FlowSynchronizationException {
        if (proposedConnection == null) {
            verifyCanDeleteWhenQueueEmpty(connection);
        }
    }

    private void verifyCanDeleteWhenQueueEmpty(final Connection connection) throws FlowSynchronizationException {
        final boolean empty = connection.getFlowFileQueue().isEmpty();
        if (empty) {
            return;
        }

        final ScheduledState scheduledState = connection.getDestination().getScheduledState();
        if (scheduledState == ScheduledState.DISABLED || scheduledState == ScheduledState.STOPPED || scheduledState == ScheduledState.STOPPING) {
            throw new FlowSynchronizationException("Cannot synchronize " + connection + " with proposed connection because doing so would require deleting the connection, " +
                "and the connection has data queued while the destination is not running. The connection must be emptied before it can be removed.");
        }
    }

    private Set<Connectable> getUpstreamComponents(final Connection connection) {
        if (connection == null) {
            return Collections.emptySet();
        }

        final Set<Connectable> components = new HashSet<>();
        findUpstreamComponents(connection, components);
        return components;
    }

    private void findUpstreamComponents(final Connection connection, final Set<Connectable> components) {
        final Connectable source = connection.getSource();
        if (source.getConnectableType() == ConnectableType.FUNNEL) {
            source.getIncomingConnections().forEach(incoming -> findUpstreamComponents(incoming, components));
        } else {
            components.add(source);
        }
    }

    private Set<Connectable> getUpstreamComponents(final VersionedConnection connection) {
        if (connection == null) {
            return Collections.emptySet();
        }

        final Set<Connectable> components = new HashSet<>();
        findUpstreamComponents(connection, components);
        return components;
    }

    private void findUpstreamComponents(final VersionedConnection connection, final Set<Connectable> components) {
        final ConnectableComponent sourceConnectable = connection.getSource();
        final Connectable source = context.getFlowManager().findConnectable(sourceConnectable.getId());
        if (sourceConnectable.getType() == ConnectableComponentType.FUNNEL) {
            source.getIncomingConnections().forEach(incoming -> findUpstreamComponents(incoming, components));
        } else {
            components.add(source);
        }
    }

    @Override
    public void synchronize(final Connection connection, final VersionedConnection proposedConnection, final ProcessGroup group, final FlowSynchronizationOptions synchronizationOptions)
        throws FlowSynchronizationException, TimeoutException {

        if (connection == null && proposedConnection == null) {
            return;
        }

        final long timeout = System.currentTimeMillis() + synchronizationOptions.getComponentStopTimeout().toMillis();

        // Stop any upstream components so that we can update the connection
        final Set<Connectable> upstream = new HashSet<>(getUpstreamComponents(connection));
        if (connection == null) {
            upstream.addAll(getUpstreamComponents(proposedConnection));
        }
        Set<Connectable> stoppedComponents;
        try {
            stoppedComponents = stopOrTerminate(upstream, timeout, synchronizationOptions);
        } catch (final TimeoutException te) {
            if (synchronizationOptions.getComponentStopTimeoutAction() == FlowSynchronizationOptions.ComponentStopTimeoutAction.THROW_TIMEOUT_EXCEPTION) {
                throw te;
            }

            LOG.info("Components upstream of {} did not stop in time. Will terminate {}", connection, upstream);
            terminateComponents(upstream, synchronizationOptions);
            stoppedComponents = upstream;
        }

        try {
            // Verify that we can synchronize the connection now that the sources are stopped.
            verifyCanSynchronize(connection, proposedConnection);

            // If the connection is to be deleted, wait for the queue to empty.
            if (proposedConnection == null) {
                try {
                    waitForQueueEmpty(connection, synchronizationOptions.getComponentStopTimeout());
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new FlowSynchronizationException("Interrupted while waiting for FlowFile queue to empty for " + connection, ie);
                }
            }

            // Stop destination component so that we can update the connection
            if (connection != null) {
                final Connectable destination = connection.getDestination();
                final boolean stopped = stopOrTerminate(destination, timeout, synchronizationOptions);
                if (stopped) {
                    stoppedComponents.add(destination);
                }
            }

            if (connection == null) {
                final Connection added = addConnection(group, proposedConnection, synchronizationOptions.getComponentIdGenerator());
                LOG.info("Successfully synchronized {} by adding it to the flow", added);
            } else if (proposedConnection == null) {
                connection.getProcessGroup().removeConnection(connection);
                LOG.info("Successfully synchronized {} by removing it from the flow", connection);
            } else {
                updateConnection(connection, proposedConnection);
                LOG.info("Successfully synchronized {} by updating it to match proposed version", connection);
            }
        } finally {
            // If not removing the connection, restart any component that we stopped.
            if (proposedConnection != null) {
                startComponents(stoppedComponents, synchronizationOptions);
            }
        }
    }

    private void waitForQueueEmpty(final Connection connection, final Duration duration) throws TimeoutException, InterruptedException {
        if (connection == null) {
            return;
        }

        final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
        final long timeoutMillis = System.currentTimeMillis() + duration.toMillis();

        while (!flowFileQueue.isEmpty()) {
            if (System.currentTimeMillis() >= timeoutMillis) {
                throw new TimeoutException("Timed out waiting for " + connection + " to empty its FlowFiles");
            }

            Thread.sleep(10L);
        }
    }

    private void terminateComponents(final Set<Connectable> components, final FlowSynchronizationOptions synchronizationOptions) {
        for (final Connectable component : components) {
            if (!(component instanceof ProcessorNode)) {
                continue;
            }

            final ProcessorNode processor = (ProcessorNode) component;
            if (!processor.isRunning()) {
                continue;
            }

            processor.getProcessGroup().stopProcessor(processor);
            processor.terminate();
            notifyScheduledStateChange((ComponentNode) processor, synchronizationOptions, org.apache.nifi.flow.ScheduledState.ENABLED);
        }
    }

    private void updateConnection(final Connection connection, final VersionedConnection proposed) {
        LOG.debug("Updating connection from {} to {} with name {} and relationships {}: {}",
            proposed.getSource(), proposed.getDestination(), proposed.getName(), proposed.getSelectedRelationships(), connection);

        connection.setBendPoints(proposed.getBends() == null ? Collections.emptyList() :
            proposed.getBends().stream()
                .map(pos -> new Position(pos.getX(), pos.getY()))
                .collect(Collectors.toList()));

        connection.setDestination(getConnectable(connection.getProcessGroup(), proposed.getDestination()));
        connection.setLabelIndex(proposed.getLabelIndex());
        connection.setName(proposed.getName());
        connection.setRelationships(proposed.getSelectedRelationships().stream()
            .map(name -> new Relationship.Builder().name(name).build())
            .collect(Collectors.toSet()));
        connection.setZIndex(proposed.getzIndex());

        final FlowFileQueue queue = connection.getFlowFileQueue();
        queue.setBackPressureDataSizeThreshold(proposed.getBackPressureDataSizeThreshold());
        queue.setBackPressureObjectThreshold(proposed.getBackPressureObjectThreshold());
        queue.setFlowFileExpiration(proposed.getFlowFileExpiration());

        final List<FlowFilePrioritizer> prioritizers = proposed.getPrioritizers() == null ? Collections.emptyList() : proposed.getPrioritizers().stream()
            .map(prioritizerName -> {
                try {
                    return context.getFlowManager().createPrioritizer(prioritizerName);
                } catch (final Exception e) {
                    throw new IllegalStateException("Failed to create Prioritizer of type " + prioritizerName + " for Connection with ID " + connection.getIdentifier());
                }
            })
            .collect(Collectors.toList());

        queue.setPriorities(prioritizers);

        final String loadBalanceStrategyName = proposed.getLoadBalanceStrategy();
        if (loadBalanceStrategyName == null) {
            queue.setLoadBalanceStrategy(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE, proposed.getPartitioningAttribute());
        } else {
            final LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.valueOf(loadBalanceStrategyName);
            final String partitioningAttribute = proposed.getPartitioningAttribute();

            queue.setLoadBalanceStrategy(loadBalanceStrategy, partitioningAttribute);
        }

        final String compressionName = proposed.getLoadBalanceCompression();
        if (compressionName == null) {
            queue.setLoadBalanceCompression(LoadBalanceCompression.DO_NOT_COMPRESS);
        } else {
            queue.setLoadBalanceCompression(LoadBalanceCompression.valueOf(compressionName));
        }
    }

    private Connection addConnection(final ProcessGroup destinationGroup, final VersionedConnection proposed, final ComponentIdGenerator componentIdGenerator) {
        LOG.debug("Adding connection from {} to {} with name {} and relationships {}", proposed.getSource(), proposed.getDestination(), proposed.getName(), proposed.getSelectedRelationships());

        final Connectable source = getConnectable(destinationGroup, proposed.getSource());
        if (source == null) {
            throw new IllegalArgumentException("Connection has a source with identifier " + proposed.getSource().getId()
                + " but no component could be found in the Process Group with a corresponding identifier");
        }

        final Connectable destination = getConnectable(destinationGroup, proposed.getDestination());
        if (destination == null) {
            throw new IllegalArgumentException("Connection has a destination with identifier " + proposed.getDestination().getId()
                + " but no component could be found in the Process Group with a corresponding identifier");
        }

        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final Connection connection = context.getFlowManager().createConnection(id, proposed.getName(), source, destination, proposed.getSelectedRelationships());
        connection.setVersionedComponentId(proposed.getIdentifier());
        destinationGroup.addConnection(connection);
        updateConnection(connection, proposed);

        context.getFlowManager().onConnectionAdded(connection);
        return connection;
    }

    private Connectable getConnectable(final ProcessGroup group, final ConnectableComponent connectableComponent) {
        // Always prefer the instance identifier, if it's available.
        final Connectable connectable = getConnectable(group, connectableComponent, ConnectableComponent::getInstanceIdentifier);
        if (connectable != null) {
            LOG.debug("Found Connectable {} in Process Group {} by Instance ID {}", connectable, group, connectableComponent.getInstanceIdentifier());
            return connectable;
        }

        // If we're synchronizing and the component is not available by the instance ID, lookup the component by the ID instead.
        final Connectable connectableById = getConnectable(group, connectableComponent, ConnectableComponent::getId);
        LOG.debug("Found no connectable in Process Group {} by Instance ID. Lookup by ID {} yielded {}", group, connectableComponent.getId(), connectableById);
        if (connectableById != null) {
            return connectableById;
        }

        final Optional<Connectable> addedComponent = connectableAdditionTracker.getComponent(group.getIdentifier(), connectableComponent.getId());
        addedComponent.ifPresent(value -> LOG.debug("Found Connectable in Process Group {} as newly added component {}", group, value));

        return addedComponent.orElse(null);
    }

    private Connectable getConnectable(final ProcessGroup group, final ConnectableComponent connectableComponent, final Function<ConnectableComponent, String> idFunction) {
        final String id = idFunction.apply(connectableComponent);
        if (id == null) {
            return null;
        }

        switch (connectableComponent.getType()) {
            case FUNNEL:
                return group.getFunnels().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny()
                    .orElse(null);
            case INPUT_PORT: {
                final Optional<Port> port = group.getInputPorts().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny();

                if (port.isPresent()) {
                    return port.get();
                }

                // Attempt to locate child group by versioned component id
                final Optional<ProcessGroup> optionalSpecifiedGroup = group.getProcessGroups().stream()
                    .filter(child -> matchesGroupId(child, connectableComponent.getGroupId()))
                    .findFirst();

                if (optionalSpecifiedGroup.isPresent()) {
                    final ProcessGroup specifiedGroup = optionalSpecifiedGroup.get();
                    return specifiedGroup.getInputPorts().stream()
                        .filter(component -> matchesId(component, id))
                        .findAny()
                        .orElse(null);
                }

                // If no child group matched the versioned component id, then look at all child groups. This is done because
                // in older versions, we did not properly map Versioned Component ID's to Ports' parent groups. As a result,
                // if the flow doesn't contain the properly mapped group id, we need to search all child groups.
                return group.getProcessGroups().stream()
                    .flatMap(gr -> gr.getInputPorts().stream())
                    .filter(component -> matchesId(component, id))
                    .findAny()
                    .orElse(null);
            }
            case OUTPUT_PORT: {
                final Optional<Port> port = group.getOutputPorts().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny();

                if (port.isPresent()) {
                    return port.get();
                }

                // Attempt to locate child group by versioned component id
                final Optional<ProcessGroup> optionalSpecifiedGroup = group.getProcessGroups().stream()
                    .filter(child -> matchesGroupId(child, connectableComponent.getGroupId()))
                    .findFirst();

                if (optionalSpecifiedGroup.isPresent()) {
                    final ProcessGroup specifiedGroup = optionalSpecifiedGroup.get();
                    return specifiedGroup.getOutputPorts().stream()
                        .filter(component -> matchesId(component, id))
                        .findAny()
                        .orElse(null);
                }

                // If no child group matched the versioned component id, then look at all child groups. This is done because
                // in older versions, we did not properly map Versioned Component ID's to Ports' parent groups. As a result,
                // if the flow doesn't contain the properly mapped group id, we need to search all child groups.
                return group.getProcessGroups().stream()
                    .flatMap(gr -> gr.getOutputPorts().stream())
                    .filter(component -> matchesId(component, id))
                    .findAny()
                    .orElse(null);
            }
            case PROCESSOR:
                return group.getProcessors().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny()
                    .orElse(null);
            case REMOTE_INPUT_PORT: {
                final String rpgId = connectableComponent.getGroupId();
                final Optional<RemoteProcessGroup> rpgOption = group.getRemoteProcessGroups().stream()
                    .filter(component -> rpgId.equals(component.getIdentifier()) || rpgId.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (rpgOption.isEmpty()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                final Optional<RemoteGroupPort> portByIdOption = rpg.getInputPorts().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny();

                if (portByIdOption.isPresent()) {
                    return portByIdOption.get();
                }

                return rpg.getInputPorts().stream()
                    .filter(component -> connectableComponent.getName().equals(component.getName()))
                    .findAny()
                    .orElse(null);
            }
            case REMOTE_OUTPUT_PORT: {
                final String rpgId = connectableComponent.getGroupId();
                final Optional<RemoteProcessGroup> rpgOption = group.getRemoteProcessGroups().stream()
                    .filter(component -> rpgId.equals(component.getIdentifier()) || rpgId.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (rpgOption.isEmpty()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                final Optional<RemoteGroupPort> portByIdOption = rpg.getOutputPorts().stream()
                    .filter(component -> matchesId(component, id))
                    .findAny();

                if (portByIdOption.isPresent()) {
                    return portByIdOption.get();
                }

                return rpg.getOutputPorts().stream()
                    .filter(component -> connectableComponent.getName().equals(component.getName()))
                    .findAny()
                    .orElse(null);
            }
        }

        return null;
    }

    @Override
    public void synchronize(final ReportingTaskNode reportingTask, final VersionedReportingTask proposed, final FlowSynchronizationOptions synchronizationOptions)
            throws FlowSynchronizationException, TimeoutException, InterruptedException, ReportingTaskInstantiationException {

        if (reportingTask == null && proposed == null) {
            return;
        }

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // If reporting task is not null, make sure that it's stopped.
            if (reportingTask != null && reportingTask.isRunning()) {
                reportingTask.stop();
            }

            if (proposed == null) {
                reportingTask.verifyCanDelete();
                context.getFlowManager().removeReportingTask(reportingTask);
                LOG.info("Successfully synchronized {} by removing it from the flow", reportingTask);
            } else if (reportingTask == null) {
                final ReportingTaskNode added = addReportingTask(proposed);
                LOG.info("Successfully synchronized {} by adding it to the flow", added);
            } else {
                updateReportingTask(reportingTask, proposed);
                // Any existing component that is modified during synchronization may have its properties reverted to a pre-migration state,
                // so we then add it to the set to allow migrateProperties to be called again to get it back to the migrated state
                createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(reportingTask, getPropertyValues(reportingTask)));
                LOG.info("Successfully synchronized {} by updating it to match proposed version", reportingTask);
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private ReportingTaskNode addReportingTask(final VersionedReportingTask reportingTask) throws ReportingTaskInstantiationException {
        final BundleCoordinate coordinate = toCoordinate(reportingTask.getBundle());
        final ReportingTaskNode taskNode = context.getFlowManager().createReportingTask(reportingTask.getType(), reportingTask.getInstanceIdentifier(), coordinate, false);
        updateReportingTask(taskNode, reportingTask);

        final Map<String, String> decryptedProperties = getDecryptedProperties(reportingTask.getProperties());
        createdAndModifiedExtensions.add(new CreatedOrModifiedExtension(taskNode, decryptedProperties));

        return taskNode;
    }

    private void updateReportingTask(final ReportingTaskNode reportingTask, final VersionedReportingTask proposed)
            throws ReportingTaskInstantiationException {
        LOG.debug("Updating Reporting Task {}", reportingTask);

        reportingTask.pauseValidationTrigger();
        try {
            reportingTask.setName(proposed.getName());
            reportingTask.setComments(proposed.getComments());
            reportingTask.setSchedulingPeriod(proposed.getSchedulingPeriod());
            reportingTask.setSchedulingStrategy(SchedulingStrategy.valueOf(proposed.getSchedulingStrategy()));
            reportingTask.setAnnotationData(proposed.getAnnotationData());

            if (!isEqual(reportingTask.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(reportingTask.getProperties().keySet());
                final Set<URL> additionalUrls = reportingTask.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(reportingTask, proposed.getType(), newBundleCoordinate, additionalUrls);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(reportingTask, proposed.getProperties(), proposed.getPropertyDescriptors().values());
            reportingTask.setProperties(proposed.getProperties(), false, sensitiveDynamicPropertyNames);

            // enable/disable/start according to the ScheduledState
            switch (proposed.getScheduledState()) {
                case DISABLED:
                    if (reportingTask.isRunning()) {
                        reportingTask.stop();
                    }
                    reportingTask.disable();
                    break;
                case ENABLED:
                    if (reportingTask.getScheduledState() == ScheduledState.DISABLED) {
                        reportingTask.enable();
                    } else if (reportingTask.isRunning()) {
                        reportingTask.stop();
                    }
                    break;
                case RUNNING:
                    if (reportingTask.getScheduledState() == ScheduledState.DISABLED) {
                        reportingTask.enable();
                    }
                    if (!reportingTask.isRunning()) {
                        reportingTask.start();
                    }
                    break;
            }
            notifyScheduledStateChange(reportingTask, syncOptions, proposed.getScheduledState());
        } finally {
            reportingTask.resumeValidationTrigger();
        }
    }

    @Override
    public void synchronize(final FlowAnalysisRuleNode flowAnalysisRule, final VersionedFlowAnalysisRule proposed, final FlowSynchronizationOptions synchronizationOptions)
            throws FlowSynchronizationException, TimeoutException, InterruptedException, FlowAnalysisRuleInstantiationException {

        if (flowAnalysisRule == null && proposed == null) {
            return;
        }

        synchronizationOptions.getComponentScheduler().pause();
        try {
            // If flow analysis rule is not null, make sure that it's disabled.
            if (flowAnalysisRule != null && flowAnalysisRule.isEnabled()) {
                flowAnalysisRule.disable();
            }

            if (proposed == null) {
                flowAnalysisRule.verifyCanDelete();
                context.getFlowManager().removeFlowAnalysisRule(flowAnalysisRule);
                LOG.info("Successfully synchronized {} by removing it from the flow", flowAnalysisRule);
            } else if (flowAnalysisRule == null) {
                final FlowAnalysisRuleNode added = addFlowAnalysisRule(proposed);
                LOG.info("Successfully synchronized {} by adding it to the flow", added);
            } else {
                updateFlowAnalysisRule(flowAnalysisRule, proposed);
                LOG.info("Successfully synchronized {} by updating it to match proposed version", flowAnalysisRule);
            }
        } finally {
            synchronizationOptions.getComponentScheduler().resume();
        }
    }

    private FlowAnalysisRuleNode addFlowAnalysisRule(final VersionedFlowAnalysisRule flowAnalysisRule) throws FlowAnalysisRuleInstantiationException {
        final BundleCoordinate coordinate = toCoordinate(flowAnalysisRule.getBundle());
        final FlowAnalysisRuleNode ruleNode = context.getFlowManager().createFlowAnalysisRule(flowAnalysisRule.getType(), flowAnalysisRule.getInstanceIdentifier(), coordinate, false);
        updateFlowAnalysisRule(ruleNode, flowAnalysisRule);
        return ruleNode;
    }

    private void updateFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRule, final VersionedFlowAnalysisRule proposed)
            throws FlowAnalysisRuleInstantiationException {
        LOG.debug("Updating Flow Analysis Rule {}", flowAnalysisRule);

        flowAnalysisRule.pauseValidationTrigger();
        try {
            flowAnalysisRule.setName(proposed.getName());
            flowAnalysisRule.setComments(proposed.getComments());
            flowAnalysisRule.setEnforcementPolicy(proposed.getEnforcementPolicy());

            if (!isEqual(flowAnalysisRule.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(flowAnalysisRule.getProperties().keySet());
                final Set<URL> additionalUrls = flowAnalysisRule.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(flowAnalysisRule, proposed.getType(), newBundleCoordinate, additionalUrls);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(flowAnalysisRule, proposed.getProperties(), proposed.getPropertyDescriptors().values());
            flowAnalysisRule.setProperties(proposed.getProperties(), false, sensitiveDynamicPropertyNames);

            switch (proposed.getScheduledState()) {
                case DISABLED:
                    if (flowAnalysisRule.isEnabled()) {
                        flowAnalysisRule.disable();
                    }
                    break;
                case ENABLED:
                    if (!flowAnalysisRule.isEnabled()) {
                        flowAnalysisRule.enable();
                    }
                    break;
            }
            notifyScheduledStateChange(flowAnalysisRule, syncOptions, proposed.getScheduledState());
        } finally {
            flowAnalysisRule.resumeValidationTrigger();
        }
    }

    private <T extends org.apache.nifi.components.VersionedComponent & Connectable> boolean matchesId(final T component, final String id) {
        return id.equals(component.getIdentifier()) || id.equals(component.getVersionedComponentId().orElse(NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())));
    }

    private boolean matchesGroupId(final ProcessGroup group, final String groupId) {
        return groupId.equals(group.getIdentifier()) || group.getVersionedComponentId().orElse(
            NiFiRegistryFlowMapper.generateVersionedComponentId(group.getIdentifier())).equals(groupId);
    }

    private void findAllProcessors(final Set<VersionedProcessor> processors, final Set<VersionedProcessGroup> childGroups, final Map<String, VersionedProcessor> map) {
        for (final VersionedProcessor processor : processors) {
            map.put(processor.getIdentifier(), processor);
        }

        for (final VersionedProcessGroup childGroup : childGroups) {
            findAllProcessors(childGroup.getProcessors(), childGroup.getProcessGroups(), map);
        }
    }

    private void findAllControllerServices(final Set<VersionedControllerService> controllerServices, final Set<VersionedProcessGroup> childGroups, final Map<String, VersionedControllerService> map) {
        for (final VersionedControllerService service : controllerServices) {
            map.put(service.getIdentifier(), service);
        }

        for (final VersionedProcessGroup childGroup : childGroups) {
            findAllControllerServices(childGroup.getControllerServices(), childGroup.getProcessGroups(), map);
        }
    }

    private void findAllConnections(final Set<VersionedConnection> connections, final Set<VersionedProcessGroup> childGroups, final Map<String, VersionedConnection> map) {
        for (final VersionedConnection connection : connections) {
            map.put(connection.getIdentifier(), connection);
        }

        for (final VersionedProcessGroup childGroup : childGroups) {
            findAllConnections(childGroup.getConnections(), childGroup.getProcessGroups(), map);
        }
    }

    /**
     * Match components of the given process group to the proposed versioned process group and verify missing components
     * are in a state that they can be safely removed. Specifically, check for removed child process groups and descendants.
     * Optionally also check for removed connections with data in their
     * queue, either because the connections were removed from a matched process group or their group itself was removed.
     *
     * @param processGroup the current process group to examine
     * @param proposedGroup the proposed versioned process group to match with
     * @param verifyConnectionRemoval whether or not to verify that connections that are not present in the proposed flow can be removed
     */
    private void verifyCanRemoveMissingComponents(final ProcessGroup processGroup, final VersionedProcessGroup proposedGroup,
                                                  final boolean verifyConnectionRemoval) {
        if (verifyConnectionRemoval) {
            final Map<String, VersionedConnection> proposedConnectionsByVersionedId = proposedGroup.getConnections().stream()
                .collect(Collectors.toMap(VersionedComponent::getIdentifier, Function.identity()));

            // match group's current connections to proposed connections to determine if they've been removed
            for (final Connection connection : processGroup.getConnections()) {
                final String versionedId = connection.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(connection.getIdentifier()));
                final VersionedConnection proposedConnection = proposedConnectionsByVersionedId.get(versionedId);
                if (proposedConnection == null) {
                    // connection doesn't exist in proposed connections, make sure it doesn't have any data in it
                    final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
                    if (!flowFileQueue.isEmpty()) {
                        throw new IllegalStateException(processGroup + " cannot be updated to the proposed flow because the proposed flow "
                            + "does not contain a match for " + connection + " and the connection currently has data in the queue.");
                    }
                }
            }
        }

        final Map<String, VersionedProcessGroup> proposedGroupsByVersionedId = proposedGroup.getProcessGroups().stream()
            .collect(Collectors.toMap(VersionedComponent::getIdentifier, Function.identity()));

        // match current child groups to proposed child groups to determine if they've been removed
        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            final String versionedId = childGroup.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(childGroup.getIdentifier()));
            final VersionedProcessGroup proposedChildGroup = proposedGroupsByVersionedId.get(versionedId);
            if (proposedChildGroup == null) {
                if (verifyConnectionRemoval) {
                    // check removed group and its descendants for connections with data in the queue
                    final Connection removedConnection = childGroup.findAllConnections().stream()
                        .filter(connection -> !connection.getFlowFileQueue().isEmpty()).findFirst().orElse(null);
                    if (removedConnection != null) {
                        throw new IllegalStateException(processGroup + " cannot be updated to the proposed flow because the proposed flow "
                            + "does not contain a match for " + removedConnection + " and the connection currently has data in the queue.");
                    }
                }
            } else {
                // child group successfully matched, recurse into verification of its contents
                verifyCanRemoveMissingComponents(childGroup, proposedChildGroup, verifyConnectionRemoval);
            }
        }
    }

    private ControllerServiceNode getVersionedControllerService(final ProcessGroup group, final String versionedComponentId) {
        if (group == null) {
            return null;
        }

        for (final ControllerServiceNode serviceNode : group.getControllerServices(false)) {
            final String serviceNodeVersionedComponentId = serviceNode.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(serviceNode.getIdentifier()));
            if (serviceNodeVersionedComponentId.equals(versionedComponentId)) {
                return serviceNode;
            }
        }

        return getVersionedControllerService(group.getParent(), versionedComponentId);
    }

    private Map<String, String> getPropertyValues(final ComponentNode componentNode) {
        final Map<String, String> propertyValues = new HashMap<>();
        if (componentNode.getRawPropertyValues() != null) {
            for (final Map.Entry<PropertyDescriptor, String> entry : componentNode.getRawPropertyValues().entrySet()) {
                propertyValues.put(entry.getKey().getName(), entry.getValue());
            }
        }
        return propertyValues;
    }

    private record CreatedOrModifiedExtension(ComponentNode extension, Map<String, String> propertyValues) {
    }

    private record ParameterValueAndReferences(String value, List<String> assetIds) { }
}
