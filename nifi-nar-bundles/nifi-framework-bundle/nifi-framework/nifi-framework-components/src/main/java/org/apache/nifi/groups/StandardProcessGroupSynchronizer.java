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

package org.apache.nifi.groups;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.encrypt.EncryptionException;
import org.apache.nifi.flow.BatchSize;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
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
import org.apache.nifi.web.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardProcessGroupSynchronizer implements ProcessGroupSynchronizer {
    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessGroupSynchronizer.class);
    private static final String TEMP_FUNNEL_ID_SUFFIX = "-temp-funnel";
    public static final String ENC_PREFIX = "enc{";
    public static final String ENC_SUFFIX = "}";

    private final ProcessGroupSynchronizationContext context;
    private final Set<String> updatedVersionedComponentIds = new HashSet<>();

    private Set<String> preExistingVariables = new HashSet<>();
    private GroupSynchronizationOptions syncOptions;

    public StandardProcessGroupSynchronizer(final ProcessGroupSynchronizationContext context) {
        this.context = context;
    }

    private void setPreExistingVariables(final Set<String> preExistingVariables) {
        this.preExistingVariables = preExistingVariables;
    }

    private void setUpdatedVersionedComponentIds(final Set<String> updatedVersionedComponentIds) {
        this.updatedVersionedComponentIds.clear();
        this.updatedVersionedComponentIds.addAll(updatedVersionedComponentIds);
    }

    public void setSynchronizationOptions(final GroupSynchronizationOptions syncOptions) {
        this.syncOptions = syncOptions;
    }

    @Override
    public void synchronize(final ProcessGroup group, final VersionedExternalFlow versionedExternalFlow, final GroupSynchronizationOptions options) {

        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(context.getExtensionManager(), context.getFlowMappingOptions());
        final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(group, context.getControllerServiceProvider(), context.getFlowRegistryClient(), true);

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Currently Loaded Flow", versionedGroup);
        final ComparableDataFlow proposedFlow = new StandardComparableDataFlow("Proposed Flow", versionedExternalFlow.getFlowContents());

        final PropertyDecryptor decryptor = options.getPropertyDecryptor();
        final FlowComparator flowComparator = new StandardFlowComparator(proposedFlow, localFlow, group.getAncestorServiceIds(),
            new StaticDifferenceDescriptor(), decryptor::decrypt, options.getComponentComparisonIdLookup());
        final FlowComparison flowComparison = flowComparator.compare();

        updatedVersionedComponentIds.clear();
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

        final Set<String> knownVariables = getKnownVariableNames(group);

        preExistingVariables.clear();

        // If we don't want to update existing variables, we need to populate the pre-existing variables so that we know which variables already existed.
        // We can't do this when updating the Variable Registry for a Process Group because variables are inherited, and the variables of the parent group
        // may already have been updated when we get to the point of updating a child's Variable Registry. As a result, we build up a Set of all known
        // Variables before we update the Variable Registries.
        if (!options.isUpdateExistingVariables()) {
            preExistingVariables.addAll(knownVariables);
        }

        context.getFlowManager().withParameterContextResolution(() -> {
            try {
                synchronize(group, versionedExternalFlow.getFlowContents(), versionedExternalFlow.getParameterContexts());
            } catch (final ProcessorInstantiationException pie) {
                throw new RuntimeException(pie);
            }
        });

        group.onComponentModified();
    }

    private void synchronize(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts)
                    throws ProcessorInstantiationException {

        // Some components, such as Processors, may have a Scheduled State of RUNNING in the proposed flow. However, if we
        // transition the service into the RUNNING state, and then we need to update a Connection that is connected to it,
        // updating the Connection will fail because the Connection's source & destination must both be stopped in order to
        // update it. To avoid that, we simply pause the scheduler. Once all updates have been made, we will resume the scheduler.
        context.getComponentScheduler().pause();

        group.setComments(proposed.getComments());

        if (syncOptions.isUpdateSettings()) {
            if (proposed.getName() != null) {
                group.setName(proposed.getName());
            }

            if (proposed.getPosition() != null) {
                group.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
            }
        }

        // Ensure that we create all Parameter Contexts before updating them. This is necessary in case the proposed incoming dataflow has
        // parameter contexts that inherit from one another and neither the inheriting nor inherited parameter context exists.
        if (versionedParameterContexts != null) {
            versionedParameterContexts.values().forEach(this::createParameterContextWithoutReferences);
        }

        updateParameterContext(group, proposed, versionedParameterContexts, context.getComponentIdGenerator());
        updateVariableRegistry(group, proposed);

        final FlowFileConcurrency flowFileConcurrency = proposed.getFlowFileConcurrency() == null ? FlowFileConcurrency.UNBOUNDED :
            FlowFileConcurrency.valueOf(proposed.getFlowFileConcurrency());
        group.setFlowFileConcurrency(flowFileConcurrency);

        final FlowFileOutboundPolicy outboundPolicy = proposed.getFlowFileOutboundPolicy() == null ? FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE :
            FlowFileOutboundPolicy.valueOf(proposed.getFlowFileOutboundPolicy());
        group.setFlowFileOutboundPolicy(outboundPolicy);

        group.setDefaultFlowFileExpiration(proposed.getDefaultFlowFileExpiration());
        group.setDefaultBackPressureObjectThreshold(proposed.getDefaultBackPressureObjectThreshold());
        group.setDefaultBackPressureDataSizeThreshold(proposed.getDefaultBackPressureDataSizeThreshold());

        final VersionedFlowCoordinates remoteCoordinates = proposed.getVersionedFlowCoordinates();
        if (remoteCoordinates == null) {
            group.disconnectVersionControl(false);
        } else {
            final String registryId = context.getFlowRegistryClient().getFlowRegistryId(remoteCoordinates.getRegistryUrl());
            final String bucketId = remoteCoordinates.getBucketId();
            final String flowId = remoteCoordinates.getFlowId();
            final int version = remoteCoordinates.getVersion();

            final FlowRegistry flowRegistry = context.getFlowRegistryClient().getFlowRegistry(registryId);
            final String registryName = flowRegistry == null ? registryId : flowRegistry.getName();

            final VersionedFlowState flowState;
            if (remoteCoordinates.getLatest() == null) {
                flowState = VersionedFlowState.SYNC_FAILURE;
            } else {
                flowState = remoteCoordinates.getLatest() ? VersionedFlowState.UP_TO_DATE : VersionedFlowState.STALE;
            }

            final VersionControlInformation vci = new StandardVersionControlInformation.Builder()
                .registryId(registryId)
                .registryName(registryName)
                .bucketId(bucketId)
                .bucketName(bucketId)
                .flowId(flowId)
                .flowName(flowId)
                .version(version)
                .flowSnapshot(syncOptions.isUpdateGroupVersionControlSnapshot() ? proposed : null)
                .status(new StandardVersionedFlowStatus(flowState, flowState.getDescription()))
                .build();

            group.setVersionControlInformation(vci, Collections.emptyMap());
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

        // Keep track of any processors that have been updated to have auto-terminated relationships so that we can set those
        // auto-terminated relationships after we've handled creating/deleting necessary connections.
        final Map<ProcessorNode, Set<Relationship>> autoTerminatedRelationships = new HashMap<>();

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
        synchronizeControllerServices(group, proposed, controllerServicesByVersionedId);

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
            synchronizeChildGroups(group, proposed, versionedParameterContexts, childGroupsByVersionedId);

            synchronizeFunnels(group, proposed, funnelsByVersionedId);
            synchronizeInputPorts(group, proposed, proposedPortFinalNames, inputPortsByVersionedId);
            synchronizeOutputPorts(group, proposed, proposedPortFinalNames, outputPortsByVersionedId);
            synchronizeLabels(group, proposed, labelsByVersionedId);
            synchronizeProcessors(group, proposed, autoTerminatedRelationships, processorsByVersionedId);
            synchronizeRemoteGroups(group, proposed, rpgsByVersionedId);
        } finally {
            // Make sure that we reset the connections
            restoreConnectionDestinations(group, proposed, connectionsByVersionedId, connectionsWithTempDestination);
            removeTemporaryFunnel(group);
        }

        // We can now add in any necessary connections, since all connectable components have now been created.
        synchronizeConnections(group, proposed, connectionsByVersionedId);

        // Once the appropriate connections have been removed, we may now update Processors' auto-terminated relationships.
        // We cannot do this above, in the 'updateProcessor' call because if a connection is removed and changed to auto-terminated,
        // then updating this in the updateProcessor call above would attempt to set the Relationship to being auto-terminated while a
        // Connection for that relationship exists. This will throw an Exception.
        autoTerminatedRelationships.forEach(ProcessorNode::setAutoTerminatedRelationships);

        // All ports have now been added/removed as necessary. We can now resolve the port names.
        updatePortsToFinalNames(proposedPortFinalNames);

        // Start all components that are queued up to be started now
        context.getComponentScheduler().resume();
    }

    private void synchronizeChildGroups(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                        final Map<String, ProcessGroup> childGroupsByVersionedId) throws ProcessorInstantiationException {

        for (final VersionedProcessGroup proposedChildGroup : proposed.getProcessGroups()) {
            final ProcessGroup childGroup = childGroupsByVersionedId.get(proposedChildGroup.getIdentifier());
            final VersionedFlowCoordinates childCoordinates = proposedChildGroup.getVersionedFlowCoordinates();

            // if there is a nested process group that is version controlled, make sure get the param contexts that go with that snapshot
            // instead of the ones from the parent which would have been passed in to this method
            Map<String, VersionedParameterContext> childParameterContexts = versionedParameterContexts;
            if (childCoordinates != null && syncOptions.isUpdateDescendantVersionedFlows()) {
                final String childParameterContextName = proposedChildGroup.getParameterContextName();
                if (childParameterContextName != null && !versionedParameterContexts.containsKey(childParameterContextName)) {
                    childParameterContexts = getVersionedParameterContexts(childCoordinates);
                } else {
                    childParameterContexts = versionedParameterContexts;
                }
            }

            if (childGroup == null) {
                final ProcessGroup added = addProcessGroup(group, proposedChildGroup, context.getComponentIdGenerator(), preExistingVariables, childParameterContexts);
                context.getFlowManager().onProcessGroupAdded(added);
                added.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
                LOG.info("Added {} to {}", added, group);
            } else if (childCoordinates == null || syncOptions.isUpdateDescendantVersionedFlows()) {

                final StandardProcessGroupSynchronizer sync = new StandardProcessGroupSynchronizer(context);
                sync.setPreExistingVariables(preExistingVariables);
                sync.setUpdatedVersionedComponentIds(updatedVersionedComponentIds);
                final GroupSynchronizationOptions options = GroupSynchronizationOptions.Builder.from(syncOptions)
                    .updateGroupSettings(true)
                    .build();

                sync.setSynchronizationOptions(options);
                sync.synchronize(childGroup, proposedChildGroup, childParameterContexts);

                LOG.info("Updated {}", childGroup);
            }
        }
    }

    private void synchronizeControllerServices(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, ControllerServiceNode> servicesByVersionedId) {
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
                service = addControllerService(group, proposedService.getIdentifier(), proposedService.getInstanceIdentifier(),
                    proposedService.getType(), proposedService.getBundle(), context.getComponentIdGenerator());

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

            updateControllerService(addedService, proposedService);
        }

        // Update all of the Controller Services to match the VersionedControllerService
        for (final Map.Entry<ControllerServiceNode, VersionedControllerService> entry : services.entrySet()) {
            final ControllerServiceNode service = entry.getKey();
            final VersionedControllerService proposedService = entry.getValue();

            if (updatedVersionedComponentIds.contains(proposedService.getIdentifier())) {
                updateControllerService(service, proposedService);
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

        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            connectionsRemoved.remove(proposedConnection.getIdentifier());
        }

        for (final String removedVersionedId : connectionsRemoved) {
            final Connection connection = connectionsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", connection, group);
            group.removeConnection(connection);
            context.getFlowManager().onConnectionRemoved(connection);
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

            // If the Connection's destination didn't change, nothing to do
            final String destinationVersionId = connection.getDestination().getVersionedComponentId().orElse(null);
            final String proposedDestinationId = proposedConnection.getDestination().getId();
            if (Objects.equals(destinationVersionId, proposedDestinationId)) {
                continue;
            }

            // Find the destination of the connection. If the destination doesn't yet exist (because it's part of the proposed Process Group but not yet added),
            // we will set the destination to a temporary destination. Then, after adding components, we will update the destinations again.
            Connectable newDestination = getConnectable(group, proposedConnection.getDestination());
            if (
                newDestination == null
                ||
                (newDestination.getConnectableType() == ConnectableType.OUTPUT_PORT && !newDestination.getProcessGroup().equals(connection.getProcessGroup()))
            ) {
                final Funnel temporaryDestination = getTemporaryFunnel(connection.getProcessGroup());
                LOG.debug("Updated Connection {} to have a temporary destination of {}", connection, temporaryDestination);
                newDestination = temporaryDestination;
                connectionsWithTempDestination.add(proposedConnection.getIdentifier());
            }

            connection.setDestination(newDestination);
        }

        return connectionsWithTempDestination;
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
                                             final Function<VersionedProcessGroup, Collection<V>> getVersionedComponents, final BiConsumer<ProcessGroup, C> removeComponent) {

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


    private void synchronizeProcessors(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<ProcessorNode, Set<Relationship>> autoTerminatedRelationships,
                                                                       final Map<String, ProcessorNode> processorsByVersionedId) throws ProcessorInstantiationException {
        for (final VersionedProcessor proposedProcessor : proposed.getProcessors()) {
            final ProcessorNode processor = processorsByVersionedId.get(proposedProcessor.getIdentifier());
            if (processor == null) {
                final ProcessorNode added = addProcessor(group, proposedProcessor, context.getComponentIdGenerator());
                context.getFlowManager().onProcessorAdded(added);

                final Set<Relationship> proposedAutoTerminated =
                    proposedProcessor.getAutoTerminatedRelationships() == null ? Collections.emptySet() : proposedProcessor.getAutoTerminatedRelationships().stream()
                        .map(added::getRelationship)
                        .collect(Collectors.toSet());
                autoTerminatedRelationships.put(added, proposedAutoTerminated);
                LOG.info("Added {} to {}", added, group);
            } else if (updatedVersionedComponentIds.contains(proposedProcessor.getIdentifier())) {
                updateProcessor(processor, proposedProcessor);

                final Set<Relationship> proposedAutoTerminated =
                    proposedProcessor.getAutoTerminatedRelationships() == null ? Collections.emptySet() : proposedProcessor.getAutoTerminatedRelationships().stream()
                        .map(processor::getRelationship)
                        .collect(Collectors.toSet());

                if (!processor.getAutoTerminatedRelationships().equals(proposedAutoTerminated)) {
                    autoTerminatedRelationships.put(processor, proposedAutoTerminated);
                }

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
    public void verifyCanSynchronize(final ProcessGroup group, final VersionedProcessGroup flowContents, final boolean verifyConnectionRemoval) {
        // Ensure no deleted child process groups contain templates and optionally no deleted connections contain data
        // in their queue. Note that this check enforces ancestry among the group components to avoid a scenario where
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

        // Ensure that all Processors are instantiable
        final Map<String, VersionedProcessor> proposedProcessors = new HashMap<>();
        findAllProcessors(flowContents, proposedProcessors);

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

        // Ensure that all Controller Services are instantiable
        final Map<String, VersionedControllerService> proposedServices = new HashMap<>();
        findAllControllerServices(flowContents, proposedServices);

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

        // Ensure that all Prioritizers are instantiable and that any load balancing configuration is correct
        // Enforcing ancestry on connection matching here is not important because all we're interested in is locating
        // new prioritizers and load balance strategy types so if a matching connection existed anywhere in the current
        // flow, then its prioritizer and load balance strategy are already validated
        final Map<String, VersionedConnection> proposedConnections = new HashMap<>();
        findAllConnections(flowContents, proposedConnections);

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

    private ProcessGroup addProcessGroup(final ProcessGroup destination, final VersionedProcessGroup proposed, final ComponentIdGenerator componentIdGenerator, final Set<String> variablesToSkip,
                                         final Map<String, VersionedParameterContext> versionedParameterContexts) throws ProcessorInstantiationException {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final ProcessGroup group = context.getFlowManager().createProcessGroup(id);
        group.setVersionedComponentId(proposed.getIdentifier());
        group.setParent(destination);
        group.setName(proposed.getName());

        destination.addProcessGroup(group);

        final StandardProcessGroupSynchronizer sync = new StandardProcessGroupSynchronizer(context);
        sync.setPreExistingVariables(variablesToSkip);
        sync.setUpdatedVersionedComponentIds(updatedVersionedComponentIds);

        final GroupSynchronizationOptions options = GroupSynchronizationOptions.Builder.from(syncOptions)
            .updateGroupSettings(true)
            .build();
        sync.setSynchronizationOptions(options);
        sync.synchronize(group, proposed, versionedParameterContexts);

        return group;
    }

    private ControllerServiceNode addControllerService(final ProcessGroup destination, final String versionedId, final String instanceId, final String type, final Bundle bundle,
                                                                         final ComponentIdGenerator componentIdGenerator) {
        final String id = componentIdGenerator.generateUuid(versionedId, instanceId, destination.getIdentifier());
        LOG.debug("Adding Controller Service with ID {} of type {}", id, type);

        final BundleCoordinate coordinate = toCoordinate(bundle);
        final boolean firstTimeAdded = true;
        final Set<URL> additionalUrls = Collections.emptySet();

        final ControllerServiceNode newService = context.getFlowManager().createControllerService(type, id, coordinate, additionalUrls, firstTimeAdded, true, null);
        newService.setVersionedComponentId(versionedId);

        destination.addControllerService(newService);

        return newService;
    }


    private void updateControllerService(final ControllerServiceNode service, final VersionedControllerService proposed) {
        LOG.debug("Updating {}", service);

        service.pauseValidationTrigger();
        try {
            service.setAnnotationData(proposed.getAnnotationData());
            service.setComments(proposed.getComments());
            service.setName(proposed.getName());

            final Map<String, String> properties = populatePropertiesMap(service, proposed.getProperties(), proposed.getPropertyDescriptors(), service.getProcessGroup());
            service.setProperties(properties, true);

            if (!isEqual(service.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(service.getRawPropertyValues().keySet());
                final Set<URL> additionalUrls = service.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(service, proposed.getType(), newBundleCoordinate, additionalUrls);
            }
        } finally {
            service.resumeValidationTrigger();
        }
    }

    private Map<String, String> populatePropertiesMap(final ComponentNode componentNode, final Map<String, String> proposedProperties,
                                                      final Map<String, VersionedPropertyDescriptor> proposedDescriptors, final ProcessGroup group) {

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
                final VersionedPropertyDescriptor descriptor = proposedDescriptors.get(propertyName);

                String value;
                if (descriptor != null && descriptor.getIdentifiesControllerService()) {

                    // Need to determine if the component's property descriptor for this service is already set to an id
                    // of an existing service that is outside the current processor group, and if it is we want to leave
                    // the property set to that value
                    String existingExternalServiceId = null;
                    final PropertyDescriptor componentDescriptor = componentNode.getPropertyDescriptor(propertyName);
                    if (componentDescriptor != null) {
                        final String componentDescriptorValue = componentNode.getEffectivePropertyValue(componentDescriptor);
                        if (componentDescriptorValue != null) {
                            final ProcessGroup parentGroup = group.getParent();
                            if (parentGroup != null) {
                                final ControllerServiceNode serviceNode = parentGroup.findControllerService(componentDescriptorValue, false, true);
                                if (serviceNode != null) {
                                    existingExternalServiceId = componentDescriptorValue;
                                }
                            }
                        }
                    }

                    // If the component's property descriptor is not already set to an id of an existing external service,
                    // then we need to take the Versioned Component ID and resolve this to the instance ID of the service
                    if (existingExternalServiceId == null) {
                        final String serviceVersionedComponentId = proposedProperties.get(propertyName);
                        String instanceId = getServiceInstanceId(serviceVersionedComponentId, group);
                        value = instanceId == null ? serviceVersionedComponentId : instanceId;
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
                final boolean sensitive = componentNode.getPropertyDescriptor(propertyName).isSensitive();
                if (sensitive && value == null) {
                    final PropertyConfiguration propertyConfiguration = componentNode.getProperty(componentNode.getPropertyDescriptor(propertyName));
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

                value = decrypt(value, syncOptions.getPropertyDecryptor());
                fullPropertyMap.put(propertyName, value);
            }
        }

        return fullPropertyMap;
    }

    private static String decrypt(final String value, final PropertyDecryptor decryptor) {
        if (value == null) {
            return null;
        }
        if (!value.startsWith(ENC_PREFIX)) {
            return value;
        }
        if (!value.endsWith(ENC_SUFFIX)) {
            return value;
        }

        try {
            return decryptor.decrypt(value.substring(ENC_PREFIX.length(), value.length() - ENC_SUFFIX.length()));
        } catch (EncryptionException e) {
            final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.xml.gz file";
            throw new EncryptionException(moreDescriptiveMessage, e);
        }
    }


    private void updateParameterContext(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                        final ComponentIdGenerator componentIdGenerator) {
        // Update the Parameter Context
        final ParameterContext currentParamContext = group.getParameterContext();
        final String proposedParameterContextName = proposed.getParameterContextName();
        if (proposedParameterContextName == null && currentParamContext != null) {
            group.setParameterContext(null);
        } else if (proposedParameterContextName != null) {
            final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(proposedParameterContextName);

            if (currentParamContext == null) {
                // Create a new Parameter Context based on the parameters provided

                // Protect against NPE in the event somehow the proposed name is not in the set of contexts
                if (versionedParameterContext == null) {
                    final String paramContextNames = StringUtils.join(versionedParameterContexts.keySet());
                    throw new IllegalStateException("Proposed parameter context name '" + proposedParameterContextName
                        + "' does not exist in set of available parameter contexts [" + paramContextNames + "]");
                }

                final ParameterContext contextByName = getParameterContextByName(versionedParameterContext.getName());
                final ParameterContext selectedParameterContext;
                if (contextByName == null) {
                    final String parameterContextId = componentIdGenerator.generateUuid(versionedParameterContext.getName(), versionedParameterContext.getName(), versionedParameterContext.getName());
                    selectedParameterContext = createParameterContext(versionedParameterContext, parameterContextId, versionedParameterContexts);
                } else {
                    selectedParameterContext = contextByName;
                    addMissingConfiguration(versionedParameterContext, selectedParameterContext, versionedParameterContexts);
                }

                group.setParameterContext(selectedParameterContext);
            } else {
                // Update the current Parameter Context so that it has any Parameters included in the proposed context
                addMissingConfiguration(versionedParameterContext, currentParamContext, versionedParameterContexts);
            }
        }
    }

    private void updateVariableRegistry(final ProcessGroup group, final VersionedProcessGroup proposed) {
        // Determine which variables have been added/removed and add/remove them from this group's variable registry.
        // We don't worry about if a variable value has changed, because variables are designed to be 'environment specific.'
        // As a result, once imported, we won't update variables to match the remote flow, but we will add any missing variables
        // and remove any variables that are no longer part of the remote flow.
        final Map<String, String> existingVariableMap = new HashMap<>();
        group.getVariableRegistry().getVariableMap().forEach((descriptor, value) -> existingVariableMap.put(descriptor.getName(), value));

        final Map<String, String> updatedVariableMap = new HashMap<>();

        // If any new variables exist in the proposed flow, add those to the variable registry.
        for (final Map.Entry<String, String> entry : proposed.getVariables().entrySet()) {
            final String variableName = entry.getKey();
            final String proposedValue = entry.getValue();
            final String existingValue = existingVariableMap.get(variableName);
            final boolean alreadyAccessible = existingVariableMap.containsKey(variableName) || preExistingVariables.contains(variableName);
            final boolean newVariable = !alreadyAccessible;

            if (newVariable || (syncOptions.isUpdateExistingVariables() && !Objects.equals(proposedValue, existingValue))) {
                updatedVariableMap.put(variableName, proposedValue);
            }
        }

        group.setVariables(updatedVariableMap);
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
        return context.getFlowManager().getParameterContextManager().getParameterContexts().stream()
            .filter(context -> context.getName().equals(contextName))
            .findAny()
            .orElse(null);
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
            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
                .name(versionedParameter.getName())
                .description(versionedParameter.getDescription())
                .sensitive(versionedParameter.isSensitive())
                .build();

            final Parameter parameter = new Parameter(descriptor, versionedParameter.getValue());
            parameters.put(versionedParameter.getName(), parameter);
        }

        return context.getFlowManager().createParameterContext(parameterContextId, versionedParameterContext.getName(), parameters, Collections.emptyList());
    }

    private ParameterContext createParameterContext(final VersionedParameterContext versionedParameterContext, final String parameterContextId,
                                                    final Map<String, VersionedParameterContext> versionedParameterContexts) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versionedParameter : versionedParameterContext.getParameters()) {
            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
                .name(versionedParameter.getName())
                .description(versionedParameter.getDescription())
                .sensitive(versionedParameter.isSensitive())
                .build();

            final Parameter parameter = new Parameter(descriptor, versionedParameter.getValue());
            parameters.put(versionedParameter.getName(), parameter);
        }

        final List<String> parameterContextRefs = new ArrayList<>();
        if (versionedParameterContext.getInheritedParameterContexts() != null) {
            versionedParameterContext.getInheritedParameterContexts().stream()
                .map(name -> createParameterReferenceId(name, versionedParameterContexts))
                .forEach(parameterContextRefs::add);
        }

        return context.getFlowManager().createParameterContext(parameterContextId, versionedParameterContext.getName(), parameters, parameterContextRefs);
    }

    private String createParameterReferenceId(final String parameterContextName, final Map<String, VersionedParameterContext> versionedParameterContexts) {
        final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(parameterContextName);
        final ParameterContext selectedParameterContext = selectParameterContext(versionedParameterContext, versionedParameterContexts);
        return selectedParameterContext.getIdentifier();
    }

    private ParameterContext selectParameterContext(final VersionedParameterContext versionedParameterContext, final Map<String, VersionedParameterContext> versionedParameterContexts) {
        final ParameterContext contextByName = getParameterContextByName(versionedParameterContext.getName());
        final ParameterContext selectedParameterContext;
        if (contextByName == null) {
            final String parameterContextId = context.getFlowMappingOptions().getComponentIdLookup().getComponentId(Optional.ofNullable(versionedParameterContext.getIdentifier()),
                versionedParameterContext.getInstanceIdentifier());
            selectedParameterContext = createParameterContext(versionedParameterContext, parameterContextId, versionedParameterContexts);
        } else {
            selectedParameterContext = contextByName;
            addMissingConfiguration(versionedParameterContext, selectedParameterContext, versionedParameterContexts);
        }

        return selectedParameterContext;
    }


    private void addMissingConfiguration(final VersionedParameterContext versionedParameterContext, final ParameterContext currentParameterContext,
                                         final Map<String, VersionedParameterContext> versionedParameterContexts) {
        final Map<String, Parameter> parameters = new HashMap<>();
        for (final VersionedParameter versionedParameter : versionedParameterContext.getParameters()) {
            final Optional<Parameter> parameterOption = currentParameterContext.getParameter(versionedParameter.getName());
            if (parameterOption.isPresent()) {
                // Skip this parameter, since it is already defined. We only want to add missing parameters
                continue;
            }

            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
                .name(versionedParameter.getName())
                .description(versionedParameter.getDescription())
                .sensitive(versionedParameter.isSensitive())
                .build();

            final Parameter parameter = new Parameter(descriptor, versionedParameter.getValue());
            parameters.put(versionedParameter.getName(), parameter);
        }

        currentParameterContext.setParameters(parameters);


        // If the current parameter context doesn't have any inherited param contexts but the versioned one does,
        // add the versioned ones.
        if (versionedParameterContext.getInheritedParameterContexts() != null && !versionedParameterContext.getInheritedParameterContexts().isEmpty()
            && currentParameterContext.getInheritedParameterContexts().isEmpty()) {
            currentParameterContext.setInheritedParameterContexts(versionedParameterContext.getInheritedParameterContexts().stream()
                .map(name -> selectParameterContext(versionedParameterContexts.get(name), versionedParameterContexts))
                .collect(Collectors.toList()));
        }
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

    private Map<String, VersionedParameterContext> getVersionedParameterContexts(final VersionedFlowCoordinates versionedFlowCoordinates) {
        final String registryId = context.getFlowRegistryClient().getFlowRegistryId(versionedFlowCoordinates.getRegistryUrl());
        if (registryId == null) {
            throw new ResourceNotFoundException("Could not find any Flow Registry registered with url: " + versionedFlowCoordinates.getRegistryUrl());
        }

        final FlowRegistry flowRegistry = context.getFlowRegistryClient().getFlowRegistry(registryId);
        if (flowRegistry == null) {
            throw new ResourceNotFoundException("Could not find any Flow Registry registered with identifier " + registryId);
        }

        final String bucketId = versionedFlowCoordinates.getBucketId();
        final String flowId = versionedFlowCoordinates.getFlowId();
        final int flowVersion = versionedFlowCoordinates.getVersion();

        try {
            final VersionedFlowSnapshot childSnapshot = flowRegistry.getFlowContents(bucketId, flowId, flowVersion, false);
            return childSnapshot.getParameterContexts();
        } catch (final NiFiRegistryException e) {
            throw new IllegalArgumentException("The Flow Registry with ID " + registryId + " reports that no Flow exists with Bucket "
                + bucketId + ", Flow " + flowId + ", Version " + flowVersion, e);
        } catch (final IOException ioe) {
            throw new IllegalStateException("Failed to communicate with Flow Registry when attempting to retrieve a versioned flow");
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

    private void updatePort(final Port port, final VersionedPort proposed, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();
        port.setComments(proposed.getComments());
        port.setName(name);
        port.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        port.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());

        context.getComponentScheduler().transitionComponentState(port, proposed.getScheduledState());
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
        destination.addInputPort(port);
        updatePort(port, proposed, temporaryName);

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
        destination.addOutputPort(port);
        updatePort(port, proposed, temporaryName);

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

    private ProcessorNode addProcessor(final ProcessGroup destination, final VersionedProcessor proposed, final ComponentIdGenerator componentIdGenerator) throws ProcessorInstantiationException {
        final String identifier = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        LOG.debug("Adding Processor with ID {} of type {}", identifier, proposed.getType());

        final BundleCoordinate coordinate = toCoordinate(proposed.getBundle());
        final ProcessorNode procNode = context.getFlowManager().createProcessor(proposed.getType(), identifier, coordinate, true);
        procNode.setVersionedComponentId(proposed.getIdentifier());

        destination.addProcessor(procNode);
        updateProcessor(procNode, proposed);

        // Notify the processor node that the configuration (properties, e.g.) has been restored
        final ProcessContext processContext = context.getProcessContextFactory().apply(procNode);
        procNode.onConfigurationRestored(processContext);

        return procNode;
    }

    private void updateProcessor(final ProcessorNode processor, final VersionedProcessor proposed) throws ProcessorInstantiationException {
        LOG.debug("Updating Processor {}", processor);

        processor.pauseValidationTrigger();
        try {
            processor.setAnnotationData(proposed.getAnnotationData());
            processor.setBulletinLevel(LogLevel.valueOf(proposed.getBulletinLevel()));
            processor.setComments(proposed.getComments());
            processor.setName(proposed.getName());
            processor.setPenalizationPeriod(proposed.getPenaltyDuration());

            final Map<String, String> properties = populatePropertiesMap(processor, proposed.getProperties(), proposed.getPropertyDescriptors(), processor.getProcessGroup());
            processor.setProperties(properties, true);
            processor.setRunDuration(proposed.getRunDurationMillis(), TimeUnit.MILLISECONDS);
            processor.setSchedulingStrategy(SchedulingStrategy.valueOf(proposed.getSchedulingStrategy()));
            processor.setScheduldingPeriod(proposed.getSchedulingPeriod());
            processor.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());
            processor.setExecutionNode(ExecutionNode.valueOf(proposed.getExecutionNode()));
            processor.setStyle(proposed.getStyle());
            processor.setYieldPeriod(proposed.getYieldDuration());
            processor.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));

            processor.setMaxBackoffPeriod(proposed.getMaxBackoffPeriod());
            processor.setRetriedRelationships(proposed.getRetriedRelationships());

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

            if (!isEqual(processor.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(processor.getProperties().keySet());
                final Set<URL> additionalUrls = processor.getAdditionalClasspathResources(descriptors);
                context.getReloadComponent().reload(processor, proposed.getType(), newBundleCoordinate, additionalUrls);
            }
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

    private RemoteProcessGroup addRemoteProcessGroup(final ProcessGroup destination, final VersionedRemoteProcessGroup proposed, final ComponentIdGenerator componentIdGenerator) {
        final String id = componentIdGenerator.generateUuid(proposed.getIdentifier(), proposed.getInstanceIdentifier(), destination.getIdentifier());
        final RemoteProcessGroup rpg = context.getFlowManager().createRemoteProcessGroup(id, proposed.getTargetUris());
        rpg.setVersionedComponentId(proposed.getIdentifier());

        destination.addRemoteProcessGroup(rpg);
        updateRemoteProcessGroup(rpg, proposed, componentIdGenerator);

        return rpg;
    }

    private void updateRemoteProcessGroup(final RemoteProcessGroup rpg, final VersionedRemoteProcessGroup proposed, final ComponentIdGenerator componentIdGenerator) {
        rpg.setComments(proposed.getComments());
        rpg.setCommunicationsTimeout(proposed.getCommunicationsTimeout());
        rpg.setInputPorts(proposed.getInputPorts() == null ? Collections.emptySet() : proposed.getInputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdGenerator, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);
        rpg.setName(proposed.getName());
        rpg.setNetworkInterface(proposed.getLocalNetworkInterface());
        rpg.setOutputPorts(proposed.getOutputPorts() == null ? Collections.emptySet() : proposed.getOutputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdGenerator, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);
        rpg.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        rpg.setProxyHost(proposed.getProxyHost());
        rpg.setProxyPort(proposed.getProxyPort());
        rpg.setProxyUser(proposed.getProxyUser());
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

    private RemoteGroupPort getRpgInputPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator) {
        return getRpgPort(port, rpg, componentIdGenerator, RemoteProcessGroup::getInputPort);
    }

    private RemoteGroupPort getRpgOutputPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator) {
        return getRpgPort(port, rpg, componentIdGenerator, RemoteProcessGroup::getOutputPort);
    }

    private RemoteGroupPort getRpgPort(final VersionedRemoteGroupPort port, final RemoteProcessGroup rpg, final ComponentIdGenerator componentIdGenerator,
                                       final BiFunction<RemoteProcessGroup, String, RemoteGroupPort> portLookup) {
        final String instanceId = port.getInstanceIdentifier();
        if (instanceId != null) {
            final RemoteGroupPort remoteGroupPort = portLookup.apply(rpg, instanceId);
            if (remoteGroupPort != null) {
                return remoteGroupPort;
            }
        }

        final String portId = componentIdGenerator.generateUuid(port.getIdentifier(), port.getInstanceIdentifier(), rpg.getIdentifier());
        final RemoteGroupPort remoteGroupPort = portLookup.apply(rpg, portId);
        return remoteGroupPort;
    }

    private void synchronizeTransmissionState(final VersionedRemoteGroupPort versionedPort, final RemoteGroupPort remoteGroupPort) {
        final ScheduledState portState = remoteGroupPort.getScheduledState();

        if (versionedPort.getScheduledState() == org.apache.nifi.flow.ScheduledState.RUNNING) {
            if (portState != ScheduledState.RUNNING) {
                context.getComponentScheduler().startComponent(remoteGroupPort);
            }
        } else {
            if (portState == ScheduledState.RUNNING) {
                context.getComponentScheduler().stopComponent(remoteGroupPort);
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
            throw new IllegalArgumentException("Connection has a source with identifier " + proposed.getIdentifier()
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
        final String id = connectableComponent.getId();

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

                if (!rpgOption.isPresent()) {
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

                if (!rpgOption.isPresent()) {
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

    private <T extends org.apache.nifi.components.VersionedComponent & Connectable> boolean matchesId(final T component, final String id) {
        return id.equals(component.getIdentifier()) || id.equals(component.getVersionedComponentId().orElse(NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())));
    }

    private boolean matchesGroupId(final ProcessGroup group, final String groupId) {
        return groupId.equals(group.getIdentifier()) || group.getVersionedComponentId().orElse(
            NiFiRegistryFlowMapper.generateVersionedComponentId(group.getIdentifier())).equals(groupId);
    }


    private void findAllProcessors(final VersionedProcessGroup group, final Map<String, VersionedProcessor> map) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            map.put(processor.getIdentifier(), processor);
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findAllProcessors(childGroup, map);
        }
    }

    private void findAllControllerServices(final VersionedProcessGroup group, final Map<String, VersionedControllerService> map) {
        for (final VersionedControllerService service : group.getControllerServices()) {
            map.put(service.getIdentifier(), service);
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findAllControllerServices(childGroup, map);
        }
    }

    private void findAllConnections(final VersionedProcessGroup group, final Map<String, VersionedConnection> map) {
        for (final VersionedConnection connection : group.getConnections()) {
            map.put(connection.getIdentifier(), connection);
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findAllConnections(childGroup, map);
        }
    }

    /**
     * Match components of the given process group to the proposed versioned process group and verify missing components
     * are in a state that they can be safely removed. Specifically, check for removed child process groups and descendants.
     * Disallow removal of groups with attached templates. Optionally also check for removed connections with data in their
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
                // child group will be removed, check group and descendants for attached templates
                final Template removedTemplate = childGroup.findAllTemplates().stream().findFirst().orElse(null);
                if (removedTemplate != null) {
                    throw new IllegalStateException(processGroup + " cannot be updated to the proposed flow because the child " + removedTemplate.getProcessGroup()
                        + " that exists locally has one or more Templates, and the proposed flow does not contain these templates. "
                        + "A Process Group cannot be deleted while it contains Templates. Please remove the Templates before re-attempting.");
                }
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


    private Set<String> getKnownVariableNames(final ProcessGroup group) {
        final Set<String> variableNames = new HashSet<>();
        populateKnownVariableNames(group, variableNames);
        return variableNames;
    }

    private void populateKnownVariableNames(final ProcessGroup group, final Set<String> knownVariables) {
        group.getVariableRegistry().getVariableMap().keySet().stream()
            .map(VariableDescriptor::getName)
            .forEach(knownVariables::add);

        final ProcessGroup parent = group.getParent();
        if (parent != null) {
            populateKnownVariableNames(parent, knownVariables);
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
}
