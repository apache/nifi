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
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterUpdate;
import org.apache.nifi.parameter.StandardParameterUpdate;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.BatchSize;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.EvolvingDifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.diff.StaticDifferenceDescriptor;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.SnippetUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class StandardProcessGroup implements ProcessGroup {
    public static final List<DropFlowFileState> AGGREGATE_DROP_FLOW_FILE_STATE_PRECEDENCES = Arrays.asList(
        DropFlowFileState.FAILURE,
        DropFlowFileState.CANCELED,
        DropFlowFileState.DROPPING_FLOWFILES,
        DropFlowFileState.WAITING_FOR_LOCK,
        DropFlowFileState.COMPLETE
    );

    private final String id;
    private final AtomicReference<ProcessGroup> parent;
    private final AtomicReference<String> name;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> comments;
    private final AtomicReference<String> defaultFlowFileExpiration;
    private final AtomicReference<Long> defaultBackPressureObjectThreshold;  // use AtomicReference vs AtomicLong to allow storing null
    private final AtomicReference<String> defaultBackPressureDataSizeThreshold;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicReference<StandardVersionControlInformation> versionControlInfo = new AtomicReference<>();
    private static final SecureRandom randomGenerator = new SecureRandom();

    private final ProcessScheduler scheduler;
    private final ControllerServiceProvider controllerServiceProvider;
    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;
    private final StateManagerProvider stateManagerProvider;
    private final FlowRegistryClient flowRegistryClient;
    private final ReloadComponent reloadComponent;

    private final Map<String, Port> inputPorts = new HashMap<>();
    private final Map<String, Port> outputPorts = new HashMap<>();
    private final Map<String, Connection> connections = new HashMap<>();
    private final Map<String, ProcessGroup> processGroups = new HashMap<>();
    private final Map<String, Label> labels = new HashMap<>();
    private final Map<String, RemoteProcessGroup> remoteGroups = new HashMap<>();
    private final Map<String, ProcessorNode> processors = new HashMap<>();
    private final Map<String, Funnel> funnels = new HashMap<>();
    private final Map<String, ControllerServiceNode> controllerServices = new HashMap<>();
    private final Map<String, Template> templates = new HashMap<>();
    private final PropertyEncryptor encryptor;
    private final MutableVariableRegistry variableRegistry;
    private final VersionControlFields versionControlFields = new VersionControlFields();
    private volatile ParameterContext parameterContext;
    private final NodeTypeProvider nodeTypeProvider;

    private FlowFileConcurrency flowFileConcurrency = FlowFileConcurrency.UNBOUNDED;
    private volatile FlowFileGate flowFileGate = new UnboundedFlowFileGate();
    private volatile FlowFileOutboundPolicy flowFileOutboundPolicy = FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE;
    private volatile BatchCounts batchCounts = new NoOpBatchCounts();
    private final DataValve dataValve;
    private final Long nifiPropertiesBackpressureCount;
    private final String nifiPropertiesBackpressureSize;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessGroup.class);
    private static final String DEFAULT_FLOWFILE_EXPIRATION = "0 sec";
    private static final long DEFAULT_BACKPRESSURE_OBJECT = 10_000L;
    private static final String DEFAULT_BACKPRESSURE_DATA_SIZE = "1 GB";


    public StandardProcessGroup(final String id, final ControllerServiceProvider serviceProvider, final ProcessScheduler scheduler,
                                final PropertyEncryptor encryptor, final ExtensionManager extensionManager,
                                final StateManagerProvider stateManagerProvider, final FlowManager flowManager, final FlowRegistryClient flowRegistryClient,
                                final ReloadComponent reloadComponent, final MutableVariableRegistry variableRegistry, final NodeTypeProvider nodeTypeProvider,
                                final NiFiProperties nifiProperties) {

        this.id = id;
        this.controllerServiceProvider = serviceProvider;
        this.parent = new AtomicReference<>();
        this.scheduler = scheduler;
        this.comments = new AtomicReference<>("");
        this.encryptor = encryptor;
        this.extensionManager = extensionManager;
        this.stateManagerProvider = stateManagerProvider;
        this.variableRegistry = variableRegistry;
        this.flowManager = flowManager;
        this.flowRegistryClient = flowRegistryClient;
        this.reloadComponent = reloadComponent;
        this.nodeTypeProvider = nodeTypeProvider;

        name = new AtomicReference<>();
        position = new AtomicReference<>(new Position(0D, 0D));

        final StateManager dataValveStateManager = stateManagerProvider.getStateManager(id + "-DataValve");
        dataValve = new StandardDataValve(this, dataValveStateManager);

        this.defaultFlowFileExpiration = new AtomicReference<>();
        this.defaultBackPressureObjectThreshold = new AtomicReference<>();
        this.defaultBackPressureDataSizeThreshold = new AtomicReference<>();

        // save only the nifi properties needed, and account for the possibility those properties are missing
        if (nifiProperties == null) {
            nifiPropertiesBackpressureCount = DEFAULT_BACKPRESSURE_OBJECT;
            nifiPropertiesBackpressureSize = DEFAULT_BACKPRESSURE_DATA_SIZE;
        } else {
            // Validate the property values.
            Long count;
            try {
                final String explicitValue = nifiProperties.getProperty(NiFiProperties.BACKPRESSURE_COUNT, String.valueOf(DEFAULT_BACKPRESSURE_OBJECT));
                count = Long.parseLong(explicitValue);
            } catch (final Exception e) {
                LOG.warn("nifi.properties has an invalid value for the '" + NiFiProperties.BACKPRESSURE_COUNT + "' property. Using default value instaed.");
                count = DEFAULT_BACKPRESSURE_OBJECT;
            }
            nifiPropertiesBackpressureCount = count;

            String size;
            try {
                size = nifiProperties.getProperty(NiFiProperties.BACKPRESSURE_SIZE, DEFAULT_BACKPRESSURE_DATA_SIZE);
                DataUnit.parseDataSize(size, DataUnit.B);
            } catch (final Exception e) {
                LOG.warn("nifi.properties has an invalid value for the '" + NiFiProperties.BACKPRESSURE_SIZE + "' property. Using default value instaed.");
                size = DEFAULT_BACKPRESSURE_DATA_SIZE;
            }
            nifiPropertiesBackpressureSize = size;
        }
    }


    @Override
    public ProcessGroup getParent() {
        return parent.get();
    }

    private ProcessGroup getRoot() {
        ProcessGroup root = this;
        while (root.getParent() != null) {
            root = root.getParent();
        }
        return root;
    }

    @Override
    public void setParent(final ProcessGroup newParent) {
        parent.set(newParent);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getParent();
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ProcessGroup, getIdentifier(), getName());
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup parentProcessGroup = getParent();
        if (parentProcessGroup == null) {
            return null;
        } else {
            return parentProcessGroup.getIdentifier();
        }
    }

    @Override
    public String getName() {
        return name.get();
    }

    @Override
    public void setName(final String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("The name of the process group must be specified.");
        }

        this.name.set(name);
    }

    @Override
    public void setPosition(final Position position) {
        this.position.set(position);
    }

    @Override
    public Position getPosition() {
        return position.get();
    }

    @Override
    public String getComments() {
        return this.comments.get();
    }

    @Override
    public void setComments(final String comments) {
        this.comments.set(comments);
    }

    @Override
    public ProcessGroupCounts getCounts() {
        int localInputPortCount = 0;
        int localOutputPortCount = 0;
        int publicInputPortCount = 0;
        int publicOutputPortCount = 0;

        int running = 0;
        int stopped = 0;
        int invalid = 0;
        int disabled = 0;
        int activeRemotePorts = 0;
        int inactiveRemotePorts = 0;

        int upToDate = 0;
        int locallyModified = 0;
        int stale = 0;
        int locallyModifiedAndStale = 0;
        int syncFailure = 0;

        readLock.lock();
        try {
            for (final ProcessorNode procNode : processors.values()) {
                if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
                    disabled++;
                } else if (procNode.isRunning()) {
                    running++;
                } else if (procNode.getValidationStatus() == ValidationStatus.INVALID) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final Port port : inputPorts.values()) {
                if (port instanceof PublicPort) {
                    publicInputPortCount++;
                } else {
                    localInputPortCount++;
                }
                if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                    disabled++;
                } else if (port.isRunning()) {
                    running++;
                } else if (!port.isValid()) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final Port port : outputPorts.values()) {
                if (port instanceof PublicPort) {
                    publicOutputPortCount++;
                } else {
                    localOutputPortCount++;
                }
                if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                    disabled++;
                } else if (port.isRunning()) {
                    running++;
                } else if (!port.isValid()) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            for (final ProcessGroup childGroup : processGroups.values()) {
                final ProcessGroupCounts childCounts = childGroup.getCounts();
                running += childCounts.getRunningCount();
                stopped += childCounts.getStoppedCount();
                invalid += childCounts.getInvalidCount();
                disabled += childCounts.getDisabledCount();

                // update the vci counts for this child group
                final VersionControlInformation vci = childGroup.getVersionControlInformation();
                if (vci != null) {
                    final VersionedFlowStatus flowStatus;
                    try {
                        flowStatus = vci.getStatus();
                    } catch (final Exception e) {
                        LOG.warn("Could not determine Version Control State for {}. Will consider state to be SYNC_FAILURE", this, e);
                        syncFailure++;
                        continue;
                    }

                    switch (flowStatus.getState()) {
                        case LOCALLY_MODIFIED:
                            locallyModified++;
                            break;
                        case LOCALLY_MODIFIED_AND_STALE:
                            locallyModifiedAndStale++;
                            break;
                        case STALE:
                            stale++;
                            break;
                        case SYNC_FAILURE:
                            syncFailure++;
                            break;
                        case UP_TO_DATE:
                            upToDate++;
                            break;
                    }
                }

                // update the vci counts for all nested groups within the child
                upToDate += childCounts.getUpToDateCount();
                locallyModified += childCounts.getLocallyModifiedCount();
                stale += childCounts.getStaleCount();
                locallyModifiedAndStale += childCounts.getLocallyModifiedAndStaleCount();
                syncFailure += childCounts.getSyncFailureCount();
            }

            for (final RemoteProcessGroup remoteGroup : getRemoteProcessGroups()) {
                // Count only input ports that have incoming connections
                for (final Port port : remoteGroup.getInputPorts()) {
                    if (port.hasIncomingConnection()) {
                        if (port.isRunning()) {
                            activeRemotePorts++;
                        } else {
                            inactiveRemotePorts++;
                        }
                    }
                }

                // Count only output ports that have outgoing connections
                for (final Port port : remoteGroup.getOutputPorts()) {
                    if (!port.getConnections().isEmpty()) {
                        if (port.isRunning()) {
                            activeRemotePorts++;
                        } else {
                            inactiveRemotePorts++;
                        }
                    }
                }

                final String authIssue = remoteGroup.getAuthorizationIssue();
                if (authIssue != null) {
                    invalid++;
                }
            }
        } finally {
            readLock.unlock();
        }

        return new ProcessGroupCounts(localInputPortCount, localOutputPortCount, publicInputPortCount, publicOutputPortCount,
            running, stopped, invalid, disabled, activeRemotePorts,
            inactiveRemotePorts, upToDate, locallyModified, stale, locallyModifiedAndStale, syncFailure);
    }

    @Override
    public boolean isRootGroup() {
        return parent.get() == null;
    }

    @Override
    public void startProcessing() {
        readLock.lock();
        try {
            enableAllControllerServices();
            findAllProcessors().stream().filter(START_PROCESSORS_FILTER).forEach(node -> {
                try {
                    node.getProcessGroup().startProcessor(node, true);
                } catch (final Throwable t) {
                    LOG.error("Unable to start processor {} due to {}", new Object[]{node.getIdentifier(), t});
                }
            });

            findAllInputPorts().stream().filter(START_PORTS_FILTER).forEach(port -> port.getProcessGroup().startInputPort(port));

            findAllOutputPorts().stream().filter(START_PORTS_FILTER).forEach(port -> {
                port.getProcessGroup().startOutputPort(port);
            });
        } finally {
            readLock.unlock();
        }

        onComponentModified();
    }

    @Override
    public void stopProcessing() {
        readLock.lock();
        try {
            findAllProcessors().stream().filter(STOP_PROCESSORS_FILTER).forEach(node -> {
                try {
                    node.getProcessGroup().stopProcessor(node);
                } catch (final Throwable t) {
                    LOG.error("Unable to stop processor {}", node.getIdentifier(), t);
                }
            });

            findAllInputPorts().stream().filter(STOP_PORTS_FILTER).forEach(port -> port.getProcessGroup().stopInputPort(port));
            findAllOutputPorts().stream().filter(STOP_PORTS_FILTER).forEach(port -> port.getProcessGroup().stopOutputPort(port));
        } finally {
            readLock.unlock();
        }

        onComponentModified();
    }

    private StateManager getStateManager(final String componentId) {
        return stateManagerProvider.getStateManager(componentId);
    }

    private void shutdown(final ProcessGroup procGroup) {
        for (final ProcessorNode node : procGroup.getProcessors()) {
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, node.getProcessor().getClass(), node.getIdentifier())) {
                final StandardProcessContext processContext = new StandardProcessContext(node, controllerServiceProvider, encryptor,
                    getStateManager(node.getIdentifier()), () -> false, nodeTypeProvider);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, node.getProcessor(), processContext);
            }
        }

        for (final RemoteProcessGroup rpg : procGroup.getRemoteProcessGroups()) {
            rpg.shutdown();
        }

        for (final Connection connection : procGroup.getConnections()) {
            connection.getFlowFileQueue().stopLoadBalancing();
        }

        // Recursively shutdown child groups.
        for (final ProcessGroup group : procGroup.getProcessGroups()) {
            shutdown(group);
        }
    }

    @Override
    public void shutdown() {
        readLock.lock();
        try {
            shutdown(this);
        } finally {
            readLock.unlock();
        }
    }

    private void verifyPortUniqueness(final Port port,
                                      final Map<String, Port> portIdMap,
                                      final Function<String, Port> getPortByName) {

        if (portIdMap.containsKey(requireNonNull(port).getIdentifier())) {
            throw new IllegalStateException("A port with the same id already exists.");
        }

        if (getPortByName.apply(port.getName()) != null) {
            throw new IllegalStateException("A port with the same name already exists.");
        }
    }

    @Override
    public void addInputPort(final Port port) {
        if (isRootGroup()) {
            if (!(port instanceof PublicPort)) {
                throw new IllegalArgumentException("Cannot add Input Port of type " + port.getClass().getName() + " to the Root Group");
            }
        }

        writeLock.lock();
        try {
            // Unique port check within the same group.
            verifyPortUniqueness(port, inputPorts, name -> getInputPortByName(name));

            port.setProcessGroup(this);
            inputPorts.put(requireNonNull(port).getIdentifier(), port);
            flowManager.onInputPortAdded(port);
            onComponentModified();

            LOG.info("Input Port {} added to {}", port, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeInputPort(final Port port) {
        writeLock.lock();
        try {
            final Port toRemove = inputPorts.get(requireNonNull(port).getIdentifier());
            if (toRemove == null) {
                throw new IllegalStateException(port.getIdentifier() + " is not an Input Port of this Process Group");
            }

            port.verifyCanDelete();
            for (final Connection conn : port.getConnections()) {
                conn.verifyCanDelete();
            }

            if (port.isRunning()) {
                stopInputPort(port);
            }

            // must copy to avoid a concurrent modification
            final Set<Connection> copy = new HashSet<>(port.getConnections());
            for (final Connection conn : copy) {
                removeConnection(conn);
            }

            final Port removed = inputPorts.remove(port.getIdentifier());
            if (removed == null) {
                throw new IllegalStateException(port.getIdentifier() + " is not an Input Port of this Process Group");
            }

            scheduler.onPortRemoved(port);
            onComponentModified();

            flowManager.onInputPortRemoved(port);
            LOG.info("Input Port {} removed from flow", port);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Port getInputPort(final String id) {
        readLock.lock();
        try {
            return inputPorts.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Port> getInputPorts() {
        readLock.lock();
        try {
            return new HashSet<>(inputPorts.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addOutputPort(final Port port) {
        if (isRootGroup()) {
            if (!(port instanceof PublicPort)) {
                throw new IllegalArgumentException("Cannot add Output Port " + port.getClass().getName() + " to the Root Group");
            }
        }

        writeLock.lock();
        try {
            // Unique port check within the same group.
            verifyPortUniqueness(port, outputPorts, this::getOutputPortByName);

            port.setProcessGroup(this);
            outputPorts.put(port.getIdentifier(), port);
            flowManager.onOutputPortAdded(port);
            onComponentModified();

            LOG.info("Output Port {} added to {}", port, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeOutputPort(final Port port) {
        writeLock.lock();
        try {
            final Port toRemove = outputPorts.get(requireNonNull(port).getIdentifier());
            toRemove.verifyCanDelete();

            if (port.isRunning()) {
                stopOutputPort(port);
            }

            if (!toRemove.getConnections().isEmpty()) {
                throw new IllegalStateException(port.getIdentifier() + " cannot be removed until its connections are removed");
            }

            final Port removed = outputPorts.remove(port.getIdentifier());
            if (removed == null) {
                throw new IllegalStateException(port.getIdentifier() + " is not an Output Port of this Process Group");
            }

            scheduler.onPortRemoved(port);
            onComponentModified();

            flowManager.onOutputPortRemoved(port);
            LOG.info("Output Port {} removed from flow", port);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Port getOutputPort(final String id) {
        readLock.lock();
        try {
            return outputPorts.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Port> getOutputPorts() {
        readLock.lock();
        try {
            return new HashSet<>(outputPorts.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public BatchCounts getBatchCounts() {
        return batchCounts;
    }

    @Override
    public void addProcessGroup(final ProcessGroup group) {
        if (StringUtils.isEmpty(group.getName())) {
            throw new IllegalArgumentException("Process Group's name must be specified");
        }

        writeLock.lock();
        try {
            group.setParent(this);
            group.getVariableRegistry().setParent(getVariableRegistry());

            processGroups.put(Objects.requireNonNull(group).getIdentifier(), group);
            flowManager.onProcessGroupAdded(group);

            group.findAllControllerServices().forEach(this::updateControllerServiceReferences);
            group.findAllProcessors().forEach(this::updateControllerServiceReferences);

            onComponentModified();

            LOG.info("{} added to {}", group, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ProcessGroup getProcessGroup(final String id) {
        readLock.lock();
        try {
            return processGroups.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<ProcessGroup> getProcessGroups() {
        readLock.lock();
        try {
            return new HashSet<>(processGroups.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeProcessGroup(final ProcessGroup group) {
        requireNonNull(group).verifyCanDelete();

        writeLock.lock();
        try {
            final ProcessGroup toRemove = processGroups.get(group.getIdentifier());
            if (toRemove == null) {
                throw new IllegalStateException(group.getIdentifier() + " is not a member of this Process Group");
            }
            toRemove.verifyCanDelete();

            removeComponents(group);
            processGroups.remove(group.getIdentifier());
            onComponentModified();

            flowManager.onProcessGroupRemoved(group);
            LOG.info("{} removed from flow", group);
        } finally {
            writeLock.unlock();
        }
    }

    private void removeComponents(final ProcessGroup group) {
        for (final Connection connection : new ArrayList<>(group.getConnections())) {
            group.removeConnection(connection);
        }

        for (final Port port : new ArrayList<>(group.getInputPorts())) {
            group.removeInputPort(port);
        }

        for (final Port port : new ArrayList<>(group.getOutputPorts())) {
            group.removeOutputPort(port);
        }

        for (final Funnel funnel : new ArrayList<>(group.getFunnels())) {
            group.removeFunnel(funnel);
        }

        for (final ProcessorNode processor : new ArrayList<>(group.getProcessors())) {
            group.removeProcessor(processor);
        }

        for (final RemoteProcessGroup rpg : new ArrayList<>(group.getRemoteProcessGroups())) {
            group.removeRemoteProcessGroup(rpg);
        }

        for (final Label label : new ArrayList<>(group.getLabels())) {
            group.removeLabel(label);
        }

        for (final ControllerServiceNode cs : group.getControllerServices(false)) {
            // Must go through Controller Service here because we need to ensure that it is removed from the cache
            controllerServiceProvider.removeControllerService(cs);
        }

        for (final ProcessGroup childGroup : new ArrayList<>(group.getProcessGroups())) {
            group.removeProcessGroup(childGroup);
        }
    }

    @Override
    public void addRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {
        writeLock.lock();
        try {
            if (remoteGroups.containsKey(requireNonNull(remoteGroup).getIdentifier())) {
                throw new IllegalStateException("RemoteProcessGroup already exists with ID " + remoteGroup.getIdentifier());
            }

            remoteGroup.setProcessGroup(this);
            remoteGroups.put(Objects.requireNonNull(remoteGroup).getIdentifier(), remoteGroup);
            onComponentModified();

            LOG.info("{} added to {}", remoteGroup, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        readLock.lock();
        try {
            return new HashSet<>(remoteGroups.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeRemoteProcessGroup(final RemoteProcessGroup remoteProcessGroup) {
        final String remoteGroupId = requireNonNull(remoteProcessGroup).getIdentifier();

        writeLock.lock();
        try {
            final RemoteProcessGroup remoteGroup = remoteGroups.get(remoteGroupId);
            if (remoteGroup == null) {
                throw new IllegalStateException(remoteProcessGroup.getIdentifier() + " is not a member of this Process Group");
            }

            remoteGroup.verifyCanDelete();
            for (final RemoteGroupPort port : remoteGroup.getOutputPorts()) {
                for (final Connection connection : port.getConnections()) {
                    connection.verifyCanDelete();
                }
            }

            onComponentModified();

            for (final RemoteGroupPort port : remoteGroup.getOutputPorts()) {
                // must copy to avoid a concurrent modification
                final Set<Connection> copy = new HashSet<>(port.getConnections());
                for (final Connection connection : copy) {
                    removeConnection(connection);
                }
            }

            try {
                remoteGroup.onRemove();
            } catch (final Exception e) {
                LOG.warn("Failed to clean up resources for {} due to {}", remoteGroup, e);
            }

            remoteGroup.getInputPorts().forEach(scheduler::onPortRemoved);
            remoteGroup.getOutputPorts().forEach(scheduler::onPortRemoved);

            scheduler.submitFrameworkTask(new Runnable() {
                @Override
                public void run() {
                    stateManagerProvider.onComponentRemoved(remoteGroup.getIdentifier());
                }
            });

            remoteGroups.remove(remoteGroupId);
            LOG.info("{} removed from flow", remoteProcessGroup);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void addProcessor(final ProcessorNode processor) {
        writeLock.lock();
        try {
            final String processorId = requireNonNull(processor).getIdentifier();
            final ProcessorNode existingProcessor = processors.get(processorId);
            if (existingProcessor != null) {
                throw new IllegalStateException("A processor is already registered to this ProcessGroup with ID " + processorId);
            }

            processor.setProcessGroup(this);
            processor.getVariableRegistry().setParent(getVariableRegistry());
            processors.put(processorId, processor);
            flowManager.onProcessorAdded(processor);
            updateControllerServiceReferences(processor);
            onComponentModified();

            LOG.info("{} added to {}", processor, this);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Looks for any property that is configured on the given component that references a Controller Service.
     * If any exists, and that Controller Service is not accessible from this Process Group, then the given
     * component will be removed from the service's referencing components.
     *
     * @param component the component whose invalid references should be removed
     */
    private void updateControllerServiceReferences(final ComponentNode component) {
        for (final Map.Entry<PropertyDescriptor, String> entry : component.getEffectivePropertyValues().entrySet()) {
            final String serviceId = entry.getValue();
            if (serviceId == null) {
                continue;
            }

            final PropertyDescriptor propertyDescriptor = entry.getKey();
            final Class<? extends ControllerService> serviceClass = propertyDescriptor.getControllerServiceDefinition();

            if (serviceClass != null) {
                final boolean validReference = isValidServiceReference(serviceId, serviceClass, component);
                final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(serviceId);
                if (serviceNode != null) {
                    if (validReference) {
                        serviceNode.addReference(component, propertyDescriptor);
                    } else {
                        serviceNode.removeReference(component, propertyDescriptor);
                    }
                }
            }
        }
    }

    private boolean isValidServiceReference(final String serviceId, final Class<? extends ControllerService> serviceClass, final ComponentNode component) {
        final Set<String> validServiceIds = controllerServiceProvider.getControllerServiceIdentifiers(serviceClass, component.getProcessGroupIdentifier());
        return validServiceIds.contains(serviceId);
    }

    @Override
    public void removeProcessor(final ProcessorNode processor) {
        boolean removed = false;
        final String id = requireNonNull(processor).getIdentifier();
        writeLock.lock();
        try {
            if (!processors.containsKey(id)) {
                throw new IllegalStateException(processor.getIdentifier() + " is not a member of this Process Group");
            }

            processor.verifyCanDelete();
            for (final Connection conn : processor.getConnections()) {
                conn.verifyCanDelete();
            }

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, processor.getProcessor().getClass(), processor.getIdentifier())) {
                final StandardProcessContext processContext = new StandardProcessContext(processor, controllerServiceProvider, encryptor,
                    getStateManager(processor.getIdentifier()), () -> false, nodeTypeProvider);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, processor.getProcessor(), processContext);
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke 'OnRemoved' methods of processor with id " + processor.getIdentifier(), e);
            }

            for (final Map.Entry<PropertyDescriptor, String> entry : processor.getEffectivePropertyValues().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                if (descriptor.getControllerServiceDefinition() != null) {
                    final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                    if (value != null) {
                        final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                        if (serviceNode != null) {
                            serviceNode.removeReference(processor, descriptor);
                        }
                    }
                }
            }

            processors.remove(id);
            onComponentModified();

            scheduler.onProcessorRemoved(processor);
            flowManager.onProcessorRemoved(processor);

            final LogRepository logRepository = LogRepositoryFactory.getRepository(processor.getIdentifier());
            if (logRepository != null) {
                logRepository.removeAllObservers();
            }

            scheduler.submitFrameworkTask(new Runnable() {
                @Override
                public void run() {
                    stateManagerProvider.onComponentRemoved(processor.getIdentifier());
                }
            });

            // must copy to avoid a concurrent modification
            final Set<Connection> copy = new HashSet<>(processor.getConnections());
            for (final Connection conn : copy) {
                removeConnection(conn);
            }

            removed = true;
            LOG.info("{} removed from flow", processor);
        } finally {
            if (removed) {
                try {
                    LogRepositoryFactory.removeRepository(processor.getIdentifier());
                    extensionManager.removeInstanceClassLoader(id);
                } catch (Throwable t) {
                }
            }
            writeLock.unlock();
        }
    }

    @Override
    public Collection<ProcessorNode> getProcessors() {
        readLock.lock();
        try {
            return new ArrayList<>(processors.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ProcessorNode getProcessor(final String id) {
        readLock.lock();
        try {
            return processors.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    private boolean isInputPort(final Connectable connectable) {
        if (connectable.getConnectableType() != ConnectableType.INPUT_PORT) {
            return false;
        }
        return findInputPort(connectable.getIdentifier()) != null;
    }

    private boolean isOutputPort(final Connectable connectable) {
        if (connectable.getConnectableType() != ConnectableType.OUTPUT_PORT) {
            return false;
        }
        return findOutputPort(connectable.getIdentifier()) != null;
    }

    @Override
    public void inheritConnection(final Connection connection) {
        writeLock.lock();
        try {
            connections.put(connection.getIdentifier(), connection);
            onComponentModified();
            connection.setProcessGroup(this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void addConnection(final Connection connection) {
        writeLock.lock();
        try {
            final String id = requireNonNull(connection).getIdentifier();
            final Connection existingConnection = connections.get(id);
            if (existingConnection != null) {
                throw new IllegalStateException("Connection already exists with ID " + id);
            }

            final Connectable source = connection.getSource();
            final Connectable destination = connection.getDestination();
            final ProcessGroup sourceGroup = source.getProcessGroup();
            final ProcessGroup destinationGroup = destination.getProcessGroup();

            // validate the connection is validate wrt to the source & destination groups
            if (isInputPort(source)) { // if source is an input port, its destination must be in the same group unless it's an input port
                if (isInputPort(destination)) { // if destination is input port, it must be in a child group.
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because destination is an Input Port that does not belong to a child Process Group");
                    }
                } else if (sourceGroup != this || destinationGroup != this) {
                    throw new IllegalStateException("Cannot add Connection to Process Group because source and destination are not both in this Process Group");
                }
            } else if (isOutputPort(source)) {
                // if source is an output port, its group must be a child of this group, and its destination must be in this
                // group (processor/output port) or a child group (input port)
                if (!processGroups.containsKey(sourceGroup.getIdentifier())) {
                    throw new IllegalStateException("Cannot add Connection to Process Group because source is an Output Port that does not belong to a child Process Group");
                }

                if (isInputPort(destination)) {
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because its destination is an Input Port that does not belong to a child Process Group");
                    }
                } else if (destinationGroup != this) {
                    throw new IllegalStateException("Cannot add Connection to Process Group because its destination does not belong to this Process Group");
                }
            } else { // source is not a port
                if (sourceGroup != this) {
                    throw new IllegalStateException("Cannot add Connection to Process Group because the source does not belong to this Process Group");
                }

                if (isOutputPort(destination)) {
                    if (destinationGroup != this) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because its destination is an Output Port but does not belong to this Process Group");
                    }
                } else if (isInputPort(destination)) {
                    if (!processGroups.containsKey(destinationGroup.getIdentifier())) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because its destination is an Input "
                            + "Port but the Input Port does not belong to a child Process Group");
                    }
                } else if (destinationGroup != this) {
                    throw new IllegalStateException("Cannot add Connection between " + source.getIdentifier() + " and " + destination.getIdentifier()
                        + " because they are in different Process Groups and neither is an Input Port or Output Port");
                }
            }

            connection.setProcessGroup(this);
            source.addConnection(connection);
            if (source != destination) {  // don't call addConnection twice if it's a self-looping connection.
                destination.addConnection(connection);
            }
            connections.put(connection.getIdentifier(), connection);
            flowManager.onConnectionAdded(connection);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Connectable getConnectable(final String id) {
        readLock.lock();
        try {
            final ProcessorNode node = processors.get(id);
            if (node != null) {
                return node;
            }

            final Port inputPort = inputPorts.get(id);
            if (inputPort != null) {
                return inputPort;
            }

            final Port outputPort = outputPorts.get(id);
            if (outputPort != null) {
                return outputPort;
            }

            final Funnel funnel = funnels.get(id);
            if (funnel != null) {
                return funnel;
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void removeConnection(final Connection connectionToRemove) {
        writeLock.lock();
        try {
            // verify that Connection belongs to this group
            final Connection connection = connections.get(requireNonNull(connectionToRemove).getIdentifier());
            if (connection == null) {
                throw new IllegalStateException("Connection " + connectionToRemove.getIdentifier() + " is not a member of this Process Group");
            }

            connectionToRemove.verifyCanDelete();

            connectionToRemove.getFlowFileQueue().stopLoadBalancing();

            final Connectable source = connectionToRemove.getSource();
            final Connectable dest = connectionToRemove.getDestination();

            // update the source & destination
            source.removeConnection(connection);
            if (source != dest) {
                dest.removeConnection(connection);
            }

            // remove the connection from our map
            connections.remove(connection.getIdentifier());
            LOG.info("{} removed from flow", connection);
            onComponentModified();

            flowManager.onConnectionRemoved(connection);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<Connection> getConnections() {
        readLock.lock();
        try {
            return new HashSet<>(connections.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Connection getConnection(final String id) {
        readLock.lock();
        try {
            return connections.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Connection findConnection(final String id) {
        final Connection connection = flowManager.getConnection(id);
        if (connection == null) {
            return null;
        }

        // We found a Connection in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(connection.getProcessGroup())) {
            return connection;
        }

        return null;
    }

    @Override
    public List<Connection> findAllConnections() {
        return findAllConnections(this);
    }

    @Override
    public DropFlowFileStatus dropAllFlowFiles(String requestIdentifier, String requestor) {
        return handleDropAllFlowFiles(requestIdentifier, queue -> queue.dropFlowFiles(requestIdentifier, requestor));
    }

    @Override
    public DropFlowFileStatus getDropAllFlowFilesStatus(String requestIdentifier) {
        return handleDropAllFlowFiles(requestIdentifier, queue -> queue.getDropFlowFileStatus(requestIdentifier));
    }

    @Override
    public DropFlowFileStatus cancelDropAllFlowFiles(String requestIdentifier) {
        return handleDropAllFlowFiles(requestIdentifier, queue -> queue.cancelDropFlowFileRequest(requestIdentifier));
    }

    private DropFlowFileStatus handleDropAllFlowFiles(String dropRequestId, Function<FlowFileQueue, DropFlowFileStatus> function) {
        DropFlowFileStatus resultDropFlowFileStatus;

        List<Connection> connections = findAllConnections(this);

        DropFlowFileRequest aggregateDropFlowFileStatus = new DropFlowFileRequest(dropRequestId);
        aggregateDropFlowFileStatus.setState(null);

        AtomicBoolean processedAtLeastOne = new AtomicBoolean(false);

        connections.stream()
            .map(Connection::getFlowFileQueue)
            .map(function::apply)
            .forEach(additionalDropFlowFileStatus -> {
                aggregate(aggregateDropFlowFileStatus, additionalDropFlowFileStatus);
                processedAtLeastOne.set(true);
            });

        if (processedAtLeastOne.get()) {
            resultDropFlowFileStatus = aggregateDropFlowFileStatus;
        } else {
            resultDropFlowFileStatus = null;
        }

        return resultDropFlowFileStatus;
    }

    private void aggregate(DropFlowFileRequest aggregateDropFlowFileStatus, DropFlowFileStatus additionalDropFlowFileStatus) {
        QueueSize aggregateOriginalSize = aggregate(aggregateDropFlowFileStatus.getOriginalSize(), additionalDropFlowFileStatus.getOriginalSize());
        QueueSize aggregateDroppedSize = aggregate(aggregateDropFlowFileStatus.getDroppedSize(), additionalDropFlowFileStatus.getDroppedSize());
        QueueSize aggregateCurrentSize = aggregate(aggregateDropFlowFileStatus.getCurrentSize(), additionalDropFlowFileStatus.getCurrentSize());
        DropFlowFileState aggregateState = aggregate(aggregateDropFlowFileStatus.getState(), additionalDropFlowFileStatus.getState());

        aggregateDropFlowFileStatus.setOriginalSize(aggregateOriginalSize);
        aggregateDropFlowFileStatus.setDroppedSize(aggregateDroppedSize);
        aggregateDropFlowFileStatus.setCurrentSize(aggregateCurrentSize);
        aggregateDropFlowFileStatus.setState(aggregateState);
    }

    private QueueSize aggregate(QueueSize size1, QueueSize size2) {
        int objectsNr = Optional.ofNullable(size1)
            .map(size -> size.getObjectCount() + size2.getObjectCount())
            .orElse(size2.getObjectCount());

        long sizeByte = Optional.ofNullable(size1)
            .map(size -> size.getByteCount() + size2.getByteCount())
            .orElse(size2.getByteCount());

        QueueSize aggregateSize = new QueueSize(objectsNr, sizeByte);

        return aggregateSize;
    }

    private DropFlowFileState aggregate(DropFlowFileState state1, DropFlowFileState state2) {
        DropFlowFileState aggregateState = DropFlowFileState.DROPPING_FLOWFILES;

        for (DropFlowFileState aggregateDropFlowFileStatePrecedence : AGGREGATE_DROP_FLOW_FILE_STATE_PRECEDENCES) {
            if (state1 == aggregateDropFlowFileStatePrecedence || state2 == aggregateDropFlowFileStatePrecedence) {
                aggregateState = aggregateDropFlowFileStatePrecedence;
                break;
            }
        }

        return aggregateState;
    }

    private List<Connection> findAllConnections(final ProcessGroup group) {
        final List<Connection> connections = new ArrayList<>(group.getConnections());
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            connections.addAll(findAllConnections(childGroup));
        }
        return connections;
    }

    @Override
    public void addLabel(final Label label) {
        writeLock.lock();
        try {
            final Label existing = labels.get(requireNonNull(label).getIdentifier());
            if (existing != null) {
                throw new IllegalStateException("A label already exists in this ProcessGroup with ID " + label.getIdentifier());
            }

            label.setProcessGroup(this);
            labels.put(label.getIdentifier(), label);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeLabel(final Label label) {
        writeLock.lock();
        try {
            final Label removed = labels.remove(requireNonNull(label).getIdentifier());
            if (removed == null) {
                throw new IllegalStateException(label + " is not a member of this Process Group.");
            }

            onComponentModified();
            LOG.info("Label with ID {} removed from flow", label.getIdentifier());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<Label> getLabels() {
        readLock.lock();
        try {
            return new HashSet<>(labels.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Label getLabel(final String id) {
        readLock.lock();
        try {
            return labels.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return inputPorts.isEmpty() && outputPorts.isEmpty() && connections.isEmpty()
                && processGroups.isEmpty() && labels.isEmpty() && processors.isEmpty() && remoteGroups.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(final String id) {
        readLock.lock();
        try {
            return remoteGroups.get(Objects.requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Future<Void> startProcessor(final ProcessorNode processor, final boolean failIfStopping) {
        readLock.lock();
        try {
            if (getProcessor(processor.getIdentifier()) == null) {
                throw new IllegalStateException("Processor is not a member of this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return CompletableFuture.completedFuture(null);
            }
            processor.reloadAdditionalResourcesIfNecessary();

            return scheduler.startProcessor(processor, failIfStopping);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Future<Void> runProcessorOnce(ProcessorNode processor, Callable<Future<Void>> stopCallback) {
        readLock.lock();
        try {
            if (getProcessor(processor.getIdentifier()) == null) {
                throw new IllegalStateException("Processor is not a member of this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("Processor is already running");
            }
            processor.reloadAdditionalResourcesIfNecessary();

            return scheduler.runProcessorOnce(processor, stopCallback);
        } catch (Exception e) {
            processor.getLogger().error("Error while running processor {} once.", new Object[]{processor}, e);
            return stopProcessor(processor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startInputPort(final Port port) {
        readLock.lock();
        try {
            if (getInputPort(port.getIdentifier()) == null) {
                throw new IllegalStateException("Port " + port.getIdentifier() + " is not a member of this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("InputPort " + port.getIdentifier() + " is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return;
            }

            scheduler.startPort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startOutputPort(final Port port) {
        readLock.lock();
        try {
            if (getOutputPort(port.getIdentifier()) == null) {
                throw new IllegalStateException("Port is not a member of this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("OutputPort is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return;
            }

            scheduler.startPort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startFunnel(final Funnel funnel) {
        readLock.lock();
        try {
            if (getFunnel(funnel.getIdentifier()) == null) {
                throw new IllegalStateException("Funnel is not a member of this Process Group");
            }

            final ScheduledState state = funnel.getScheduledState();
            if (state == ScheduledState.RUNNING) {
                return;
            }
            scheduler.startFunnel(funnel);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Future<Void> stopProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            }

            return scheduler.stopProcessor(processor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void terminateProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state != ScheduledState.STOPPED && state != ScheduledState.RUN_ONCE) {
                throw new IllegalStateException("Cannot terminate processor with ID " + processor.getIdentifier() + " because it is not stopped");
            }

            scheduler.terminateProcessor(processor);
        } finally {
            readLock.unlock();
        }

    }

    @Override
    public void stopInputPort(final Port port) {
        readLock.lock();
        try {
            if (!inputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Input Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("InputPort is disabled");
            } else if (state == ScheduledState.STOPPED) {
                return;
            }

            scheduler.stopPort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void stopOutputPort(final Port port) {
        readLock.lock();
        try {
            if (!outputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Output Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("OutputPort is disabled");
            } else if (state == ScheduledState.STOPPED) {
                return;
            }

            scheduler.stopPort(port);
        } finally {
            readLock.unlock();
        }
    }

    private void stopFunnel(final Funnel funnel) {
        readLock.lock();
        try {
            if (!funnels.containsKey(funnel.getIdentifier())) {
                throw new IllegalStateException("No Funnel with ID " + funnel.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = funnel.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Funnel is disabled");
            } else if (state == ScheduledState.STOPPED) {
                return;
            }

            scheduler.stopFunnel(funnel);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void enableInputPort(final Port port) {
        readLock.lock();
        try {
            if (!inputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Input Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("InputPort is currently running");
            }

            scheduler.enablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void enableOutputPort(final Port port) {
        readLock.lock();
        try {
            if (!outputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No Output Port with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("OutputPort is currently running");
            }

            scheduler.enablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void enableProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No Processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.STOPPED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("Processor is currently running");
            }

            scheduler.enableProcessor(processor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void enableAllControllerServices() {
        // Enable all valid controller services in this process group
        controllerServiceProvider.enableControllerServices(controllerServices.values());

        // Enable all controller services for child process groups
        for (ProcessGroup pg : processGroups.values()) {
            pg.enableAllControllerServices();
        }
    }

    @Override
    public void disableInputPort(final Port port) {
        readLock.lock();
        try {
            if (!inputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No InputPort with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("InputPort is currently running");
            }

            scheduler.disablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void disableOutputPort(final Port port) {
        readLock.lock();
        try {
            if (!outputPorts.containsKey(port.getIdentifier())) {
                throw new IllegalStateException("No OutputPort with ID " + port.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("OutputPort is currently running");
            }

            scheduler.disablePort(port);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void disableProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No Processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                return;
            } else if (state == ScheduledState.RUNNING) {
                throw new IllegalStateException("Processor is currently running");
            }

            scheduler.disableProcessor(processor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof StandardProcessGroup) {
            final StandardProcessGroup other = (StandardProcessGroup) obj;
            return (getIdentifier().equals(other.getIdentifier()));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getIdentifier()).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("identifier", getIdentifier())
            .append("name", getName())
            .toString();
    }

    @Override
    public ProcessGroup findProcessGroup(final String id) {
        if (requireNonNull(id).equals(getIdentifier())) {
            return this;
        }

        final ProcessGroup group = flowManager.getGroup(id);
        if (group == null) {
            return null;
        }

        // We found a Process Group in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(group.getParent())) {
            return group;
        }

        return null;
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups() {
        return findAllProcessGroups(this);
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups(final Predicate<ProcessGroup> filter) {
        final List<ProcessGroup> matching = new ArrayList<>();
        if (filter.test(this)) {
            matching.add(this);
        }

        for (final ProcessGroup group : getProcessGroups()) {
            matching.addAll(group.findAllProcessGroups(filter));
        }

        return matching;
    }

    private List<ProcessGroup> findAllProcessGroups(final ProcessGroup start) {
        final List<ProcessGroup> allProcessGroups = new ArrayList<>(start.getProcessGroups());
        for (final ProcessGroup childGroup : start.getProcessGroups()) {
            allProcessGroups.addAll(findAllProcessGroups(childGroup));
        }
        return allProcessGroups;
    }

    @Override
    public List<RemoteProcessGroup> findAllRemoteProcessGroups() {
        return findAllRemoteProcessGroups(this);
    }

    private List<RemoteProcessGroup> findAllRemoteProcessGroups(final ProcessGroup start) {
        final List<RemoteProcessGroup> remoteGroups = new ArrayList<>(start.getRemoteProcessGroups());
        for (final ProcessGroup childGroup : start.getProcessGroups()) {
            remoteGroups.addAll(findAllRemoteProcessGroups(childGroup));
        }
        return remoteGroups;
    }

    @Override
    public RemoteProcessGroup findRemoteProcessGroup(final String id) {
        return findRemoteProcessGroup(requireNonNull(id), this);
    }

    private RemoteProcessGroup findRemoteProcessGroup(final String id, final ProcessGroup start) {
        RemoteProcessGroup remoteGroup = start.getRemoteProcessGroup(id);
        if (remoteGroup != null) {
            return remoteGroup;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            remoteGroup = findRemoteProcessGroup(id, group);
            if (remoteGroup != null) {
                return remoteGroup;
            }
        }

        return null;
    }

    @Override
    public ProcessorNode findProcessor(final String id) {
        final ProcessorNode node = flowManager.getProcessorNode(id);
        if (node == null) {
            return null;
        }

        // We found a Processor in the Controller, but we only want to return it if
        // the Process Group is this or is a child of this.
        if (isOwner(node.getProcessGroup())) {
            return node;
        }

        return null;
    }

    private boolean isOwner(ProcessGroup owner) {
        while (owner != this && owner != null) {
            owner = owner.getParent();
        }

        return owner == this;

    }

    @Override
    public List<ProcessorNode> findAllProcessors() {
        return findAllProcessors(this);
    }

    private List<ProcessorNode> findAllProcessors(final ProcessGroup start) {
        final List<ProcessorNode> allNodes = new ArrayList<>(start.getProcessors());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allNodes.addAll(findAllProcessors(group));
        }
        return allNodes;
    }

    @Override
    public RemoteGroupPort findRemoteGroupPort(final String identifier) {
        readLock.lock();
        try {
            for (final RemoteProcessGroup remoteGroup : remoteGroups.values()) {
                final RemoteGroupPort remoteInPort = remoteGroup.getInputPort(identifier);
                if (remoteInPort != null) {
                    return remoteInPort;
                }

                final RemoteGroupPort remoteOutPort = remoteGroup.getOutputPort(identifier);
                if (remoteOutPort != null) {
                    return remoteOutPort;
                }
            }

            for (final ProcessGroup childGroup : processGroups.values()) {
                final RemoteGroupPort childGroupRemoteGroupPort = childGroup.findRemoteGroupPort(identifier);
                if (childGroupRemoteGroupPort != null) {
                    return childGroupRemoteGroupPort;
                }
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Label findLabel(final String id) {
        return findLabel(id, this);
    }

    private Label findLabel(final String id, final ProcessGroup start) {
        Label label = start.getLabel(id);
        if (label != null) {
            return label;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            label = findLabel(id, group);
            if (label != null) {
                return label;
            }
        }

        return null;
    }

    @Override
    public List<Label> findAllLabels() {
        return findAllLabels(this);
    }

    private List<Label> findAllLabels(final ProcessGroup start) {
        final List<Label> allLabels = new ArrayList<>(start.getLabels());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allLabels.addAll(findAllLabels(group));
        }
        return allLabels;
    }

    @Override
    public Port findInputPort(final String id) {
        final Port port = flowManager.getInputPort(id);
        if (port == null) {
            return null;
        }

        if (isOwner(port.getProcessGroup())) {
            return port;
        }

        return null;
    }

    @Override
    public List<Port> findAllInputPorts() {
        return findAllInputPorts(this);
    }

    private List<Port> findAllInputPorts(final ProcessGroup start) {
        final List<Port> allOutputPorts = new ArrayList<>(start.getInputPorts());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allOutputPorts.addAll(findAllInputPorts(group));
        }
        return allOutputPorts;
    }

    @Override
    public Port findOutputPort(final String id) {
        final Port port = flowManager.getOutputPort(id);
        if (port == null) {
            return null;
        }

        if (isOwner(port.getProcessGroup())) {
            return port;
        }

        return null;
    }

    @Override
    public List<Port> findAllOutputPorts() {
        return findAllOutputPorts(this);
    }

    private List<Port> findAllOutputPorts(final ProcessGroup start) {
        final List<Port> allOutputPorts = new ArrayList<>(start.getOutputPorts());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allOutputPorts.addAll(findAllOutputPorts(group));
        }
        return allOutputPorts;
    }

    @Override
    public List<Funnel> findAllFunnels() {
        return findAllFunnels(this);
    }

    private List<Funnel> findAllFunnels(final ProcessGroup start) {
        final List<Funnel> allFunnels = new ArrayList<>(start.getFunnels());
        for (final ProcessGroup group : start.getProcessGroups()) {
            allFunnels.addAll(findAllFunnels(group));
        }
        return allFunnels;
    }

    @Override
    public Port getInputPortByName(final String name) {
        return getPortByName(name, this, new InputPortRetriever());
    }

    @Override
    public Port getOutputPortByName(final String name) {
        return getPortByName(name, this, new OutputPortRetriever());
    }

    private interface PortRetriever {

        Port getPort(ProcessGroup group, String id);

        Set<Port> getPorts(ProcessGroup group);
    }

    private static class InputPortRetriever implements PortRetriever {

        @Override
        public Set<Port> getPorts(final ProcessGroup group) {
            return group.getInputPorts();
        }

        @Override
        public Port getPort(final ProcessGroup group, final String id) {
            return group.getInputPort(id);
        }
    }

    private static class OutputPortRetriever implements PortRetriever {

        @Override
        public Set<Port> getPorts(final ProcessGroup group) {
            return group.getOutputPorts();
        }

        @Override
        public Port getPort(final ProcessGroup group, final String id) {
            return group.getOutputPort(id);
        }
    }

    private Port getPortByName(final String name, final ProcessGroup group, final PortRetriever retriever) {
        for (final Port port : retriever.getPorts(group)) {
            if (port.getName().equals(name)) {
                return port;
            }
        }

        return null;
    }

    @Override
    public void addFunnel(final Funnel funnel) {
        addFunnel(funnel, true);
    }

    @Override
    public void addFunnel(final Funnel funnel, final boolean autoStart) {
        writeLock.lock();
        try {
            final Funnel existing = funnels.get(requireNonNull(funnel).getIdentifier());
            if (existing != null) {
                throw new IllegalStateException("A funnel already exists in this ProcessGroup with ID " + funnel.getIdentifier());
            }

            funnel.setProcessGroup(this);
            funnels.put(funnel.getIdentifier(), funnel);
            flowManager.onFunnelAdded(funnel);

            if (autoStart) {
                startFunnel(funnel);
            }

            onComponentModified();
            LOG.info("{} added to {}", funnel, this);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Funnel getFunnel(final String id) {
        readLock.lock();
        try {
            return funnels.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Funnel findFunnel(final String id) {
        final Funnel funnel = flowManager.getFunnel(id);
        if (funnel == null) {
            return funnel;
        }

        if (isOwner(funnel.getProcessGroup())) {
            return funnel;
        }

        return null;
    }

    @Override
    public ControllerServiceNode findControllerService(final String id, final boolean includeDescendants, final boolean includeAncestors) {
        ControllerServiceNode serviceNode;
        if (includeDescendants) {
            serviceNode = findDescendantControllerService(id, this);
        } else {
            serviceNode = getControllerService(id);
        }

        if (serviceNode == null && includeAncestors) {
            serviceNode = findAncestorControllerService(id, getParent());
        }

        return serviceNode;
    }

    private ControllerServiceNode findAncestorControllerService(final String id, final ProcessGroup start) {
        if (start == null) {
            return null;
        }

        final ControllerServiceNode serviceNode = start.getControllerService(id);
        if (serviceNode != null) {
            return serviceNode;
        }

        final ProcessGroup parent = start.getParent();
        return findAncestorControllerService(id, parent);
    }

    private ControllerServiceNode findDescendantControllerService(final String id, final ProcessGroup start) {
        ControllerServiceNode service = start.getControllerService(id);
        if (service != null) {
            return service;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            service = findDescendantControllerService(id, group);
            if (service != null) {
                return service;
            }
        }

        return null;
    }

    @Override
    public Set<ControllerServiceNode> findAllControllerServices() {
        return findAllControllerServices(this);
    }

    private Set<ControllerServiceNode> findAllControllerServices(ProcessGroup start) {
        final Set<ControllerServiceNode> services = start.getControllerServices(false);
        for (final ProcessGroup group : start.getProcessGroups()) {
            services.addAll(findAllControllerServices(group));
        }

        return services;
    }

    @Override
    public void removeFunnel(final Funnel funnel) {
        writeLock.lock();
        try {
            final Funnel existing = funnels.get(requireNonNull(funnel).getIdentifier());
            if (existing == null) {
                throw new IllegalStateException("Funnel " + funnel.getIdentifier() + " is not a member of this ProcessGroup");
            }

            funnel.verifyCanDelete();
            for (final Connection conn : funnel.getConnections()) {
                conn.verifyCanDelete();
            }

            stopFunnel(funnel);

            // must copy to avoid a concurrent modification
            final Set<Connection> copy = new HashSet<>(funnel.getConnections());
            for (final Connection conn : copy) {
                removeConnection(conn);
            }

            funnels.remove(funnel.getIdentifier());
            onComponentModified();

            flowManager.onFunnelRemoved(funnel);
            LOG.info("{} removed from flow", funnel);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<Funnel> getFunnels() {
        readLock.lock();
        try {
            return new HashSet<>(funnels.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addControllerService(final ControllerServiceNode service) {
        writeLock.lock();
        try {
            final String id = requireNonNull(service).getIdentifier();
            final ControllerServiceNode existingService = controllerServices.get(id);
            if (existingService != null) {
                throw new IllegalStateException("A Controller Service is already registered to this ProcessGroup with ID " + id);
            }

            service.setProcessGroup(this);
            service.getVariableRegistry().setParent(getVariableRegistry());
            this.controllerServices.put(service.getIdentifier(), service);
            LOG.info("{} added to {}", service, this);
            updateControllerServiceReferences(service);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ControllerServiceNode getControllerService(final String id) {
        readLock.lock();
        try {
            return controllerServices.get(requireNonNull(id));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<ControllerServiceNode> getControllerServices(final boolean recursive) {
        final Set<ControllerServiceNode> services = new HashSet<>();

        readLock.lock();
        try {
            services.addAll(controllerServices.values());
        } finally {
            readLock.unlock();
        }

        if (recursive) {
            final ProcessGroup parentGroup = parent.get();
            if (parentGroup != null) {
                services.addAll(parentGroup.getControllerServices(true));
            }
        }

        return services;
    }

    @Override
    public void removeControllerService(final ControllerServiceNode service) {
        boolean removed = false;
        writeLock.lock();
        try {
            final ControllerServiceNode existing = controllerServices.get(requireNonNull(service).getIdentifier());
            if (existing == null) {
                throw new IllegalStateException("ControllerService " + service.getIdentifier() + " is not a member of this Process Group");
            }

            service.verifyCanDelete();

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, service.getControllerServiceImplementation().getClass(), service.getIdentifier())) {
                final ConfigurationContext configurationContext = new StandardConfigurationContext(service, controllerServiceProvider, null, variableRegistry);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, service.getControllerServiceImplementation(), configurationContext);
            }

            for (final Map.Entry<PropertyDescriptor, String> entry : service.getEffectivePropertyValues().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                if (descriptor.getControllerServiceDefinition() != null) {
                    final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                    if (value != null) {
                        final ControllerServiceNode referencedNode = controllerServiceProvider.getControllerServiceNode(value);
                        if (referencedNode != null) {
                            referencedNode.removeReference(service, descriptor);
                        }
                    }
                }
            }

            controllerServices.remove(service.getIdentifier());
            onComponentModified();

            // For any component that references this Controller Service, find the component's Process Group
            // and notify the Process Group that a component has been modified. This way, we know to re-calculate
            // whether or not the Process Group has local modifications.
            service.getReferences().getReferencingComponents().stream()
                .map(ComponentNode::getProcessGroupIdentifier)
                .filter(id -> !id.equals(getIdentifier()))
                .forEach(groupId -> {
                    final ProcessGroup descendant = findProcessGroup(groupId);
                    if (descendant != null) {
                        descendant.onComponentModified();
                    }
                });

            stateManagerProvider.onComponentRemoved(service.getIdentifier());

            removed = true;
            LOG.info("{} removed from {}", service, this);
        } finally {
            if (removed) {
                try {
                    extensionManager.removeInstanceClassLoader(service.getIdentifier());
                } catch (Throwable t) {
                }
            }
            writeLock.unlock();
        }
    }

    @Override
    public void addTemplate(final Template template) {
        requireNonNull(template);

        writeLock.lock();
        try {
            final String id = template.getDetails().getId();
            if (id == null) {
                throw new IllegalStateException("Cannot add template that has no ID");
            }

            if (templates.containsKey(id)) {
                throw new IllegalStateException("Process Group already contains a Template with ID " + id);
            }

            templates.put(id, template);
            template.setProcessGroup(this);
            LOG.info("{} added to {}", template, this);
            onComponentModified();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Template getTemplate(final String id) {
        readLock.lock();
        try {
            return templates.get(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Template findTemplate(final String id) {
        return findTemplate(id, this);
    }

    private Template findTemplate(final String id, final ProcessGroup start) {
        final Template template = start.getTemplate(id);
        if (template != null) {
            return template;
        }

        for (final ProcessGroup child : start.getProcessGroups()) {
            final Template childTemplate = findTemplate(id, child);
            if (childTemplate != null) {
                return childTemplate;
            }
        }

        return null;
    }

    @Override
    public Set<Template> getTemplates() {
        readLock.lock();
        try {
            return new HashSet<>(templates.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Template> findAllTemplates() {
        return findAllTemplates(this);
    }

    private Set<Template> findAllTemplates(final ProcessGroup group) {
        final Set<Template> templates = new HashSet<>(group.getTemplates());
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            templates.addAll(findAllTemplates(childGroup));
        }
        return templates;
    }

    @Override
    public void removeTemplate(final Template template) {
        writeLock.lock();
        try {
            final Template existing = templates.get(requireNonNull(template).getIdentifier());
            if (existing == null) {
                throw new IllegalStateException("Template " + template.getIdentifier() + " is not a member of this ProcessGroup");
            }

            templates.remove(template.getIdentifier());
            onComponentModified();

            LOG.info("{} removed from flow", template);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void remove(final Snippet snippet) {
        writeLock.lock();
        try {
            // ensure that all components are valid
            verifyContents(snippet);

            final Set<Connectable> connectables = getAllConnectables(snippet);
            final Set<String> connectionIdsToRemove = new HashSet<>(getKeys(snippet.getConnections()));
            // Remove all connections that are the output of any Connectable.
            for (final Connectable connectable : connectables) {
                for (final Connection conn : connectable.getConnections()) {
                    if (!connections.containsKey(conn.getIdentifier())) {
                        throw new IllegalStateException("Connectable component " + connectable.getIdentifier()
                            + " cannot be removed because it has incoming connections from the parent Process Group");
                    }
                    connectionIdsToRemove.add(conn.getIdentifier());
                }
            }

            // verify that all connections can be removed
            for (final String id : connectionIdsToRemove) {
                connections.get(id).verifyCanDelete();
            }

            // verify that all processors are stopped and have no active threads
            for (final String procId : snippet.getProcessors().keySet()) {
                final ProcessorNode procNode = getProcessor(procId);
                if (procNode.isRunning()) {
                    throw new IllegalStateException("Processor " + procNode.getIdentifier() + " cannot be removed because it is running");
                }
                final int activeThreadCount = procNode.getActiveThreadCount();
                if (activeThreadCount != 0) {
                    throw new IllegalStateException("Processor " + procNode.getIdentifier() + " cannot be removed because it still has " + activeThreadCount + " active threads");
                }
            }

            // verify that none of the connectables have incoming connections that are not in the Snippet.
            final Set<String> connectionIds = snippet.getConnections().keySet();
            for (final Connectable connectable : connectables) {
                for (final Connection conn : connectable.getIncomingConnections()) {
                    if (!connectionIds.contains(conn.getIdentifier()) && !connectables.contains(conn.getSource())) {
                        throw new IllegalStateException("Connectable component " + connectable.getIdentifier() + " cannot be removed because it has incoming connections "
                            + "that are not selected to be deleted");
                    }
                }
            }

            // verify that all of the ProcessGroups in the snippet are empty
            for (final String groupId : snippet.getProcessGroups().keySet()) {
                final ProcessGroup toRemove = getProcessGroup(groupId);
                toRemove.verifyCanDelete(true);
            }

            onComponentModified();

            for (final String id : connectionIdsToRemove) {
                removeConnection(connections.get(id));
            }
            for (final String id : getKeys(snippet.getInputPorts())) {
                removeInputPort(inputPorts.get(id));
            }
            for (final String id : getKeys(snippet.getOutputPorts())) {
                removeOutputPort(outputPorts.get(id));
            }
            for (final String id : getKeys(snippet.getFunnels())) {
                removeFunnel(funnels.get(id));
            }
            for (final String id : getKeys(snippet.getLabels())) {
                removeLabel(labels.get(id));
            }
            for (final String id : getKeys(snippet.getProcessors())) {
                removeProcessor(processors.get(id));
            }
            for (final String id : getKeys(snippet.getRemoteProcessGroups())) {
                removeRemoteProcessGroup(remoteGroups.get(id));
            }
            for (final String id : getKeys(snippet.getProcessGroups())) {
                removeProcessGroup(processGroups.get(id));
            }
        } finally {
            writeLock.unlock();
        }
    }

    private Set<String> getKeys(final Map<String, Revision> map) {
        return (map == null) ? Collections.emptySet() : map.keySet();
    }

    @Override
    public void move(final Snippet snippet, final ProcessGroup destination) {
        writeLock.lock();
        try {
            verifyContents(snippet);
            verifyDestinationNotInSnippet(snippet, destination);
            SnippetUtils.verifyNoVersionControlConflicts(snippet, this, destination);

            if (!isDisconnected(snippet)) {
                throw new IllegalStateException("One or more components within the snippet is connected to a component outside of the snippet. Only a disconnected snippet may be moved.");
            }

            if (destination.isRootGroup() && (
                snippet.getInputPorts().keySet().stream().map(this::getInputPort).anyMatch(port -> port instanceof LocalPort)
                    || snippet.getOutputPorts().keySet().stream().map(this::getOutputPort).anyMatch(port -> port instanceof LocalPort))) {
                throw new IllegalStateException("Cannot move local Ports into the root group");
            }

            onComponentModified();

            for (final String id : getKeys(snippet.getInputPorts())) {
                destination.addInputPort(inputPorts.remove(id));
            }
            for (final String id : getKeys(snippet.getOutputPorts())) {
                destination.addOutputPort(outputPorts.remove(id));
            }
            for (final String id : getKeys(snippet.getFunnels())) {
                destination.addFunnel(funnels.remove(id));
            }
            for (final String id : getKeys(snippet.getLabels())) {
                destination.addLabel(labels.remove(id));
            }
            for (final String id : getKeys(snippet.getProcessGroups())) {
                destination.addProcessGroup(processGroups.remove(id));
            }
            for (final String id : getKeys(snippet.getProcessors())) {
                destination.addProcessor(processors.remove(id));
            }
            for (final String id : getKeys(snippet.getRemoteProcessGroups())) {
                destination.addRemoteProcessGroup(remoteGroups.remove(id));
            }
            for (final String id : getKeys(snippet.getConnections())) {
                destination.inheritConnection(connections.remove(id));
            }
        } finally {
            writeLock.unlock();
        }
    }

    private Set<Connectable> getAllConnectables(final Snippet snippet) {
        final Set<Connectable> connectables = new HashSet<>();
        for (final String id : getKeys(snippet.getInputPorts())) {
            connectables.add(getInputPort(id));
        }
        for (final String id : getKeys(snippet.getOutputPorts())) {
            connectables.add(getOutputPort(id));
        }
        for (final String id : getKeys(snippet.getFunnels())) {
            connectables.add(getFunnel(id));
        }
        for (final String id : getKeys(snippet.getProcessors())) {
            connectables.add(getProcessor(id));
        }
        return connectables;
    }

    private boolean isDisconnected(final Snippet snippet) {
        final Set<Connectable> connectables = getAllConnectables(snippet);

        for (final String id : getKeys(snippet.getRemoteProcessGroups())) {
            final RemoteProcessGroup remoteGroup = getRemoteProcessGroup(id);
            connectables.addAll(remoteGroup.getInputPorts());
            connectables.addAll(remoteGroup.getOutputPorts());
        }

        final Set<String> connectionIds = snippet.getConnections().keySet();
        for (final Connectable connectable : connectables) {
            for (final Connection conn : connectable.getIncomingConnections()) {
                if (!connectionIds.contains(conn.getIdentifier())) {
                    return false;
                }
            }

            for (final Connection conn : connectable.getConnections()) {
                if (!connectionIds.contains(conn.getIdentifier())) {
                    return false;
                }
            }
        }

        final Set<Connectable> recursiveConnectables = new HashSet<>(connectables);
        for (final String id : snippet.getProcessGroups().keySet()) {
            final ProcessGroup childGroup = getProcessGroup(id);
            recursiveConnectables.addAll(findAllConnectables(childGroup, true));
        }

        for (final String id : connectionIds) {
            final Connection connection = getConnection(id);
            if (!recursiveConnectables.contains(connection.getSource()) || !recursiveConnectables.contains(connection.getDestination())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Set<Positionable> findAllPositionables() {
        final Set<Positionable> positionables = new HashSet<>();
        positionables.addAll(findAllConnectables(this, true));
        List<ProcessGroup> allProcessGroups = findAllProcessGroups();
        positionables.addAll(allProcessGroups);
        positionables.addAll(findAllRemoteProcessGroups());
        positionables.addAll(findAllLabels());
        return positionables;
    }

    private Set<Connectable> findAllConnectables(final ProcessGroup group, final boolean includeRemotePorts) {
        final Set<Connectable> set = new HashSet<>();
        set.addAll(group.getInputPorts());
        set.addAll(group.getOutputPorts());
        set.addAll(group.getFunnels());
        set.addAll(group.getProcessors());
        if (includeRemotePorts) {
            for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
                set.addAll(remoteGroup.getInputPorts());
                set.addAll(remoteGroup.getOutputPorts());
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            set.addAll(findAllConnectables(childGroup, includeRemotePorts));
        }

        return set;
    }

    /**
     * Verifies that all ID's defined within the given snippet reference
     * components within this ProcessGroup. If this is not the case, throws
     * {@link IllegalStateException}.
     *
     * @param snippet the snippet
     *
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if the snippet contains an ID that
     * references a component that is not part of this ProcessGroup
     */
    private void verifyContents(final Snippet snippet) throws NullPointerException, IllegalStateException {
        requireNonNull(snippet);

        verifyAllKeysExist(snippet.getInputPorts().keySet(), inputPorts, "Input Port");
        verifyAllKeysExist(snippet.getOutputPorts().keySet(), outputPorts, "Output Port");
        verifyAllKeysExist(snippet.getFunnels().keySet(), funnels, "Funnel");
        verifyAllKeysExist(snippet.getLabels().keySet(), labels, "Label");
        verifyAllKeysExist(snippet.getProcessGroups().keySet(), processGroups, "Process Group");
        verifyAllKeysExist(snippet.getProcessors().keySet(), processors, "Processor");
        verifyAllKeysExist(snippet.getRemoteProcessGroups().keySet(), remoteGroups, "Remote Process Group");
        verifyAllKeysExist(snippet.getConnections().keySet(), connections, "Connection");
    }

    /**
     * Verifies that a move request cannot attempt to move a process group into itself.
     *
     * @param snippet the snippet
     * @param destination the destination
     *
     * @throws IllegalStateException if the snippet contains an ID that is equal to the identifier of the destination
     */
    private void verifyDestinationNotInSnippet(final Snippet snippet, final ProcessGroup destination) throws IllegalStateException {
        if (snippet.getProcessGroups() != null && destination != null) {
            snippet.getProcessGroups().forEach((processGroupId, revision) -> {
                if (processGroupId.equals(destination.getIdentifier())) {
                    throw new IllegalStateException("Unable to move Process Group into itself.");
                }
            });
        }
    }

    /**
     * <p>
     * Verifies that all ID's specified by the given set exist as keys in the
     * given Map. If any of the ID's does not exist as a key in the map, will
     * throw {@link IllegalStateException} indicating the ID that is invalid and
     * specifying the Component Type.
     * </p>
     *
     * <p>
     * If the ids given are null, will do no validation.
     * </p>
     *
     * @param ids ids
     * @param map map
     * @param componentType type
     */
    private void verifyAllKeysExist(final Set<String> ids, final Map<String, ?> map, final String componentType) {
        if (ids != null) {
            for (final String id : ids) {
                if (!map.containsKey(id)) {
                    throw new IllegalStateException("ID " + id + " does not refer to a(n) " + componentType + " in this ProcessGroup");
                }
            }
        }
    }

    @Override
    public void verifyCanAddTemplate(final String name) {
        // ensure the name is specified
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Template name cannot be blank.");
        }

        for (final Template template : getRoot().findAllTemplates()) {
            final TemplateDTO existingDto = template.getDetails();

            // ensure a template with this name doesnt already exist
            if (name.equals(existingDto.getName())) {
                throw new IllegalStateException(String.format("A template named '%s' already exists.", name));
            }
        }
    }

    @Override
    public void verifyCanDelete() {
        verifyCanDelete(false);
    }

    @Override
    public void verifyCanDelete(final boolean ignorePortConnections) {
        verifyCanDelete(ignorePortConnections, false);
    }

    @Override
    public void verifyCanDelete(final boolean ignoreConnections, final boolean ignoreTemplates) {
        readLock.lock();
        try {
            for (final Port port : inputPorts.values()) {
                port.verifyCanDelete(true);
            }

            for (final Port port : outputPorts.values()) {
                port.verifyCanDelete(true);
            }

            for (final ProcessorNode procNode : processors.values()) {
                procNode.verifyCanDelete(true);
            }

            for (final Connection connection : connections.values()) {
                connection.verifyCanDelete();
            }

            for (final ControllerServiceNode cs : controllerServices.values()) {
                cs.verifyCanDelete();
            }

            for (final ProcessGroup childGroup : processGroups.values()) {
                // For nested child groups we can ignore the input/output port
                // connections as they will be being deleted anyway.
                childGroup.verifyCanDelete(true, ignoreTemplates);
            }

            if (!ignoreTemplates && !templates.isEmpty()) {
                throw new IllegalStateException(String.format("Cannot delete Process Group because it contains %s Templates. The Templates must be deleted first.", templates.size()));
            }

            if (!ignoreConnections) {
                for (final Port port : inputPorts.values()) {
                    for (final Connection connection : port.getIncomingConnections()) {
                        if (connection.getSource().equals(port)) {
                            connection.verifyCanDelete();
                        } else {
                            throw new IllegalStateException("Cannot delete Process Group because Input Port " + port.getIdentifier()
                                + " has at least one incoming connection from a component outside of the Process Group. Delete this connection first.");
                        }
                    }
                }

                for (final Port port : outputPorts.values()) {
                    for (final Connection connection : port.getConnections()) {
                        if (connection.getDestination().equals(port)) {
                            connection.verifyCanDelete();
                        } else {
                            throw new IllegalStateException("Cannot delete Process Group because Output Port " + port.getIdentifier()
                                + " has at least one outgoing connection to a component outside of the Process Group. Delete this connection first.");
                        }
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStop(Connectable connectable) {
        final ScheduledState state = connectable.getScheduledState();
        if (state == ScheduledState.DISABLED) {
            throw new IllegalStateException("Cannot stop component with id " + connectable + " because it is currently disabled.");
        }
    }

    @Override
    public void verifyCanStop() {
    }

    @Override
    public void verifyCanStart(Connectable connectable) {
        readLock.lock();
        try {
            if (connectable.getScheduledState() == ScheduledState.STOPPED) {
                if (scheduler.getActiveThreadCount(connectable) > 0) {
                    throw new IllegalStateException("Cannot start component with id" + connectable.getIdentifier() + " because it is currently stopping");
                }

                connectable.verifyCanStart();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanStart() {
        readLock.lock();
        try {
            for (final Connectable connectable : findAllConnectables(this, false)) {
                verifyCanStart(connectable);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanDelete(final Snippet snippet) throws IllegalStateException {
        readLock.lock();
        try {
            if (!id.equals(snippet.getParentGroupId())) {
                throw new IllegalStateException("Snippet belongs to ProcessGroup with ID " + snippet.getParentGroupId() + " but this ProcessGroup has id " + id);
            }

            if (!isDisconnected(snippet)) {
                throw new IllegalStateException("One or more components within the snippet is connected to a component outside of the snippet. Only a disconnected snippet may be moved.");
            }

            for (final String id : snippet.getConnections().keySet()) {
                final Connection connection = getConnection(id);
                if (connection == null) {
                    throw new IllegalStateException("Snippet references Connection with ID " + id + ", which does not exist in this ProcessGroup");
                }

                connection.verifyCanDelete();
            }

            for (final String id : snippet.getFunnels().keySet()) {
                final Funnel funnel = getFunnel(id);
                if (funnel == null) {
                    throw new IllegalStateException("Snippet references Funnel with ID " + id + ", which does not exist in this ProcessGroup");
                }

                funnel.verifyCanDelete(true);
            }

            for (final String id : snippet.getInputPorts().keySet()) {
                final Port port = getInputPort(id);
                if (port == null) {
                    throw new IllegalStateException("Snippet references Input Port with ID " + id + ", which does not exist in this ProcessGroup");
                }

                port.verifyCanDelete(true);
            }

            for (final String id : snippet.getLabels().keySet()) {
                final Label label = getLabel(id);
                if (label == null) {
                    throw new IllegalStateException("Snippet references Label with ID " + id + ", which does not exist in this ProcessGroup");
                }
            }

            for (final String id : snippet.getOutputPorts().keySet()) {
                final Port port = getOutputPort(id);
                if (port == null) {
                    throw new IllegalStateException("Snippet references Output Port with ID " + id + ", which does not exist in this ProcessGroup");
                }
                port.verifyCanDelete(true);
            }

            for (final String id : snippet.getProcessGroups().keySet()) {
                final ProcessGroup group = getProcessGroup(id);
                if (group == null) {
                    throw new IllegalStateException("Snippet references Process Group with ID " + id + ", which does not exist in this ProcessGroup");
                }
                group.verifyCanDelete(true);
            }

            for (final String id : snippet.getProcessors().keySet()) {
                final ProcessorNode processor = getProcessor(id);
                if (processor == null) {
                    throw new IllegalStateException("Snippet references Processor with ID " + id + ", which does not exist in this ProcessGroup");
                }
                processor.verifyCanDelete(true);
            }

            for (final String id : snippet.getRemoteProcessGroups().keySet()) {
                final RemoteProcessGroup group = getRemoteProcessGroup(id);
                if (group == null) {
                    throw new IllegalStateException("Snippet references Remote Process Group with ID " + id + ", which does not exist in this ProcessGroup");
                }
                group.verifyCanDelete(true);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void verifyCanMove(final Snippet snippet, final ProcessGroup newProcessGroup) throws IllegalStateException {
        readLock.lock();
        try {
            if (!id.equals(snippet.getParentGroupId())) {
                throw new IllegalStateException("Snippet belongs to ProcessGroup with ID " + snippet.getParentGroupId() + " but this ProcessGroup has id " + id);
            }

            verifyContents(snippet);
            verifyDestinationNotInSnippet(snippet, newProcessGroup);

            if (!isDisconnected(snippet)) {
                throw new IllegalStateException("One or more components within the snippet is connected to a component outside of the snippet. Only a disconnected snippet may be moved.");
            }

            for (final String id : snippet.getInputPorts().keySet()) {
                final Port port = getInputPort(id);
                final String portName = port.getName();

                if (newProcessGroup.getInputPortByName(portName) != null) {
                    throw new IllegalStateException("Cannot perform Move Operation because of a naming conflict with another port in the destination Process Group");
                }
            }

            for (final String id : snippet.getOutputPorts().keySet()) {
                final Port port = getOutputPort(id);
                final String portName = port.getName();

                if (newProcessGroup.getOutputPortByName(portName) != null) {
                    throw new IllegalStateException("Cannot perform Move Operation because of a naming conflict with another port in the destination Process Group");
                }
            }

            final ParameterContext currentParameterContext = getParameterContext();
            final String currentParameterContextId = currentParameterContext == null ? null : currentParameterContext.getIdentifier();
            final ParameterContext destinationParameterContext = newProcessGroup.getParameterContext();
            final String destinationParameterContextId = destinationParameterContext == null ? null : destinationParameterContext.getIdentifier();

            final boolean parameterContextsDiffer = !Objects.equals(currentParameterContextId, destinationParameterContextId);

            final Set<ProcessorNode> processors = findAllProcessors(snippet);
            for (final ProcessorNode processorNode : processors) {
                for (final PropertyDescriptor descriptor : processorNode.getProperties().keySet()) {
                    final Class<? extends ControllerService> serviceDefinition = descriptor.getControllerServiceDefinition();

                    // if this descriptor identifies a controller service
                    if (serviceDefinition != null) {
                        final String serviceId = processorNode.getEffectivePropertyValue(descriptor);

                        // if the processor is configured with a service
                        if (serviceId != null) {
                            // get all the available services
                            final Set<String> currentControllerServiceIds = controllerServiceProvider.getControllerServiceIdentifiers(serviceDefinition, getIdentifier());
                            final Set<String> proposedControllerServiceIds = controllerServiceProvider.getControllerServiceIdentifiers(serviceDefinition, newProcessGroup.getIdentifier());

                            // ensure the configured service is an allowed service if it's still a valid service
                            if (currentControllerServiceIds.contains(serviceId) && !proposedControllerServiceIds.contains(serviceId)) {
                                throw new IllegalStateException("Cannot perform Move Operation because Processor with ID " + processorNode.getIdentifier()
                                    + " references a service that is not available in the destination Process Group");
                            }
                        }
                    }

                    // If Parameter is used and the Parameter Contexts are different, then the Processor must be stopped.
                    if (parameterContextsDiffer && processorNode.isRunning() && processorNode.isReferencingParameter()) {
                        throw new IllegalStateException("Cannot perform Move Operation because Processor with ID " + processorNode.getIdentifier() + " references one or more Parameters, and the " +
                            "Processor is running, and the destination Process Group is bound to a different Parameter Context that the current Process Group. This would result in changing the " +
                            "configuration of the Processor while it is running, which is not allowed. You must first stop the Processor before moving it to another Process Group if the " +
                            "destination's Parameter Context is not the same.");
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private Set<ProcessorNode> findAllProcessors(final Snippet snippet) {
        final Set<ProcessorNode> processors = new HashSet<>();

        snippet.getProcessors().keySet().stream()
            .map(this::getProcessor)
            .forEach(processors::add);

        for (final String groupId : snippet.getProcessGroups().keySet()) {
            processors.addAll(getProcessGroup(groupId).findAllProcessors());
        }

        return processors;
    }

    @Override
    public ParameterContext getParameterContext() {
        return parameterContext;
    }

    @Override
    public void setParameterContext(final ParameterContext parameterContext) {
        verifyCanSetParameterContext(parameterContext);

        // Determine which parameters have changed so that components can be appropriately updated.
        final Map<String, ParameterUpdate> updatedParameters = mapParameterUpdates(this.parameterContext, parameterContext);
        LOG.debug("Parameter Context for {} changed from {} to {}. This resulted in {} Parameter Updates ({}). Notifying Processors/Controller Services of the updates.",
            this, this.parameterContext, parameterContext, updatedParameters.size(), updatedParameters);

        this.parameterContext = parameterContext;

        if (!updatedParameters.isEmpty()) {
            // Notify components that parameters have been updated
            onParameterContextUpdated(updatedParameters);
        }
    }

    @Override
    public void onParameterContextUpdated(final Map<String, ParameterUpdate> updatedParameters) {
        readLock.lock();
        try {
            getProcessors().forEach(proc -> proc.onParametersModified(updatedParameters));
            getControllerServices(false).forEach(cs -> cs.onParametersModified(updatedParameters));
        } finally {
            readLock.unlock();
        }
    }

    private Map<String, ParameterUpdate> mapParameterUpdates(final ParameterContext previousParameterContext, final ParameterContext updatedParameterContext) {
        if (previousParameterContext == null && updatedParameterContext == null) {
            return Collections.emptyMap();
        }
        if (updatedParameterContext == null) {
            return createParameterUpdates(previousParameterContext, (descriptor, value) -> new StandardParameterUpdate(descriptor.getName(), value, null, descriptor.isSensitive()));
        }
        if (previousParameterContext == null) {
            return createParameterUpdates(updatedParameterContext, (descriptor, value) -> new StandardParameterUpdate(descriptor.getName(), null, value, descriptor.isSensitive()));
        }

        // For each Parameter in the updated parameter context, add a ParameterUpdate to our map
        final Map<String, ParameterUpdate> updatedParameters = new HashMap<>();
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : updatedParameterContext.getParameters().entrySet()) {
            final ParameterDescriptor updatedDescriptor = entry.getKey();
            final Parameter updatedParameter = entry.getValue();

            final Optional<Parameter> previousParameterOption = previousParameterContext.getParameter(updatedDescriptor);
            final String previousValue = previousParameterOption.map(Parameter::getValue).orElse(null);
            final String updatedValue = updatedParameter.getValue();

            if (!Objects.equals(previousValue, updatedValue)) {
                final ParameterUpdate parameterUpdate = new StandardParameterUpdate(updatedDescriptor.getName(), previousValue, updatedValue, updatedDescriptor.isSensitive());
                updatedParameters.put(updatedDescriptor.getName(), parameterUpdate);
            }
        }

        // For each Parameter that was in the previous parameter context that is not in the updated Paramter Context, add a ParameterUpdate to our map with `null` for the updated value
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : previousParameterContext.getParameters().entrySet()) {
            final ParameterDescriptor previousDescriptor = entry.getKey();
            final Parameter previousParameter = entry.getValue();

            final Optional<Parameter> updatedParameterOption = updatedParameterContext.getParameter(previousDescriptor);
            if (updatedParameterOption.isPresent()) {
                // The value exists in both Parameter Contexts. If it was changed, a Parameter Update has already been added to the map, above.
                continue;
            }

            final ParameterUpdate parameterUpdate = new StandardParameterUpdate(previousDescriptor.getName(), previousParameter.getValue(), null, previousDescriptor.isSensitive());
            updatedParameters.put(previousDescriptor.getName(), parameterUpdate);
        }

        return updatedParameters;
    }

    private Map<String, ParameterUpdate> createParameterUpdates(final ParameterContext parameterContext, final BiFunction<ParameterDescriptor, String, ParameterUpdate> parameterUpdateMapper) {
        final Map<String, ParameterUpdate> updatedParameters = new HashMap<>();

        for (final Map.Entry<ParameterDescriptor, Parameter> entry : parameterContext.getParameters().entrySet()) {
            final ParameterDescriptor parameterDescriptor = entry.getKey();
            final Parameter parameter = entry.getValue();

            final ParameterUpdate parameterUpdate = parameterUpdateMapper.apply(parameterDescriptor, parameter.getValue());
            updatedParameters.put(parameterDescriptor.getName(), parameterUpdate);
        }

        return updatedParameters;
    }

    @Override
    public void verifyCanSetParameterContext(final ParameterContext parameterContext) {
        readLock.lock();
        try {
            if (Objects.equals(parameterContext, getParameterContext())) {
                return;
            }

            for (final ProcessorNode processor : processors.values()) {
                final boolean referencingParam = processor.isReferencingParameter();
                if (!referencingParam) {
                    continue;
                }

                if (processor.isRunning()) {
                    throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + processor + " is referencing at least one Parameter and is running");
                }

                verifyParameterSensitivityIsValid(processor, parameterContext);
            }

            for (final ControllerServiceNode service : controllerServices.values()) {
                final boolean referencingParam = service.isReferencingParameter();
                if (!referencingParam) {
                    continue;
                }

                if (service.getState() != ControllerServiceState.DISABLED) {
                    throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + service + " is referencing at least one Parameter and is not disabled");
                }

                verifyParameterSensitivityIsValid(service, parameterContext);
            }
        } finally {
            readLock.unlock();
        }
    }

    private void verifyParameterSensitivityIsValid(final ComponentNode component, final ParameterContext parameterContext) {
        if (parameterContext == null) {
            return;
        }

        final Map<PropertyDescriptor, PropertyConfiguration> properties = component.getProperties();
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : properties.entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                final String paramName = reference.getParameterName();
                final Optional<Parameter> parameter = parameterContext.getParameter(paramName);

                if (parameter.isPresent()) {
                    final PropertyDescriptor propertyDescriptor = entry.getKey();
                    if (parameter.get().getDescriptor().isSensitive() && !propertyDescriptor.isSensitive()) {
                        throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + component + " is referencing Parameter '" + paramName
                            + "' from the '" + propertyDescriptor.getDisplayName() + "' property and the Parameter is sensitive. Sensitive Parameters may only be referenced " +
                            "by sensitive properties.");
                    }

                    if (!parameter.get().getDescriptor().isSensitive() && propertyDescriptor.isSensitive()) {
                        throw new IllegalStateException("Cannot change Parameter Context for " + this + " because " + component + " is referencing Parameter '" + paramName
                            + "' from a sensitive property and the Parameter is not sensitive. Sensitive properties may only reference " +
                            "by Sensitive Parameters.");
                    }
                }
            }
        }
    }

    @Override
    public MutableVariableRegistry getVariableRegistry() {
        return variableRegistry;
    }

    @Override
    public void verifyCanUpdateVariables(final Map<String, String> updatedVariables) {
        if (updatedVariables == null || updatedVariables.isEmpty()) {
            return;
        }

        readLock.lock();
        try {
            final Set<String> updatedVariableNames = getUpdatedVariables(updatedVariables);
            if (updatedVariableNames.isEmpty()) {
                return;
            }

            // Determine any Processors that references the variable
            for (final ProcessorNode processor : getProcessors()) {
                if (!processor.isRunning()) {
                    continue;
                }

                for (final VariableImpact impact : getVariableImpact(processor)) {
                    for (final String variableName : updatedVariableNames) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + processor + ", which is currently running");
                        }
                    }
                }
            }

            // Determine any Controller Service that references the variable.
            for (final ControllerServiceNode service : getControllerServices(false)) {
                if (!service.isActive()) {
                    continue;
                }

                for (final VariableImpact impact : getVariableImpact(service)) {
                    for (final String variableName : updatedVariableNames) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + service + ", which is currently running");
                        }
                    }
                }
            }

            // For any child Process Group that does not override the variable, also include its references.
            // If a child group has a value for the same variable, though, then that means that the child group
            // is overriding the variable and its components are actually referencing a different variable.
            for (final ProcessGroup childGroup : getProcessGroups()) {
                for (final String variableName : updatedVariableNames) {
                    final ComponentVariableRegistry childRegistry = childGroup.getVariableRegistry();
                    final VariableDescriptor descriptor = childRegistry.getVariableKey(variableName);
                    final boolean overridden = childRegistry.getVariableMap().containsKey(descriptor);
                    if (!overridden) {
                        final Set<ComponentNode> affectedComponents = childGroup.getComponentsAffectedByVariable(variableName);

                        for (final ComponentNode affectedComponent : affectedComponents) {
                            if (affectedComponent instanceof ProcessorNode) {
                                final ProcessorNode affectedProcessor = (ProcessorNode) affectedComponent;
                                if (affectedProcessor.isRunning()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + affectedComponent + ", which is currently running.");
                                }
                            } else if (affectedComponent instanceof ControllerServiceNode) {
                                final ControllerServiceNode affectedService = (ControllerServiceNode) affectedComponent;
                                if (affectedService.isActive()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + affectedComponent + ", which is currently active.");
                                }
                            } else if (affectedComponent instanceof ReportingTaskNode) {
                                final ReportingTaskNode affectedReportingTask = (ReportingTaskNode) affectedComponent;
                                if (affectedReportingTask.isRunning()) {
                                    throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + affectedComponent + ", which is currently running.");
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String componentId) {
        writeLock.lock();
        try {
            final String currentId = versionedComponentId.get();

            if (currentId == null) {
                versionedComponentId.set(componentId);
            } else if (currentId.equals(componentId)) {
                return;
            } else if (componentId == null) {
                versionedComponentId.set(null);
            } else {
                throw new IllegalStateException(this + " is already under version control with a different Versioned Component ID");
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<ComponentNode> getComponentsAffectedByVariable(final String variableName) {
        final Set<ComponentNode> affected = new HashSet<>();

        // Determine any Processors that references the variable
        for (final ProcessorNode processor : getProcessors()) {
            for (final VariableImpact impact : getVariableImpact(processor)) {
                if (impact.isImpacted(variableName)) {
                    affected.add(processor);
                }
            }
        }

        // Determine any Controller Service that references the variable. If Service A references a variable,
        // then that means that any other component that references that service is also affected, so recursively
        // find any references to that service and add it.
        for (final ControllerServiceNode service : getControllerServices(false)) {
            for (final VariableImpact impact : getVariableImpact(service)) {
                if (impact.isImpacted(variableName)) {
                    affected.add(service);

                    final ControllerServiceReference reference = service.getReferences();
                    affected.addAll(reference.findRecursiveReferences(ComponentNode.class));
                }
            }
        }

        // For any child Process Group that does not override the variable, also include its references.
        // If a child group has a value for the same variable, though, then that means that the child group
        // is overriding the variable and its components are actually referencing a different variable.
        for (final ProcessGroup childGroup : getProcessGroups()) {
            final ComponentVariableRegistry childRegistry = childGroup.getVariableRegistry();
            final VariableDescriptor descriptor = childRegistry.getVariableKey(variableName);
            final boolean overridden = childRegistry.getVariableMap().containsKey(descriptor);
            if (!overridden) {
                affected.addAll(childGroup.getComponentsAffectedByVariable(variableName));
            }
        }

        return affected;
    }

    private Set<String> getUpdatedVariables(final Map<String, String> newVariableValues) {
        final Set<String> updatedVariableNames = new HashSet<>();

        final MutableVariableRegistry registry = getVariableRegistry();
        for (final Map.Entry<String, String> entry : newVariableValues.entrySet()) {
            final String varName = entry.getKey();
            final String newValue = entry.getValue();

            final String curValue = registry.getVariableValue(varName);
            if (!Objects.equals(newValue, curValue)) {
                updatedVariableNames.add(varName);
            }
        }

        return updatedVariableNames;
    }

    private List<VariableImpact> getVariableImpact(final ComponentNode component) {
        return component.getEffectivePropertyValues().keySet().stream()
            .map(descriptor -> {
                final String configuredVal = component.getEffectivePropertyValue(descriptor);
                return configuredVal == null ? descriptor.getDefaultValue() : configuredVal;
            })
            .map(propVal -> Query.prepare(propVal).getVariableImpact())
            .collect(Collectors.toList());
    }

    @Override
    public void setVariables(final Map<String, String> variables) {
        writeLock.lock();
        try {
            verifyCanUpdateVariables(variables);

            if (variables == null) {
                return;
            }

            final Map<VariableDescriptor, String> variableMap = new HashMap<>();
            // cannot use Collectors.toMap because value may be null
            variables.forEach((key, value) -> variableMap.put(new VariableDescriptor(key), value));

            variableRegistry.setVariables(variableMap);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public VersionControlInformation getVersionControlInformation() {
        return versionControlInfo.get();
    }

    @Override
    public void onComponentModified() {
        // We no longer know if or how the Process Group has changed, so the next time that we
        // get the local modifications, we must re-calculate it. We cannot simply assume that
        // the flow was modified now, because if a Processor Property changed from 'A' to 'B',
        // then back to 'A', then we have to know that it was not modified. So we set it to null
        // to indicate that we must calculate the local modifications.
        final StandardVersionControlInformation svci = this.versionControlInfo.get();
        if (svci == null) {
            // This group is not under version control directly. Notify parent.
            final ProcessGroup parentGroup = parent.get();
            if (parentGroup != null) {
                parentGroup.onComponentModified();
            }
        }

        versionControlFields.setFlowDifferences(null);
    }

    @Override
    public void setVersionControlInformation(final VersionControlInformation versionControlInformation, final Map<String, String> versionedComponentIds) {
        final StandardVersionControlInformation svci = new StandardVersionControlInformation(
            versionControlInformation.getRegistryIdentifier(),
            versionControlInformation.getRegistryName(),
            versionControlInformation.getBucketIdentifier(),
            versionControlInformation.getFlowIdentifier(),
            versionControlInformation.getVersion(),
            stripContentsFromRemoteDescendantGroups(versionControlInformation.getFlowSnapshot(), true),
            versionControlInformation.getStatus()) {

            @Override
            public String getRegistryName() {
                final String registryId = versionControlInformation.getRegistryIdentifier();
                final FlowRegistry registry = flowRegistryClient.getFlowRegistry(registryId);
                return registry == null ? registryId : registry.getName();
            }

            private boolean isModified() {
                if (versionControlInformation.getVersion() == 0) {
                    return true;
                }

                Set<FlowDifference> differences = versionControlFields.getFlowDifferences();
                if (differences == null) {
                    differences = getModifications();
                    if (differences == null) {
                        return false;
                    }

                    versionControlFields.setFlowDifferences(differences);
                }

                return !differences.isEmpty();
            }

            @Override
            public VersionedFlowStatus getStatus() {
                // If current state is a sync failure, then
                final String syncFailureExplanation = versionControlFields.getSyncFailureExplanation();
                if (syncFailureExplanation != null) {
                    return new StandardVersionedFlowStatus(VersionedFlowState.SYNC_FAILURE, syncFailureExplanation);
                }

                try {
                    final boolean modified = isModified();
                    if (!modified) {
                        final VersionControlInformation vci = StandardProcessGroup.this.versionControlInfo.get();
                        if (vci.getFlowSnapshot() == null) {
                            return new StandardVersionedFlowStatus(VersionedFlowState.SYNC_FAILURE, "Process Group has not yet been synchronized with Flow Registry");
                        }
                    }

                    final boolean stale = versionControlFields.isStale();

                    final VersionedFlowState flowState;
                    if (modified && stale) {
                        flowState = VersionedFlowState.LOCALLY_MODIFIED_AND_STALE;
                    } else if (modified) {
                        flowState = VersionedFlowState.LOCALLY_MODIFIED;
                    } else if (stale) {
                        flowState = VersionedFlowState.STALE;
                    } else {
                        flowState = VersionedFlowState.UP_TO_DATE;
                    }

                    return new StandardVersionedFlowStatus(flowState, flowState.getDescription());
                } catch (final Exception e) {
                    LOG.warn("Could not correctly determine Versioned Flow Status for {}. Will consider state to be SYNC_FAILURE", this, e);
                    return new StandardVersionedFlowStatus(VersionedFlowState.SYNC_FAILURE, "Could not properly determine flow status due to: " + e);
                }
            }
        };

        svci.setBucketName(versionControlInformation.getBucketName());
        svci.setFlowName(versionControlInformation.getFlowName());
        svci.setFlowDescription(versionControlInformation.getFlowDescription());

        final VersionedFlowState flowState = versionControlInformation.getStatus().getState();
        versionControlFields.setStale(flowState == VersionedFlowState.STALE || flowState == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE);
        versionControlFields.setLocallyModified(flowState == VersionedFlowState.LOCALLY_MODIFIED || flowState == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE);
        versionControlFields.setSyncFailureExplanation(flowState == VersionedFlowState.SYNC_FAILURE ? versionControlInformation.getStatus().getStateExplanation() : null);

        writeLock.lock();
        try {
            updateVersionedComponentIds(this, versionedComponentIds);
            this.versionControlInfo.set(svci);
            versionControlFields.setFlowDifferences(null);

            final ProcessGroup parent = getParent();
            if (parent != null) {
                parent.onComponentModified();
            }

            scheduler.submitFrameworkTask(() -> synchronizeWithFlowRegistry(flowRegistryClient));
        } finally {
            writeLock.unlock();
        }
    }

    private VersionedProcessGroup stripContentsFromRemoteDescendantGroups(final VersionedProcessGroup processGroup, final boolean topLevel) {
        if (processGroup == null) {
            return null;
        }

        final VersionedProcessGroup copy = new VersionedProcessGroup();
        copy.setComments(processGroup.getComments());
        copy.setComponentType(processGroup.getComponentType());
        copy.setGroupIdentifier(processGroup.getGroupIdentifier());
        copy.setIdentifier(processGroup.getIdentifier());
        copy.setName(processGroup.getName());
        copy.setFlowFileConcurrency(processGroup.getFlowFileConcurrency());
        copy.setFlowFileOutboundPolicy(processGroup.getFlowFileOutboundPolicy());
        copy.setDefaultFlowFileExpiration(processGroup.getDefaultFlowFileExpiration());
        copy.setDefaultBackPressureObjectThreshold(processGroup.getDefaultBackPressureObjectThreshold());
        copy.setDefaultBackPressureDataSizeThreshold(processGroup.getDefaultBackPressureDataSizeThreshold());
        copy.setPosition(processGroup.getPosition());
        copy.setVersionedFlowCoordinates(topLevel ? null : processGroup.getVersionedFlowCoordinates());
        copy.setConnections(processGroup.getConnections());
        copy.setControllerServices(processGroup.getControllerServices());
        copy.setFunnels(processGroup.getFunnels());
        copy.setInputPorts(processGroup.getInputPorts());
        copy.setOutputPorts(processGroup.getOutputPorts());
        copy.setProcessors(processGroup.getProcessors());
        copy.setRemoteProcessGroups(processGroup.getRemoteProcessGroups());
        copy.setVariables(processGroup.getVariables());
        copy.setLabels(processGroup.getLabels());

        final Set<VersionedProcessGroup> copyChildren = new HashSet<>();

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            if (childGroup.getVersionedFlowCoordinates() == null) {
                copyChildren.add(stripContentsFromRemoteDescendantGroups(childGroup, false));
            } else {
                final VersionedProcessGroup childCopy = new VersionedProcessGroup();
                childCopy.setComments(childGroup.getComments());
                childCopy.setComponentType(childGroup.getComponentType());
                childCopy.setGroupIdentifier(childGroup.getGroupIdentifier());
                childCopy.setIdentifier(childGroup.getIdentifier());
                childCopy.setName(childGroup.getName());
                childCopy.setPosition(childGroup.getPosition());
                childCopy.setVersionedFlowCoordinates(childGroup.getVersionedFlowCoordinates());
                childCopy.setFlowFileConcurrency(childGroup.getFlowFileConcurrency());
                childCopy.setFlowFileOutboundPolicy(childGroup.getFlowFileOutboundPolicy());
                childCopy.setDefaultFlowFileExpiration(childGroup.getDefaultFlowFileExpiration());
                childCopy.setDefaultBackPressureObjectThreshold(childGroup.getDefaultBackPressureObjectThreshold());
                childCopy.setDefaultBackPressureDataSizeThreshold(childGroup.getDefaultBackPressureDataSizeThreshold());

                copyChildren.add(childCopy);
            }
        }

        copy.setProcessGroups(copyChildren);
        return copy;
    }

    @Override
    public void disconnectVersionControl(final boolean removeVersionedComponentIds) {
        writeLock.lock();
        try {
            this.versionControlInfo.set(null);

            if (removeVersionedComponentIds) {
                // remove version component ids from each component (until another versioned PG is encountered)
                applyVersionedComponentIds(this, id -> null);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void updateVersionedComponentIds(final ProcessGroup processGroup, final Map<String, String> versionedComponentIds) {
        if (versionedComponentIds == null || versionedComponentIds.isEmpty()) {
            return;
        }

        applyVersionedComponentIds(processGroup, versionedComponentIds::get);

        // If we versioned any parent groups' Controller Services, set their versioned component id's too.
        final ProcessGroup parent = processGroup.getParent();
        if (parent != null) {
            for (final ControllerServiceNode service : parent.getControllerServices(true)) {
                if (!service.getVersionedComponentId().isPresent()) {
                    final String versionedId = versionedComponentIds.get(service.getIdentifier());
                    if (versionedId != null) {
                        service.setVersionedComponentId(versionedId);
                    }
                }
            }
        }
    }

    private void applyVersionedComponentIds(final ProcessGroup processGroup, final Function<String, String> lookup) {
        processGroup.setVersionedComponentId(lookup.apply(processGroup.getIdentifier()));

        processGroup.getConnections()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getProcessors()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getInputPorts()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getOutputPorts()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getLabels()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getFunnels()
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));
        processGroup.getControllerServices(false)
            .forEach(component -> component.setVersionedComponentId(lookup.apply(component.getIdentifier())));

        processGroup.getRemoteProcessGroups()
            .forEach(rpg -> {
                rpg.setVersionedComponentId(lookup.apply(rpg.getIdentifier()));

                rpg.getInputPorts().forEach(port -> port.setVersionedComponentId(lookup.apply(port.getIdentifier())));
                rpg.getOutputPorts().forEach(port -> port.setVersionedComponentId(lookup.apply(port.getIdentifier())));
            });

        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            if (childGroup.getVersionControlInformation() == null) {
                applyVersionedComponentIds(childGroup, lookup);
            } else if (!childGroup.getVersionedComponentId().isPresent()) {
                childGroup.setVersionedComponentId(lookup.apply(childGroup.getIdentifier()));
            }
        }
    }

    @Override
    public void synchronizeWithFlowRegistry(final FlowRegistryClient flowRegistryClient) {
        final StandardVersionControlInformation vci = versionControlInfo.get();
        if (vci == null) {
            return;
        }

        final String registryId = vci.getRegistryIdentifier();
        final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
        if (flowRegistry == null) {
            final String message = String.format("Unable to synchronize Process Group with Flow Registry because Process Group was placed under Version Control using Flow Registry "
                + "with identifier %s but cannot find any Flow Registry with this identifier", registryId);
            versionControlFields.setSyncFailureExplanation(message);

            LOG.error("Unable to synchronize {} with Flow Registry because Process Group was placed under Version Control using Flow Registry "
                + "with identifier {} but cannot find any Flow Registry with this identifier", this, registryId);
            return;
        }

        final VersionedProcessGroup snapshot = vci.getFlowSnapshot();
        if (snapshot == null && vci.getVersion() > 0) {
            // We have not yet obtained the snapshot from the Flow Registry, so we need to request the snapshot of our local version of the flow from the Flow Registry.
            // This allows us to know whether or not the flow has been modified since it was last synced with the Flow Registry.
            try {
                final VersionedFlowSnapshot registrySnapshot = flowRegistry.getFlowContents(vci.getBucketIdentifier(), vci.getFlowIdentifier(), vci.getVersion(), false);
                final VersionedProcessGroup registryFlow = registrySnapshot.getFlowContents();
                vci.setFlowSnapshot(registryFlow);
            } catch (final IOException | NiFiRegistryException e) {
                final String message = String.format("Failed to synchronize Process Group with Flow Registry because could not retrieve version %s of flow with identifier %s in bucket %s",
                    vci.getVersion(), vci.getFlowIdentifier(), vci.getBucketIdentifier());
                versionControlFields.setSyncFailureExplanation(message);

                final String logErrorMessage = "Failed to synchronize {} with Flow Registry because could not retrieve version {} of flow with identifier {} in bucket {}";

                // No need to print a full stacktrace for connection refused
                if (e instanceof ConnectException) {
                    LOG.error(logErrorMessage + " due to: {}", this, vci.getVersion(), vci.getFlowIdentifier(), vci.getBucketIdentifier(), e.getLocalizedMessage());
                } else {
                    LOG.error(logErrorMessage, this, vci.getVersion(), vci.getFlowIdentifier(), vci.getBucketIdentifier(), e);
                }
                return;
            }
        }

        try {
            final VersionedFlow versionedFlow = flowRegistry.getVersionedFlow(vci.getBucketIdentifier(), vci.getFlowIdentifier());
            final int latestVersion = (int) versionedFlow.getVersionCount();
            vci.setBucketName(versionedFlow.getBucketName());
            vci.setFlowName(versionedFlow.getName());
            vci.setFlowDescription(versionedFlow.getDescription());
            vci.setRegistryName(flowRegistry.getName());

            if (latestVersion == vci.getVersion()) {
                versionControlFields.setStale(false);
                if (latestVersion == 0) {
                    LOG.debug("{} does not have any version in the Registry", this);
                    versionControlFields.setLocallyModified(true);
                } else {
                    LOG.debug("{} is currently at the most recent version ({}) of the flow that is under Version Control", this, latestVersion);
                }
            } else {
                LOG.info("{} is not the most recent version of the flow that is under Version Control; current version is {}; most recent version is {}",
                    this, vci.getVersion(), latestVersion);
                versionControlFields.setStale(true);
            }

            versionControlFields.setSyncFailureExplanation(null);
        } catch (final IOException | NiFiRegistryException e) {
            final String message = String.format("Failed to synchronize Process Group with Flow Registry : " + e.getMessage());
            versionControlFields.setSyncFailureExplanation(message);

            LOG.error("Failed to synchronize {} with Flow Registry because could not determine the most recent version of the Flow in the Flow Registry", this, e);
        }
    }

    @Override
    public void updateFlow(final VersionedFlowSnapshot proposedSnapshot, final String componentIdSeed, final boolean verifyNotDirty, final boolean updateSettings,
                           final boolean updateDescendantVersionedFlows) {
        writeLock.lock();
        try {
            verifyCanUpdate(proposedSnapshot, true, verifyNotDirty);

            final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(extensionManager);
            final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(this, controllerServiceProvider, flowRegistryClient, true);

            final ComparableDataFlow localFlow = new StandardComparableDataFlow("Current Flow", versionedGroup);
            final ComparableDataFlow proposedFlow = new StandardComparableDataFlow("New Flow", proposedSnapshot.getFlowContents());

            final FlowComparator flowComparator = new StandardFlowComparator(proposedFlow, localFlow, getAncestorServiceIds(), new StaticDifferenceDescriptor());
            final FlowComparison flowComparison = flowComparator.compare();

            final Set<String> updatedVersionedComponentIds = new HashSet<>();
            for (final FlowDifference diff : flowComparison.getDifferences()) {
                // Ignore these as local differences for now because we can't do anything with it
                if (diff.getDifferenceType() == DifferenceType.BUNDLE_CHANGED) {
                    continue;
                }

                // If this update adds a new Controller Service, then we need to check if the service already exists at a higher level
                // and if so compare our VersionedControllerService to the existing service.
                if (diff.getDifferenceType() == DifferenceType.COMPONENT_ADDED) {
                    final VersionedComponent component = diff.getComponentA() == null ? diff.getComponentB() : diff.getComponentA();
                    if (ComponentType.CONTROLLER_SERVICE == component.getComponentType()) {
                        final ControllerServiceNode serviceNode = getVersionedControllerService(this, component.getIdentifier());
                        if (serviceNode != null) {
                            final VersionedControllerService versionedService = mapper.mapControllerService(serviceNode, controllerServiceProvider,
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
                final String differencesByLine = flowComparison.getDifferences().stream()
                    .map(FlowDifference::toString)
                    .collect(Collectors.joining("\n"));

                // TODO: Until we move to NiFi Registry 0.6.0, avoid using proposedSnapshot.toString() because it throws a NullPointerException
                final String proposedSnapshotDetails = "VersionedFlowSnapshot[flowContentsId=" + proposedSnapshot.getFlowContents().getIdentifier()
                    + ", flowContentsName=" + proposedSnapshot.getFlowContents().getName() + ", NoMetadataAvailable]";
                LOG.info("Updating {} to {}; there are {} differences to take into account:\n{}", this, proposedSnapshotDetails,
                    flowComparison.getDifferences().size(), differencesByLine);
            }

            final Set<String> knownVariables = getKnownVariableNames();

            final StandardVersionControlInformation originalVci = this.versionControlInfo.get();
            try {
                updateProcessGroup(this, proposedSnapshot.getFlowContents(), componentIdSeed, updatedVersionedComponentIds, false, updateSettings, updateDescendantVersionedFlows, knownVariables,
                    proposedSnapshot.getParameterContexts());
            } catch (final Throwable t) {
                // The proposed snapshot may not have any Versioned Flow Coordinates. As a result, the call to #updateProcessGroup may
                // set this PG's Version Control Info to null. During the normal flow of control,
                // the Version Control Information is set appropriately at the end. However, if an Exception is thrown, we need to ensure
                // that we don't leave the Version Control Info as null. It's also important to note here that the Atomic Reference is used
                // as a means of retrieving the value without obtaining a read lock, but the value is never updated outside of a write lock.
                // As a result, it is safe to use the get() and then the set() methods of the AtomicReference without introducing the 'check-then-modify' problem.
                if (this.versionControlInfo.get() == null) {
                    this.versionControlInfo.set(originalVci);
                }

                throw t;
            }
        } catch (final ProcessorInstantiationException pie) {
            throw new IllegalStateException("Failed to update flow", pie);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<String> getAncestorServiceIds() {
        final Set<String> ancestorServiceIds;
        ProcessGroup parentGroup = getParent();

        if (parentGroup == null) {
            ancestorServiceIds = Collections.emptySet();
        } else {
            // We want to map the Controller Service to its Versioned Component ID, if it has one.
            // If it does not have one, we want to generate it in the same way that our Flow Mapper does
            // because this allows us to find the Controller Service when doing a Flow Diff.
            ancestorServiceIds = parentGroup.getControllerServices(true).stream()
                .map(cs -> cs.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(cs.getIdentifier())))
                .collect(Collectors.toSet());
        }

        return ancestorServiceIds;
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

    private Set<String> getKnownVariableNames() {
        final Set<String> variableNames = new HashSet<>();
        populateKnownVariableNames(this, variableNames);
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

    private void updateProcessGroup(final ProcessGroup group, final VersionedProcessGroup proposed, final String componentIdSeed,
                                    final Set<String> updatedVersionedComponentIds, final boolean updatePosition, final boolean updateName, final boolean updateDescendantVersionedGroups,
                                    final Set<String> variablesToSkip, final Map<String, VersionedParameterContext> versionedParameterContexts) throws ProcessorInstantiationException {

        // During the flow update, we will use temporary names for process group ports. This is because port names must be
        // unique within a process group, but during an update we might temporarily be in a state where two ports have the same name.
        // For example, if a process group update involves removing/renaming port A, and then adding/updating port B where B is given
        // A's former name. This is a valid state by the end of the flow update, but for a brief moment there may be two ports with the
        // same name. To avoid this conflict, we keep the final names in a map indexed by port id, use a temporary name for each port
        // during the update, and after all ports have been added/updated/removed, we set the final names on all ports.
        final Map<Port, String> proposedPortFinalNames = new HashMap<>();

        group.setComments(proposed.getComments());

        if (updateName) {
            group.setName(proposed.getName());
        }

        if (updatePosition && proposed.getPosition() != null) {
            group.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        }

        updateParameterContext(group, proposed, versionedParameterContexts, componentIdSeed);
        updateVariableRegistry(group, proposed, variablesToSkip);

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
            final String registryId = flowRegistryClient.getFlowRegistryId(remoteCoordinates.getRegistryUrl());
            final String bucketId = remoteCoordinates.getBucketId();
            final String flowId = remoteCoordinates.getFlowId();
            final int version = remoteCoordinates.getVersion();

            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            final String registryName = flowRegistry == null ? registryId : flowRegistry.getName();

            final VersionedFlowState flowState = remoteCoordinates.getLatest() ? VersionedFlowState.UP_TO_DATE : VersionedFlowState.STALE;

            final VersionControlInformation vci = new StandardVersionControlInformation.Builder()
                .registryId(registryId)
                .registryName(registryName)
                .bucketId(bucketId)
                .bucketName(bucketId)
                .flowId(flowId)
                .flowName(flowId)
                .version(version)
                .flowSnapshot(proposed)
                .status(new StandardVersionedFlowStatus(flowState, flowState.getDescription()))
                .build();

            group.setVersionControlInformation(vci, Collections.emptyMap());
        }

        // Controller Services
        // Controller Services have to be handled a bit differently than other components. This is because Processors and Controller
        // Services may reference other Controller Services. Since we may be adding Service A, which depends on Service B, before adding
        // Service B, we need to ensure that we create all Controller Services first and then call updateControllerService for each
        // Controller Service. This way, we ensure that all services have been created before setting the properties. This allows us to
        // properly obtain the correct mapping of Controller Service VersionedComponentID to Controller Service instance id.
        final Map<String, ControllerServiceNode> servicesByVersionedId = group.getControllerServices(false).stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));

        final Set<String> controllerServicesRemoved = new HashSet<>(servicesByVersionedId.keySet());

        final Map<ControllerServiceNode, VersionedControllerService> services = new HashMap<>();

        // Add any Controller Service that does not yet exist.
        final Map<String, ControllerServiceNode> servicesAdded = new HashMap<>();
        for (final VersionedControllerService proposedService : proposed.getControllerServices()) {
            ControllerServiceNode service = servicesByVersionedId.get(proposedService.getIdentifier());
            if (service == null) {
                service = addControllerService(group, proposedService, componentIdSeed);
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

            controllerServicesRemoved.remove(proposedService.getIdentifier());
        }

        // Before we can update child groups, we must first remove any connections that are connected to those child groups' input/output ports.
        // We cannot add or update connections yet, though. That must be done at the end, as it's possible that the component that is the source/destination of the connection
        // has not yet been added.
        final Map<String, Connection> connectionsByVersionedId = group.getConnections().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> connectionsRemoved = new HashSet<>(connectionsByVersionedId.keySet());

        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            connectionsRemoved.remove(proposedConnection.getIdentifier());
        }

        // Connections must be the first thing to remove, not the last. Otherwise, we will fail
        // to remove a component if it has a connection going to it!
        for (final String removedVersionedId : connectionsRemoved) {
            final Connection connection = connectionsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", connection, group);
            group.removeConnection(connection);
            flowManager.onConnectionRemoved(connection);
        }

        // Child groups
        final Map<String, ProcessGroup> childGroupsByVersionedId = group.getProcessGroups().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> childGroupsRemoved = new HashSet<>(childGroupsByVersionedId.keySet());

        for (final VersionedProcessGroup proposedChildGroup : proposed.getProcessGroups()) {
            final ProcessGroup childGroup = childGroupsByVersionedId.get(proposedChildGroup.getIdentifier());
            final VersionedFlowCoordinates childCoordinates = proposedChildGroup.getVersionedFlowCoordinates();

            // if there is a nested process group that is versioned controlled, make sure get the param contexts that go with that snapshot
            // instead of the ones from the parent which would have been passed in to this method
            Map<String, VersionedParameterContext> childParameterContexts = versionedParameterContexts;
            if (childCoordinates != null && updateDescendantVersionedGroups) {
                childParameterContexts = getVersionedParameterContexts(childCoordinates);
            }

            if (childGroup == null) {
                final ProcessGroup added = addProcessGroup(group, proposedChildGroup, componentIdSeed, variablesToSkip, childParameterContexts);
                flowManager.onProcessGroupAdded(added);
                added.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
                LOG.info("Added {} to {}", added, this);
            } else if (childCoordinates == null || updateDescendantVersionedGroups) {
                updateProcessGroup(childGroup, proposedChildGroup, componentIdSeed, updatedVersionedComponentIds, true, true, updateDescendantVersionedGroups,
                    variablesToSkip, childParameterContexts);
                LOG.info("Updated {}", childGroup);
            }

            childGroupsRemoved.remove(proposedChildGroup.getIdentifier());
        }

        // Funnels
        final Map<String, Funnel> funnelsByVersionedId = group.getFunnels().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> funnelsRemoved = new HashSet<>(funnelsByVersionedId.keySet());

        for (final VersionedFunnel proposedFunnel : proposed.getFunnels()) {
            final Funnel funnel = funnelsByVersionedId.get(proposedFunnel.getIdentifier());
            if (funnel == null) {
                final Funnel added = addFunnel(group, proposedFunnel, componentIdSeed);
                flowManager.onFunnelAdded(added);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedFunnel.getIdentifier())) {
                updateFunnel(funnel, proposedFunnel);
                LOG.info("Updated {}", funnel);
            } else {
                funnel.setPosition(new Position(proposedFunnel.getPosition().getX(), proposedFunnel.getPosition().getY()));
            }

            funnelsRemoved.remove(proposedFunnel.getIdentifier());
        }

        // Input Ports
        final Map<String, Port> inputPortsByVersionedId = group.getInputPorts().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> inputPortsRemoved = new HashSet<>(inputPortsByVersionedId.keySet());

        for (final VersionedPort proposedPort : proposed.getInputPorts()) {
            final Port port = inputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                final Port added = addInputPort(group, proposedPort, componentIdSeed, temporaryName);
                proposedPortFinalNames.put(added, proposedPort.getName());
                flowManager.onInputPortAdded(added);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                proposedPortFinalNames.put(port, proposedPort.getName());
                updatePort(port, proposedPort, temporaryName);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }

            inputPortsRemoved.remove(proposedPort.getIdentifier());
        }

        // Output Ports
        final Map<String, Port> outputPortsByVersionedId = group.getOutputPorts().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> outputPortsRemoved = new HashSet<>(outputPortsByVersionedId.keySet());

        for (final VersionedPort proposedPort : proposed.getOutputPorts()) {
            final Port port = outputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                final Port added = addOutputPort(group, proposedPort, componentIdSeed, temporaryName);
                proposedPortFinalNames.put(added, proposedPort.getName());
                flowManager.onOutputPortAdded(added);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                final String temporaryName = generateTemporaryPortName(proposedPort);
                proposedPortFinalNames.put(port, proposedPort.getName());
                updatePort(port, proposedPort, temporaryName);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }

            outputPortsRemoved.remove(proposedPort.getIdentifier());
        }

        // Labels
        final Map<String, Label> labelsByVersionedId = group.getLabels().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> labelsRemoved = new HashSet<>(labelsByVersionedId.keySet());

        for (final VersionedLabel proposedLabel : proposed.getLabels()) {
            final Label label = labelsByVersionedId.get(proposedLabel.getIdentifier());
            if (label == null) {
                final Label added = addLabel(group, proposedLabel, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedLabel.getIdentifier())) {
                updateLabel(label, proposedLabel);
                LOG.info("Updated {}", label);
            } else {
                label.setPosition(new Position(proposedLabel.getPosition().getX(), proposedLabel.getPosition().getY()));
            }

            labelsRemoved.remove(proposedLabel.getIdentifier());
        }

        // Processors
        final Map<String, ProcessorNode> processorsByVersionedId = group.getProcessors().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> processorsRemoved = new HashSet<>(processorsByVersionedId.keySet());
        final Map<ProcessorNode, Set<Relationship>> autoTerminatedRelationships = new HashMap<>();

        for (final VersionedProcessor proposedProcessor : proposed.getProcessors()) {
            final ProcessorNode processor = processorsByVersionedId.get(proposedProcessor.getIdentifier());
            if (processor == null) {
                final ProcessorNode added = addProcessor(group, proposedProcessor, componentIdSeed);
                flowManager.onProcessorAdded(added);

                final Set<Relationship> proposedAutoTerminated =
                    proposedProcessor.getAutoTerminatedRelationships() == null ? Collections.emptySet() : proposedProcessor.getAutoTerminatedRelationships().stream()
                        .map(added::getRelationship)
                        .collect(Collectors.toSet());
                autoTerminatedRelationships.put(added, proposedAutoTerminated);
                LOG.info("Added {} to {}", added, this);
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

            processorsRemoved.remove(proposedProcessor.getIdentifier());
        }

        // Remote Groups
        final Map<String, RemoteProcessGroup> rpgsByVersionedId = group.getRemoteProcessGroups().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier())), Function.identity()));
        final Set<String> rpgsRemoved = new HashSet<>(rpgsByVersionedId.keySet());

        for (final VersionedRemoteProcessGroup proposedRpg : proposed.getRemoteProcessGroups()) {
            final RemoteProcessGroup rpg = rpgsByVersionedId.get(proposedRpg.getIdentifier());
            if (rpg == null) {
                final RemoteProcessGroup added = addRemoteProcessGroup(group, proposedRpg, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedRpg.getIdentifier())) {
                updateRemoteProcessGroup(rpg, proposedRpg, componentIdSeed);
                LOG.info("Updated {}", rpg);
            } else {
                rpg.setPosition(new Position(proposedRpg.getPosition().getX(), proposedRpg.getPosition().getY()));
            }

            rpgsRemoved.remove(proposedRpg.getIdentifier());
        }

        // Add and update Connections
        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            final Connection connection = connectionsByVersionedId.get(proposedConnection.getIdentifier());
            if (connection == null) {
                final Connection added = addConnection(group, proposedConnection, componentIdSeed);
                flowManager.onConnectionAdded(added);
                LOG.info("Added {} to {}", added, this);
            } else if (isUpdateable(connection)) {
                // If the connection needs to be updated, then the source and destination will already have
                // been stopped (else, the validation above would fail). So if the source or the destination is running,
                // then we know that we don't need to update the connection.
                updateConnection(connection, proposedConnection);
                LOG.info("Updated {}", connection);
            }
        }

        // Remove components that exist in the local flow but not the remote flow.

        // Once the appropriate connections have been removed, we may now update Processors' auto-terminated relationships.
        // We cannot do this above, in the 'updateProcessor' call because if a connection is removed and changed to auto-terminated,
        // then updating this in the updateProcessor call above would attempt to set the Relationship to being auto-terminated while a
        // Connection for that relationship exists. This will throw an Exception.
        autoTerminatedRelationships.forEach(ProcessorNode::setAutoTerminatedRelationships);

        // Remove all controller services no longer in use
        for (final String removedVersionedId : controllerServicesRemoved) {
            final ControllerServiceNode service = servicesByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", service, group);
            // Must remove Controller Service through Flow Controller in order to remove from cache
            controllerServiceProvider.removeControllerService(service);
        }

        for (final String removedVersionedId : funnelsRemoved) {
            final Funnel funnel = funnelsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", funnel, group);
            group.removeFunnel(funnel);
        }

        for (final String removedVersionedId : inputPortsRemoved) {
            final Port port = inputPortsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", port, group);
            group.removeInputPort(port);
        }

        for (final String removedVersionedId : outputPortsRemoved) {
            final Port port = outputPortsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", port, group);
            group.removeOutputPort(port);
        }

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

        for (final String removedVersionedId : labelsRemoved) {
            final Label label = labelsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", label, group);
            group.removeLabel(label);
        }

        for (final String removedVersionedId : processorsRemoved) {
            final ProcessorNode processor = processorsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", processor, group);
            group.removeProcessor(processor);
        }

        for (final String removedVersionedId : rpgsRemoved) {
            final RemoteProcessGroup rpg = rpgsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", rpg, group);
            group.removeRemoteProcessGroup(rpg);
        }

        for (final String removedVersionedId : childGroupsRemoved) {
            final ProcessGroup childGroup = childGroupsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", childGroup, group);
            group.removeProcessGroup(childGroup);
        }
    }

    private Map<String, VersionedParameterContext> getVersionedParameterContexts(final VersionedFlowCoordinates versionedFlowCoordinates) {
        final String registryId = flowRegistryClient.getFlowRegistryId(versionedFlowCoordinates.getRegistryUrl());
        if (registryId == null) {
            throw new ResourceNotFoundException("Could not find any Flow Registry registered with url: " + versionedFlowCoordinates.getRegistryUrl());
        }

        final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
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
            throw new IllegalStateException(
                "Failed to communicate with Flow Registry when attempting to retrieve a versioned flow");
        }
    }

    private ParameterContext createParameterContext(final VersionedParameterContext versionedParameterContext, final String parameterContextId) {
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

        return flowManager.createParameterContext(parameterContextId, versionedParameterContext.getName(), parameters);
    }

    private void addMissingParameters(final VersionedParameterContext versionedParameterContext, final ParameterContext currentParameterContext) {
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
    }

    private ParameterContext getParameterContextByName(final String contextName) {
        return flowManager.getParameterContextManager().getParameterContexts().stream()
            .filter(context -> context.getName().equals(contextName))
            .findAny()
            .orElse(null);
    }

    private void updateParameterContext(final ProcessGroup group, final VersionedProcessGroup proposed, final Map<String, VersionedParameterContext> versionedParameterContexts,
                                        final String componentIdSeed) {
        // Update the Parameter Context
        final ParameterContext currentParamContext = group.getParameterContext();
        final String proposedParameterContextName = proposed.getParameterContextName();
        if (proposedParameterContextName != null) {
            if (currentParamContext == null) {
                // Create a new Parameter Context based on the parameters provided
                final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(proposedParameterContextName);

                // Protect against NPE in the event somehow the proposed name is not in the set of contexts
                if (versionedParameterContext == null) {
                    final String paramContextNames = StringUtils.join(versionedParameterContexts.keySet());
                    throw new IllegalStateException("Proposed parameter context name '" + proposedParameterContextName
                        + "' does not exist in set of available parameter contexts [" + paramContextNames + "]");
                }

                final ParameterContext contextByName = getParameterContextByName(versionedParameterContext.getName());
                final ParameterContext selectedParameterContext;
                if (contextByName == null) {
                    final String parameterContextId = generateUuid(versionedParameterContext.getName(), versionedParameterContext.getName(), componentIdSeed);
                    selectedParameterContext = createParameterContext(versionedParameterContext, parameterContextId);
                } else {
                    selectedParameterContext = contextByName;
                    addMissingParameters(versionedParameterContext, selectedParameterContext);
                }

                group.setParameterContext(selectedParameterContext);
            } else {
                // Update the current Parameter Context so that it has any Parameters included in the proposed context
                final VersionedParameterContext versionedParameterContext = versionedParameterContexts.get(proposedParameterContextName);
                addMissingParameters(versionedParameterContext, currentParamContext);
            }
        }
    }

    private void updateVariableRegistry(final ProcessGroup group, final VersionedProcessGroup proposed, final Set<String> variablesToSkip) {
        // Determine which variables have been added/removed and add/remove them from this group's variable registry.
        // We don't worry about if a variable value has changed, because variables are designed to be 'environment specific.'
        // As a result, once imported, we won't update variables to match the remote flow, but we will add any missing variables
        // and remove any variables that are no longer part of the remote flow.
        final Set<String> existingVariableNames = group.getVariableRegistry().getVariableMap().keySet().stream()
            .map(VariableDescriptor::getName)
            .collect(Collectors.toSet());

        final Map<String, String> updatedVariableMap = new HashMap<>();

        // If any new variables exist in the proposed flow, add those to the variable registry.
        for (final Map.Entry<String, String> entry : proposed.getVariables().entrySet()) {
            if (!existingVariableNames.contains(entry.getKey()) && !variablesToSkip.contains(entry.getKey())) {
                updatedVariableMap.put(entry.getKey(), entry.getValue());
            }
        }

        group.setVariables(updatedVariableMap);
    }

    private String getPublicPortFinalName(final PublicPort publicPort, final String proposedFinalName) {
        final Optional<Port> existingPublicPort;
        if (TransferDirection.RECEIVE == publicPort.getDirection()) {
            existingPublicPort = flowManager.getPublicInputPort(proposedFinalName);
        } else {
            existingPublicPort = flowManager.getPublicOutputPort(proposedFinalName);
        }

        if (existingPublicPort.isPresent() && !existingPublicPort.get().getIdentifier().equals(publicPort.getIdentifier())) {
            return getPublicPortFinalName(publicPort, "Copy of " + proposedFinalName);
        } else {
            return proposedFinalName;
        }
    }

    private boolean isUpdateable(final Connection connection) {
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
        writeLock.lock();
        try {
            port.setName(name);
        } finally {
            writeLock.unlock();
        }
    }

    private String generateUuid(final String propposedId, final String destinationGroupId, final String seed) {
        long msb = UUID.nameUUIDFromBytes((propposedId + destinationGroupId).getBytes(StandardCharsets.UTF_8)).getMostSignificantBits();

        UUID uuid;
        if (StringUtils.isBlank(seed)) {
            long lsb = randomGenerator.nextLong();
            // since msb is extracted from type-one UUID, the type-one semantics will be preserved
            uuid = new UUID(msb, lsb);
        } else {
            UUID seedId = UUID.nameUUIDFromBytes((propposedId + destinationGroupId + seed).getBytes(StandardCharsets.UTF_8));
            uuid = new UUID(msb, seedId.getLeastSignificantBits());
        }
        LOG.debug("Generating UUID {} from currentId={}, seed={}", uuid, propposedId, seed);
        return uuid.toString();
    }

    private ProcessGroup addProcessGroup(final ProcessGroup destination, final VersionedProcessGroup proposed, final String componentIdSeed, final Set<String> variablesToSkip,
                                         final Map<String, VersionedParameterContext> versionedParameterContexts)
        throws ProcessorInstantiationException {
        final ProcessGroup group = flowManager.createProcessGroup(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed));
        group.setVersionedComponentId(proposed.getIdentifier());
        group.setParent(destination);
        updateProcessGroup(group, proposed, componentIdSeed, Collections.emptySet(), true, true, true, variablesToSkip, versionedParameterContexts);
        destination.addProcessGroup(group);
        return group;
    }

    private void updateConnection(final Connection connection, final VersionedConnection proposed) {
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
                    return flowManager.createPrioritizer(prioritizerName);
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

    private Connection addConnection(final ProcessGroup destinationGroup, final VersionedConnection proposed, final String componentIdSeed) {
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

        final Connection connection = flowManager.createConnection(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), proposed.getName(), source, destination,
            proposed.getSelectedRelationships());
        connection.setVersionedComponentId(proposed.getIdentifier());
        destinationGroup.addConnection(connection);
        updateConnection(connection, proposed);

        flowManager.onConnectionAdded(connection);
        return connection;
    }

    private Connectable getConnectable(final ProcessGroup group, final ConnectableComponent connectableComponent) {
        final String id = connectableComponent.getId();

        switch (connectableComponent.getType()) {
            case FUNNEL:
                return group.getFunnels().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny()
                    .orElse(null);
            case INPUT_PORT: {
                final Optional<Port> port = group.getInputPorts().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (port.isPresent()) {
                    return port.get();
                }

                // Attempt to locate child group by versioned component id
                final Optional<ProcessGroup> optionalSpecifiedGroup = group.getProcessGroups().stream()
                    .filter(child -> child.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(child.getIdentifier())).equals(connectableComponent.getGroupId()))
                    .findFirst();

                if (optionalSpecifiedGroup.isPresent()) {
                    final ProcessGroup specifiedGroup = optionalSpecifiedGroup.get();
                    return specifiedGroup.getInputPorts().stream()
                        .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                            NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                        .findAny()
                        .orElse(null);
                }

                // If no child group matched the versioned component id, then look at all child groups. This is done because
                // in older versions, we did not properly map Versioned Component ID's to Ports' parent groups. As a result,
                // if the flow doesn't contain the properly mapped group id, we need to search all child groups.
                return group.getProcessGroups().stream()
                    .flatMap(gr -> gr.getInputPorts().stream())
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny()
                    .orElse(null);
            }
            case OUTPUT_PORT: {
                final Optional<Port> port = group.getOutputPorts().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (port.isPresent()) {
                    return port.get();
                }

                // Attempt to locate child group by versioned component id
                final Optional<ProcessGroup> optionalSpecifiedGroup = group.getProcessGroups().stream()
                    .filter(child -> child.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(child.getIdentifier())).equals(connectableComponent.getGroupId()))
                    .findFirst();

                if (optionalSpecifiedGroup.isPresent()) {
                    final ProcessGroup specifiedGroup = optionalSpecifiedGroup.get();
                    return specifiedGroup.getOutputPorts().stream()
                        .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                            NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                        .findAny()
                        .orElse(null);
                }

                // If no child group matched the versioned component id, then look at all child groups. This is done because
                // in older versions, we did not properly map Versioned Component ID's to Ports' parent groups. As a result,
                // if the flow doesn't contain the properly mapped group id, we need to search all child groups.
                return group.getProcessGroups().stream()
                    .flatMap(gr -> gr.getOutputPorts().stream())
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny()
                    .orElse(null);
            }
            case PROCESSOR:
                return group.getProcessors().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny()
                    .orElse(null);
            case REMOTE_INPUT_PORT: {
                final String rpgId = connectableComponent.getGroupId();
                final Optional<RemoteProcessGroup> rpgOption = group.getRemoteProcessGroups().stream()
                    .filter(component -> rpgId.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (!rpgOption.isPresent()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                final Optional<RemoteGroupPort> portByIdOption = rpg.getInputPorts().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
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
                    .filter(component -> rpgId.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
                    .findAny();

                if (!rpgOption.isPresent()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                final Optional<RemoteGroupPort> portByIdOption = rpg.getOutputPorts().stream()
                    .filter(component -> id.equals(component.getVersionedComponentId().orElse(
                        NiFiRegistryFlowMapper.generateVersionedComponentId(component.getIdentifier()))))
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

    private void updateControllerService(final ControllerServiceNode service, final VersionedControllerService proposed) {
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
                reloadComponent.reload(service, proposed.getType(), newBundleCoordinate, additionalUrls);
            }
        } finally {
            service.resumeValidationTrigger();
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

    private ControllerServiceNode addControllerService(final ProcessGroup destination, final VersionedControllerService proposed, final String componentIdSeed) {
        final String type = proposed.getType();
        final String id = generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed);

        final Bundle bundle = proposed.getBundle();
        final BundleCoordinate coordinate = toCoordinate(bundle);
        final boolean firstTimeAdded = true;
        final Set<URL> additionalUrls = Collections.emptySet();

        final ControllerServiceNode newService = flowManager.createControllerService(type, id, coordinate, additionalUrls, firstTimeAdded, true);
        newService.setVersionedComponentId(proposed.getIdentifier());

        destination.addControllerService(newService);
        updateControllerService(newService, proposed);

        return newService;
    }

    private void updateFunnel(final Funnel funnel, final VersionedFunnel proposed) {
        funnel.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
    }

    private Funnel addFunnel(final ProcessGroup destination, final VersionedFunnel proposed, final String componentIdSeed) {
        final Funnel funnel = flowManager.createFunnel(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed));
        funnel.setVersionedComponentId(proposed.getIdentifier());
        destination.addFunnel(funnel);
        updateFunnel(funnel, proposed);

        return funnel;
    }

    private void updatePort(final Port port, final VersionedPort proposed, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();
        port.setComments(proposed.getComments());
        port.setName(name);
        port.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        port.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());
    }

    private Port addInputPort(final ProcessGroup destination, final VersionedPort proposed, final String componentIdSeed, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();

        final Port port;
        if (proposed.isAllowRemoteAccess()) {
            port = flowManager.createPublicInputPort(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), name);
        } else {
            port = flowManager.createLocalInputPort(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), name);
        }

        port.setVersionedComponentId(proposed.getIdentifier());
        destination.addInputPort(port);
        updatePort(port, proposed, temporaryName);

        return port;
    }

    private Port addOutputPort(final ProcessGroup destination, final VersionedPort proposed, final String componentIdSeed, final String temporaryName) {
        final String name = temporaryName != null ? temporaryName : proposed.getName();

        final Port port;
        if (proposed.isAllowRemoteAccess()) {
            port = flowManager.createPublicOutputPort(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), name);
        } else {
            port = flowManager.createLocalOutputPort(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), name);
        }

        port.setVersionedComponentId(proposed.getIdentifier());
        destination.addOutputPort(port);
        updatePort(port, proposed, temporaryName);

        return port;
    }

    private Label addLabel(final ProcessGroup destination, final VersionedLabel proposed, final String componentIdSeed) {
        final Label label = flowManager.createLabel(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), proposed.getLabel());
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
    }

    private ProcessorNode addProcessor(final ProcessGroup destination, final VersionedProcessor proposed, final String componentIdSeed) throws ProcessorInstantiationException {
        final BundleCoordinate coordinate = toCoordinate(proposed.getBundle());
        final ProcessorNode procNode = flowManager.createProcessor(proposed.getType(), generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), coordinate, true);
        procNode.setVersionedComponentId(proposed.getIdentifier());

        destination.addProcessor(procNode);
        updateProcessor(procNode, proposed);

        return procNode;
    }

    private void updateProcessor(final ProcessorNode processor, final VersionedProcessor proposed) throws ProcessorInstantiationException {
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

            if (proposed.getScheduledState() == org.apache.nifi.registry.flow.ScheduledState.DISABLED) {
                processor.getProcessGroup().disableProcessor(processor);
            } else if (processor.getScheduledState() == ScheduledState.DISABLED) {
                processor.getProcessGroup().enableProcessor(processor);
            }

            if (!isEqual(processor.getBundleCoordinate(), proposed.getBundle())) {
                final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
                final List<PropertyDescriptor> descriptors = new ArrayList<>(processor.getProperties().keySet());
                final Set<URL> additionalUrls = processor.getAdditionalClasspathResources(descriptors);
                reloadComponent.reload(processor, proposed.getType(), newBundleCoordinate, additionalUrls);
            }
        } finally {
            processor.resumeValidationTrigger();
        }
    }

    private Map<String, String> populatePropertiesMap(final ComponentNode componentNode, final Map<String, String> proposedProperties,
                                                      final Map<String, VersionedPropertyDescriptor> proposedDescriptors, final ProcessGroup group) {

        // since VersionedPropertyDescriptor currently doesn't know if it is sensitive or not,
        // keep track of which property descriptors are sensitive from the current properties
        final Set<String> sensitiveProperties = new HashSet<>();

        final Map<String, String> fullPropertyMap = new HashMap<>();
        for (final PropertyDescriptor property : componentNode.getRawPropertyValues().keySet()) {
            if (property.isSensitive()) {
                sensitiveProperties.add(property.getName());
            } else {
                fullPropertyMap.put(property.getName(), null);
            }
        }

        if (proposedProperties != null) {
            // Build a Set of all properties that are included in either the currently configured property values or the proposed values.
            final Set<String> updatedPropertyNames = new HashSet<>();
            updatedPropertyNames.addAll(proposedProperties.keySet());
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
                            final ControllerServiceNode serviceNode = findAncestorControllerService(componentDescriptorValue, getParent());
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
                if (sensitiveProperties.contains(propertyName) && value == null) {
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

                fullPropertyMap.put(propertyName, value);
            }
        }

        return fullPropertyMap;
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

    private RemoteProcessGroup addRemoteProcessGroup(final ProcessGroup destination, final VersionedRemoteProcessGroup proposed, final String componentIdSeed) {
        final RemoteProcessGroup rpg = flowManager.createRemoteProcessGroup(generateUuid(proposed.getIdentifier(), destination.getIdentifier(), componentIdSeed), proposed.getTargetUris());
        rpg.setVersionedComponentId(proposed.getIdentifier());

        destination.addRemoteProcessGroup(rpg);
        updateRemoteProcessGroup(rpg, proposed, componentIdSeed);

        return rpg;
    }

    private void updateRemoteProcessGroup(final RemoteProcessGroup rpg, final VersionedRemoteProcessGroup proposed, final String componentIdSeed) {
        rpg.setComments(proposed.getComments());
        rpg.setCommunicationsTimeout(proposed.getCommunicationsTimeout());
        rpg.setInputPorts(proposed.getInputPorts() == null ? Collections.emptySet() : proposed.getInputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdSeed, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);
        rpg.setName(proposed.getName());
        rpg.setNetworkInterface(proposed.getLocalNetworkInterface());
        rpg.setOutputPorts(proposed.getOutputPorts() == null ? Collections.emptySet() : proposed.getOutputPorts().stream()
            .map(port -> createPortDescriptor(port, componentIdSeed, rpg.getIdentifier()))
            .collect(Collectors.toSet()), false);
        rpg.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        rpg.setProxyHost(proposed.getProxyHost());
        rpg.setProxyPort(proposed.getProxyPort());
        rpg.setProxyUser(proposed.getProxyUser());
        rpg.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(proposed.getTransportProtocol()));
        rpg.setYieldDuration(proposed.getYieldDuration());
    }

    private RemoteProcessGroupPortDescriptor createPortDescriptor(final VersionedRemoteGroupPort proposed, final String componentIdSeed, final String rpgId) {
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
        descriptor.setId(generateUuid(proposed.getIdentifier(), rpgId, componentIdSeed));
        descriptor.setName(proposed.getName());
        descriptor.setUseCompression(proposed.isUseCompression());
        return descriptor;
    }

    private Set<FlowDifference> getModifications() {
        final StandardVersionControlInformation vci = versionControlInfo.get();

        // If this group is not under version control, then we need to notify the parent
        // group (if any) that a modification has taken place. Otherwise, we need to
        // compare the current the flow with the 'versioned snapshot' of the flow in order
        // to determine if the flows are different.
        // We cannot simply say 'if something changed then this flow is different than the versioned snapshot'
        // because if we do this, and a user adds a processor then subsequently removes it, then the logic would
        // say that the flow is modified. There would be no way to ever go back to the flow not being modified.
        // So we have to perform a diff of the flows and see if they are the same.
        if (vci == null) {
            return null;
        }

        if (vci.getFlowSnapshot() == null) {
            // we haven't retrieved the flow from the Flow Registry yet, so we don't know if it's been modified.
            // As a result, we will just return an empty optional
            return null;
        }

        try {
            final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(extensionManager);
            final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(this, controllerServiceProvider, flowRegistryClient, false);

            final ComparableDataFlow currentFlow = new StandardComparableDataFlow("Local Flow", versionedGroup);
            final ComparableDataFlow snapshotFlow = new StandardComparableDataFlow("Versioned Flow", vci.getFlowSnapshot());

            final FlowComparator flowComparator = new StandardFlowComparator(snapshotFlow, currentFlow, getAncestorServiceIds(), new EvolvingDifferenceDescriptor());
            final FlowComparison comparison = flowComparator.compare();
            final Set<FlowDifference> differences = comparison.getDifferences().stream()
                .filter(difference -> difference.getDifferenceType() != DifferenceType.BUNDLE_CHANGED)
                .filter(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS)
                .filter(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES)
                .filter(FlowDifferenceFilters.FILTER_IGNORABLE_VERSIONED_FLOW_COORDINATE_CHANGES)
                .filter(diff -> !FlowDifferenceFilters.isNewPropertyWithDefaultValue(diff, flowManager))
                .filter(diff -> !FlowDifferenceFilters.isNewRelationshipAutoTerminatedAndDefaulted(diff, versionedGroup, flowManager))
                .filter(diff -> !FlowDifferenceFilters.isScheduledStateNew(diff))
                .collect(Collectors.toCollection(HashSet::new));

            LOG.debug("There are {} differences between this Local Flow and the Versioned Flow: {}", differences.size(), differences);
            return differences;
        } catch (final RuntimeException e) {
            throw new RuntimeException("Could not compute differences between local flow and Versioned Flow in NiFi Registry for " + this, e);
        }
    }

    @Override
    public void verifyCanUpdate(final VersionedFlowSnapshot updatedFlow, final boolean verifyConnectionRemoval, final boolean verifyNotDirty) {
        readLock.lock();
        try {
            // flow id match and not dirty check concepts are only applicable to versioned flows
            final VersionControlInformation versionControlInfo = getVersionControlInformation();
            if (versionControlInfo != null) {
                if (!versionControlInfo.getFlowIdentifier().equals(updatedFlow.getSnapshotMetadata().getFlowIdentifier())) {
                    throw new IllegalStateException(this + " is under version control but the given flow does not match the flow that this Process Group is synchronized with");
                }

                if (verifyNotDirty) {
                    final VersionedFlowState flowState = versionControlInfo.getStatus().getState();
                    final boolean modified = flowState == VersionedFlowState.LOCALLY_MODIFIED || flowState == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE;

                    final Set<FlowDifference> modifications = getModifications();

                    if (modified) {
                        final String changes = modifications.stream()
                            .map(FlowDifference::toString)
                            .collect(Collectors.joining("\n"));

                        LOG.error("Cannot change the Version of the flow for {} because the Process Group has been modified ({} modifications) "
                                + "since it was last synchronized with the Flow Registry. The following differences were found:\n{}",
                            this, modifications.size(), changes);

                        throw new IllegalStateException("Cannot change the Version of the flow for " + this
                            + " because the Process Group has been modified (" + modifications.size()
                            + " modifications) since it was last synchronized with the Flow Registry. The Process Group must be"
                            + " reverted to its original form before changing the version.");
                    }
                }

                verifyNoDescendantsWithLocalModifications("be updated");
            }

            final VersionedProcessGroup flowContents = updatedFlow.getFlowContents();

            // Ensure no deleted child process groups contain templates and optionally no deleted connections contain data
            // in their queue. Note that this check enforces ancestry among the group components to avoid a scenario where
            // a component is matched by id, but it does not exist in the same hierarchy and thus will be removed and
            // re-added when the update is performed
            verifyCanRemoveMissingComponents(this, flowContents, verifyConnectionRemoval);

            // Determine which input ports were removed from this process group
            final Map<String, Port> removedInputPortsByVersionId = new HashMap<>();
            getInputPorts()
                .forEach(port -> removedInputPortsByVersionId.put(port.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(port.getIdentifier())), port));
            flowContents.getInputPorts().stream()
                .map(VersionedPort::getIdentifier)
                .forEach(removedInputPortsByVersionId::remove);

            // Ensure that there are no incoming connections for any Input Port that was removed.
            for (final Port inputPort : removedInputPortsByVersionId.values()) {
                final List<Connection> incomingConnections = inputPort.getIncomingConnections();
                if (!incomingConnections.isEmpty()) {
                    throw new IllegalStateException(this + " cannot be updated to the proposed flow because the proposed flow "
                        + "does not contain the Input Port " + inputPort + " and the Input Port currently has an incoming connection");
                }
            }

            // Determine which output ports were removed from this process group
            final Map<String, Port> removedOutputPortsByVersionId = new HashMap<>();
            getOutputPorts()
                .forEach(port -> removedOutputPortsByVersionId.put(port.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(port.getIdentifier())), port));
            flowContents.getOutputPorts().stream()
                .map(VersionedPort::getIdentifier)
                .forEach(removedOutputPortsByVersionId::remove);

            // Ensure that there are no outgoing connections for any Output Port that was removed.
            for (final Port outputPort : removedOutputPortsByVersionId.values()) {
                final Set<Connection> outgoingConnections = outputPort.getConnections();
                if (!outgoingConnections.isEmpty()) {
                    throw new IllegalStateException(this + " cannot be updated to the proposed flow because the proposed flow "
                        + "does not contain the Output Port " + outputPort + " and the Output Port currently has an outgoing connection");
                }
            }

            // Ensure that all Processors are instantiable
            final Map<String, VersionedProcessor> proposedProcessors = new HashMap<>();
            findAllProcessors(flowContents, proposedProcessors);

            findAllProcessors()
                .forEach(proc -> proposedProcessors.remove(proc.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(proc.getIdentifier()))));

            for (final VersionedProcessor processorToAdd : proposedProcessors.values()) {
                final String processorToAddClass = processorToAdd.getType();
                final BundleCoordinate processorToAddCoordinate = toCoordinate(processorToAdd.getBundle());

                // Get the exact bundle requested, if it exists.
                final Bundle bundle = processorToAdd.getBundle();
                final BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
                final org.apache.nifi.bundle.Bundle resolved = extensionManager.getBundle(coordinate);

                if (resolved == null) {
                    // Could not resolve the bundle explicitly. Check for possible bundles.
                    final List<org.apache.nifi.bundle.Bundle> possibleBundles = extensionManager.getBundles(processorToAddClass);
                    final boolean bundleExists = possibleBundles.stream()
                        .anyMatch(b -> processorToAddCoordinate.equals(b.getBundleDetails().getCoordinate()));

                    if (!bundleExists && possibleBundles.size() != 1) {
                        throw new IllegalArgumentException("Unknown bundle " + processorToAddCoordinate.toString() + " for processor type " + processorToAddClass);
                    }
                }
            }

            // Ensure that all Controller Services are instantiable
            final Map<String, VersionedControllerService> proposedServices = new HashMap<>();
            findAllControllerServices(flowContents, proposedServices);

            findAllControllerServices()
                .forEach(service -> proposedServices.remove(service.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(service.getIdentifier()))));

            for (final VersionedControllerService serviceToAdd : proposedServices.values()) {
                final String serviceToAddClass = serviceToAdd.getType();
                final BundleCoordinate serviceToAddCoordinate = toCoordinate(serviceToAdd.getBundle());

                final org.apache.nifi.bundle.Bundle resolved = extensionManager.getBundle(serviceToAddCoordinate);
                if (resolved == null) {
                    final List<org.apache.nifi.bundle.Bundle> possibleBundles = extensionManager.getBundles(serviceToAddClass);
                    final boolean bundleExists = possibleBundles.stream()
                        .anyMatch(b -> serviceToAddCoordinate.equals(b.getBundleDetails().getCoordinate()));

                    if (!bundleExists && possibleBundles.size() != 1) {
                        throw new IllegalArgumentException("Unknown bundle " + serviceToAddCoordinate.toString() + " for service type " + serviceToAddClass);
                    }
                }
            }

            // Ensure that all Prioritizers are instantiate-able and that any load balancing configuration is correct
            // Enforcing ancestry on connection matching here is not important because all we're interested in is locating
            // new prioritizers and load balance strategy types so if a matching connection existed anywhere in the current
            // flow, then its prioritizer and load balance strategy are already validated
            final Map<String, VersionedConnection> proposedConnections = new HashMap<>();
            findAllConnections(flowContents, proposedConnections);

            findAllConnections()
                .forEach(conn -> proposedConnections.remove(conn.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(conn.getIdentifier()))));

            for (final VersionedConnection connectionToAdd : proposedConnections.values()) {
                if (connectionToAdd.getPrioritizers() != null) {
                    for (final String prioritizerType : connectionToAdd.getPrioritizers()) {
                        try {
                            flowManager.createPrioritizer(prioritizerType);
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
        } finally {
            readLock.unlock();
        }
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
                .collect(Collectors.toMap(component -> component.getIdentifier(), Function.identity()));

            // match group's current connections to proposed connections to determine if they've been removed
            for (final Connection connection : processGroup.getConnections()) {
                final String versionedId = connection.getVersionedComponentId().orElse(
                    NiFiRegistryFlowMapper.generateVersionedComponentId(connection.getIdentifier()));
                final VersionedConnection proposedConnection = proposedConnectionsByVersionedId.get(versionedId);
                if (proposedConnection == null) {
                    // connection doesn't exist in proposed connections, make sure it doesn't have any data in it
                    final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
                    if (!flowFileQueue.isEmpty()) {
                        throw new IllegalStateException(this + " cannot be updated to the proposed flow because the proposed flow "
                            + "does not contain a match for " + connection + " and the connection currently has data in the queue.");
                    }
                }
            }
        }

        final Map<String, VersionedProcessGroup> proposedGroupsByVersionedId = proposedGroup.getProcessGroups().stream()
            .collect(Collectors.toMap(component -> component.getIdentifier(), Function.identity()));

        // match current child groups to proposed child groups to determine if they've been removed
        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            final String versionedId = childGroup.getVersionedComponentId().orElse(
                NiFiRegistryFlowMapper.generateVersionedComponentId(childGroup.getIdentifier()));
            final VersionedProcessGroup proposedChildGroup = proposedGroupsByVersionedId.get(versionedId);
            if (proposedChildGroup == null) {
                // child group will be removed, check group and descendants for attached templates
                final Template removedTemplate = findAllTemplates(childGroup).stream().findFirst().orElse(null);
                if (removedTemplate != null) {
                    throw new IllegalStateException(this + " cannot be updated to the proposed flow because the child " + removedTemplate.getProcessGroup()
                        + " that exists locally has one or more Templates, and the proposed flow does not contain these templates. "
                        + "A Process Group cannot be deleted while it contains Templates. Please remove the Templates before re-attempting.");
                }
                if (verifyConnectionRemoval) {
                    // check removed group and its descendants for connections with data in the queue
                    final Connection removedConnection = findAllConnections(childGroup).stream()
                        .filter(connection -> !connection.getFlowFileQueue().isEmpty()).findFirst().orElse(null);
                    if (removedConnection != null) {
                        throw new IllegalStateException(this + " cannot be updated to the proposed flow because the proposed flow "
                            + "does not contain a match for " + removedConnection + " and the connection currently has data in the queue.");
                    }
                }
            } else {
                // child group successfully matched, recurse into verification of its contents
                verifyCanRemoveMissingComponents(childGroup, proposedChildGroup, verifyConnectionRemoval);
            }
        }
    }

    @Override
    public void verifyCanSaveToFlowRegistry(final String registryId, final String bucketId, final String flowId, final String saveAction) {
        verifyNoDescendantsWithLocalModifications("be saved to a Flow Registry");

        final StandardVersionControlInformation vci = versionControlInfo.get();
        if (vci != null) {
            if (flowId != null && flowId.equals(vci.getFlowIdentifier())) {
                // Flow ID is the same. We want to publish the Process Group as the next version of the Flow.
                // In order to do this, we have to ensure that the Process Group is 'current'.
                final VersionedFlowState state = vci.getStatus().getState();
                if (state == VersionedFlowState.STALE
                    || (state == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE && VersionedFlowDTO.COMMIT_ACTION.equals(saveAction))) {
                    throw new IllegalStateException("Cannot update Version Control Information for Process Group with ID " + getIdentifier()
                        + " because the Process Group in the flow is not synchronized with the most recent version of the Flow in the Flow Registry. "
                        + "In order to publish a new version of the Flow, the Process Group must first be in synch with the latest version in the Flow Registry.");
                }

                // Flow ID matches. We want to publish the Process Group as the next version of the Flow, so we must
                // ensure that all other parameters match as well.
                if (!bucketId.equals(vci.getBucketIdentifier())) {
                    throw new IllegalStateException("Cannot update Version Control Information for Process Group with ID " + getIdentifier()
                        + " because the Process Group is currently synchronized with a different Versioned Flow than the one specified in the request.");
                }

                if (!registryId.equals(vci.getRegistryIdentifier())) {
                    throw new IllegalStateException("Cannot update Version Control Information for Process Group with ID " + getIdentifier()
                        + " because the Process Group is currently synchronized with a different Versioned Flow than the one specified in the request.");
                }
            } else if (flowId != null) {
                // Flow ID is specified but different. This is not allowed, because Flow ID's are automatically generated,
                // and if the client is specifying an ID then it is either trying to assign the ID of the Flow or it is
                // attempting to save a new version of a different flow. Saving a new version of a different Flow is
                // not allowed because the Process Group must be in synch with the latest version of the flow before that
                // can be done.
                throw new IllegalStateException("Cannot update Version Control Information for Process Group with ID " + getIdentifier()
                    + " because the Process Group is currently synchronized with a different Versioned Flow than the one specified in the request.");
            }
        }
    }

    @Override
    public void verifyCanRevertLocalModifications() {
        final StandardVersionControlInformation svci = versionControlInfo.get();
        if (svci == null) {
            throw new IllegalStateException("Cannot revert local modifications to Process Group because the Process Group is not under Version Control.");
        }

        verifyNoDescendantsWithLocalModifications("have its local modifications reverted");
    }

    @Override
    public void verifyCanShowLocalModifications() {

    }

    private void verifyNoDescendantsWithLocalModifications(final String action) {
        for (final ProcessGroup descendant : findAllProcessGroups()) {
            final VersionControlInformation descendantVci = descendant.getVersionControlInformation();
            if (descendantVci != null) {
                final VersionedFlowState flowState = descendantVci.getStatus().getState();
                final boolean modified = flowState == VersionedFlowState.LOCALLY_MODIFIED || flowState == VersionedFlowState.LOCALLY_MODIFIED_AND_STALE;

                if (modified) {
                    throw new IllegalStateException("Process Group cannot " + action + " because it contains a child or descendant Process Group that is under Version Control and "
                        + "has local modifications. Each descendant Process Group that is under Version Control must first be reverted or have its changes pushed to the Flow Registry before "
                        + "this action can be performed on the parent Process Group.");
                }

                if (flowState == VersionedFlowState.SYNC_FAILURE) {
                    throw new IllegalStateException("Process Group cannot " + action + " because it contains a child or descendant Process Group that is under Version Control and "
                        + "is not synchronized with the Flow Registry. Each descendant Process Group must first be synchronized with the Flow Registry before this action can be "
                        + "performed on the parent Process Group. NiFi will continue to attempt to communicate with the Flow Registry periodically in the background.");
                }
            }
        }
    }

    @Override
    public FlowFileGate getFlowFileGate() {
        return flowFileGate;
    }

    @Override
    public FlowFileConcurrency getFlowFileConcurrency() {
        readLock.lock();
        try {
            return flowFileConcurrency;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void setFlowFileConcurrency(final FlowFileConcurrency flowFileConcurrency) {
        writeLock.lock();
        try {
            if (this.flowFileConcurrency == flowFileConcurrency) {
                return;
            }

            this.flowFileConcurrency = flowFileConcurrency;
            switch (flowFileConcurrency) {
                case UNBOUNDED:
                    flowFileGate = new UnboundedFlowFileGate();
                    break;
                case SINGLE_FLOWFILE_PER_NODE:
                    flowFileGate = new SingleConcurrencyFlowFileGate();
                    break;
                case SINGLE_BATCH_PER_NODE:
                    flowFileGate = new SingleBatchFlowFileGate();
            }

            setBatchCounts(getFlowFileOutboundPolicy(), flowFileConcurrency);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isDataQueued() {
        return isDataQueued(connection -> true);
    }

    @Override
    public boolean isDataQueuedForProcessing() {
        // Data is queued for processing if a connection has data queued and the connection's destination is NOT an Output Port.
        return isDataQueued(connection -> connection.getDestination().getConnectableType() != ConnectableType.OUTPUT_PORT);
    }

    private boolean isDataQueued(final Predicate<Connection> connectionFilter) {
        readLock.lock();
        try {
            for (final Connection connection : this.connections.values()) {
                // If the connection doesn't pass the filter, just skip over it.
                if (!connectionFilter.test(connection)) {
                    continue;
                }

                final boolean queueEmpty = connection.getFlowFileQueue().isEmpty();
                if (!queueEmpty) {
                    return true;
                }
            }

            for (final ProcessGroup child : this.processGroups.values()) {
                // Check if the child Process Group has any data enqueued. Note that we call #isDataQueued here and NOT
                // #isDataQueeudForProcesing. I.e., regardless of whether this is called from #isDataQueued or #isDataQueuedForProcessing,
                // for child groups, we only call #isDataQueued. This is because if data is queued up for the Output Port of a child group,
                // it is still considered to be data that is being processed by this Process Group.
                if (child.isDataQueued()) {
                    return true;
                }
            }

            return false;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public FlowFileOutboundPolicy getFlowFileOutboundPolicy() {
        return flowFileOutboundPolicy;
    }

    @Override
    public void setFlowFileOutboundPolicy(final FlowFileOutboundPolicy flowFileOutboundPolicy) {
        this.flowFileOutboundPolicy = flowFileOutboundPolicy;
        setBatchCounts(flowFileOutboundPolicy, getFlowFileConcurrency());
    }

    private synchronized void setBatchCounts(final FlowFileOutboundPolicy outboundPolicy, final FlowFileConcurrency flowFileConcurrency) {
        if (outboundPolicy == FlowFileOutboundPolicy.BATCH_OUTPUT && flowFileConcurrency == FlowFileConcurrency.SINGLE_FLOWFILE_PER_NODE) {
            if (batchCounts instanceof NoOpBatchCounts) {
                final StateManager stateManager = stateManagerProvider.getStateManager(getIdentifier());
                batchCounts = new StandardBatchCounts(this, stateManager);
            }
        } else {
            if (batchCounts != null) {
                batchCounts.reset();
            }

            batchCounts = new NoOpBatchCounts();
        }
    }

    @Override
    public DataValve getDataValve(final Port port) {
        final ProcessGroup portGroupsParent = port.getProcessGroup().getParent();
        return portGroupsParent == null ? getDataValve() : portGroupsParent.getDataValve();
    }

    @Override
    public DataValve getDataValve() {
        return dataValve;
    }

    @Override
    public void setDefaultFlowFileExpiration(final String defaultFlowFileExpiration) {
        // use default if value not provided
        if (StringUtils.isBlank(defaultFlowFileExpiration)) {
            this.defaultFlowFileExpiration.set(DEFAULT_FLOWFILE_EXPIRATION);
        } else {
            // Validate entry: must include time unit label
            Pattern pattern = Pattern.compile(FormatUtils.TIME_DURATION_REGEX);
            String caseAdjustedExpiration = defaultFlowFileExpiration.toLowerCase();
            if (pattern.matcher(caseAdjustedExpiration).matches()) {
                this.defaultFlowFileExpiration.set(caseAdjustedExpiration);
            } else {
                throw new IllegalArgumentException("The Default FlowFile Expiration of the process group must contain a valid time unit.");
            }
        }
    }

    @Override
    public String getDefaultFlowFileExpiration() {
        // Use value in this object if it has been set. Otherwise, inherit from parent group; if at root group, use the default.
        if (defaultFlowFileExpiration.get() == null) {
            if (isRootGroup()) {
                return DEFAULT_FLOWFILE_EXPIRATION;
            } else {
                return parent.get().getDefaultFlowFileExpiration();
            }
        }
        return defaultFlowFileExpiration.get();
    }

    @Override
    public void setDefaultBackPressureObjectThreshold(final Long defaultBackPressureObjectThreshold) {
        // use default if value not provided
        if (defaultBackPressureObjectThreshold == null) {
            this.defaultBackPressureObjectThreshold.set(nifiPropertiesBackpressureCount);
        } else {
            this.defaultBackPressureObjectThreshold.set(defaultBackPressureObjectThreshold);
        }
    }

    @Override
    public Long getDefaultBackPressureObjectThreshold() {
        // Use value in this object if it has been set. Otherwise, inherit from parent group; if at root group, obtain from nifi properties.
        if (defaultBackPressureObjectThreshold.get() == null) {
            if (isRootGroup()) {
                return nifiPropertiesBackpressureCount;
            } else {
                return getParent().getDefaultBackPressureObjectThreshold();
            }
        }

        return defaultBackPressureObjectThreshold.get();
    }

    @Override
    public void setDefaultBackPressureDataSizeThreshold(final String defaultBackPressureDataSizeThreshold) {
        // use default if value not provided
        if (StringUtils.isBlank(defaultBackPressureDataSizeThreshold)) {
            this.defaultBackPressureDataSizeThreshold.set(nifiPropertiesBackpressureSize);
        } else {
            DataUnit.parseDataSize(defaultBackPressureDataSizeThreshold, DataUnit.B);
            this.defaultBackPressureDataSizeThreshold.set(defaultBackPressureDataSizeThreshold.toUpperCase());
        }
    }

    @Override
    public String getDefaultBackPressureDataSizeThreshold() {
        // Use value in this object if it has been set. Otherwise, inherit from parent group; if at root group, obtain from nifi properties.
        if (StringUtils.isEmpty(defaultBackPressureDataSizeThreshold.get())) {
            if (isRootGroup()) {
                return nifiPropertiesBackpressureSize;
            } else {
                return parent.get().getDefaultBackPressureDataSizeThreshold();
            }
        }

        return defaultBackPressureDataSizeThreshold.get();
    }
}
