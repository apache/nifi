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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.RemoteFlowCoordinates;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.UnknownResourceException;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StandardProcessGroup implements ProcessGroup {

    private final String id;
    private final AtomicReference<ProcessGroup> parent;
    private final AtomicReference<String> name;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> comments;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicReference<StandardVersionControlInformation> versionControlInfo = new AtomicReference<>();

    private final StandardProcessScheduler scheduler;
    private final ControllerServiceProvider controllerServiceProvider;
    private final FlowController flowController;

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
    private final StringEncryptor encryptor;
    private final MutableVariableRegistry variableRegistry;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessGroup.class);

    public StandardProcessGroup(final String id, final ControllerServiceProvider serviceProvider, final StandardProcessScheduler scheduler,
            final NiFiProperties nifiProps, final StringEncryptor encryptor, final FlowController flowController,
            final MutableVariableRegistry variableRegistry) {
        this.id = id;
        this.controllerServiceProvider = serviceProvider;
        this.parent = new AtomicReference<>();
        this.scheduler = scheduler;
        this.comments = new AtomicReference<>("");
        this.encryptor = encryptor;
        this.flowController = flowController;
        this.variableRegistry = variableRegistry;

        name = new AtomicReference<>();
        position = new AtomicReference<>(new Position(0D, 0D));
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
            throw new IllegalArgumentException("The name cannot be blank.");
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
        int inputPortCount = 0;
        int outputPortCount = 0;

        int running = 0;
        int stopped = 0;
        int invalid = 0;
        int disabled = 0;
        int activeRemotePorts = 0;
        int inactiveRemotePorts = 0;

        readLock.lock();
        try {
            for (final ProcessorNode procNode : processors.values()) {
                if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
                    disabled++;
                } else if (procNode.isRunning()) {
                    running++;
                } else if (!procNode.isValid()) {
                    invalid++;
                } else {
                    stopped++;
                }
            }

            inputPortCount = inputPorts.size();
            for (final Port port : inputPorts.values()) {
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

            outputPortCount = outputPorts.size();
            for (final Port port : outputPorts.values()) {
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
            }

            for (final RemoteProcessGroup remoteGroup : findAllRemoteProcessGroups()) {
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

        return new ProcessGroupCounts(inputPortCount, outputPortCount, running, stopped,
                invalid, disabled, activeRemotePorts, inactiveRemotePorts);
    }

    @Override
    public boolean isRootGroup() {
        return parent.get() == null;
    }

    @Override
    public void startProcessing() {
        readLock.lock();
        try {
            findAllProcessors().stream().filter(SCHEDULABLE_PROCESSORS).forEach(node -> {
                try {
                    node.getProcessGroup().startProcessor(node, true);
                } catch (final Throwable t) {
                    LOG.error("Unable to start processor {} due to {}", new Object[]{node.getIdentifier(), t});
                }
            });

            findAllInputPorts().stream().filter(SCHEDULABLE_PORTS).forEach(port -> {
                port.getProcessGroup().startInputPort(port);
            });

            findAllOutputPorts().stream().filter(SCHEDULABLE_PORTS).forEach(port -> {
                port.getProcessGroup().startOutputPort(port);
            });
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void stopProcessing() {
        readLock.lock();
        try {
            findAllProcessors().stream().filter(UNSCHEDULABLE_PROCESSORS).forEach(node -> {
                try {
                    node.getProcessGroup().stopProcessor(node);
                } catch (final Throwable t) {
                    LOG.error("Unable to stop processor {} due to {}", new Object[]{node.getIdentifier(), t});
                }
            });

            findAllInputPorts().stream().filter(UNSCHEDULABLE_PORTS).forEach(port -> {
                port.getProcessGroup().stopInputPort(port);
            });

            findAllOutputPorts().stream().filter(UNSCHEDULABLE_PORTS).forEach(port -> {
                port.getProcessGroup().stopOutputPort(port);
            });
        } finally {
            readLock.unlock();
        }
    }

    private StateManager getStateManager(final String componentId) {
        return flowController.getStateManagerProvider().getStateManager(componentId);
    }

    private void shutdown(final ProcessGroup procGroup) {
        for (final ProcessorNode node : procGroup.getProcessors()) {
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(node.getProcessor().getClass(), node.getIdentifier())) {
                final StandardProcessContext processContext = new StandardProcessContext(node, controllerServiceProvider, encryptor, getStateManager(node.getIdentifier()));
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, node.getProcessor(), processContext);
            }
        }

        for (final RemoteProcessGroup rpg : procGroup.getRemoteProcessGroups()) {
            rpg.shutdown();
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

    @Override
    public void addInputPort(final Port port) {
        if (isRootGroup()) {
            if (!(port instanceof RootGroupPort)) {
                throw new IllegalArgumentException("Cannot add Input Port of type " + port.getClass().getName() + " to the Root Group");
            }
        } else if (!(port instanceof LocalPort)) {
            throw new IllegalArgumentException("Cannot add Input Port of type " + port.getClass().getName() + " to a non-root group");
        }

        writeLock.lock();
        try {
            if (inputPorts.containsKey(requireNonNull(port).getIdentifier())
                    || getInputPortByName(port.getName()) != null) {
                throw new IllegalStateException("The input port name or identifier is not available to be added.");
            }

            port.setProcessGroup(this);
            inputPorts.put(requireNonNull(port).getIdentifier(), port);
            flowController.onInputPortAdded(port);
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

            flowController.onInputPortRemoved(port);
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
            if (!(port instanceof RootGroupPort)) {
                throw new IllegalArgumentException("Cannot add Output Port " + port.getClass().getName() + " to the Root Group");
            }
        } else if (!(port instanceof LocalPort)) {
            throw new IllegalArgumentException("Cannot add Output Port " + port.getClass().getName() + " to a non-root group");
        }

        writeLock.lock();
        try {
            if (outputPorts.containsKey(requireNonNull(port).getIdentifier())
                    || getOutputPortByName(port.getName()) != null) {
                throw new IllegalStateException("Output Port with given identifier or name is not available");
            }

            port.setProcessGroup(this);
            outputPorts.put(port.getIdentifier(), port);
            flowController.onOutputPortAdded(port);
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

            flowController.onOutputPortRemoved(port);
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
    public void addProcessGroup(final ProcessGroup group) {
        if (StringUtils.isEmpty(group.getName())) {
            throw new IllegalArgumentException("Process Group's name must be specified");
        }

        writeLock.lock();
        try {
            group.setParent(this);
            group.getVariableRegistry().setParent(getVariableRegistry());

            processGroups.put(Objects.requireNonNull(group).getIdentifier(), group);
            flowController.onProcessGroupAdded(group);
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
            flowController.onProcessGroupRemoved(group);
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
            group.removeControllerService(cs);
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
            flowController.onProcessorAdded(processor);
        } finally {
            writeLock.unlock();
        }
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

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(processor.getProcessor().getClass(), processor.getIdentifier())) {
                final StandardProcessContext processContext = new StandardProcessContext(processor, controllerServiceProvider, encryptor, getStateManager(processor.getIdentifier()));
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, processor.getProcessor(), processContext);
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke 'OnRemoved' methods of processor with id " + processor.getIdentifier(), e);
            }

            for (final Map.Entry<PropertyDescriptor, String> entry : processor.getProperties().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                if (descriptor.getControllerServiceDefinition() != null) {
                    final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                    if (value != null) {
                        final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                        if (serviceNode != null) {
                            serviceNode.removeReference(processor);
                        }
                    }
                }
            }

            processors.remove(id);
            flowController.onProcessorRemoved(processor);
            LogRepositoryFactory.getRepository(processor.getIdentifier()).removeAllObservers();

            final StateManagerProvider stateManagerProvider = flowController.getStateManagerProvider();
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
                    ExtensionManager.removeInstanceClassLoader(id);
                } catch (Throwable t) {
                }
            }
            writeLock.unlock();
        }
    }

    @Override
    public Set<ProcessorNode> getProcessors() {
        readLock.lock();
        try {
            return new LinkedHashSet<>(processors.values());
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
            flowController.onConnectionAdded(connection);
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
            flowController.onConnectionRemoved(connection);
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
        final Connection connection = flowController.getConnection(id);
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
    public CompletableFuture<Void> startProcessor(final ProcessorNode processor, final boolean failIfStopping) {
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

            return scheduler.startProcessor(processor, failIfStopping);
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
    public CompletableFuture<Void> stopProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.STOPPED) {
                return CompletableFuture.completedFuture(null);
            }

            return scheduler.stopProcessor(processor);
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
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("identifier", getIdentifier()).toString();
    }

    @Override
    public ProcessGroup findProcessGroup(final String id) {
        if (requireNonNull(id).equals(getIdentifier())) {
            return this;
        }

        final ProcessGroup group = flowController.getGroup(id);
        if (group == null) {
            return null;
        }

        // We found a Processor in the Controller, but we only want to return it if
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
        final ProcessorNode node = flowController.getProcessorNode(id);
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

        if (owner == this) {
            return true;
        }

        return false;
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
    public Connectable findLocalConnectable(final String identifier) {
        return findLocalConnectable(identifier, this);
    }

    private static Connectable findLocalConnectable(final String identifier, final ProcessGroup group) {
        final ProcessorNode procNode = group.getProcessor(identifier);
        if (procNode != null) {
            return procNode;
        }

        final Port inPort = group.getInputPort(identifier);
        if (inPort != null) {
            return inPort;
        }

        final Port outPort = group.getOutputPort(identifier);
        if (outPort != null) {
            return outPort;
        }

        final Funnel funnel = group.getFunnel(identifier);
        if (funnel != null) {
            return funnel;
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final Connectable childGroupConnectable = findLocalConnectable(identifier, childGroup);
            if (childGroupConnectable != null) {
                return childGroupConnectable;
            }
        }

        return null;
    }

    @Override
    public RemoteGroupPort findRemoteGroupPort(final String identifier) {
        return findRemoteGroupPort(identifier, this);
    }

    private static RemoteGroupPort findRemoteGroupPort(final String identifier, final ProcessGroup group) {
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            final RemoteGroupPort remoteInPort = remoteGroup.getInputPort(identifier);
            if (remoteInPort != null) {
                return remoteInPort;
            }

            final RemoteGroupPort remoteOutPort = remoteGroup.getOutputPort(identifier);
            if (remoteOutPort != null) {
                return remoteOutPort;
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final RemoteGroupPort childGroupRemoteGroupPort = findRemoteGroupPort(identifier, childGroup);
            if (childGroupRemoteGroupPort != null) {
                return childGroupRemoteGroupPort;
            }
        }

        return null;
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
        final Port port = flowController.getInputPort(id);
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
        final Port port = flowController.getOutputPort(id);
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
            flowController.onFunnelAdded(funnel);

            if (autoStart) {
                startFunnel(funnel);
            }
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
        final Funnel funnel = flowController.getFunnel(id);
        if (funnel == null) {
            return funnel;
        }

        if (isOwner(funnel.getProcessGroup())) {
            return funnel;
        }

        return null;
    }


    @Override
    public ControllerServiceNode findControllerService(final String id) {
        return findControllerService(id, this);
    }

    private ControllerServiceNode findControllerService(final String id, final ProcessGroup start) {
        ControllerServiceNode service = start.getControllerService(id);
        if (service != null) {
            return service;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            service = findControllerService(id, group);
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

    public Set<ControllerServiceNode> findAllControllerServices(ProcessGroup start) {
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
            flowController.onFunnelRemoved(funnel);
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
        readLock.lock();
        try {
            final Set<ControllerServiceNode> services = new HashSet<>();
            services.addAll(controllerServices.values());

            if (recursive && parent.get() != null) {
                services.addAll(parent.get().getControllerServices(true));
            }

            return services;
        } finally {
            readLock.unlock();
        }
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

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(service.getControllerServiceImplementation().getClass(), service.getIdentifier())) {
                final ConfigurationContext configurationContext = new StandardConfigurationContext(service, controllerServiceProvider, null, variableRegistry);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, service.getControllerServiceImplementation(), configurationContext);
            }

            for (final Map.Entry<PropertyDescriptor, String> entry : service.getProperties().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                if (descriptor.getControllerServiceDefinition() != null) {
                    final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                    if (value != null) {
                        final ControllerServiceNode referencedNode = getControllerService(value);
                        if (referencedNode != null) {
                            referencedNode.removeReference(service);
                        }
                    }
                }
            }

            controllerServices.remove(service.getIdentifier());
            flowController.getStateManagerProvider().onComponentRemoved(service.getIdentifier());

            removed = true;
            LOG.info("{} removed from {}", service, this);
        } finally {
            if (removed) {
                try {
                    ExtensionManager.removeInstanceClassLoader(service.getIdentifier());
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
                final int activeThreadCount = scheduler.getActiveThreadCount(procNode);
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

            if (!isDisconnected(snippet)) {
                throw new IllegalStateException("One or more components within the snippet is connected to a component outside of the snippet. Only a disconnected snippet may be moved.");
            }

            if (isRootGroup() && (!snippet.getInputPorts().isEmpty() || !snippet.getOutputPorts().isEmpty())) {
                throw new IllegalStateException("Cannot move Ports out of the root group");
            }

            if (destination.isRootGroup() && (!snippet.getInputPorts().isEmpty() || !snippet.getOutputPorts().isEmpty())) {
                throw new IllegalStateException("Cannot move Ports into the root group");
            }

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
    public void verifyCanDelete(final boolean ignoreConnections) {
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
                childGroup.verifyCanDelete(true);
            }

            if (!templates.isEmpty()) {
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

            if (isRootGroup() && (!snippet.getInputPorts().isEmpty() || !snippet.getOutputPorts().isEmpty())) {
                throw new IllegalStateException("Cannot move Ports from the Root Group to a Non-Root Group");
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

            for (final String id : snippet.getProcessors().keySet()) {
                final ProcessorNode processorNode = getProcessor(id);
                for (final PropertyDescriptor descriptor : processorNode.getProperties().keySet()) {
                    final Class<? extends ControllerService> serviceDefinition = descriptor.getControllerServiceDefinition();

                    // if this descriptor identifies a controller service
                    if (serviceDefinition != null) {
                        final String serviceId = processorNode.getProperty(descriptor);

                        // if the processor is configured with a service
                        if (serviceId != null) {
                            // get all the available services
                            final Set<String> currentControllerServiceIds = controllerServiceProvider.getControllerServiceIdentifiers(serviceDefinition, getIdentifier());
                            final Set<String> proposedControllerServiceIds = controllerServiceProvider.getControllerServiceIdentifiers(serviceDefinition, newProcessGroup.getIdentifier());

                            // ensure the configured service is an allowed service if it's still a valid service
                            if (currentControllerServiceIds.contains(serviceId) && !proposedControllerServiceIds.contains(serviceId)) {
                                throw new IllegalStateException("Cannot perform Move Operation because a Processor references a service that is not available in the destination Process Group");
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

            for (final ProcessorNode processor : findAllProcessors()) {
                if (!processor.isRunning()) {
                    continue;
                }

                for (final String variableName : updatedVariableNames) {
                    for (final VariableImpact impact : getVariableImpact(processor)) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + processor + ", which is currently running");
                        }
                    }
                }
            }

            for (final ControllerServiceNode service : findAllControllerServices()) {
                if (!service.isActive()) {
                    continue;
                }

                for (final String variableName : updatedVariableNames) {
                    for (final VariableImpact impact : getVariableImpact(service)) {
                        if (impact.isImpacted(variableName)) {
                            throw new IllegalStateException("Cannot update variable '" + variableName + "' because it is referenced by " + service + ", which is currently running");
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
    public Set<ConfiguredComponent> getComponentsAffectedByVariable(final String variableName) {
        final Set<ConfiguredComponent> affected = new HashSet<>();

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
                    affected.addAll(reference.findRecursiveReferences(ConfiguredComponent.class));
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

    private List<VariableImpact> getVariableImpact(final ConfiguredComponent component) {
        return component.getProperties().keySet().stream()
            .map(descriptor -> {
                final String configuredVal = component.getProperty(descriptor);
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
            variables.entrySet().stream() // cannot use Collectors.toMap because value may be null
                .forEach(entry -> variableMap.put(new VariableDescriptor(entry.getKey()), entry.getValue()));

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
    public void setVersionControlInformation(final VersionControlInformation versionControlInformation, final Map<String, String> versionedComponentIds) {
        final StandardVersionControlInformation svci = new StandardVersionControlInformation(versionControlInformation.getRegistryIdentifier(),
            versionControlInformation.getBucketIdentifier(),
            versionControlInformation.getFlowIdentifier(),
            versionControlInformation.getVersion(),
            versionControlInformation.getFlowSnapshot(),
            versionControlInformation.getModified().orElse(null),
            versionControlInformation.getCurrent().orElse(null)) {

            @Override
            public Optional<Boolean> getModified() {
                return StandardProcessGroup.this.isModified();
            }
        };

        writeLock.lock();
        try {
            updateVersionedComponentIds(this, versionedComponentIds);
            this.versionControlInfo.set(svci);
        } finally {
            writeLock.unlock();
        }
    }

    private void updateVersionedComponentIds(final ProcessGroup processGroup, final Map<String, String> versionedComponentIds) {
        if (versionedComponentIds == null || versionedComponentIds.isEmpty()) {
            return;
        }

        processGroup.setVersionedComponentId(versionedComponentIds.get(processGroup.getIdentifier()));

        processGroup.getConnections().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getProcessors().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getInputPorts().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getOutputPorts().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getLabels().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getFunnels().stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));
        processGroup.getControllerServices(false).stream()
            .forEach(component -> component.setVersionedComponentId(versionedComponentIds.get(component.getIdentifier())));

        processGroup.getRemoteProcessGroups().stream()
            .forEach(rpg -> {
                rpg.setVersionedComponentId(versionedComponentIds.get(rpg.getIdentifier()));

                rpg.getInputPorts().stream()
                    .forEach(port -> port.setVersionedComponentId(versionedComponentIds.get(port.getIdentifier())));

                rpg.getOutputPorts().stream()
                    .forEach(port -> port.setVersionedComponentId(versionedComponentIds.get(port.getIdentifier())));
            });

        processGroup.getProcessGroups().stream()
            .forEach(childGroup -> updateVersionedComponentIds(childGroup, versionedComponentIds));
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
            LOG.error("Unable to synchronize {} with Flow Registry because Process Group was placed under Version Control using Flow Registry "
                + "with identifier {} but cannot find any Flow Registry with this identifier", this, registryId);
            return;
        }

        try {
            final int latestVersion = flowRegistry.getLatestVersion(vci.getBucketIdentifier(), vci.getFlowIdentifier());

            if (latestVersion == vci.getVersion()) {
                LOG.debug("{} is currently at the most recent version ({}) of the flow that is under Version Control", this, latestVersion);
                vci.setCurrent(true);
            } else {
                vci.setCurrent(false);
                LOG.info("{} is not the most recent version of the flow that is under Version Control; current version is {}; most recent version is {}",
                    new Object[] {this, vci.getVersion(), latestVersion});
            }
        } catch (final IOException | UnknownResourceException e) {
            LOG.error("Failed to synchronize {} with Flow Registry because could not determine the most recent version of the Flow in the Flow Registry", this, e);
        }

        final VersionedProcessGroup snapshot = vci.getFlowSnapshot();
        if (snapshot == null) {
            // We have not yet obtained the snapshot from the Flow Registry, so we need to request the snapshot of our local version of the flow from the Flow Registry.
            // This allows us to know whether or not the flow has been modified since it was last synced with the Flow Registry.
            try {
                final VersionedFlowSnapshot registrySnapshot = flowRegistry.getFlowContents(vci.getBucketIdentifier(), vci.getFlowIdentifier(), vci.getVersion());
                final VersionedProcessGroup registryFlow = registrySnapshot.getFlowContents();
                vci.setFlowSnapshot(registryFlow);
            } catch (final IOException | UnknownResourceException e) {
                LOG.error("Failed to synchronize {} with Flow Registry because could not retrieve version {} of flow with identifier {} in bucket {}",
                    new Object[] {this, vci.getVersion(), vci.getFlowIdentifier(), vci.getBucketIdentifier()}, e);
                return;
            }
        }
    }


    @Override
    public void updateFlow(final VersionedFlowSnapshot proposedSnapshot, final String componentIdSeed, final boolean verifyNotDirty) {
        writeLock.lock();
        try {
            verifyCanUpdate(proposedSnapshot, true, verifyNotDirty); // TODO: Should perform more verification... verifyCanDelete, verifyCanUpdate, etc. Recursively if child is under VC also

            final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper();
            final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(this, flowController.getFlowRegistryClient());

            final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", versionedGroup);
            final ComparableDataFlow remoteFlow = new StandardComparableDataFlow("Remote Flow", proposedSnapshot.getFlowContents());

            final FlowComparator flowComparator = new StandardFlowComparator(localFlow, remoteFlow);
            final FlowComparison flowComparison = flowComparator.compare();

            final Set<String> updatedVersionedComponentIds = flowComparison.getDifferences().stream()
                .filter(diff -> diff.getDifferenceType() != DifferenceType.POSITION_CHANGED)
                .map(diff -> diff.getComponentA() == null ? diff.getComponentB().getIdentifier() : diff.getComponentA().getIdentifier())
                .collect(Collectors.toSet());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Updating {} to {}; there are {} differences to take into account: {}", this, proposedSnapshot, flowComparison.getDifferences().size(), flowComparison.getDifferences());
            } else {
                // TODO: Remove the actual differences from the info level log. It can be extremely verbose. Is here only for testing purposes becuase it's much more convenient
                // than having to remember to enable DEBUG level logging every time a full build is done.
                LOG.info("Updating {} to {}; there are {} differences to take into account: {}", this, proposedSnapshot, flowComparison.getDifferences().size(), flowComparison.getDifferences());
            }

            updateProcessGroup(this, proposedSnapshot.getFlowContents(), componentIdSeed, updatedVersionedComponentIds, false);
        } catch (final ProcessorInstantiationException pie) {
            throw new RuntimeException(pie);
        } finally {
            writeLock.unlock();
        }
    }


    private void updateProcessGroup(final ProcessGroup group, final VersionedProcessGroup proposed, final String componentIdSeed,
        final Set<String> updatedVersionedComponentIds, final boolean updatePosition) throws ProcessorInstantiationException {

        group.setComments(proposed.getComments());
        group.setName(proposed.getName());
        if (updatePosition && proposed.getPosition() != null) {
            group.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        }

        // Determine which variables have been added/removed and add/remove them from this group's variable registry.
        // We don't worry about if a variable value has changed, because variables are designed to be 'environment specific.'
        // As a result, once imported, we won't update variables to match the remote flow, but we will add any missing variables
        // and remove any variables that are no longer part of the remote flow.
        final Set<String> existingVariableNames = group.getVariableRegistry().getVariableMap().keySet().stream()
            .map(VariableDescriptor::getName)
            .collect(Collectors.toSet());

        final Set<String> variablesRemoved = new HashSet<>(existingVariableNames);
        variablesRemoved.removeAll(proposed.getVariables().keySet());
        final Map<String, String> updatedVariableMap = new HashMap<>();
        variablesRemoved.forEach(var -> updatedVariableMap.put(var, null));

        // If any new variables exist in the proposed flow, add those to the variable registry.
        for (final Map.Entry<String, String> entry : proposed.getVariables().entrySet()) {
            if (!existingVariableNames.contains(entry.getKey())) {
                updatedVariableMap.put(entry.getKey(), entry.getValue());
            }
        }

        group.setVariables(updatedVariableMap);

        final RemoteFlowCoordinates remoteCoordinates = proposed.getRemoteFlowCoordinates();
        if (remoteCoordinates != null) {
            final String registryId = flowController.getFlowRegistryClient().getFlowRegistryId(remoteCoordinates.getRegistryUrl());
            final String bucketId = remoteCoordinates.getBucketId();
            final String flowId = remoteCoordinates.getFlowId();
            final int version = remoteCoordinates.getVersion();

            final VersionControlInformation vci = new StandardVersionControlInformation(registryId, bucketId, flowId, version, proposed, false, true);
            group.setVersionControlInformation(vci, Collections.emptyMap());
        }

        // Child groups
        // TODO: Need to take into account if child group is under version control pointing to a different Versioned Flow and if so need to handle it differently.
        final Map<String, ProcessGroup> childGroupsByVersionedId = group.getProcessGroups().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> childGroupsRemoved = new HashSet<>(childGroupsByVersionedId.keySet());

        for (final VersionedProcessGroup proposedChildGroup : proposed.getProcessGroups()) {
            final ProcessGroup childGroup = childGroupsByVersionedId.get(proposedChildGroup.getIdentifier());

            if (childGroup == null) {
                final ProcessGroup added = addProcessGroup(proposedChildGroup, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else {
                updateProcessGroup(childGroup, proposedChildGroup, componentIdSeed, updatedVersionedComponentIds, true);
                LOG.info("Updated {}", childGroup);
            }

            childGroupsRemoved.remove(proposedChildGroup.getIdentifier());
        }


        // Controller Services
        final Map<String, ControllerServiceNode> servicesByVersionedId = group.getControllerServices(false).stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> controllerServicesRemoved = new HashSet<>(servicesByVersionedId.keySet());

        for (final VersionedControllerService proposedService : proposed.getControllerServices()) {
            final ControllerServiceNode service = servicesByVersionedId.get(proposedService.getIdentifier());
            if (service == null) {
                final ControllerServiceNode added = addControllerService(proposedService, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedService.getIdentifier())) {
                updateControllerService(service, proposedService);
                LOG.info("Updated {}", service);
            }

            controllerServicesRemoved.remove(proposedService.getIdentifier());
        }


        // Funnels
        final Map<String, Funnel> funnelsByVersionedId = group.getFunnels().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> funnelsRemoved = new HashSet<>(funnelsByVersionedId.keySet());

        for (final VersionedFunnel proposedFunnel : proposed.getFunnels()) {
            final Funnel funnel = funnelsByVersionedId.get(proposedFunnel.getIdentifier());
            if (funnel == null) {
                final Funnel added = addFunnel(proposedFunnel, componentIdSeed);
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
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> inputPortsRemoved = new HashSet<>(inputPortsByVersionedId.keySet());

        for (final VersionedPort proposedPort : proposed.getInputPorts()) {
            final Port port = inputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final Port added = addInputPort(proposedPort, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                updatePort(port, proposedPort);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }

            inputPortsRemoved.remove(proposedPort.getIdentifier());
        }

        // Output Ports
        final Map<String, Port> outputPortsByVersionedId = group.getOutputPorts().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> outputPortsRemoved = new HashSet<>(outputPortsByVersionedId.keySet());

        for (final VersionedPort proposedPort : proposed.getOutputPorts()) {
            final Port port = outputPortsByVersionedId.get(proposedPort.getIdentifier());
            if (port == null) {
                final Port added = addOutputPort(proposedPort, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedPort.getIdentifier())) {
                updatePort(port, proposedPort);
                LOG.info("Updated {}", port);
            } else {
                port.setPosition(new Position(proposedPort.getPosition().getX(), proposedPort.getPosition().getY()));
            }

            outputPortsRemoved.remove(proposedPort.getIdentifier());
        }


        // Labels
        final Map<String, Label> labelsByVersionedId = group.getLabels().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> labelsRemoved = new HashSet<>(labelsByVersionedId.keySet());

        for (final VersionedLabel proposedLabel : proposed.getLabels()) {
            final Label label = labelsByVersionedId.get(proposedLabel.getIdentifier());
            if (label == null) {
                final Label added = addLabel(proposedLabel, componentIdSeed);
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
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> processorsRemoved = new HashSet<>(processorsByVersionedId.keySet());

        for (final VersionedProcessor proposedProcessor : proposed.getProcessors()) {
            final ProcessorNode processor = processorsByVersionedId.get(proposedProcessor.getIdentifier());
            if (processor == null) {
                final ProcessorNode added = addProcessor(proposedProcessor, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedProcessor.getIdentifier())) {
                updateProcessor(processor, proposedProcessor);
                LOG.info("Updated {}", processor);
            } else {
                processor.setPosition(new Position(proposedProcessor.getPosition().getX(), proposedProcessor.getPosition().getY()));
            }

            processorsRemoved.remove(proposedProcessor.getIdentifier());
        }


        // Remote Groups
        final Map<String, RemoteProcessGroup> rpgsByVersionedId = group.getRemoteProcessGroups().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> rpgsRemoved = new HashSet<>(rpgsByVersionedId.keySet());

        for (final VersionedRemoteProcessGroup proposedRpg : proposed.getRemoteProcessGroups()) {
            final RemoteProcessGroup rpg = rpgsByVersionedId.get(proposedRpg.getIdentifier());
            if (rpg == null) {
                final RemoteProcessGroup added = addRemoteProcessGroup(proposedRpg, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (updatedVersionedComponentIds.contains(proposedRpg.getIdentifier())) {
                updateRemoteProcessGroup(rpg, proposedRpg);
                LOG.info("Updated {}", rpg);
            } else {
                rpg.setPosition(new Position(proposedRpg.getPosition().getX(), proposedRpg.getPosition().getY()));
            }

            rpgsRemoved.remove(proposedRpg.getIdentifier());
        }


        // Connections
        final Map<String, Connection> connectionsByVersionedId = group.getConnections().stream()
            .collect(Collectors.toMap(component -> component.getVersionedComponentId().orElse(component.getIdentifier()), Function.identity()));
        final Set<String> connectionsRemoved = new HashSet<>(connectionsByVersionedId.keySet());

        for (final VersionedConnection proposedConnection : proposed.getConnections()) {
            final Connection connection = connectionsByVersionedId.get(proposedConnection.getIdentifier());
            if (connection == null) {
                final Connection added = addConnection(proposedConnection, componentIdSeed);
                LOG.info("Added {} to {}", added, this);
            } else if (!connection.getSource().isRunning() && !connection.getDestination().isRunning()) {
                // If the connection needs to be updated, then the source and destination will already have
                // been stopped (else, the validation above would fail). So if the source or the destination is running,
                // then we know that we don't need to update the connection.
                updateConnection(connection, proposedConnection);
                LOG.info("Updated {}", connection);
            }

            connectionsRemoved.remove(proposedConnection.getIdentifier());
        }

        // Remove components that exist in the local flow but not the remote flow.

        // Connections must be the first thing to remove, not the last. Otherwise, we will fail
        // to remove a component if it has a connection going to it!
        for (final String removedVersionedId : connectionsRemoved) {
            final Connection connection = connectionsByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", connection, group);
            group.removeConnection(connection);
        }

        for (final String removedVersionedId : controllerServicesRemoved) {
            final ControllerServiceNode service = servicesByVersionedId.get(removedVersionedId);
            LOG.info("Removing {} from {}", service, group);
            group.removeControllerService(service);
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

    protected String generateUuid(final String componentIdSeed) {
        UUID uuid;
        if (componentIdSeed == null) {
            uuid = ComponentIdGenerator.generateId();
        } else {
            try {
                UUID seedId = UUID.fromString(componentIdSeed);
                uuid = new UUID(seedId.getMostSignificantBits(), componentIdSeed.hashCode());
            } catch (Exception e) {
                LOG.warn("Provided 'seed' does not represent UUID. Will not be able to extract most significant bits for ID generation.");
                uuid = UUID.nameUUIDFromBytes(componentIdSeed.getBytes(StandardCharsets.UTF_8));
            }
        }

        return uuid.toString();
    }


    private ProcessGroup addProcessGroup(final VersionedProcessGroup proposed, final String componentIdSeed) throws ProcessorInstantiationException {
        final ProcessGroup group = flowController.createProcessGroup(generateUuid(componentIdSeed));
        group.setVersionedComponentId(proposed.getIdentifier());
        addProcessGroup(group);
        updateProcessGroup(group, proposed, componentIdSeed, Collections.emptySet(), true);
        return group;
    }

    private void updateConnection(final Connection connection, final VersionedConnection proposed) {
        connection.setBendPoints(proposed.getBends().stream()
            .map(pos -> new Position(pos.getX(), pos.getY()))
            .collect(Collectors.toList()));

        connection.setDestination(getConnectable(proposed.getDestination()));
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

        final List<FlowFilePrioritizer> prioritizers = proposed.getPrioritizers().stream()
            .map(prioritizerName -> {
                try {
                    return flowController.createPrioritizer(prioritizerName);
                } catch (final Exception e) {
                    throw new RuntimeException("Failed to create Prioritizer of type " + prioritizerName + " for Connection with ID " + connection.getIdentifier());
                }
            })
            .collect(Collectors.toList());

        queue.setPriorities(prioritizers);
    }

    private Connection addConnection(final VersionedConnection proposed, final String componentIdSeed) {
        final Connectable source = getConnectable(proposed.getSource());
        if (source == null) {
            throw new IllegalArgumentException("Connection has a source with identifier " + proposed.getIdentifier()
                + " but no component could be found in the Process Group with a corresponding identifier");
        }

        final Connectable destination = getConnectable(proposed.getDestination());
        if (destination == null) {
            throw new IllegalArgumentException("Connection has a destination with identifier " + proposed.getIdentifier()
                + " but no component could be found in the Process Group with a corresponding identifier");
        }

        final Connection connection = flowController.createConnection(generateUuid(componentIdSeed), proposed.getName(), source, destination, proposed.getSelectedRelationships());
        connection.setVersionedComponentId(proposed.getIdentifier());
        addConnection(connection);
        updateConnection(connection, proposed);

        return connection;
    }

    private Connectable getConnectable(final ConnectableComponent connectableComponent) {
        final String id = connectableComponent.getId();

        switch (connectableComponent.getType()) {
            case FUNNEL:
                return getFunnels().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            case INPUT_PORT:
                return getInputPorts().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            case OUTPUT_PORT:
                return getOutputPorts().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            case PROCESSOR:
                return getProcessors().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            case REMOTE_INPUT_PORT: {
                final String rpgId = connectableComponent.getGroupId();
                final Optional<RemoteProcessGroup> rpgOption = getRemoteProcessGroups().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny();

                if (!rpgOption.isPresent()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                return rpg.getInputPorts().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            }
            case REMOTE_OUTPUT_PORT: {
                final String rpgId = connectableComponent.getGroupId();
                final Optional<RemoteProcessGroup> rpgOption = getRemoteProcessGroups().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny();

                if (!rpgOption.isPresent()) {
                    throw new IllegalArgumentException("Connection refers to a Port with ID " + id + " within Remote Process Group with ID "
                        + rpgId + " but could not find a Remote Process Group corresponding to that ID");
                }

                final RemoteProcessGroup rpg = rpgOption.get();
                return rpg.getOutputPorts().stream()
                    .filter(component -> component.getVersionedComponentId().isPresent())
                    .filter(component -> id.equals(component.getVersionedComponentId().get()))
                    .findAny()
                    .orElse(null);
            }
        }

        return null;
    }

    private void updateControllerService(final ControllerServiceNode service, final VersionedControllerService proposed) {
        service.setAnnotationData(proposed.getAnnotationData());
        service.setComments(proposed.getComments());
        service.setName(proposed.getName());
        service.setProperties(populatePropertiesMap(service.getProperties(), proposed.getProperties()));

        if (!isEqual(service.getBundleCoordinate(), proposed.getBundle())) {
            final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
            final List<PropertyDescriptor> descriptors = new ArrayList<>(service.getProperties().keySet());
            final Set<URL> additionalUrls = service.getAdditionalClasspathResources(descriptors);
            flowController.reload(service, proposed.getType(), newBundleCoordinate, additionalUrls);
        }
    }

    private boolean isEqual(final BundleCoordinate coordinate, final Bundle bundle) {
        if (!bundle.getGroup().equals(coordinate.getGroup())) {
            return false;
        }

        if (!bundle.getArtifact().equals(coordinate.getId())) {
            return false;
        }

        if (!bundle.getVersion().equals(coordinate.getVersion())) {
            return false;
        }

        return true;
    }

    private BundleCoordinate toCoordinate(final Bundle bundle) {
        return new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }

    private ControllerServiceNode addControllerService(final VersionedControllerService proposed, final String componentIdSeed) {
        final String type = proposed.getType();
        final String id = generateUuid(componentIdSeed);

        final Bundle bundle = proposed.getBundle();
        final BundleCoordinate coordinate = toCoordinate(bundle);
        final boolean firstTimeAdded = true;
        final Set<URL> additionalUrls = Collections.emptySet();

        final ControllerServiceNode newService = flowController.createControllerService(type, id, coordinate, additionalUrls, firstTimeAdded);
        newService.setVersionedComponentId(proposed.getIdentifier());

        addControllerService(newService);
        updateControllerService(newService, proposed);

        return newService;
    }

    private void updateFunnel(final Funnel funnel, final VersionedFunnel proposed) {
        funnel.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
    }

    private Funnel addFunnel(final VersionedFunnel proposed, final String componentIdSeed) {
        final Funnel funnel = flowController.createFunnel(generateUuid(componentIdSeed));
        funnel.setVersionedComponentId(proposed.getIdentifier());
        addFunnel(funnel);
        updateFunnel(funnel, proposed);

        return funnel;
    }

    private void updatePort(final Port port, final VersionedPort proposed) {
        port.setComments(proposed.getComments());
        port.setName(proposed.getName());
        port.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
    }

    private Port addInputPort(final VersionedPort proposed, final String componentIdSeed) {
        final Port port = flowController.createLocalInputPort(generateUuid(componentIdSeed), proposed.getName());
        port.setVersionedComponentId(proposed.getIdentifier());
        addInputPort(port);
        updatePort(port, proposed);

        return port;
    }

    private Port addOutputPort(final VersionedPort proposed, final String componentIdSeed) {
        final Port port = flowController.createLocalInputPort(generateUuid(componentIdSeed), proposed.getName());
        port.setVersionedComponentId(proposed.getIdentifier());
        addOutputPort(port);
        updatePort(port, proposed);

        return port;
    }

    private Label addLabel(final VersionedLabel proposed, final String componentIdSeed) {
        final Label label = flowController.createLabel(generateUuid(componentIdSeed), proposed.getLabel());
        label.setVersionedComponentId(proposed.getIdentifier());
        addLabel(label);
        updateLabel(label, proposed);

        return label;
    }

    private void updateLabel(final Label label, final VersionedLabel proposed) {
        label.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        label.setSize(new Size(proposed.getWidth(), proposed.getHeight()));
        label.setStyle(proposed.getStyle());
        label.setValue(proposed.getLabel());
    }

    private ProcessorNode addProcessor(final VersionedProcessor proposed, final String componentIdSeed) throws ProcessorInstantiationException {
        final BundleCoordinate coordinate = toCoordinate(proposed.getBundle());
        final ProcessorNode procNode = flowController.createProcessor(proposed.getType(), generateUuid(componentIdSeed), coordinate, true);
        procNode.setVersionedComponentId(proposed.getIdentifier());

        addProcessor(procNode);
        updateProcessor(procNode, proposed);

        return procNode;
    }

    private void updateProcessor(final ProcessorNode processor, final VersionedProcessor proposed) throws ProcessorInstantiationException {
        processor.setAnnotationData(proposed.getAnnotationData());
        processor.setBulletinLevel(LogLevel.valueOf(proposed.getBulletinLevel()));
        processor.setComments(proposed.getComments());
        processor.setMaxConcurrentTasks(proposed.getConcurrentlySchedulableTaskCount());
        processor.setExecutionNode(ExecutionNode.valueOf(proposed.getExecutionNode()));
        processor.setName(proposed.getName());
        processor.setPenalizationPeriod(proposed.getPenaltyDuration());
        processor.setProperties(populatePropertiesMap(processor.getProperties(), proposed.getProperties()));
        processor.setRunDuration(proposed.getRunDurationMillis(), TimeUnit.MILLISECONDS);
        processor.setScheduldingPeriod(proposed.getSchedulingPeriod());
        processor.setSchedulingStrategy(SchedulingStrategy.valueOf(proposed.getSchedulingStrategy()));
        processor.setStyle(proposed.getStyle());
        processor.setYieldPeriod(proposed.getYieldDuration());
        processor.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));

        processor.setAutoTerminatedRelationships(proposed.getAutoTerminatedRelationships().stream()
            .map(relName -> processor.getRelationship(relName))
            .collect(Collectors.toSet()));

        if (!isEqual(processor.getBundleCoordinate(), proposed.getBundle())) {
            final BundleCoordinate newBundleCoordinate = toCoordinate(proposed.getBundle());
            final List<PropertyDescriptor> descriptors = new ArrayList<>(processor.getProperties().keySet());
            final Set<URL> additionalUrls = processor.getAdditionalClasspathResources(descriptors);
            flowController.reload(processor, proposed.getType(), newBundleCoordinate, additionalUrls);
        }
    }

    private Map<String, String> populatePropertiesMap(final Map<PropertyDescriptor, String> currentProperties, final Map<String, String> proposedProperties) {
        final Map<String, String> fullPropertyMap = new HashMap<>();
        for (final PropertyDescriptor property : currentProperties.keySet()) {
            fullPropertyMap.put(property.getName(), null);
        }

        fullPropertyMap.putAll(proposedProperties);
        return fullPropertyMap;
    }

    private RemoteProcessGroup addRemoteProcessGroup(final VersionedRemoteProcessGroup proposed, final String componentIdSeed) {
        final RemoteProcessGroup rpg = flowController.createRemoteProcessGroup(generateUuid(componentIdSeed), proposed.getTargetUris());
        rpg.setVersionedComponentId(proposed.getIdentifier());

        addRemoteProcessGroup(rpg);
        updateRemoteProcessGroup(rpg, proposed);

        return rpg;
    }

    private void updateRemoteProcessGroup(final RemoteProcessGroup rpg, final VersionedRemoteProcessGroup proposed) {
        rpg.setComments(proposed.getComments());
        rpg.setCommunicationsTimeout(proposed.getCommunicationsTimeout());
        rpg.setInputPorts(proposed.getInputPorts().stream()
            .map(port -> createPortDescriptor(port))
            .collect(Collectors.toSet()));
        rpg.setName(proposed.getName());
        rpg.setNetworkInterface(proposed.getLocalNetworkInterface());
        rpg.setOutputPorts(proposed.getOutputPorts().stream()
            .map(port -> createPortDescriptor(port))
            .collect(Collectors.toSet()));
        rpg.setPosition(new Position(proposed.getPosition().getX(), proposed.getPosition().getY()));
        rpg.setProxyHost(proposed.getProxyHost());
        rpg.setProxyPort(proposed.getProxyPort());
        rpg.setProxyUser(proposed.getProxyUser());
        rpg.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(proposed.getTransportProtocol()));
        rpg.setYieldDuration(proposed.getYieldDuration());
    }

    private RemoteProcessGroupPortDescriptor createPortDescriptor(final VersionedRemoteGroupPort proposed) {
        final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
        descriptor.setVersionedComponentId(proposed.getIdentifier());
        descriptor.setBatchCount(proposed.getBatchSize().getCount());
        descriptor.setBatchDuration(proposed.getBatchSize().getDuration());
        descriptor.setBatchSize(proposed.getBatchSize().getSize());
        descriptor.setComments(proposed.getComments());
        descriptor.setConcurrentlySchedulableTaskCount(proposed.getConcurrentlySchedulableTaskCount());
        descriptor.setGroupId(proposed.getGroupId());
        descriptor.setId(UUID.randomUUID().toString()); // TODO: Need to address this issue of port id's
        descriptor.setName(proposed.getName());
        descriptor.setUseCompression(proposed.isUseCompression());
        return descriptor;
    }


    public Optional<Boolean> isModified() {
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
            return Optional.of(Boolean.FALSE);
        }

        if (vci.getFlowSnapshot() == null) {
            // we haven't retrieved the flow from the Flow Registry yet, so we don't know if it's been modified.
            // As a result, we will just return an empty optional
            return Optional.empty();
        }

        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper();
        final VersionedProcessGroup versionedGroup = mapper.mapProcessGroup(this, flowController.getFlowRegistryClient());

        final ComparableDataFlow currentFlow = new ComparableDataFlow() {
            @Override
            public VersionedProcessGroup getContents() {
                return versionedGroup;
            }

            @Override
            public String getName() {
                return "Local Flow";
            }
        };

        final ComparableDataFlow snapshotFlow = new ComparableDataFlow() {
            @Override
            public VersionedProcessGroup getContents() {
                return vci.getFlowSnapshot();
            }

            @Override
            public String getName() {
                return "Flow Under Version Control";
            }
        };

        final FlowComparator flowComparator = new StandardFlowComparator(currentFlow, snapshotFlow);
        final FlowComparison comparison = flowComparator.compare();
        final Set<FlowDifference> differences = comparison.getDifferences();
        final boolean modified = differences.stream()
            .filter(diff -> diff.getDifferenceType() != DifferenceType.POSITION_CHANGED)
            .filter(diff -> diff.getDifferenceType() != DifferenceType.STYLE_CHANGED)
            .findAny()
            .isPresent();

        LOG.debug("There are {} differences between this flow and the versioned snapshot of this flow: {}", differences.size(), differences);
        return Optional.of(modified);
    }


    @Override
    public void verifyCanUpdate(final VersionedFlowSnapshot updatedFlow, final boolean verifyConnectionRemoval, final boolean verifyNotDirty) {
        readLock.lock();
        try {
            final VersionControlInformation versionControlInfo = getVersionControlInformation();
            if (versionControlInfo == null) {
                throw new IllegalStateException("Cannot update the Version of the flow for " + this
                    + " because the Process Group is not currently under Version Control");
            }

            if (!versionControlInfo.getFlowIdentifier().equals(updatedFlow.getSnapshotMetadata().getFlowIdentifier())) {
                throw new IllegalStateException(this + " is under version control but the given flow does not match the flow that this Process Group is synchronized with");
            }

            if (verifyNotDirty) {
                final Optional<Boolean> modifiedOption = versionControlInfo.getModified();
                if (!modifiedOption.isPresent()) {
                    throw new IllegalStateException(this + " cannot be updated to a different version of the flow because the local flow "
                        + "has not yet been synchronized with the Flow Registry. The Process Group must be"
                        + " synched with the Flow Registry before continuing. This will happen periodically in the background, so please try the request again later");
                }

                if (Boolean.TRUE.equals(modifiedOption.get())) {
                    throw new IllegalStateException("Cannot change the Version of the flow for " + this
                        + " because the Process Group has been modified since it was last synchronized with the Flow Registry. The Process Group must be"
                        + " restored to its original form before changing the version");
                }
            }

            final VersionedProcessGroup flowContents = updatedFlow.getFlowContents();
            if (verifyConnectionRemoval) {
                // Determine which Connections have been removed.
                final Map<String, Connection> removedConnectionByVersionedId = new HashMap<>();
                findAllConnections().stream()
                    .filter(conn -> conn.getVersionedComponentId().isPresent())
                    .forEach(conn -> removedConnectionByVersionedId.put(conn.getVersionedComponentId().get(), conn));

                final Set<String> proposedFlowConnectionIds = new HashSet<>();
                findAllConnectionIds(flowContents, proposedFlowConnectionIds);

                for (final String proposedConnectionId : proposedFlowConnectionIds) {
                    removedConnectionByVersionedId.remove(proposedConnectionId);
                }

                // If any connection that was removed has data in it, throw an IllegalStateException
                for (final Connection connection : removedConnectionByVersionedId.values()) {
                    final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
                    if (!flowFileQueue.isEmpty()) {
                        throw new IllegalStateException(this + " cannot be updated to the proposed version of the flow because the "
                            + "proposed version does not contain "
                            + connection + " and the connection currently has data in the queue.");
                    }
                }
            }

            // Determine which input ports were removed from this process group
            final Map<String, Port> removedInputPortsByVersionId = new HashMap<>();
            getInputPorts().stream()
                .filter(port -> port.getVersionedComponentId().isPresent())
                .forEach(port -> removedInputPortsByVersionId.put(port.getVersionedComponentId().get(), port));
            flowContents.getInputPorts().stream()
                .map(VersionedPort::getIdentifier)
                .forEach(id -> removedInputPortsByVersionId.remove(id));

            // Ensure that there are no incoming connections for any Input Port that was removed.
            for (final Port inputPort : removedInputPortsByVersionId.values()) {
                final List<Connection> incomingConnections = inputPort.getIncomingConnections();
                if (!incomingConnections.isEmpty()) {
                    throw new IllegalStateException(this + " cannot be updated to the proposed version of the flow because the proposed version does not contain the Input Port "
                        + inputPort + " and the Input Port currently has an incoming connections");
                }
            }

            // Determine which output ports were removed from this process group
            final Map<String, Port> removedOutputPortsByVersionId = new HashMap<>();
            getOutputPorts().stream()
                .filter(port -> port.getVersionedComponentId().isPresent())
                .forEach(port -> removedOutputPortsByVersionId.put(port.getVersionedComponentId().get(), port));
            flowContents.getOutputPorts().stream()
                .map(VersionedPort::getIdentifier)
                .forEach(id -> removedOutputPortsByVersionId.remove(id));

            // Ensure that there are no outgoing connections for any Output Port that was removed.
            for (final Port outputPort : removedOutputPortsByVersionId.values()) {
                final Set<Connection> outgoingConnections = outputPort.getConnections();
                if (!outgoingConnections.isEmpty()) {
                    throw new IllegalStateException(this + " cannot be updated to the proposed version of the flow because the proposed version does not contain the Output Port "
                        + outputPort + " and the Output Port currently has an outgoing connections");
                }
            }

            // Find any Process Groups that may have been deleted. If we find any Process Group that was deleted, and that Process Group
            // has Templates, then we fail because the Templates have to be removed first.
            final Map<String, VersionedProcessGroup> proposedProcessGroups = new HashMap<>();
            findAllProcessGroups(updatedFlow.getFlowContents(), proposedProcessGroups);

            for (final ProcessGroup childGroup : findAllProcessGroups()) {
                if (childGroup.getTemplates().isEmpty()) {
                    continue;
                }

                final Optional<String> versionedIdOption = childGroup.getVersionedComponentId();
                if (!versionedIdOption.isPresent()) {
                    continue;
                }

                final String versionedId = versionedIdOption.get();
                if (!proposedProcessGroups.containsKey(versionedId)) {
                    // Process Group was removed.
                    throw new IllegalStateException(this + " cannot be updated to the proposed version of the flow because the child " + childGroup
                        + " that exists locally has one or more Templates, and the proposed flow does not contain this Process Group. "
                        + "A Process Group cannot be deleted while it contains Templates. Please remove the Templates before attempting to chnage the version of the flow.");
                }
            }


            // Ensure that all Processors are instantiate-able.
            final Map<String, VersionedProcessor> proposedProcessors = new HashMap<>();
            findAllProcessors(updatedFlow.getFlowContents(), proposedProcessors);

            findAllProcessors().stream()
                .filter(proc -> proc.getVersionedComponentId().isPresent())
                .forEach(proc -> proposedProcessors.remove(proc.getVersionedComponentId().get()));

            for (final VersionedProcessor processorToAdd : proposedProcessors.values()) {
                final BundleCoordinate coordinate = toCoordinate(processorToAdd.getBundle());
                try {
                    flowController.createProcessor(processorToAdd.getType(), UUID.randomUUID().toString(), coordinate, false);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to create Processor of type " + processorToAdd.getType(), e);
                }
            }

            // Ensure that all Controller Services are instantiate-able.
            final Map<String, VersionedControllerService> proposedServices = new HashMap<>();
            findAllControllerServices(updatedFlow.getFlowContents(), proposedServices);

            findAllControllerServices().stream()
                .filter(service -> service.getVersionedComponentId().isPresent())
                .forEach(service -> proposedServices.remove(service.getVersionedComponentId().get()));

            for (final VersionedControllerService serviceToAdd : proposedServices.values()) {
                final BundleCoordinate coordinate = toCoordinate(serviceToAdd.getBundle());
                try {
                    flowController.createControllerService(serviceToAdd.getType(), UUID.randomUUID().toString(), coordinate, Collections.emptySet(), false);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to create Controller Service of type " + serviceToAdd.getType(), e);
                }
            }

            // Ensure that all Prioritizers are instantiate-able.
            final Map<String, VersionedConnection> proposedConnections = new HashMap<>();
            findAllConnections(updatedFlow.getFlowContents(), proposedConnections);

            findAllConnections().stream()
                .filter(conn -> conn.getVersionedComponentId().isPresent())
                .forEach(conn -> proposedConnections.remove(conn.getVersionedComponentId().get()));

            for (final VersionedConnection connectionToAdd : proposedConnections.values()) {
                for (final String prioritizerType : connectionToAdd.getPrioritizers()) {
                    try {
                        flowController.createPrioritizer(prioritizerType);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unable to create Prioritizer of type " + prioritizerType, e);
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private void findAllConnectionIds(final VersionedProcessGroup group, final Set<String> ids) {
        for (final VersionedConnection connection : group.getConnections()) {
            ids.add(connection.getIdentifier());
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findAllConnectionIds(childGroup, ids);
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

    private void findAllProcessGroups(final VersionedProcessGroup group, final Map<String, VersionedProcessGroup> map) {
        map.put(group.getIdentifier(), group);

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            findAllProcessGroups(child, map);
        }
    }
}
