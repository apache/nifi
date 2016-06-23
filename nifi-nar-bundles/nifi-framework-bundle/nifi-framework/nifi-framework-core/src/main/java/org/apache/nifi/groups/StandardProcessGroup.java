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

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

public final class StandardProcessGroup implements ProcessGroup {

    private final String id;
    private final AtomicReference<ProcessGroup> parent;
    private final AtomicReference<String> name;
    private final AtomicReference<Position> position;
    private final AtomicReference<String> comments;

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
    private final VariableRegistry variableRegistry;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private static final Logger LOG = LoggerFactory.getLogger(StandardProcessGroup.class);

    public StandardProcessGroup(final String id, final ControllerServiceProvider serviceProvider, final StandardProcessScheduler scheduler,
         final NiFiProperties nifiProps, final StringEncryptor encryptor, final FlowController flowController,
        final VariableRegistry variableRegistry) {
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
                    node.getProcessGroup().startProcessor(node);
                } catch (final Throwable t) {
                    LOG.error("Unable to start {} due to {}", new Object[]{node, t});
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
                    LOG.error("Unable to stop {} due to {}", new Object[]{node, t});
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
            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                final StandardProcessContext processContext = new StandardProcessContext(node, controllerServiceProvider, encryptor, getStateManager(node.getIdentifier()),variableRegistry);
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
            if (inputPorts.containsKey(requireNonNull(port).getIdentifier())) {
                throw new IllegalStateException("Input Port with ID " + port.getIdentifier() + " already exists");
            }

            if (getInputPortByName(port.getName()) != null) {
                throw new IllegalStateException("Input Port with name " + port.getName() + " already exists");
            }

            port.setProcessGroup(this);
            inputPorts.put(requireNonNull(port).getIdentifier(), port);
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
                throw new IllegalStateException(port + " is not an Input Port of this Process Group");
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
                throw new IllegalStateException(port + " is not an Input Port of this Process Group");
            }

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
                throw new IllegalArgumentException("Cannot add Output Port of type " + port.getClass().getName() + " to the Root Group");
            }
        } else if (!(port instanceof LocalPort)) {
            throw new IllegalArgumentException("Cannot add Output Port of type " + port.getClass().getName() + " to a non-root group");
        }

        writeLock.lock();
        try {
            if (outputPorts.containsKey(requireNonNull(port).getIdentifier())) {
                throw new IllegalStateException("Output Port with ID " + port.getIdentifier() + " already exists");
            }

            if (getOutputPortByName(port.getName()) != null) {
                throw new IllegalStateException("Output Port with Name " + port.getName() + " already exists");
            }

            port.setProcessGroup(this);
            outputPorts.put(port.getIdentifier(), port);
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
                throw new IllegalStateException(port + " cannot be removed until its connections are removed");
            }

            final Port removed = outputPorts.remove(port.getIdentifier());
            if (removed == null) {
                throw new IllegalStateException(port + " is not an Output Port of this Process Group");
            }

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
            processGroups.put(Objects.requireNonNull(group).getIdentifier(), group);
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
                throw new IllegalStateException(group + " is not a member of this Process Group");
            }
            toRemove.verifyCanDelete();

            removeComponents(group);
            processGroups.remove(group.getIdentifier());
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
                throw new IllegalStateException(remoteProcessGroup + " is not a member of this Process Group");
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
            processors.put(processorId, processor);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeProcessor(final ProcessorNode processor) {
        final String id = requireNonNull(processor).getIdentifier();
        writeLock.lock();
        try {
            if (!processors.containsKey(id)) {
                throw new IllegalStateException(processor + " is not a member of this Process Group");
            }

            processor.verifyCanDelete();
            for (final Connection conn : processor.getConnections()) {
                conn.verifyCanDelete();
            }

            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                final StandardProcessContext processContext = new StandardProcessContext(processor, controllerServiceProvider, encryptor, getStateManager(processor.getIdentifier()),variableRegistry);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, processor.getProcessor(), processContext);
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke 'OnRemoved' methods of " + processor, e);
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

            LOG.info("{} removed from flow", processor);
        } finally {
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
                } else {
                    if (destinationGroup != this) {
                        throw new IllegalStateException("Cannot add Connection to Process Group because its destination does not belong to this Process Group");
                    }
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
                    throw new IllegalStateException("Cannot add Connection between " + source + " and " + destination
                        + " because they are in different Process Groups and neither is an Input Port or Output Port");
                }
            }

            connection.setProcessGroup(this);
            source.addConnection(connection);
            if (source != destination) {  // don't call addConnection twice if it's a self-looping connection.
                destination.addConnection(connection);
            }
            connections.put(connection.getIdentifier(), connection);
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
                throw new IllegalStateException(connectionToRemove + " is not a member of this Process Group");
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
        return findConnection(id, this);
    }

    private Connection findConnection(final String id, final ProcessGroup start) {
        Connection connection = start.getConnection(id);
        if (connection != null) {
            return connection;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            connection = findConnection(id, group);
            if (connection != null) {
                return connection;
            }
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
    public void startProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (getProcessor(processor.getIdentifier()) == null) {
                throw new IllegalStateException("Processor is not a member of this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.RUNNING) {
                return;
            }

            scheduler.startProcessor(processor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void startInputPort(final Port port) {
        readLock.lock();
        try {
            if (getInputPort(port.getIdentifier()) == null) {
                throw new IllegalStateException(port + " is not a member of this Process Group");
            }

            final ScheduledState state = port.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("InputPort " + port + " is disabled");
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
    public void stopProcessor(final ProcessorNode processor) {
        readLock.lock();
        try {
            if (!processors.containsKey(processor.getIdentifier())) {
                throw new IllegalStateException("No processor with ID " + processor.getIdentifier() + " belongs to this Process Group");
            }

            final ScheduledState state = processor.getScheduledState();
            if (state == ScheduledState.DISABLED) {
                throw new IllegalStateException("Processor is disabled");
            } else if (state == ScheduledState.STOPPED) {
                return;
            }

            scheduler.stopProcessor(processor);
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
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("name", name).toString();
    }

    @Override
    public ProcessGroup findProcessGroup(final String id) {
        return findProcessGroup(requireNonNull(id), this);
    }

    private ProcessGroup findProcessGroup(final String id, final ProcessGroup start) {
        if (id.equals(start.getIdentifier())) {
            return start;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            final ProcessGroup matching = findProcessGroup(id, group);
            if (matching != null) {
                return matching;
            }
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
        return findProcessor(id, this);
    }

    private ProcessorNode findProcessor(final String id, final ProcessGroup start) {
        ProcessorNode node = start.getProcessor(id);
        if (node != null) {
            return node;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            node = findProcessor(id, group);
            if (node != null) {
                return node;
            }
        }

        return null;
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
    public Connectable findConnectable(final String identifier) {
        return findConnectable(identifier, this);
    }

    private static Connectable findConnectable(final String identifier, final ProcessGroup group) {
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
            final Connectable childGroupConnectable = findConnectable(identifier, childGroup);
            if (childGroupConnectable != null) {
                return childGroupConnectable;
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
        return findPort(id, this, new InputPortRetriever());
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
        return findPort(id, this, new OutputPortRetriever());
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

    private Port findPort(final String id, final ProcessGroup group, final PortRetriever retriever) {
        Port port = retriever.getPort(group, id);
        if (port != null) {
            return port;
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            port = findPort(id, childGroup, retriever);
            if (port != null) {
                return port;
            }
        }

        return null;
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
        return findFunnel(id, this);
    }

    private Funnel findFunnel(final String id, final ProcessGroup start) {
        Funnel funnel = start.getFunnel(id);
        if (funnel != null) {
            return funnel;
        }

        for (final ProcessGroup group : start.getProcessGroups()) {
            funnel = findFunnel(id, group);
            if (funnel != null) {
                return funnel;
            }
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
                throw new IllegalStateException(funnel + " is not a member of this ProcessGroup");
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
        writeLock.lock();
        try {
            final ControllerServiceNode existing = controllerServices.get(requireNonNull(service).getIdentifier());
            if (existing == null) {
                throw new IllegalStateException(service + " is not a member of this Process Group");
            }

            service.verifyCanDelete();

            try (final NarCloseable x = NarCloseable.withNarLoader()) {
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

            LOG.info("{} removed from {}", service, this);
        } finally {
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
                throw new IllegalStateException(template + " is not a member of this ProcessGroup");
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
                        throw new IllegalStateException(connectable + " cannot be removed because it has incoming connections from the parent Process Group");
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
                    throw new IllegalStateException(procNode + " cannot be removed because it is running");
                }
                final int activeThreadCount = scheduler.getActiveThreadCount(procNode);
                if (activeThreadCount != 0) {
                    throw new IllegalStateException(procNode + " cannot be removed because it still has " + activeThreadCount + " active threads");
                }
            }

            // verify that none of the connectables have incoming connections that are not in the Snippet.
            final Set<String> connectionIds = snippet.getConnections().keySet();
            for (final Connectable connectable : connectables) {
                for (final Connection conn : connectable.getIncomingConnections()) {
                    if (!connectionIds.contains(conn.getIdentifier()) && !connectables.contains(conn.getSource())) {
                        throw new IllegalStateException(connectable + " cannot be removed because it has incoming connections "
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
        Set<Positionable> positionables = Sets.newHashSet();
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
     * Verifies that all ID's defined within the given snippet reference components within this ProcessGroup. If this is not the case, throws {@link IllegalStateException}.
     *
     * @param snippet the snippet
     * @throws NullPointerException if the argument is null
     * @throws IllegalStateException if the snippet contains an ID that references a component that is not part of this ProcessGroup
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
     * <p>
     * Verifies that all ID's specified by the given set exist as keys in the given Map. If any of the ID's does not exist as a key in the map, will throw {@link IllegalStateException} indicating the
     * ID that is invalid and specifying the Component Type.
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
                            throw new IllegalStateException("Cannot delete Process Group because Input Port " + port +
                                " has at least one incoming connection from a component outside of the Process Group. Delete this connection first.");
                        }
                    }
                }

                for (final Port port : outputPorts.values()) {
                    for (final Connection connection : port.getConnections()) {
                        if (connection.getDestination().equals(port)) {
                            connection.verifyCanDelete();
                        } else {
                            throw new IllegalStateException("Cannot delete Process Group because Output Port " + port +
                                " has at least one outgoing connection to a component outside of the Process Group. Delete this connection first.");
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
                    throw new IllegalStateException("Cannot start " + connectable + " because it is currently stopping");
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
                    throw new IllegalStateException("Cannot perform Move Operation because the destination Process Group already has an Input Port with the name " + portName);
                }
            }

            for (final String id : snippet.getOutputPorts().keySet()) {
                final Port port = getOutputPort(id);
                final String portName = port.getName();

                if (newProcessGroup.getOutputPortByName(portName) != null) {
                    throw new IllegalStateException("Cannot perform Move Operation because the destination Process Group already has an Output Port with the name " + portName);
                }
            }
        } finally {
            readLock.unlock();
        }
    }
}
