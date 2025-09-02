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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An implementation of ProcessGroupFacade that implements the functionality for a standalone
 * (non-clustered) NiFi instance.
 */
public class StandaloneProcessGroupFacade implements ProcessGroupFacade {
    private final ProcessGroup processGroup;
    private final VersionedProcessGroup flowDefinition;
    private final ProcessScheduler processScheduler;
    private final ParameterContext parameterContext;
    private final Map<String, VersionedProcessor> processorMap;
    private final Map<String, VersionedControllerService> controllerServiceMap;
    private final Map<String, VersionedConnection> connectionMap;
    private final Map<String, VersionedProcessGroup> processGroupMap;
    private final ControllerServiceProvider controllerServiceProvider;
    private final StandaloneProcessGroupLifecycle lifecycle;
    private final StatelessGroupLifecycle statelessGroupLifecycle;
    private final ComponentContextProvider componentContextProvider;
    private final ComponentLog connectorLogger;
    private final ExtensionManager extensionManager;
    private final AssetManager assetManager;

    public StandaloneProcessGroupFacade(final ProcessGroup processGroup, final VersionedProcessGroup flowDefinition, final ProcessScheduler processScheduler,
            final ParameterContext parameterContext, final ControllerServiceProvider controllerServiceProvider, final ComponentContextProvider componentContextProvider,
            final ComponentLog connectorLogger, final ExtensionManager extensionManager, final AssetManager assetManager) {
        this.processGroup = processGroup;
        this.flowDefinition = flowDefinition;
        this.processScheduler = processScheduler;
        this.parameterContext = parameterContext;
        this.controllerServiceProvider = controllerServiceProvider;
        this.componentContextProvider = componentContextProvider;
        this.connectorLogger = connectorLogger;
        this.extensionManager = extensionManager;
        this.assetManager = assetManager;

        this.processorMap = mapProcessors(flowDefinition);
        this.controllerServiceMap = mapControllerServices(flowDefinition);
        this.connectionMap = mapConnections(flowDefinition);
        this.processGroupMap = mapProcessGroups(flowDefinition);

        final ExecutionEngine executionEngine = processGroup.resolveExecutionEngine();
        if (executionEngine == ExecutionEngine.STATELESS) {
            this.statelessGroupLifecycle = new StandaloneStatelessGroupLifecycle(processGroup, processScheduler);
        } else {
            this.statelessGroupLifecycle = new IllegalExecutionEngineStatelessGroupLifecycle(processGroup);
        }

        this.lifecycle = new StandaloneProcessGroupLifecycle(processGroup, controllerServiceProvider, statelessGroupLifecycle,
            id -> getProcessGroup(id).getLifecycle());
    }

    private Map<String, VersionedProcessor> mapProcessors(final VersionedProcessGroup flowDefinition) {
        final Map<String, VersionedProcessor> processors = new HashMap<>();
        flowDefinition.getProcessors().forEach(processor -> processors.put(processor.getIdentifier(), processor));
        return processors;
    }

    private Map<String, VersionedControllerService> mapControllerServices(final VersionedProcessGroup flowDefinition) {
        final Map<String, VersionedControllerService> controllerServices = new HashMap<>();
        flowDefinition.getControllerServices().forEach(controllerService -> controllerServices.put(controllerService.getIdentifier(), controllerService));
        return controllerServices;
    }

    private Map<String, VersionedConnection> mapConnections(final VersionedProcessGroup flowDefinition) {
        final Map<String, VersionedConnection> connections = new HashMap<>();
        flowDefinition.getConnections().forEach(connection -> connections.put(connection.getIdentifier(), connection));
        return connections;
    }

    private Map<String, VersionedProcessGroup> mapProcessGroups(final VersionedProcessGroup flowDefinition) {
        final Map<String, VersionedProcessGroup> processGroups = new HashMap<>();
        flowDefinition.getProcessGroups().forEach(processGroup -> processGroups.put(processGroup.getIdentifier(), processGroup));
        return processGroups;
    }

    @Override
    public VersionedProcessGroup getDefinition() {
        return flowDefinition;
    }

    @Override
    public ProcessorFacade getProcessor(final String id) {
        final ProcessorNode processorNode = lookupProcessorNode(id);
        if (processorNode == null) {
            return null;
        }

        final VersionedProcessor processor = processorMap.get(id);
        if (processor == null) {
            return null;
        }

        return new StandaloneProcessorFacade(processorNode, processor, processScheduler, parameterContext,
            componentContextProvider, connectorLogger, extensionManager, assetManager);
    }

    private ProcessorNode lookupProcessorNode(final String versionedComponentId) {
        for (final ProcessorNode processorNode : processGroup.getProcessors()) {
            final Optional<String> versionedId = processorNode.getVersionedComponentId();
            if (versionedId.isPresent() && versionedId.get().equals(versionedComponentId)) {
                return processorNode;
            }
        }

        return null;
    }

    @Override
    public Set<ProcessorFacade> getProcessors() {
        if (processorMap.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ProcessorFacade> processors = new HashSet<>();
        for (final VersionedProcessor versionedProcessor : processorMap.values()) {
            final ProcessorNode processorNode = processGroup.getProcessor(versionedProcessor.getInstanceIdentifier());
            if (processorNode != null) {
                final ProcessorFacade processorFacade = new StandaloneProcessorFacade(processorNode, versionedProcessor, processScheduler,
                    parameterContext, componentContextProvider, connectorLogger, extensionManager, assetManager);
                processors.add(processorFacade);
            }
        }

        return processors;
    }

    @Override
    public ControllerServiceFacade getControllerService(final String id) {
        final ControllerServiceNode controllerServiceNode = lookupControllerServiceNode(id);
        if (controllerServiceNode == null) {
            return null;
        }

        final VersionedControllerService controllerService = controllerServiceMap.get(id);
        if (controllerService == null) {
            return null;
        }

        return new StandaloneControllerServiceFacade(controllerServiceNode, controllerService, parameterContext, processScheduler,
            componentContextProvider, connectorLogger, extensionManager, assetManager);
    }

    private ControllerServiceNode lookupControllerServiceNode(final String versionedComponentId) {
        for (final ControllerServiceNode controllerServiceNode : processGroup.getControllerServices(false)) {
            final Optional<String> versionedId = controllerServiceNode.getVersionedComponentId();
            if (versionedId.isPresent() && versionedId.get().equals(versionedComponentId)) {
                return controllerServiceNode;
            }
        }

        return null;
    }

    @Override
    public Set<ControllerServiceFacade> getControllerServices() {
        if (controllerServiceMap.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ControllerServiceFacade> controllerServices = new HashSet<>();
        for (final VersionedControllerService versionedControllerService : controllerServiceMap.values()) {
            final ControllerServiceNode controllerServiceNode = processGroup.getControllerService(versionedControllerService.getInstanceIdentifier());
            if (controllerServiceNode != null) {
                final ControllerServiceFacade serviceFacade = new StandaloneControllerServiceFacade(controllerServiceNode, versionedControllerService, parameterContext,
                    processScheduler, componentContextProvider, connectorLogger, extensionManager, assetManager);
                controllerServices.add(serviceFacade);
            }
        }

        return controllerServices;
    }

    @Override
    public Set<ControllerServiceFacade> getControllerServices(final ControllerServiceReferenceScope controllerServiceReferenceScope,
                final ControllerServiceReferenceHierarchy controllerServiceReferenceHierarchy) {

        final boolean recursive = (controllerServiceReferenceHierarchy == ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS);
        if (controllerServiceReferenceScope == ControllerServiceReferenceScope.INCLUDE_ALL) {
            final Set<ControllerServiceFacade> facades = new HashSet<>();
            collectControllerServiceFacades(this, facades, facade -> true, recursive);
            return facades;
        } else {
            final Set<ControllerServiceNode> serviceNodes = lifecycle.findReferencedServices(recursive);
            final Set<String> versionedComponentIds = serviceNodes.stream()
                .map(ControllerServiceNode::getVersionedComponentId)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

            final Set<ControllerServiceFacade> facades = new HashSet<>();
            final Predicate<ControllerServiceFacade> serviceFacadeFilter = facade -> versionedComponentIds.contains(facade.getDefinition().getIdentifier());
            collectControllerServiceFacades(this, facades, serviceFacadeFilter, recursive);
            return facades;
        }
    }

    private void collectControllerServiceFacades(final ProcessGroupFacade group, final Set<ControllerServiceFacade> facades, final Predicate<ControllerServiceFacade> filter,
                final boolean recursive) {

        for (final ControllerServiceFacade serviceFacade : group.getControllerServices()) {
            if (filter.test(serviceFacade)) {
                facades.add(serviceFacade);
            }
        }

        if (recursive) {
            for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
                collectControllerServiceFacades(childGroup, facades, filter, true);
            }
        }
    }

    @Override
    public ConnectionFacade getConnection(final String id) {
        final Connection connection = lookupConnection(id);
        if (connection == null) {
            return null;
        }

        final VersionedConnection versionedConnection = connectionMap.get(id);
        if (versionedConnection == null) {
            return null;
        }

        return new StandaloneConnectionFacade(connection, versionedConnection);
    }

    private Connection lookupConnection(final String versionedComponentId) {
        for (final Connection connection : processGroup.getConnections()) {
            final Optional<String> versionedId = connection.getVersionedComponentId();
            if (versionedId.isPresent() && versionedId.get().equals(versionedComponentId)) {
                return connection;
            }
        }

        return null;
    }

    @Override
    public Set<ConnectionFacade> getConnections() {
        if (connectionMap.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ConnectionFacade> connections = new HashSet<>();
        for (final VersionedConnection versionedConnection : connectionMap.values()) {
            final Connection connection = processGroup.getConnection(versionedConnection.getInstanceIdentifier());
            if (connection != null) {
                connections.add(new StandaloneConnectionFacade(connection, versionedConnection));
            }
        }

        return connections;
    }

    @Override
    public ProcessGroupFacade getProcessGroup(final String id) {
        final ProcessGroup childProcessGroup = lookupProcessGroup(processGroup, id);
        if (childProcessGroup == null) {
            return null;
        }

        final VersionedProcessGroup versionedProcessGroup = processGroupMap.get(id);
        if (versionedProcessGroup == null) {
            return null;
        }

        return new StandaloneProcessGroupFacade(childProcessGroup, versionedProcessGroup, processScheduler, parameterContext,
            controllerServiceProvider, componentContextProvider, connectorLogger, extensionManager, assetManager);
    }

    private ProcessGroup lookupProcessGroup(final ProcessGroup start, final String versionedComponentId) {
        for (final ProcessGroup childProcessGroup : start.getProcessGroups()) {
            final Optional<String> versionedId = childProcessGroup.getVersionedComponentId();
            if (versionedId.isPresent() && versionedId.get().equals(versionedComponentId)) {
                return childProcessGroup;
            }
        }

        for (final ProcessGroup childProcessGroup : start.getProcessGroups()) {
            final ProcessGroup found = lookupProcessGroup(childProcessGroup, versionedComponentId);
            if (found != null) {
                return found;
            }
        }

        return null;
    }

    @Override
    public Set<ProcessGroupFacade> getProcessGroups() {
        if (processGroupMap.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ProcessGroupFacade> processGroups = new HashSet<>();
        for (final VersionedProcessGroup versionedProcessGroup : processGroupMap.values()) {
            final ProcessGroup childProcessGroup = processGroup.findProcessGroup(versionedProcessGroup.getInstanceIdentifier());
            if (childProcessGroup != null) {
                final ProcessGroupFacade groupFacade = new StandaloneProcessGroupFacade(childProcessGroup, versionedProcessGroup, processScheduler, parameterContext,
                    controllerServiceProvider, componentContextProvider, connectorLogger, extensionManager, assetManager);
                processGroups.add(groupFacade);
            }
        }

        return processGroups;
    }

    @Override
    public QueueSize getQueueSize() {
        return processGroup.getQueueSize();
    }

    @Override
    public boolean isFlowEmpty() {
        return processGroup.isEmpty();
    }

    @Override
    public StatelessGroupLifecycle getStatelessLifecycle() {
        return statelessGroupLifecycle;
    }

    @Override
    public ProcessGroupLifecycle getLifecycle() {
        return lifecycle;
    }
}
