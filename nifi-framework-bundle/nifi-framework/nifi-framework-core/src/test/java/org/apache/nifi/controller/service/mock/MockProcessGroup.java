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

package org.apache.nifi.controller.service.mock;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.groups.BatchCounts;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.DataValve;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileGate;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.NoOpBatchCounts;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;
import org.apache.nifi.groups.StatelessGroupScheduledState;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterUpdate;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.remote.RemoteGroupPort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Predicate;

public class MockProcessGroup implements ProcessGroup {
    private final Map<String, ControllerServiceNode> serviceMap = new HashMap<>();
    private final Map<String, ProcessorNode> processorMap = new HashMap<>();
    private final Map<String, Port> inputPortMap = new HashMap<>();
    private final Map<String, Port> outputPortMap = new HashMap<>();
    private final FlowManager flowManager;
    private VersionControlInformation versionControlInfo;
    private ParameterContext parameterContext;
    private String defaultFlowfileExpiration;
    private long defaultBackPressureObjectThreshold;
    private String defaultBackPressureDataSizeThreshold;

    public MockProcessGroup(final FlowManager flowManager) {
        this.flowManager = flowManager;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return null;
    }

    @Override
    public ProcessGroup getParent() {
        return null;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public void setParent(final ProcessGroup group) {

    }

    @Override
    public String getIdentifier() {
        return "unit test group id";
    }

    @Override
    public String getName() {
        return "unit test group";
    }

    @Override
    public void setName(final String name) {

    }

    @Override
    public void setPosition(final Position position) {

    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public String getComments() {
        return null;
    }

    @Override
    public void setComments(final String comments) {

    }

    @Override
    public ProcessGroupCounts getCounts() {
        return null;
    }

    @Override
    public void startProcessing() {
    }

    @Override
    public void startComponents() {
    }

    @Override
    public CompletableFuture<Void> stopProcessing() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopComponents() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public StatelessGroupScheduledState getStatelessScheduledState() {
        return StatelessGroupScheduledState.STOPPED;
    }

    @Override
    public StatelessGroupScheduledState getDesiredStatelessScheduledState() {
        return StatelessGroupScheduledState.STOPPED;
    }

    @Override
    public boolean isStatelessActive() {
        return false;
    }

    @Override
    public void setExecutionEngine(final ExecutionEngine executionEngine) {
    }

    @Override
    public Optional<StatelessGroupNode> getStatelessGroupNode() {
        return Optional.empty();
    }

    @Override
    public void enableProcessor(final ProcessorNode processor) {

    }

    @Override
    public void enableInputPort(final Port port) {

    }

    @Override
    public void enableOutputPort(final Port port) {

    }

    @Override
    public CompletableFuture<Void> startProcessor(final ProcessorNode processor, final boolean failIfStopping) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<Void> runProcessorOnce(ProcessorNode processor, Callable<Future<Void>> stopCallback) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void startInputPort(final Port port) {

    }

    @Override
    public void startOutputPort(final Port port) {

    }

    @Override
    public void startFunnel(final Funnel funnel) {

    }

    @Override
    public CompletableFuture<Void> stopProcessor(final ProcessorNode processor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void stopInputPort(final Port port) {

    }

    @Override
    public void stopOutputPort(final Port port) {

    }

    @Override
    public void disableProcessor(final ProcessorNode processor) {

    }

    @Override
    public void disableInputPort(final Port port) {

    }

    @Override
    public void disableOutputPort(final Port port) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isRootGroup() {
        return false;
    }

    @Override
    public void addInputPort(final Port port) {
        port.setProcessGroup(this);
        inputPortMap.put(port.getIdentifier(), port);
        if (flowManager != null) {
            flowManager.onInputPortAdded(port);
        }
    }

    @Override
    public void removeInputPort(final Port port) {
        inputPortMap.remove(port.getIdentifier());
        if (flowManager != null) {
            flowManager.onInputPortRemoved(port);
        }
    }

    @Override
    public Set<Port> getInputPorts() {
        return new HashSet<>(inputPortMap.values());
    }

    @Override
    public Port getInputPort(final String id) {
        return inputPortMap.get(id);
    }

    @Override
    public void addOutputPort(final Port port) {
        port.setProcessGroup(this);
        outputPortMap.put(port.getIdentifier(), port);
        if (flowManager != null) {
            flowManager.onInputPortAdded(port);
        }
    }

    @Override
    public void removeOutputPort(final Port port) {
        outputPortMap.remove(port.getIdentifier());
        if (flowManager != null) {
            flowManager.onInputPortRemoved(port);
        }
    }

    @Override
    public Port getOutputPort(final String id) {
        return outputPortMap.get(id);
    }

    @Override
    public Set<Port> getOutputPorts() {
        return new HashSet<>(outputPortMap.values());
    }

    @Override
    public void addProcessGroup(final ProcessGroup group) {

    }

    @Override
    public ProcessGroup getProcessGroup(final String id) {
        return null;
    }

    @Override
    public Set<ProcessGroup> getProcessGroups() {
        return null;
    }

    @Override
    public void removeProcessGroup(final ProcessGroup group) {

    }

    @Override
    public void addProcessor(final ProcessorNode processor) {
        processor.setProcessGroup(this);
        processorMap.put(processor.getIdentifier(), processor);
        if (flowManager != null) {
            flowManager.onProcessorAdded(processor);
        }
    }

    @Override
    public void removeProcessor(final ProcessorNode processor) {
        processorMap.remove(processor.getIdentifier());
        if (flowManager != null) {
            flowManager.onProcessorRemoved(processor);
        }
    }

    @Override
    public Set<ProcessorNode> getProcessors() {
        return new HashSet<>(processorMap.values());
    }

    @Override
    public ProcessorNode getProcessor(final String id) {
        return processorMap.get(id);
    }

    @Override
    public Set<Positionable> findAllPositionables() {
        return null;
    }

    @Override
    public Connectable getConnectable(final String id) {
        return null;
    }

    @Override
    public void addConnection(final Connection connection) {

    }

    @Override
    public void removeConnection(final Connection connection) {

    }

    @Override
    public void inheritConnection(final Connection connection) {

    }

    @Override
    public Connection getConnection(final String id) {
        return null;
    }

    @Override
    public Set<Connection> getConnections() {
        return null;
    }

    @Override
    public Connection findConnection(final String id) {
        return null;
    }

    @Override
    public List<Connection> findAllConnections() {
        return null;
    }

    @Override
    public DropFlowFileStatus dropAllFlowFiles(String requestIdentifier, String requestor) {
        return null;
    }

    @Override
    public DropFlowFileStatus getDropAllFlowFilesStatus(String requestIdentifier) {
        return null;
    }

    @Override
    public DropFlowFileStatus cancelDropAllFlowFiles(String requestIdentifier) {
        return null;
    }

    @Override
    public Funnel findFunnel(final String id) {
        return null;
    }

    @Override
    public Set<String> getAncestorServiceIds() {
        return null;
    }

    @Override
    public ControllerServiceNode findControllerService(final String id, final boolean includeDescendants, final boolean includeAncestors) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> findAllControllerServices() {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void addRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {

    }

    @Override
    public void removeRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {

    }

    @Override
    public RemoteProcessGroup getRemoteProcessGroup(final String id) {
        return null;
    }

    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups() {
        return null;
    }

    @Override
    public void addLabel(final Label label) {

    }

    @Override
    public void removeLabel(final Label label) {

    }

    @Override
    public Set<Label> getLabels() {
        return null;
    }

    @Override
    public Label getLabel(final String id) {
        return null;
    }

    @Override
    public ProcessGroup findProcessGroup(final String id) {
        return null;
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups() {
        return null;
    }

    @Override
    public List<ProcessGroup> findAllProcessGroups(final Predicate<ProcessGroup> filter) {
        return Collections.emptyList();
    }

    @Override
    public RemoteProcessGroup findRemoteProcessGroup(final String id) {
        return null;
    }

    @Override
    public List<RemoteProcessGroup> findAllRemoteProcessGroups() {
        return null;
    }

    @Override
    public ProcessorNode findProcessor(final String id) {
        return processorMap.get(id);
    }

    @Override
    public List<ProcessorNode> findAllProcessors() {
        return new ArrayList<>(processorMap.values());
    }

    @Override
    public Label findLabel(final String id) {
        return null;
    }

    @Override
    public List<Label> findAllLabels() {
        return null;
    }

    @Override
    public Port findInputPort(final String id) {
        return null;
    }

    @Override
    public List<Port> findAllInputPorts() {
        return null;
    }

    @Override
    public Port getInputPortByName(final String name) {
        return null;
    }

    @Override
    public Port findOutputPort(final String id) {
        return null;
    }

    @Override
    public List<Port> findAllOutputPorts() {
        return null;
    }

    @Override
    public Port getOutputPortByName(final String name) {
        return null;
    }

    @Override
    public void addFunnel(final Funnel funnel) {

    }

    @Override
    public void addFunnel(final Funnel funnel, final boolean autoStart) {

    }

    @Override
    public Set<Funnel> getFunnels() {
        return null;
    }

    @Override
    public Funnel getFunnel(final String id) {
        return null;
    }

    @Override
    public List<Funnel> findAllFunnels() {
        return null;
    }

    @Override
    public void removeFunnel(final Funnel funnel) {

    }

    @Override
    public void addControllerService(final ControllerServiceNode service) {
        serviceMap.put(service.getIdentifier(), service);
        service.setProcessGroup(this);
    }

    @Override
    public ControllerServiceNode getControllerService(final String id) {
        return serviceMap.get(id);
    }

    @Override
    public Set<ControllerServiceNode> getControllerServices(final boolean recursive) {
        return new HashSet<>(serviceMap.values());
    }

    @Override
    public void removeControllerService(final ControllerServiceNode service) {
        serviceMap.remove(service.getIdentifier());
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void remove(final Snippet snippet) {

    }

    @Override
    public RemoteGroupPort findRemoteGroupPort(String identifier) {
        return null;
    }

    @Override
    public void move(final Snippet snippet, final ProcessGroup destination) {

    }

    @Override
    public void verifyCanDelete() {

    }

    @Override
    public void verifyCanDelete(final boolean ignorePortConnections) {

    }

    @Override
    public void verifyCanStart() {

    }

    @Override
    public void verifyCanScheduleComponentsIndividually() {
    }

    @Override
    public void verifyCanStop() {

    }

    @Override
    public void verifyCanDelete(final Snippet snippet) {

    }

    @Override
    public void verifyCanMove(final Snippet snippet, final ProcessGroup newProcessGroup) {

    }

    @Override
    public void verifyCanStart(final Connectable connectable) {
    }

    @Override
    public void verifyCanStop(final Connectable connectable) {
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.empty();
    }

    @Override
    public void setVersionedComponentId(String versionedComponentId) {
    }

    @Override
    public VersionControlInformation getVersionControlInformation() {
        return versionControlInfo;
    }

    @Override
    public void verifyCanUpdate(VersionedExternalFlow updatedFlow, boolean verifyConnectionRemoval, boolean verifyNotDirty) {
    }

    @Override
    public void verifyCanSaveToFlowRegistry(String registryId, FlowLocation flowLocation, String action) {
    }

    @Override
    public void synchronizeWithFlowRegistry(FlowManager flowRegistry) {
    }

    @Override
    public ComponentAdditions addVersionedComponents(VersionedComponentAdditions additions, String componentIdSeed) {
        return new ComponentAdditions.Builder().build();
    }

    @Override
    public void updateFlow(VersionedExternalFlow proposedFlow, String componentIdSeed, boolean verifyNotDirty, boolean updateSettings, boolean updateDescendantVerisonedFlows) {
    }

    @Override
    public void synchronizeFlow(final VersionedExternalFlow proposedSnapshot, final FlowSynchronizationOptions synchronizationOptions, final FlowMappingOptions flowMappingOptions) {
    }

    @Override
    public void setVersionControlInformation(VersionControlInformation versionControlInformation, Map<String, String> versionedComponentIds) {
        this.versionControlInfo = versionControlInformation;
    }

    @Override
    public void disconnectVersionControl(final boolean removeVersionedComponentIds) {
        this.versionControlInfo = null;
    }

    @Override
    public void verifyCanRevertLocalModifications() {
    }

    @Override
    public void verifyCanShowLocalModifications() {
    }

    @Override
    public void onComponentModified() {
    }

    @Override
    public void setParameterContext(final ParameterContext parameterContext) {
        this.parameterContext = parameterContext;
    }

    @Override
    public ParameterContext getParameterContext() {
        return parameterContext;
    }

    @Override
    public void verifyCanSetParameterContext(ParameterContext context) {
    }

    @Override
    public void onParameterContextUpdated(final Map<String, ParameterUpdate> updatedParameters) {
    }

    @Override
    public FlowFileGate getFlowFileGate() {
        return new FlowFileGate() {
            @Override
            public boolean tryClaim(Port port) {
                return true;
            }

            @Override
            public void releaseClaim(Port port) {
            }
        };
    }

    @Override
    public FlowFileConcurrency getFlowFileConcurrency() {
        return FlowFileConcurrency.UNBOUNDED;
    }

    @Override
    public void setFlowFileConcurrency(final FlowFileConcurrency flowFileConcurrency) {
    }

    @Override
    public FlowFileOutboundPolicy getFlowFileOutboundPolicy() {
        return FlowFileOutboundPolicy.STREAM_WHEN_AVAILABLE;
    }

    @Override
    public void setFlowFileOutboundPolicy(final FlowFileOutboundPolicy outboundPolicy) {
    }

    @Override
    public boolean isDataQueued() {
        return false;
    }

    @Override
    public boolean isDataQueuedForProcessing() {
        return false;
    }

    @Override
    public BatchCounts getBatchCounts() {
        return new NoOpBatchCounts();
    }

    public DataValve getDataValve(Port port) {
        return null;
    }

    @Override
    public DataValve getDataValve() {
        return null;
    }

    @Override
    public boolean referencesParameterContext(final ParameterContext parameterContext) {
        return false;
    }

    @Override
    public String getDefaultFlowFileExpiration() {
        return defaultFlowfileExpiration;
    }

    @Override
    public void setDefaultFlowFileExpiration(String defaultFlowFileExpiration) {
        this.defaultFlowfileExpiration = defaultFlowFileExpiration;
    }

    @Override
    public Long getDefaultBackPressureObjectThreshold() {
        return defaultBackPressureObjectThreshold;
    }

    @Override
    public void setDefaultBackPressureObjectThreshold(Long defaultBackPressureObjectThreshold) {
        this.defaultBackPressureObjectThreshold = defaultBackPressureObjectThreshold;
    }

    @Override
    public String getDefaultBackPressureDataSizeThreshold() {
        return defaultBackPressureDataSizeThreshold;
    }

    @Override
    public void setDefaultBackPressureDataSizeThreshold(String defaultBackPressureDataSizeThreshold) {
        this.defaultBackPressureDataSizeThreshold = defaultBackPressureDataSizeThreshold;
    }

    @Override
    public QueueSize getQueueSize() {
        return null;
    }

    @Override
    public String getLogFileSuffix() {
        return null;
    }

    public ExecutionEngine getExecutionEngine() {
        return ExecutionEngine.STANDARD;
    }

    @Override
    public ExecutionEngine resolveExecutionEngine() {
        return ExecutionEngine.STANDARD;
    }

    @Override
    public void verifyCanSetExecutionEngine(final ExecutionEngine executionEngine) {
    }

    @Override
    public void verifyCanUpdateExecutionEngine() {
    }

    @Override
    public void setMaxConcurrentTasks(final int maxConcurrentTasks) {
    }

    @Override
    public int getMaxConcurrentTasks() {
        return 0;
    }

    @Override
    public void setStatelessFlowTimeout(final String timeout) {
    }

    @Override
    public String getStatelessFlowTimeout() {
        return null;
    }

    @Override
    public void setLogFileSuffix(String logFileSuffix) {

    }

    @Override
    public void terminateProcessor(ProcessorNode processor) {
    }

}
