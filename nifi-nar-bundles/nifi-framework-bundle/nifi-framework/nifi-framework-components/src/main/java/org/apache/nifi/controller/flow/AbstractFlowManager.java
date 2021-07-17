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

package org.apache.nifi.controller.flow;

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.ReflectionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;

import static java.util.Objects.requireNonNull;

public abstract class AbstractFlowManager implements FlowManager {
    private final ConcurrentMap<String, ProcessGroup> allProcessGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProcessorNode> allProcessors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allInputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allOutputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Funnel> allFunnels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReportingTaskNode> allReportingTasks = new ConcurrentHashMap<>();

    private final FlowFileEventRepository flowFileEventRepository;
    private final ParameterContextManager parameterContextManager;
    private final FlowRegistryClient flowRegistryClient;
    private final BooleanSupplier flowInitializedCheck;

    private volatile ControllerServiceProvider controllerServiceProvider;
    private volatile ProcessGroup rootGroup;

    public AbstractFlowManager(final FlowFileEventRepository flowFileEventRepository, final ParameterContextManager parameterContextManager,
                               final FlowRegistryClient flowRegistryClient, final BooleanSupplier flowInitializedCheck) {
        this.flowFileEventRepository = flowFileEventRepository;
        this.parameterContextManager = parameterContextManager;
        this.flowRegistryClient = flowRegistryClient;
        this.flowInitializedCheck = flowInitializedCheck;
    }

    public void initialize(final ControllerServiceProvider controllerServiceProvider) {
        this.controllerServiceProvider = controllerServiceProvider;
    }

    public ProcessGroup getGroup(final String id) {
        return allProcessGroups.get(requireNonNull(id));
    }

    public void onProcessGroupAdded(final ProcessGroup group) {
        allProcessGroups.put(group.getIdentifier(), group);
    }

    public void onProcessGroupRemoved(final ProcessGroup group) {
        allProcessGroups.remove(group.getIdentifier());
    }

    public void onProcessorAdded(final ProcessorNode procNode) {
        allProcessors.put(procNode.getIdentifier(), procNode);
    }

    public void onProcessorRemoved(final ProcessorNode procNode) {
        String identifier = procNode.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allProcessors.remove(identifier);
    }

    public Connectable findConnectable(final String id) {
        final ProcessorNode procNode = getProcessorNode(id);
        if (procNode != null) {
            return procNode;
        }

        final Port inPort = getInputPort(id);
        if (inPort != null) {
            return inPort;
        }

        final Port outPort = getOutputPort(id);
        if (outPort != null) {
            return outPort;
        }

        final Funnel funnel = getFunnel(id);
        if (funnel != null) {
            return funnel;
        }

        final RemoteGroupPort remoteGroupPort = getRootGroup().findRemoteGroupPort(id);
        return remoteGroupPort;
    }

    public ProcessorNode getProcessorNode(final String id) {
        return allProcessors.get(id);
    }

    public void onConnectionAdded(final Connection connection) {
        allConnections.put(connection.getIdentifier(), connection);

        if (isFlowInitialized()) {
            connection.getFlowFileQueue().startLoadBalancing();
        }
    }

    protected boolean isFlowInitialized() {
        return flowInitializedCheck.getAsBoolean();
    }

    public void onConnectionRemoved(final Connection connection) {
        String identifier = connection.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allConnections.remove(identifier);
    }

    public Connection getConnection(final String id) {
        return allConnections.get(id);
    }

    public Set<Connection> findAllConnections() {
        return new HashSet<>(allConnections.values());
    }


    public void setRootGroup(final ProcessGroup rootGroup) {
        if (this.rootGroup != null && this.rootGroup.isEmpty()) {
            allProcessGroups.remove(this.rootGroup.getIdentifier());
        }

        this.rootGroup = rootGroup;
        allProcessGroups.put(ROOT_GROUP_ID_ALIAS, rootGroup);
        allProcessGroups.put(rootGroup.getIdentifier(), rootGroup);
    }

    public ProcessGroup getRootGroup() {
        return rootGroup;
    }

    @Override
    public String getRootGroupId() {
        return rootGroup.getIdentifier();
    }

    public boolean areGroupsSame(final String id1, final String id2) {
        if (id1 == null || id2 == null) {
            return false;
        } else if (id1.equals(id2)) {
            return true;
        } else {
            final String comparable1 = id1.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id1;
            final String comparable2 = id2.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id2;
            return comparable1.equals(comparable2);
        }
    }

    @Override
    public Map<String, Integer> getComponentCounts() {
        final Map<String, Integer> componentCounts = new LinkedHashMap<>();
        componentCounts.put("Processors", allProcessors.size());
        componentCounts.put("Controller Services", getAllControllerServices().size());
        componentCounts.put("Reporting Tasks", getAllReportingTasks().size());
        componentCounts.put("Process Groups", allProcessGroups.size() - 2); // -2 to account for the root group because we don't want it in our counts and the 'root group alias' key.
        componentCounts.put("Remote Process Groups", getRootGroup().findAllRemoteProcessGroups().size());

        int localInputPorts = 0;
        int publicInputPorts = 0;
        for (final Port port : allInputPorts.values()) {
            if (port instanceof PublicPort) {
                publicInputPorts++;
            } else {
                localInputPorts++;
            }
        }

        int localOutputPorts = 0;
        int publicOutputPorts = 0;
        for (final Port port : allOutputPorts.values()) {
            if (port instanceof PublicPort) {
                localOutputPorts++;
            } else {
                publicOutputPorts++;
            }
        }

        componentCounts.put("Local Input Ports", localInputPorts);
        componentCounts.put("Local Output Ports", localOutputPorts);
        componentCounts.put("Public Input Ports", publicInputPorts);
        componentCounts.put("Public Output Ports", publicOutputPorts);

        return componentCounts;
    }

    @Override
    public void purge() {
        verifyCanPurge();

        final ProcessGroup rootGroup = getRootGroup();

        // Delete templates from all levels first. This allows us to avoid having to purge each individual Process Group recursively
        // and instead just delete all child Process Groups after removing the connections to/from those Process Groups.
        for (final ProcessGroup group : rootGroup.findAllProcessGroups()) {
            group.getTemplates().forEach(group::removeTemplate);
        }
        rootGroup.getTemplates().forEach(rootGroup::removeTemplate);

        rootGroup.getConnections().forEach(rootGroup::removeConnection);
        rootGroup.getProcessors().forEach(rootGroup::removeProcessor);
        rootGroup.getFunnels().forEach(rootGroup::removeFunnel);
        rootGroup.getInputPorts().forEach(rootGroup::removeInputPort);
        rootGroup.getOutputPorts().forEach(rootGroup::removeOutputPort);
        rootGroup.getLabels().forEach(rootGroup::removeLabel);
        rootGroup.getRemoteProcessGroups().forEach(rootGroup::removeRemoteProcessGroup);

        rootGroup.getProcessGroups().forEach(rootGroup::removeProcessGroup);

        rootGroup.getControllerServices(false).forEach(controllerServiceProvider::removeControllerService);

        getRootControllerServices().forEach(this::removeRootControllerService);
        getAllReportingTasks().forEach(this::removeReportingTask);

        for (final String registryId : flowRegistryClient.getRegistryIdentifiers()) {
            flowRegistryClient.removeFlowRegistry(registryId);
        }

        for (final ParameterContext parameterContext : parameterContextManager.getParameterContexts()) {
            parameterContextManager.removeParameterContext(parameterContext.getIdentifier());
        }
    }

    private void verifyCanPurge() {
        for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
            serviceNode.verifyCanDelete();
        }

        for (final ReportingTaskNode reportingTask : getAllReportingTasks()) {
            reportingTask.verifyCanDelete();
        }

        final ProcessGroup rootGroup = getRootGroup();
        rootGroup.verifyCanDelete(true, true);
    }

    public Set<ControllerServiceNode> getAllControllerServices() {
        final Set<ControllerServiceNode> allServiceNodes = new HashSet<>();
        allServiceNodes.addAll(controllerServiceProvider.getNonRootControllerServices());
        allServiceNodes.addAll(getRootControllerServices());
        return allServiceNodes;
    }

    public ControllerServiceNode getControllerServiceNode(final String id) {
        return controllerServiceProvider.getControllerServiceNode(id);
    }

    public void onInputPortAdded(final Port inputPort) {
        allInputPorts.put(inputPort.getIdentifier(), inputPort);
    }

    public void onInputPortRemoved(final Port inputPort) {
        String identifier = inputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allInputPorts.remove(identifier);
    }

    public Port getInputPort(final String id) {
        return allInputPorts.get(id);
    }

    public void onOutputPortAdded(final Port outputPort) {
        allOutputPorts.put(outputPort.getIdentifier(), outputPort);
    }

    public void onOutputPortRemoved(final Port outputPort) {
        String identifier = outputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allOutputPorts.remove(identifier);
    }

    public Port getOutputPort(final String id) {
        return allOutputPorts.get(id);
    }

    public void onFunnelAdded(final Funnel funnel) {
        allFunnels.put(funnel.getIdentifier(), funnel);
    }

    public void onFunnelRemoved(final Funnel funnel) {
        String identifier = funnel.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allFunnels.remove(identifier);
    }

    public Funnel getFunnel(final String id) {
        return allFunnels.get(id);
    }

    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate) {
        return createProcessor(type, id, coordinate, true);
    }

    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final boolean firstTimeAdded) {
        return createProcessor(type, id, coordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate) {
        return createReportingTask(type, bundleCoordinate, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createReportingTask(type, UUID.randomUUID().toString(), bundleCoordinate, firstTimeAdded);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createReportingTask(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ReportingTaskNode getReportingTaskNode(final String taskId) {
        return allReportingTasks.get(taskId);
    }

    @Override
    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        final ReportingTaskNode existing = allReportingTasks.get(reportingTaskNode.getIdentifier());
        if (existing == null || existing != reportingTaskNode) {
            throw new IllegalStateException("Reporting Task " + reportingTaskNode + " does not exist in this Flow");
        }

        reportingTaskNode.verifyCanDelete();

        final Class<?> taskClass = reportingTaskNode.getReportingTask().getClass();
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(getExtensionManager(), taskClass, reportingTaskNode.getReportingTask().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, reportingTaskNode.getReportingTask(), reportingTaskNode.getConfigurationContext());
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : reportingTaskNode.getEffectivePropertyValues().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                    if (serviceNode != null) {
                        serviceNode.removeReference(reportingTaskNode, descriptor);
                    }
                }
            }
        }

        allReportingTasks.remove(reportingTaskNode.getIdentifier());
        LogRepositoryFactory.removeRepository(reportingTaskNode.getIdentifier());
        getProcessScheduler().onReportingTaskRemoved(reportingTaskNode);

        getExtensionManager().removeInstanceClassLoader(reportingTaskNode.getIdentifier());
    }

    public void onReportingTaskAdded(final ReportingTaskNode taskNode) {
        allReportingTasks.put(taskNode.getIdentifier(), taskNode);
    }

    protected abstract ExtensionManager getExtensionManager();

    protected abstract ProcessScheduler getProcessScheduler();

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return new HashSet<>(allReportingTasks.values());
    }

    @Override
    public ParameterContextManager getParameterContextManager() {
        return parameterContextManager;
    }

    @Override
    public ParameterContext createParameterContext(final String id, final String name, final Map<String, Parameter> parameters) {
        final boolean namingConflict = parameterContextManager.getParameterContexts().stream()
            .anyMatch(paramContext -> paramContext.getName().equals(name));

        if (namingConflict) {
            throw new IllegalStateException("Cannot create Parameter Context with name '" + name + "' because a Parameter Context already exists with that name");
        }

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(this);
        final ParameterContext parameterContext = new StandardParameterContext(id, name, referenceManager, getParameterContextParent());
        parameterContext.setParameters(parameters);
        parameterContextManager.addParameterContext(parameterContext);
        return parameterContext;
    }

    protected abstract Authorizable getParameterContextParent();
}
