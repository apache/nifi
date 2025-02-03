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
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
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
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.ReferenceOnlyParameterContext;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterReferenceManager;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.validation.RuleViolationsManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class AbstractFlowManager implements FlowManager {
    private final ConcurrentMap<String, ProcessGroup> allProcessGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProcessorNode> allProcessors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allInputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allOutputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Funnel> allFunnels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReportingTaskNode> allReportingTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FlowAnalysisRuleNode> allFlowAnalysisRules = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParameterProviderNode> allParameterProviders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FlowRegistryClientNode> allFlowRegistryClients = new ConcurrentHashMap<>();

    private final FlowFileEventRepository flowFileEventRepository;
    private final ParameterContextManager parameterContextManager;
    private final BooleanSupplier flowInitializedCheck;

    private volatile ControllerServiceProvider controllerServiceProvider;
    private volatile PythonBridge pythonBridge;
    private volatile FlowAnalyzer flowAnalyzer;
    private volatile RuleViolationsManager ruleViolationsManager;
    private volatile ProcessGroup rootGroup;

    private final ThreadLocal<Boolean> withParameterContextResolution = ThreadLocal.withInitial(() -> false);

    public AbstractFlowManager(final FlowFileEventRepository flowFileEventRepository, final ParameterContextManager parameterContextManager,
                               final BooleanSupplier flowInitializedCheck) {
        this.flowFileEventRepository = flowFileEventRepository;
        this.parameterContextManager = parameterContextManager;
        this.flowInitializedCheck = flowInitializedCheck;
    }

    public void initialize(
            final ControllerServiceProvider controllerServiceProvider,
            final PythonBridge pythonBridge,
            final FlowAnalyzer flowAnalyzer,
            final RuleViolationsManager ruleViolationsManager
    ) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.pythonBridge = pythonBridge;
        this.flowAnalyzer = flowAnalyzer;
        this.ruleViolationsManager = ruleViolationsManager;
    }

    @Override
    public ProcessGroup getGroup(final String id) {
        return allProcessGroups.get(requireNonNull(id));
    }

    @Override
    public void onProcessGroupAdded(final ProcessGroup group) {
        allProcessGroups.put(group.getIdentifier(), group);
    }

    @Override
    public void onProcessGroupRemoved(final ProcessGroup group) {
        String identifier = group.getIdentifier();
        allProcessGroups.remove(identifier);

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public void onProcessorAdded(final ProcessorNode procNode) {
        allProcessors.put(procNode.getIdentifier(), procNode);
    }

    @Override
    public void onProcessorRemoved(final ProcessorNode procNode) {
        final String identifier = procNode.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allProcessors.remove(identifier);
        pythonBridge.onProcessorRemoved(identifier, procNode.getComponentType(), procNode.getBundleCoordinate().getVersion());

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public Set<ProcessorNode> findAllProcessors(final Predicate<ProcessorNode> filter) {
        return allProcessors.values().stream()
            .filter(filter)
            .collect(Collectors.toSet());
    }

    @Override
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

        final ProcessGroup group = getGroup(id);
        if (group != null) {
            return group.getStatelessGroupNode().orElse(null);
        }

        final RemoteGroupPort remoteGroupPort = getRootGroup().findRemoteGroupPort(id);
        return remoteGroupPort;
    }

    @Override
    public ProcessorNode getProcessorNode(final String id) {
        return allProcessors.get(id);
    }

    @Override
    public void onConnectionAdded(final Connection connection) {
        allConnections.put(connection.getIdentifier(), connection);

        if (isFlowInitialized()) {
            connection.getFlowFileQueue().startLoadBalancing();
        }
    }

    protected boolean isFlowInitialized() {
        return flowInitializedCheck.getAsBoolean();
    }

    @Override
    public void onConnectionRemoved(final Connection connection) {
        String identifier = connection.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allConnections.remove(identifier);

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public Connection getConnection(final String id) {
        return allConnections.get(id);
    }

    @Override
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

    @Override
    public ProcessGroup getRootGroup() {
        return rootGroup;
    }

    @Override
    public String getRootGroupId() {
        return rootGroup.getIdentifier();
    }

    @Override
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
        componentCounts.put("Flow Analysis Rules", getAllFlowAnalysisRules().size());
        componentCounts.put("Process Groups", allProcessGroups.size() - 2); // -2 to account for the root group because we don't want it in our counts and the 'root group alias' key.
        componentCounts.put("Remote Process Groups", getRootGroup().findAllRemoteProcessGroups().size());
        componentCounts.put("Parameter Providers", getAllParameterProviders().size());
        componentCounts.put("Flow Registry Clients", getAllFlowRegistryClients().size());

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
        getAllFlowAnalysisRules().forEach(this::removeFlowAnalysisRule);
        getAllParameterProviders().forEach(this::removeParameterProvider);

        getAllFlowRegistryClients().forEach(this::removeFlowRegistryClient);

        for (final ParameterContext parameterContext : parameterContextManager.getParameterContexts()) {
            parameterContextManager.removeParameterContext(parameterContext.getIdentifier());
        }

        LogRepositoryFactory.purge();
    }

    private void verifyCanPurge() {
        for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
            serviceNode.verifyCanDelete();
        }

        for (final ReportingTaskNode reportingTask : getAllReportingTasks()) {
            reportingTask.verifyCanDelete();
        }

        for (final FlowAnalysisRuleNode flowAnalysisRule : getAllFlowAnalysisRules()) {
            flowAnalysisRule.verifyCanDelete();
        }

        for (final ParameterProviderNode parameterProvider : getAllParameterProviders()) {
            parameterProvider.verifyCanDelete();
        }

        final ProcessGroup rootGroup = getRootGroup();
        rootGroup.verifyCanDelete(true);
    }

    @Override
    public Set<ControllerServiceNode> getAllControllerServices() {
        final Set<ControllerServiceNode> allServiceNodes = new HashSet<>();
        allServiceNodes.addAll(controllerServiceProvider.getNonRootControllerServices());
        allServiceNodes.addAll(getRootControllerServices());
        return allServiceNodes;
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String id) {
        return controllerServiceProvider.getControllerServiceNode(id);
    }

    @Override
    public void onInputPortAdded(final Port inputPort) {
        allInputPorts.put(inputPort.getIdentifier(), inputPort);
    }

    @Override
    public void onInputPortRemoved(final Port inputPort) {
        String identifier = inputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allInputPorts.remove(identifier);

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public Port getInputPort(final String id) {
        return allInputPorts.get(id);
    }

    @Override
    public void onOutputPortAdded(final Port outputPort) {
        allOutputPorts.put(outputPort.getIdentifier(), outputPort);
    }

    @Override
    public void onOutputPortRemoved(final Port outputPort) {
        String identifier = outputPort.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allOutputPorts.remove(identifier);

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public Port getOutputPort(final String id) {
        return allOutputPorts.get(id);
    }

    @Override
    public void onFunnelAdded(final Funnel funnel) {
        allFunnels.put(funnel.getIdentifier(), funnel);
    }

    @Override
    public void onFunnelRemoved(final Funnel funnel) {
        String identifier = funnel.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allFunnels.remove(identifier);

        removeRuleViolationsForSubject(identifier);
    }

    @Override
    public Funnel getFunnel(final String id) {
        return allFunnels.get(id);
    }

    @Override
    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate) {
        return createProcessor(type, id, coordinate, true);
    }

    @Override
    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final boolean firstTimeAdded) {
        return createProcessor(type, id, coordinate, Collections.emptySet(), firstTimeAdded, true, null);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate) {
        return createReportingTask(type, bundleCoordinate, true);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createReportingTask(type, UUID.randomUUID().toString(), bundleCoordinate, firstTimeAdded);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createReportingTask(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true, null);
    }

    @Override
    public ReportingTaskNode getReportingTaskNode(final String taskId) {
        return allReportingTasks.get(taskId);
    }

    @Override
    public ParameterProviderNode createParameterProvider(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createParameterProvider(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    @Override
    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        final ReportingTaskNode existing = allReportingTasks.get(reportingTaskNode.getIdentifier());
        if (existing == null || existing != reportingTaskNode) {
            throw new IllegalStateException("Reporting Task " + reportingTaskNode + " does not exist in this Flow");
        }

        reportingTaskNode.verifyCanDelete();

        final Class<?> taskClass = reportingTaskNode.getReportingTask().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), taskClass, reportingTaskNode.getReportingTask().getIdentifier())) {
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

    @Override
    public FlowAnalysisRuleNode createFlowAnalysisRule(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) {
        return createFlowAnalysisRule(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true, null);
    }

    @Override
    public FlowAnalysisRuleNode getFlowAnalysisRuleNode(final String taskId) {
        return allFlowAnalysisRules.get(taskId);
    }

    @Override
    public void removeFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRuleNode) {
        final FlowAnalysisRuleNode existing = allFlowAnalysisRules.get(flowAnalysisRuleNode.getIdentifier());
        if (existing == null || existing != flowAnalysisRuleNode) {
            throw new IllegalStateException("Flow Analysis Rule " + flowAnalysisRuleNode + " does not exist in this Flow");
        }

        flowAnalysisRuleNode.verifyCanDelete();

        final Class<?> taskClass = flowAnalysisRuleNode.getFlowAnalysisRule().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), taskClass, flowAnalysisRuleNode.getFlowAnalysisRule().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, flowAnalysisRuleNode.getFlowAnalysisRule(), flowAnalysisRuleNode.getConfigurationContext());
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : flowAnalysisRuleNode.getEffectivePropertyValues().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                    if (serviceNode != null) {
                        serviceNode.removeReference(flowAnalysisRuleNode, descriptor);
                    }
                }
            }
        }

        allFlowAnalysisRules.remove(flowAnalysisRuleNode.getIdentifier());
        LogRepositoryFactory.removeRepository(flowAnalysisRuleNode.getIdentifier());

        getExtensionManager().removeInstanceClassLoader(flowAnalysisRuleNode.getIdentifier());
    }

    @Override
    public Set<FlowAnalysisRuleNode> getAllFlowAnalysisRules() {
        return new HashSet<>(allFlowAnalysisRules.values());
    }

    protected void onFlowAnalysisRuleAdded(final FlowAnalysisRuleNode flowAnalysisRuleNode) {
        allFlowAnalysisRules.put(flowAnalysisRuleNode.getIdentifier(), flowAnalysisRuleNode);
    }

    @Override
    public ParameterProviderNode getParameterProvider(final String id) {
        return id == null ? null : allParameterProviders.get(id);
    }

    @Override
    public void removeParameterProvider(final ParameterProviderNode parameterProvider) {
        final ParameterProviderNode existing = allParameterProviders.get(parameterProvider.getIdentifier());
        if (existing == null || existing != parameterProvider) {
            throw new IllegalStateException("Parameter Provider " + parameterProvider + " does not exist in this Flow");
        }

        final Class<?> taskClass = parameterProvider.getParameterProvider().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), taskClass, parameterProvider.getParameterProvider().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, parameterProvider.getParameterProvider(), parameterProvider.getConfigurationContext());
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : parameterProvider.getEffectivePropertyValues().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                    if (serviceNode != null) {
                        serviceNode.removeReference(parameterProvider, descriptor);
                    }
                }
            }
        }

        allParameterProviders.remove(parameterProvider.getIdentifier());
        LogRepositoryFactory.removeRepository(parameterProvider.getIdentifier());

        getExtensionManager().removeInstanceClassLoader(parameterProvider.getIdentifier());
    }

    @Override
    public Set<ParameterProviderNode> getAllParameterProviders() {
        return new HashSet<>(allParameterProviders.values());
    }

    public void onParameterProviderAdded(final ParameterProviderNode parameterProviderNode) {
        allParameterProviders.put(parameterProviderNode.getIdentifier(), parameterProviderNode);
    }

    protected abstract ExtensionManager getExtensionManager();

    protected abstract ProcessScheduler getProcessScheduler();

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return new HashSet<>(allReportingTasks.values());
    }

    @Override
    public FlowRegistryClientNode getFlowRegistryClient(final String id) {
        if (id == null) {
            return null;
        }
        return allFlowRegistryClients.get(id);
    }

    @Override
    public Set<FlowRegistryClientNode> getAllFlowRegistryClients() {
        return new HashSet<>(allFlowRegistryClients.values());
    }

    public void onFlowRegistryClientAdded(final FlowRegistryClientNode clientNode) {
        allFlowRegistryClients.put(clientNode.getIdentifier(), clientNode);
    }

    public void onFlowRegistryClientRemoved(final FlowRegistryClientNode clientNode) {
        allFlowRegistryClients.remove(clientNode.getIdentifier());
    }

    @Override
    public ParameterContextManager getParameterContextManager() {
        return parameterContextManager;
    }

    @Override
    public ParameterContext createParameterContext(final String id, final String name, final String description,
                                                   final Map<String, Parameter> parameters, final List<String> inheritedContextIds,
                                                   final ParameterProviderConfiguration parameterProviderConfiguration) {
        final boolean namingConflict = parameterContextManager.getParameterContexts().stream()
                .anyMatch(paramContext -> paramContext.getName().equals(name));

        if (namingConflict) {
            throw new IllegalStateException("Cannot create Parameter Context with name '" + name + "' because a Parameter Context already exists with that name");
        }

        final ParameterReferenceManager referenceManager = new StandardParameterReferenceManager(this);
        final ParameterContext parameterContext = new StandardParameterContext.Builder()
                .id(id)
                .name(name)
                .parameterReferenceManager(referenceManager)
                .parentAuthorizable(getParameterContextParent())
                .parameterProviderLookup(this)
                .parameterProviderConfiguration(parameterProviderConfiguration)
                .build();
        parameterContext.setParameters(parameters);
        parameterContext.setDescription(description);

        if (inheritedContextIds != null && !inheritedContextIds.isEmpty()) {
            if (!withParameterContextResolution.get()) {
                throw new IllegalStateException("A ParameterContext with inherited ParameterContexts may only be created from within a call to AbstractFlowManager#withParameterContextResolution");
            }
            final List<ParameterContext> parameterContextList = new ArrayList<>();
            for (final String inheritedContextId : inheritedContextIds) {
                parameterContextList.add(lookupParameterContext(inheritedContextId));
            }
            parameterContext.setInheritedParameterContexts(parameterContextList);
        }

        parameterContextManager.addParameterContext(parameterContext);
        return parameterContext;
    }

    @Override
    public void withParameterContextResolution(final Runnable parameterContextAction) {
        withParameterContextResolution.set(true);
        try {
            parameterContextAction.run();
        } finally {
            withParameterContextResolution.set(false);
        }

        for (final ParameterContext parameterContext : parameterContextManager.getParameterContexts()) {
            // if a param context in the manager itself is reference-only, it means there is a reference to a param
            // context that no longer exists
            if (parameterContext instanceof ReferenceOnlyParameterContext) {
                throw new IllegalStateException(String.format("A Parameter Context tries to inherit from another Parameter Context [%s] that does not exist",
                        parameterContext.getIdentifier()));
            }

            // resolve any references nested in the actual param contexts
            final List<ParameterContext> inheritedParamContexts = new ArrayList<>();
            for (final ParameterContext inheritedParamContext : parameterContext.getInheritedParameterContexts()) {
                if (inheritedParamContext instanceof ReferenceOnlyParameterContext) {
                    inheritedParamContexts.add(parameterContextManager.getParameterContext(inheritedParamContext.getIdentifier()));
                } else {
                    inheritedParamContexts.add(inheritedParamContext);
                }
            }
            parameterContext.setInheritedParameterContexts(inheritedParamContexts);
        }

        // if any reference-only inherited param contexts still exist, it means they couldn't be resolved
        for (final ParameterContext parameterContext : parameterContextManager.getParameterContexts()) {
            for (final ParameterContext inheritedParamContext : parameterContext.getInheritedParameterContexts()) {
                if (inheritedParamContext instanceof ReferenceOnlyParameterContext) {
                    throw new IllegalStateException(String.format("Parameter Context [%s] tries to inherit from a Parameter Context [%s] that does not exist",
                            parameterContext.getName(), inheritedParamContext.getIdentifier()));
                }
            }
        }
    }

    /**
     * Looks up a ParameterContext by ID.  If not found, registers a ReferenceOnlyParameterContext, preventing
     * chicken-egg scenarios where a referenced ParameterContext is not registered before the referencing one.
     * @param id A parameter context ID
     * @return The matching ParameterContext, or ReferenceOnlyParameterContext if not found yet
     */
    private ParameterContext lookupParameterContext(final String id) {
        if (!parameterContextManager.hasParameterContext(id)) {
            parameterContextManager.addParameterContext(new ReferenceOnlyParameterContext(id));
        }

        return parameterContextManager.getParameterContext(id);
    }

    protected abstract Authorizable getParameterContextParent();

    @Override
    public Optional<FlowAnalyzer> getFlowAnalyzer() {
        return Optional.ofNullable(flowAnalyzer);
    }

    @Override
    public Optional<RuleViolationsManager> getRuleViolationsManager() {
        return Optional.ofNullable(ruleViolationsManager);
    }

    private void removeRuleViolationsForSubject(String identifier) {
        if (ruleViolationsManager != null) {
            ruleViolationsManager.removeRuleViolationsForSubject(identifier);
        }
    }
}
