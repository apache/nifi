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

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.FlowSnippet;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.StandardFlowSnippet;
import org.apache.nifi.controller.StandardFunnel;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.ProcessorLogObserver;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.StandardRemoteProcessGroup;
import org.apache.nifi.remote.StandardRootGroupPort;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public class StandardFlowManager implements FlowManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardFlowManager.class);

    private final NiFiProperties nifiProperties;
    private final BulletinRepository bulletinRepository;
    private final StandardProcessScheduler processScheduler;
    private final Authorizer authorizer;
    private final SSLContext sslContext;
    private final FlowController flowController;
    private final FlowFileEventRepository flowFileEventRepository;

    private final boolean isSiteToSiteSecure;

    private volatile ProcessGroup rootGroup;
    private final ConcurrentMap<String, ProcessGroup> allProcessGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProcessorNode> allProcessors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReportingTaskNode> allReportingTasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ControllerServiceNode> rootControllerServices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allInputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allOutputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Funnel> allFunnels = new ConcurrentHashMap<>();

    public StandardFlowManager(final NiFiProperties nifiProperties, final SSLContext sslContext, final FlowController flowController, final FlowFileEventRepository flowFileEventRepository) {
        this.nifiProperties = nifiProperties;
        this.flowController = flowController;
        this.bulletinRepository = flowController.getBulletinRepository();
        this.processScheduler = flowController.getProcessScheduler();
        this.authorizer = flowController.getAuthorizer();
        this.sslContext = sslContext;
        this.flowFileEventRepository = flowFileEventRepository;

        this.isSiteToSiteSecure = Boolean.TRUE.equals(nifiProperties.isSiteToSiteSecure());
    }

    public Port createRemoteInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardRootGroupPort(id, name, null, TransferDirection.RECEIVE, ConnectableType.INPUT_PORT,
            authorizer, bulletinRepository, processScheduler, isSiteToSiteSecure, nifiProperties);
    }

    public Port createRemoteOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardRootGroupPort(id, name, null, TransferDirection.SEND, ConnectableType.OUTPUT_PORT,
            authorizer, bulletinRepository, processScheduler, isSiteToSiteSecure, nifiProperties);
    }

    public RemoteProcessGroup createRemoteProcessGroup(final String id, final String uris) {
        return new StandardRemoteProcessGroup(requireNonNull(id), uris, null, processScheduler, bulletinRepository, sslContext, nifiProperties);
    }

    public void setRootGroup(final ProcessGroup rootGroup) {
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

    private void verifyPortIdDoesNotExist(final String id) {
        final ProcessGroup rootGroup = getRootGroup();
        Port port = rootGroup.findOutputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
        port = rootGroup.findInputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
    }

    public Label createLabel(final String id, final String text) {
        return new StandardLabel(requireNonNull(id).intern(), text);
    }

    public Funnel createFunnel(final String id) {
        return new StandardFunnel(id.intern(), null, processScheduler);
    }

    public Port createLocalInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, null, ConnectableType.INPUT_PORT, processScheduler);
    }

    public Port createLocalOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, null, ConnectableType.OUTPUT_PORT, processScheduler);
    }

    public ProcessGroup createProcessGroup(final String id) {
        final MutableVariableRegistry mutableVariableRegistry = new MutableVariableRegistry(flowController.getVariableRegistry());

        final ProcessGroup group = new StandardProcessGroup(requireNonNull(id), flowController.getControllerServiceProvider(), processScheduler, nifiProperties, flowController.getEncryptor(),
            flowController, mutableVariableRegistry);
        allProcessGroups.put(group.getIdentifier(), group);

        return group;
    }

    public void instantiateSnippet(final ProcessGroup group, final FlowSnippetDTO dto) throws ProcessorInstantiationException {
        requireNonNull(group);
        requireNonNull(dto);

        final FlowSnippet snippet = new StandardFlowSnippet(dto, flowController.getExtensionManager());
        snippet.validate(group);
        snippet.instantiate(this, group);

        group.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);
    }

    public FlowFilePrioritizer createPrioritizer(final String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        FlowFilePrioritizer prioritizer;

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> prioritizerBundles = flowController.getExtensionManager().getBundles(type);
            if (prioritizerBundles.size() == 0) {
                throw new IllegalStateException(String.format("The specified class '%s' is not known to this nifi.", type));
            }
            if (prioritizerBundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple bundles found for the specified class '%s', only one is allowed.", type));
            }

            final Bundle bundle = prioritizerBundles.get(0);
            final ClassLoader detectedClassLoaderForType = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForType);

            Thread.currentThread().setContextClassLoader(detectedClassLoaderForType);
            final Class<? extends FlowFilePrioritizer> prioritizerClass = rawClass.asSubclass(FlowFilePrioritizer.class);
            final Object processorObj = prioritizerClass.newInstance();
            prioritizer = prioritizerClass.cast(processorObj);

            return prioritizer;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
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

    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate) {
        return createProcessor(type, id, coordinate, true);
    }

    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final boolean firstTimeAdded) {
        return createProcessor(type, id, coordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final Set<URL> additionalUrls,
                                         final boolean firstTimeAdded, final boolean registerLogObserver) {

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ProcessorNode procNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(coordinate)
            .extensionManager(extensionManager)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .variableRegistry(flowController.getVariableRegistry())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .extensionManager(extensionManager)
            .buildProcessor();

        LogRepositoryFactory.getRepository(procNode.getIdentifier()).setLogger(procNode.getLogger());
        if (registerLogObserver) {
            logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, procNode.getBulletinLevel(), new ProcessorLogObserver(bulletinRepository, procNode));
        }

        if (firstTimeAdded) {
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, procNode.getProcessor());
            } catch (final Exception e) {
                if (registerLogObserver) {
                    logRepository.removeObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID);
                }
                throw new ComponentLifeCycleException("Failed to invoke @OnAdded methods of " + procNode.getProcessor(), e);
            }

            if (firstTimeAdded) {
                try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, procNode.getProcessor());
                }
            }
        }

        return procNode;
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
        if (remoteGroupPort != null) {
            return remoteGroupPort;
        }

        return null;
    }

    public ProcessorNode getProcessorNode(final String id) {
        return allProcessors.get(id);
    }

    public void onConnectionAdded(final Connection connection) {
        allConnections.put(connection.getIdentifier(), connection);

        if (flowController.isInitialized()) {
            connection.getFlowFileQueue().startLoadBalancing();
        }
    }

    public void onConnectionRemoved(final Connection connection) {
        String identifier = connection.getIdentifier();
        flowFileEventRepository.purgeTransferEvents(identifier);
        allConnections.remove(identifier);
    }

    public Connection getConnection(final String id) {
        return allConnections.get(id);
    }

    public Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        return flowController.createConnection(id, name, source, destination, relationshipNames);
    }

    public Set<Connection> findAllConnections() {
        return new HashSet<>(allConnections.values());
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

    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
                                                 final boolean firstTimeAdded, final boolean register) {
        if (type == null || id == null || bundleCoordinate == null) {
            throw new NullPointerException();
        }

        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final ReportingTaskNode taskNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .extensionManager(flowController.getExtensionManager())
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .variableRegistry(flowController.getVariableRegistry())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .flowController(flowController)
            .extensionManager(extensionManager)
            .buildReportingTask();

        LogRepositoryFactory.getRepository(taskNode.getIdentifier()).setLogger(taskNode.getLogger());

        if (firstTimeAdded) {
            final Class<?> taskClass = taskNode.getReportingTask().getClass();
            final String identifier = taskNode.getReportingTask().getIdentifier();

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), taskClass, identifier)) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, taskNode.getReportingTask());
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, taskNode.getReportingTask());
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + taskNode.getReportingTask(), e);
            }
        }

        if (register) {
            allReportingTasks.put(id, taskNode);

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                new ReportingTaskLogObserver(bulletinRepository, taskNode));
        }

        return taskNode;
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
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), taskClass, reportingTaskNode.getReportingTask().getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, reportingTaskNode.getReportingTask(), reportingTaskNode.getConfigurationContext());
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : reportingTaskNode.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = flowController.getControllerServiceProvider().getControllerServiceNode(value);
                    if (serviceNode != null) {
                        serviceNode.removeReference(reportingTaskNode);
                    }
                }
            }
        }

        allReportingTasks.remove(reportingTaskNode.getIdentifier());
        LogRepositoryFactory.removeRepository(reportingTaskNode.getIdentifier());
        processScheduler.onReportingTaskRemoved(reportingTaskNode);

        flowController.getExtensionManager().removeInstanceClassLoader(reportingTaskNode.getIdentifier());
    }

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return new HashSet<>(allReportingTasks.values());
    }

    public Set<ControllerServiceNode> getRootControllerServices() {
        return new HashSet<>(rootControllerServices.values());
    }

    public void addRootControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = rootControllerServices.putIfAbsent(serviceNode.getIdentifier(), serviceNode);
        if (existing != null) {
            throw new IllegalStateException("Controller Service with ID " + serviceNode.getIdentifier() + " already exists at the Controller level");
        }
    }

    public ControllerServiceNode getRootControllerService(final String serviceIdentifier) {
        return rootControllerServices.get(serviceIdentifier);
    }

    public void removeRootControllerService(final ControllerServiceNode service) {
        final ControllerServiceNode existing = rootControllerServices.get(requireNonNull(service).getIdentifier());
        if (existing == null) {
            throw new IllegalStateException(service + " is not a member of this Process Group");
        }

        service.verifyCanDelete();

        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final VariableRegistry variableRegistry = flowController.getVariableRegistry();

        try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, service.getControllerServiceImplementation().getClass(), service.getIdentifier())) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(service, flowController.getControllerServiceProvider(), null, variableRegistry);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, service.getControllerServiceImplementation(), configurationContext);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : service.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode referencedNode = getRootControllerService(value);
                    if (referencedNode != null) {
                        referencedNode.removeReference(service);
                    }
                }
            }
        }

        rootControllerServices.remove(service.getIdentifier());
        flowController.getStateManagerProvider().onComponentRemoved(service.getIdentifier());

        extensionManager.removeInstanceClassLoader(service.getIdentifier());

        logger.info("{} removed from Flow Controller", service, this);
    }

    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded,
                                                         final boolean registerLogObserver) {
        // make sure the first reference to LogRepository happens outside of a NarCloseable so that we use the framework's ClassLoader
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final ControllerServiceProvider controllerServiceProvider = flowController.getControllerServiceProvider();

        final ControllerServiceNode serviceNode = new ExtensionBuilder()
            .identifier(id)
            .type(type)
            .bundleCoordinate(bundleCoordinate)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .processScheduler(processScheduler)
            .nodeTypeProvider(flowController)
            .validationTrigger(flowController.getValidationTrigger())
            .reloadComponent(flowController.getReloadComponent())
            .variableRegistry(flowController.getVariableRegistry())
            .addClasspathUrls(additionalUrls)
            .kerberosConfig(flowController.createKerberosConfig(nifiProperties))
            .stateManagerProvider(flowController.getStateManagerProvider())
            .extensionManager(extensionManager)
            .buildControllerService();

        LogRepositoryFactory.getRepository(serviceNode.getIdentifier()).setLogger(serviceNode.getLogger());
        if (registerLogObserver) {
            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN, new ControllerServiceLogObserver(bulletinRepository, serviceNode));
        }

        if (firstTimeAdded) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, service.getClass(), service.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }

            final ControllerService serviceImpl = serviceNode.getControllerServiceImplementation();
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, serviceImpl.getClass(), serviceImpl.getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, serviceImpl);
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + serviceImpl, e);
            }
        }

        controllerServiceProvider.onControllerServiceAdded(serviceNode);

        return serviceNode;
    }

    public Set<ControllerServiceNode> getAllControllerServices() {
        final Set<ControllerServiceNode> allServiceNodes = new HashSet<>();
        allServiceNodes.addAll(flowController.getControllerServiceProvider().getNonRootControllerServices());
        allServiceNodes.addAll(rootControllerServices.values());
        return allServiceNodes;
    }

    public ControllerServiceNode getControllerServiceNode(final String id) {
        return flowController.getControllerServiceProvider().getControllerServiceNode(id);
    }

}
