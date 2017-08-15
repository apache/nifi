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
package org.apache.nifi.controller.service;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardControllerServiceProvider implements ControllerServiceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceProvider.class);

    private final ProcessScheduler processScheduler;
    private final BulletinRepository bulletinRepo;
    private final StateManagerProvider stateManagerProvider;
    private final VariableRegistry variableRegistry;
    private final FlowController flowController;
    private final NiFiProperties nifiProperties;

    private final ConcurrentMap<String, ControllerServiceNode> serviceCache = new ConcurrentHashMap<>();

    public StandardControllerServiceProvider(final FlowController flowController, final ProcessScheduler scheduler, final BulletinRepository bulletinRepo,
            final StateManagerProvider stateManagerProvider, final VariableRegistry variableRegistry, final NiFiProperties nifiProperties) {

        this.flowController = flowController;
        this.processScheduler = scheduler;
        this.bulletinRepo = bulletinRepo;
        this.stateManagerProvider = stateManagerProvider;
        this.variableRegistry = variableRegistry;
        this.nifiProperties = nifiProperties;
    }

    private StateManager getStateManager(final String componentId) {
        return stateManagerProvider.getStateManager(componentId);
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded) {
        if (type == null || id == null || bundleCoordinate == null) {
            throw new NullPointerException();
        }

        ClassLoader cl = null;
        final ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Class<?> rawClass;
            try {
                final Bundle csBundle = ExtensionManager.getBundle(bundleCoordinate);
                if (csBundle == null) {
                    throw new ControllerServiceInstantiationException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
                }

                cl = ExtensionManager.createInstanceClassLoader(type, id, csBundle, additionalUrls);
                Thread.currentThread().setContextClassLoader(cl);
                rawClass = Class.forName(type, false, cl);
            } catch (final Exception e) {
                logger.error("Could not create Controller Service of type " + type + " for ID " + id + "; creating \"Ghost\" implementation", e);
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
                return createGhostControllerService(type, id, bundleCoordinate);
            }

            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);

            final ControllerService originalService = controllerServiceClass.newInstance();
            final StandardControllerServiceInvocationHandler invocationHandler = new StandardControllerServiceInvocationHandler(originalService);

            // extract all interfaces... controllerServiceClass is non null so getAllInterfaces is non null
            final List<Class<?>> interfaceList = ClassUtils.getAllInterfaces(controllerServiceClass);
            final Class<?>[] interfaces = interfaceList.toArray(new Class<?>[interfaceList.size()]);

            final ControllerService proxiedService;
            if (cl == null) {
                proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), interfaces, invocationHandler);
            } else {
                proxiedService = (ControllerService) Proxy.newProxyInstance(cl, interfaces, invocationHandler);
            }
            logger.info("Created Controller Service of type {} with identifier {}", type, id);

            final ComponentLog serviceLogger = new SimpleProcessLogger(id, originalService);
            originalService.initialize(new StandardControllerServiceInitializationContext(id, serviceLogger, this, getStateManager(id), nifiProperties));

            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(this, variableRegistry);

            final LoggableComponent<ControllerService> originalLoggableComponent = new LoggableComponent<>(originalService, bundleCoordinate, serviceLogger);
            final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(proxiedService, bundleCoordinate, serviceLogger);

            final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(originalLoggableComponent, proxiedLoggableComponent, invocationHandler,
                    id, validationContextFactory, this, componentVarRegistry, flowController);
            serviceNode.setName(rawClass.getSimpleName());

            invocationHandler.setServiceNode(serviceNode);

            if (firstTimeAdded) {
                try (final NarCloseable x = NarCloseable.withComponentNarLoader(originalService.getClass(), originalService.getIdentifier())) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, originalService);
                } catch (final Exception e) {
                    throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + originalService, e);
                }
            }

            serviceCache.putIfAbsent(id, serviceNode);

            return serviceNode;
        } catch (final Throwable t) {
            throw new ControllerServiceInstantiationException(t);
        } finally {
            if (currentContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
            }
        }
    }

    private ControllerServiceNode createGhostControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate) {
        final ControllerServiceInvocationHandler invocationHandler = new ControllerServiceInvocationHandler() {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                final String methodName = method.getName();

                if ("validate".equals(methodName)) {
                    final ValidationResult result = new ValidationResult.Builder()
                            .input("Any Property")
                            .subject("Missing Controller Service")
                            .valid(false)
                            .explanation("Controller Service could not be created because the Controller Service Type (" + type + ") could not be found")
                            .build();
                    return Collections.singleton(result);
                } else if ("getPropertyDescriptor".equals(methodName)) {
                    final String propertyName = (String) args[0];
                    return new PropertyDescriptor.Builder()
                            .name(propertyName)
                            .description(propertyName)
                            .sensitive(true)
                            .required(true)
                            .build();
                } else if ("getPropertyDescriptors".equals(methodName)) {
                    return Collections.emptyList();
                } else if ("onPropertyModified".equals(methodName)) {
                    return null;
                } else if ("getIdentifier".equals(methodName)) {
                    return id;
                } else if ("toString".equals(methodName)) {
                    return "GhostControllerService[id=" + id + ", type=" + type + "]";
                } else if ("hashCode".equals(methodName)) {
                    return 91 * type.hashCode() + 41 * id.hashCode();
                } else if ("equals".equals(methodName)) {
                    return proxy == args[0];
                } else {
                    throw new IllegalStateException("Controller Service could not be created because the Controller Service Type (" + type + ") could not be found");
                }
            }
            @Override
            public void setServiceNode(ControllerServiceNode serviceNode) {
                // nothing to do
            }
        };

        final ControllerService proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[]{ControllerService.class}, invocationHandler);

        final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
        final String componentType = "(Missing) " + simpleClassName;

        final LoggableComponent<ControllerService> proxiedLoggableComponent = new LoggableComponent<>(proxiedService, bundleCoordinate, null);

        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ControllerServiceNode serviceNode = new StandardControllerServiceNode(proxiedLoggableComponent, proxiedLoggableComponent, invocationHandler, id,
                new StandardValidationContextFactory(this, variableRegistry), this, componentType, type, componentVarRegistry, flowController, true);

        serviceCache.putIfAbsent(id, serviceNode);
        return serviceNode;
    }

    @Override
    public Set<ConfiguredComponent> disableReferencingServices(final ControllerServiceNode serviceNode) {
        // Get a list of all Controller Services that need to be disabled, in the order that they need to be disabled.
        final List<ControllerServiceNode> toDisable = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);

        final Set<ControllerServiceNode> serviceSet = new HashSet<>(toDisable);

        final Set<ConfiguredComponent> updated = new HashSet<>();
        for (final ControllerServiceNode nodeToDisable : toDisable) {
            if (nodeToDisable.isActive()) {
                nodeToDisable.verifyCanDisable(serviceSet);
                updated.add(nodeToDisable);
            }
        }

        Collections.reverse(toDisable);
        processScheduler.disableControllerServices(toDisable);
        return updated;
    }

    @Override
    public Set<ConfiguredComponent> scheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = serviceNode.getReferences().findRecursiveReferences(ReportingTaskNode.class);

        final Set<ConfiguredComponent> updated = new HashSet<>();

        // verify that  we can start all components (that are not disabled) before doing anything
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.verifyCanStart();
                updated.add(node);
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.verifyCanStart();
                updated.add(node);
            }
        }

        // start all of the components that are not disabled
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.getProcessGroup().startProcessor(node);
                updated.add(node);
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                processScheduler.schedule(node);
                updated.add(node);
            }
        }

        return updated;
    }

    @Override
    public Set<ConfiguredComponent> unscheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = serviceNode.getReferences().findRecursiveReferences(ReportingTaskNode.class);

        final Set<ConfiguredComponent> updated = new HashSet<>();

        // verify that  we can stop all components (that are running) before doing anything
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() == ScheduledState.RUNNING) {
                node.verifyCanStop();
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() == ScheduledState.RUNNING) {
                node.verifyCanStop();
            }
        }

        // stop all of the components that are running
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() == ScheduledState.RUNNING) {
                node.getProcessGroup().stopProcessor(node);
                updated.add(node);
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() == ScheduledState.RUNNING) {
                processScheduler.unschedule(node);
                updated.add(node);
            }
        }

        return updated;
    }

    @Override
    public CompletableFuture<Void> enableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanEnable();
        return processScheduler.enableControllerService(serviceNode);
    }

    @Override
    public void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes) {
        boolean shouldStart = true;

        Iterator<ControllerServiceNode> serviceIter = serviceNodes.iterator();
        while (serviceIter.hasNext() && shouldStart) {
            ControllerServiceNode controllerServiceNode = serviceIter.next();
            List<ControllerServiceNode> requiredServices = ((StandardControllerServiceNode) controllerServiceNode).getRequiredControllerServices();
            for (ControllerServiceNode requiredService : requiredServices) {
                if (!requiredService.isActive() && !serviceNodes.contains(requiredService)) {
                    shouldStart = false;
                    logger.debug("Will not start {} because required service {} is not active and is not part of the collection of things to start", serviceNodes, requiredService);
                }
            }
        }

        if (shouldStart) {
            for (ControllerServiceNode controllerServiceNode : serviceNodes) {
                try {
                    if (!controllerServiceNode.isActive()) {
                        final Future<Void> future = enableControllerServiceDependenciesFirst(controllerServiceNode);

                        try {
                            future.get(30, TimeUnit.SECONDS);
                            logger.debug("Successfully enabled {}; service state = {}", controllerServiceNode, controllerServiceNode.getState());
                        } catch (final Exception e) {
                            logger.warn("Failed to enable service {}", controllerServiceNode, e);
                            // Nothing we can really do. Will attempt to enable this service anyway.
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to enable " + controllerServiceNode, e);
                    if (this.bulletinRepo != null) {
                        this.bulletinRepo.addBulletin(BulletinFactory.createBulletin("Controller Service",
                                Severity.ERROR.name(), "Could not start " + controllerServiceNode + " due to " + e));
                    }
                }
            }
        }
    }

    private Future<Void> enableControllerServiceDependenciesFirst(ControllerServiceNode serviceNode) {
        final Map<ControllerServiceNode, Future<Void>> futures = new HashMap<>();

        for (ControllerServiceNode depNode : serviceNode.getRequiredControllerServices()) {
            if (!depNode.isActive()) {
                logger.debug("Before enabling {}, will enable dependent Controller Service {}", serviceNode, depNode);
                futures.put(depNode, this.enableControllerServiceDependenciesFirst(depNode));
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("All dependent services for {} have now begun enabling. Will wait for them to complete", serviceNode);
        }

        for (final Map.Entry<ControllerServiceNode, Future<Void>> entry : futures.entrySet()) {
            final ControllerServiceNode dependentService = entry.getKey();
            final Future<Void> future = entry.getValue();

            try {
                future.get(30, TimeUnit.SECONDS);
                logger.debug("Successfully enabled dependent service {}; service state = {}", dependentService, dependentService.getState());
            } catch (final Exception e) {
                logger.error("Failed to enable service {}, so may be unable to enable {}", dependentService, serviceNode, e);
                // Nothing we can really do. Will attempt to enable this service anyway.
            }
        }

        logger.debug("All dependent services have been enabled for {}; will now start service itself", serviceNode);
        return this.enableControllerService(serviceNode);
    }

    static List<List<ControllerServiceNode>> determineEnablingOrder(final Map<String, ControllerServiceNode> serviceNodeMap) {
        final List<List<ControllerServiceNode>> orderedNodeLists = new ArrayList<>();

        for (final ControllerServiceNode node : serviceNodeMap.values()) {
            final List<ControllerServiceNode> branch = new ArrayList<>();
            determineEnablingOrder(serviceNodeMap, node, branch, new HashSet<ControllerServiceNode>());
            orderedNodeLists.add(branch);
        }

        return orderedNodeLists;
    }

    private static void determineEnablingOrder(
            final Map<String, ControllerServiceNode> serviceNodeMap,
            final ControllerServiceNode contextNode,
            final List<ControllerServiceNode> orderedNodes,
            final Set<ControllerServiceNode> visited) {
        if (visited.contains(contextNode)) {
            return;
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : contextNode.getProperties().entrySet()) {
            if (entry.getKey().getControllerServiceDefinition() != null) {
                final String referencedServiceId = entry.getValue();
                if (referencedServiceId != null) {
                    final ControllerServiceNode referencedNode = serviceNodeMap.get(referencedServiceId);
                    if (!orderedNodes.contains(referencedNode)) {
                        visited.add(contextNode);
                        determineEnablingOrder(serviceNodeMap, referencedNode, orderedNodes, visited);
                    }
                }
            }
        }

        if (!orderedNodes.contains(contextNode)) {
            orderedNodes.add(contextNode);
        }
    }

    @Override
    public CompletableFuture<Void> disableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanDisable();
        return processScheduler.disableControllerService(serviceNode);
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? null : node.getProxiedControllerService();
    }

    private ProcessGroup getRootGroup() {
        return flowController.getGroup(flowController.getRootGroupId());
    }

    @Override
    public ControllerService getControllerServiceForComponent(final String serviceIdentifier, final String componentId) {
        // Find the Process Group that owns the component.
        ProcessGroup groupOfInterest = null;

        final ProcessorNode procNode = flowController.getProcessorNode(componentId);
        if (procNode == null) {
            final ControllerServiceNode serviceNode = getControllerServiceNode(componentId);
            if (serviceNode == null) {
                final ReportingTaskNode taskNode = flowController.getReportingTaskNode(componentId);
                if (taskNode == null) {
                    throw new IllegalStateException("Could not find any Processor, Reporting Task, or Controller Service with identifier " + componentId);
                }

                // we have confirmed that the component is a reporting task. We can only reference Controller Services
                // that are scoped at the FlowController level in this case.
                final ControllerServiceNode rootServiceNode = flowController.getRootControllerService(serviceIdentifier);
                return (rootServiceNode == null) ? null : rootServiceNode.getProxiedControllerService();
            } else {
                groupOfInterest = serviceNode.getProcessGroup();
            }
        } else {
            groupOfInterest = procNode.getProcessGroup();
        }

        if (groupOfInterest == null) {
            final ControllerServiceNode rootServiceNode = flowController.getRootControllerService(serviceIdentifier);
            return (rootServiceNode == null) ? null : rootServiceNode.getProxiedControllerService();
        }

        final Set<ControllerServiceNode> servicesForGroup = groupOfInterest.getControllerServices(true);
        for (final ControllerServiceNode serviceNode : servicesForGroup) {
            if (serviceIdentifier.equals(serviceNode.getIdentifier())) {
                return serviceNode.getProxiedControllerService();
            }
        }

        return null;
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? false : ControllerServiceState.ENABLED == node.getState();
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? false : ControllerServiceState.ENABLING == node.getState();
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String serviceIdentifier) {
        final ControllerServiceNode rootServiceNode = flowController.getRootControllerService(serviceIdentifier);
        if (rootServiceNode != null) {
            return rootServiceNode;
        }

        return serviceCache.get(serviceIdentifier);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType, final String groupId) {
        final Set<ControllerServiceNode> serviceNodes;
        if (groupId == null) {
            serviceNodes = flowController.getRootControllerServices();
        } else {
            ProcessGroup group = getRootGroup();
            if (!FlowController.ROOT_GROUP_ID_ALIAS.equals(groupId) && !group.getIdentifier().equals(groupId)) {
                group = group.findProcessGroup(groupId);
            }

            if (group == null) {
                return Collections.emptySet();
            }

            serviceNodes = group.getControllerServices(true);
        }

        return serviceNodes.stream()
            .filter(service -> serviceType.isAssignableFrom(service.getProxiedControllerService().getClass()))
            .map(ControllerServiceNode::getIdentifier)
            .collect(Collectors.toSet());
    }


    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? null : node.getName();
    }

    @Override
    public void removeControllerService(final ControllerServiceNode serviceNode) {
        final ProcessGroup group = requireNonNull(serviceNode).getProcessGroup();
        if (group == null) {
            flowController.removeRootControllerService(serviceNode);
            return;
        }

        group.removeControllerService(serviceNode);
        ExtensionManager.removeInstanceClassLoader(serviceNode.getIdentifier());
        serviceCache.remove(serviceNode.getIdentifier());
    }

    @Override
    public Set<ControllerServiceNode> getAllControllerServices() {
        final Set<ControllerServiceNode> allServices = new HashSet<>();
        allServices.addAll(flowController.getRootControllerServices());
        allServices.addAll(serviceCache.values());

        return allServices;
    }


    @Override
    public Set<ConfiguredComponent> enableReferencingServices(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> recursiveReferences = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
        logger.debug("Enabling the following Referencing Services for {}: {}", serviceNode, recursiveReferences);
        return enableReferencingServices(serviceNode, recursiveReferences);
    }

    private Set<ConfiguredComponent> enableReferencingServices(final ControllerServiceNode serviceNode, final List<ControllerServiceNode> recursiveReferences) {
        if (!serviceNode.isActive()) {
            serviceNode.verifyCanEnable(new HashSet<>(recursiveReferences));
        }

        final Set<ConfiguredComponent> updated = new HashSet<>();

        final Set<ControllerServiceNode> ifEnabled = new HashSet<>();
        for (final ControllerServiceNode nodeToEnable : recursiveReferences) {
            if (!nodeToEnable.isActive()) {
                nodeToEnable.verifyCanEnable(ifEnabled);
                ifEnabled.add(nodeToEnable);
            }
        }

        for (final ControllerServiceNode nodeToEnable : recursiveReferences) {
            if (!nodeToEnable.isActive()) {
                logger.debug("Enabling {} because it references {}", nodeToEnable, serviceNode);
                enableControllerService(nodeToEnable);
                updated.add(nodeToEnable);
            }
        }

        return updated;
    }

    @Override
    public void verifyCanEnableReferencingServices(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> referencingServices = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
        final Set<ControllerServiceNode> referencingServiceSet = new HashSet<>(referencingServices);

        for (final ControllerServiceNode referencingService : referencingServices) {
            referencingService.verifyCanEnable(referencingServiceSet);
        }
    }

    @Override
    public void verifyCanScheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> referencingServices = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
        final List<ReportingTaskNode> referencingReportingTasks = serviceNode.getReferences().findRecursiveReferences(ReportingTaskNode.class);
        final List<ProcessorNode> referencingProcessors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);

        final Set<ControllerServiceNode> referencingServiceSet = new HashSet<>(referencingServices);

        for (final ReportingTaskNode taskNode : referencingReportingTasks) {
            if (taskNode.getScheduledState() != ScheduledState.DISABLED) {
                taskNode.verifyCanStart(referencingServiceSet);
            }
        }

        for (final ProcessorNode procNode : referencingProcessors) {
            if (procNode.getScheduledState() != ScheduledState.DISABLED) {
                procNode.verifyCanStart(referencingServiceSet);
            }
        }
    }

    @Override
    public void verifyCanDisableReferencingServices(final ControllerServiceNode serviceNode) {
        // Get a list of all Controller Services that need to be disabled, in the order that they need to be disabled.
        final List<ControllerServiceNode> toDisable = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
        final Set<ControllerServiceNode> serviceSet = new HashSet<>(toDisable);

        for (final ControllerServiceNode nodeToDisable : toDisable) {
            if (nodeToDisable.isActive()) {
                nodeToDisable.verifyCanDisable(serviceSet);
            }
        }
    }

    @Override
    public void verifyCanStopReferencingComponents(final ControllerServiceNode serviceNode) {
        // we can always stop referencing components
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Cannot obtain Controller Service Identifiers for service type " + serviceType + " without providing a Process Group Identifier");
    }
}
