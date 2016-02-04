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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardControllerServiceProvider implements ControllerServiceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceProvider.class);

    private final ProcessScheduler processScheduler;
    private final ConcurrentMap<String, ControllerServiceNode> controllerServices;
    private static final Set<Method> validDisabledMethods;
    private final BulletinRepository bulletinRepo;
    private final StateManagerProvider stateManagerProvider;

    static {
        // methods that are okay to be called when the service is disabled.
        final Set<Method> validMethods = new HashSet<>();
        for (final Method method : ControllerService.class.getMethods()) {
            validMethods.add(method);
        }
        for (final Method method : Object.class.getMethods()) {
            validMethods.add(method);
        }
        validDisabledMethods = Collections.unmodifiableSet(validMethods);
    }

    public StandardControllerServiceProvider(final ProcessScheduler scheduler, final BulletinRepository bulletinRepo, final StateManagerProvider stateManagerProvider) {
        // the following 2 maps must be updated atomically, but we do not lock around them because they are modified
        // only in the createControllerService method, and both are modified before the method returns
        this.controllerServices = new ConcurrentHashMap<>();
        this.processScheduler = scheduler;
        this.bulletinRepo = bulletinRepo;
        this.stateManagerProvider = stateManagerProvider;
    }

    private Class<?>[] getInterfaces(final Class<?> cls) {
        final List<Class<?>> allIfcs = new ArrayList<>();
        populateInterfaces(cls, allIfcs);
        return allIfcs.toArray(new Class<?>[allIfcs.size()]);
    }

    private void populateInterfaces(final Class<?> cls, final List<Class<?>> interfacesDefinedThusFar) {
        final Class<?>[] ifc = cls.getInterfaces();
        if (ifc != null && ifc.length > 0) {
            for (final Class<?> i : ifc) {
                interfacesDefinedThusFar.add(i);
            }
        }

        final Class<?> superClass = cls.getSuperclass();
        if (superClass != null) {
            populateInterfaces(superClass, interfacesDefinedThusFar);
        }
    }

    private StateManager getStateManager(final String componentId) {
        return stateManagerProvider.getStateManager(componentId);
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final boolean firstTimeAdded) {
        if (type == null || id == null) {
            throw new NullPointerException();
        }

        final ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader cl = ExtensionManager.getClassLoader(type);
            final Class<?> rawClass;
            if (cl == null) {
                rawClass = Class.forName(type);
            } else {
                Thread.currentThread().setContextClassLoader(cl);
                rawClass = Class.forName(type, false, cl);
            }

            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);

            final ControllerService originalService = controllerServiceClass.newInstance();
            final ObjectHolder<ControllerServiceNode> serviceNodeHolder = new ObjectHolder<>(null);
            final InvocationHandler invocationHandler = new InvocationHandler() {
                @Override
                public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

                    final String methodName = method.getName();
                    if ("initialize".equals(methodName) || "onPropertyModified".equals(methodName)) {
                        throw new UnsupportedOperationException(method + " may only be invoked by the NiFi framework");
                    }

                    final ControllerServiceNode node = serviceNodeHolder.get();
                    final ControllerServiceState state = node.getState();
                    final boolean disabled = state != ControllerServiceState.ENABLED; // only allow method call if service state is ENABLED.
                    if (disabled && !validDisabledMethods.contains(method)) {
                        // Use nar class loader here because we are implicitly calling toString() on the original implementation.
                        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service " + originalService + " because the Controller Service is disabled");
                        } catch (final Throwable e) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service with identifier " + id + " because the Controller Service is disabled");
                        }
                    }

                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return method.invoke(originalService, args);
                    } catch (final InvocationTargetException e) {
                        // If the ControllerService throws an Exception, it'll be wrapped in an InvocationTargetException. We want
                        // to instead re-throw what the ControllerService threw, so we pull it out of the InvocationTargetException.
                        throw e.getCause();
                    }
                }
            };

            final ControllerService proxiedService;
            if (cl == null) {
                proxiedService = (ControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), getInterfaces(controllerServiceClass), invocationHandler);
            } else {
                proxiedService = (ControllerService) Proxy.newProxyInstance(cl, getInterfaces(controllerServiceClass), invocationHandler);
            }
            logger.info("Created Controller Service of type {} with identifier {}", type, id);

            final ComponentLog serviceLogger = new SimpleProcessLogger(id, originalService);
            originalService.initialize(new StandardControllerServiceInitializationContext(id, serviceLogger, this, getStateManager(id)));

            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(this);

            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(proxiedService, originalService, id, validationContextFactory, this);
            serviceNodeHolder.set(serviceNode);
            serviceNode.setName(rawClass.getSimpleName());

            if (firstTimeAdded) {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, originalService);
                } catch (final Exception e) {
                    throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + originalService, e);
                }
            }

            this.controllerServices.put(id, serviceNode);
            return serviceNode;
        } catch (final Throwable t) {
            throw new ControllerServiceInstantiationException(t);
        } finally {
            if (currentContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
            }
        }
    }

    @Override
    public void disableReferencingServices(final ControllerServiceNode serviceNode) {
        // Get a list of all Controller Services that need to be disabled, in the order that they need to be
        // disabled.
        final List<ControllerServiceNode> toDisable = findRecursiveReferences(serviceNode, ControllerServiceNode.class);

        final Set<ControllerServiceNode> serviceSet = new HashSet<>(toDisable);

        for (final ControllerServiceNode nodeToDisable : toDisable) {
            if (nodeToDisable.isActive()) {
                nodeToDisable.verifyCanDisable(serviceSet);
            }
        }

        Collections.reverse(toDisable);
        processScheduler.disableControllerServices(toDisable);
    }

    @Override
    public void scheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = findRecursiveReferences(serviceNode, ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = findRecursiveReferences(serviceNode, ReportingTaskNode.class);

        // verify that  we can start all components (that are not disabled) before doing anything
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.verifyCanStart();
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.verifyCanStart();
            }
        }

        // start all of the components that are not disabled
        for (final ProcessorNode node : processors) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                node.getProcessGroup().startProcessor(node);
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() != ScheduledState.DISABLED) {
                processScheduler.schedule(node);
            }
        }
    }

    @Override
    public void unscheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = findRecursiveReferences(serviceNode, ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = findRecursiveReferences(serviceNode, ReportingTaskNode.class);

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
            }
        }
        for (final ReportingTaskNode node : reportingTasks) {
            if (node.getScheduledState() == ScheduledState.RUNNING) {
                processScheduler.unschedule(node);
            }
        }
    }

    @Override
    public void enableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanEnable();
        processScheduler.enableControllerService(serviceNode);
    }

    @Override
    public void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes) {
        final Set<ControllerServiceNode> servicesToEnable = new HashSet<>();
        // Ensure that all nodes are already disabled
        for (final ControllerServiceNode serviceNode : serviceNodes) {
            final ControllerServiceState curState = serviceNode.getState();
            if (ControllerServiceState.DISABLED.equals(curState)) {
                servicesToEnable.add(serviceNode);
            } else {
                logger.warn("Cannot enable {} because it is not disabled; current state is {}", serviceNode, curState);
            }
        }

        // determine the order to load the services. We have to ensure that if service A references service B, then B
        // is enabled first, and so on.
        final Map<String, ControllerServiceNode> idToNodeMap = new HashMap<>();
        for (final ControllerServiceNode node : servicesToEnable) {
            idToNodeMap.put(node.getIdentifier(), node);
        }

        // We can have many Controller Services dependent on one another. We can have many of these
        // disparate lists of Controller Services that are dependent on one another. We refer to each
        // of these as a branch.
        final List<List<ControllerServiceNode>> branches = determineEnablingOrder(idToNodeMap);

        if (branches.isEmpty()) {
            logger.info("No Controller Services to enable");
            return;
        } else {
            logger.info("Will enable {} Controller Services", servicesToEnable.size());
        }

        final Set<ControllerServiceNode> enabledNodes = Collections.synchronizedSet(new HashSet<ControllerServiceNode>());
        final ExecutorService executor = Executors.newFixedThreadPool(Math.min(10, branches.size()));
        for (final List<ControllerServiceNode> branch : branches) {
            final Runnable enableBranchRunnable = new Runnable() {
                @Override
                public void run() {
                    logger.debug("Enabling Controller Service Branch {}", branch);

                    for (final ControllerServiceNode serviceNode : branch) {
                        try {
                            if (!enabledNodes.contains(serviceNode)) {
                                enabledNodes.add(serviceNode);

                                logger.info("Enabling {}", serviceNode);
                                try {
                                    processScheduler.enableControllerService(serviceNode);
                                } catch (final Exception e) {
                                    logger.error("Failed to enable " + serviceNode + " due to " + e);
                                    if (logger.isDebugEnabled()) {
                                        logger.error("", e);
                                    }

                                    if (bulletinRepo != null) {
                                        bulletinRepo.addBulletin(BulletinFactory.createBulletin(
                                            "Controller Service", Severity.ERROR.name(), "Could not start " + serviceNode + " due to " + e));
                                    }
                                }
                            }

                            // wait for service to finish enabling.
                            while (ControllerServiceState.ENABLING.equals(serviceNode.getState())) {
                                try {
                                    Thread.sleep(100L);
                                } catch (final InterruptedException ie) {
                                }
                            }

                            logger.info("State for {} is now {}", serviceNode, serviceNode.getState());
                        } catch (final Exception e) {
                            logger.error("Failed to enable {} due to {}", serviceNode, e.toString());
                            if (logger.isDebugEnabled()) {
                                logger.error("", e);
                            }
                        }
                    }
                }
            };

            executor.submit(enableBranchRunnable);
        }

        executor.shutdown();
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
    public void disableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanDisable();
        processScheduler.disableControllerService(serviceNode);
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return node == null ? null : node.getProxiedControllerService();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return node == null ? false : ControllerServiceState.ENABLED == node.getState();
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return node == null ? false : ControllerServiceState.ENABLING == node.getState();
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String serviceIdentifier) {
        return controllerServices.get(serviceIdentifier);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> identifiers = new HashSet<>();
        for (final Map.Entry<String, ControllerServiceNode> entry : controllerServices.entrySet()) {
            Class<? extends ControllerService> c = entry.getValue().getProxiedControllerService().getClass();
            if (requireNonNull(serviceType).isAssignableFrom(entry.getValue().getProxiedControllerService().getClass())) {
                identifiers.add(entry.getKey());
            }
        }

        return identifiers;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? null : node.getName();
    }

    @Override
    public void removeControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = controllerServices.get(serviceNode.getIdentifier());
        if (existing == null || existing != serviceNode) {
            throw new IllegalStateException("Controller Service " + serviceNode + " does not exist in this Flow");
        }

        serviceNode.verifyCanDelete();

        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(serviceNode, this, null);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, serviceNode.getControllerServiceImplementation(), configurationContext);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : serviceNode.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode referencedNode = getControllerServiceNode(value);
                    if (referencedNode != null) {
                        referencedNode.removeReference(serviceNode);
                    }
                }
            }
        }

        controllerServices.remove(serviceNode.getIdentifier());

        stateManagerProvider.onComponentRemoved(serviceNode.getIdentifier());
    }

    @Override
    public Set<ControllerServiceNode> getAllControllerServices() {
        return new HashSet<>(controllerServices.values());
    }

    /**
     * Returns a List of all components that reference the given referencedNode (either directly or indirectly through another service) that are also of the given componentType. The list that is
     * returned is in the order in which they will need to be 'activated' (enabled/started).
     *
     * @param referencedNode node
     * @param componentType type
     * @return list of components
     */
    private <T> List<T> findRecursiveReferences(final ControllerServiceNode referencedNode, final Class<T> componentType) {
        final List<T> references = new ArrayList<>();

        for (final ConfiguredComponent referencingComponent : referencedNode.getReferences().getReferencingComponents()) {
            if (componentType.isAssignableFrom(referencingComponent.getClass())) {
                references.add(componentType.cast(referencingComponent));
            }

            if (referencingComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode referencingNode = (ControllerServiceNode) referencingComponent;

                // find components recursively that depend on referencingNode.
                final List<T> recursive = findRecursiveReferences(referencingNode, componentType);

                // For anything that depends on referencing node, we want to add it to the list, but we know
                // that it must come after the referencing node, so we first remove any existing occurrence.
                references.removeAll(recursive);
                references.addAll(recursive);
            }
        }

        return references;
    }

    @Override
    public void enableReferencingServices(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> recursiveReferences = findRecursiveReferences(serviceNode, ControllerServiceNode.class);
        enableReferencingServices(serviceNode, recursiveReferences);
    }

    private void enableReferencingServices(final ControllerServiceNode serviceNode, final List<ControllerServiceNode> recursiveReferences) {
        if (!serviceNode.isActive()) {
            serviceNode.verifyCanEnable(new HashSet<>(recursiveReferences));
        }

        final Set<ControllerServiceNode> ifEnabled = new HashSet<>();
        for (final ControllerServiceNode nodeToEnable : recursiveReferences) {
            if (!nodeToEnable.isActive()) {
                nodeToEnable.verifyCanEnable(ifEnabled);
                ifEnabled.add(nodeToEnable);
            }
        }

        for (final ControllerServiceNode nodeToEnable : recursiveReferences) {
            if (!nodeToEnable.isActive()) {
                enableControllerService(nodeToEnable);
            }
        }
    }

    @Override
    public void verifyCanEnableReferencingServices(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> referencingServices = findRecursiveReferences(serviceNode, ControllerServiceNode.class);
        final Set<ControllerServiceNode> referencingServiceSet = new HashSet<>(referencingServices);

        for (final ControllerServiceNode referencingService : referencingServices) {
            referencingService.verifyCanEnable(referencingServiceSet);
        }
    }

    @Override
    public void verifyCanScheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> referencingServices = findRecursiveReferences(serviceNode, ControllerServiceNode.class);
        final List<ReportingTaskNode> referencingReportingTasks = findRecursiveReferences(serviceNode, ReportingTaskNode.class);
        final List<ProcessorNode> referencingProcessors = findRecursiveReferences(serviceNode, ProcessorNode.class);

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
        // Get a list of all Controller Services that need to be disabled, in the order that they need to be
        // disabled.
        final List<ControllerServiceNode> toDisable = findRecursiveReferences(serviceNode, ControllerServiceNode.class);
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
}
