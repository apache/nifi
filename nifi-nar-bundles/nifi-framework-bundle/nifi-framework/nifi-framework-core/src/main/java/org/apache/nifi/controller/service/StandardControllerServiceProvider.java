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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StandardControllerServiceProvider implements ControllerServiceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceProvider.class);

    private final StandardProcessScheduler processScheduler;
    private final BulletinRepository bulletinRepo;
    private final FlowController flowController;
    private final FlowManager flowManager;

    private final ConcurrentMap<String, ControllerServiceNode> serviceCache = new ConcurrentHashMap<>();

    public StandardControllerServiceProvider(final FlowController flowController, final StandardProcessScheduler scheduler, final BulletinRepository bulletinRepo) {
        this.flowController = flowController;
        this.processScheduler = scheduler;
        this.bulletinRepo = bulletinRepo;
        this.flowManager = flowController.getFlowManager();
    }

    @Override
    public void onControllerServiceAdded(final ControllerServiceNode serviceNode) {
        serviceCache.putIfAbsent(serviceNode.getIdentifier(), serviceNode);
    }

    @Override
    public Set<ComponentNode> disableReferencingServices(final ControllerServiceNode serviceNode) {
        // Get a list of all Controller Services that need to be disabled, in the order that they need to be disabled.
        final List<ControllerServiceNode> toDisable = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);

        final Set<ControllerServiceNode> serviceSet = new HashSet<>(toDisable);

        final Set<ComponentNode> updated = new HashSet<>();
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
    public Set<ComponentNode> scheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = serviceNode.getReferences().findRecursiveReferences(ReportingTaskNode.class);

        final Set<ComponentNode> updated = new HashSet<>();

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
                node.getProcessGroup().startProcessor(node, true);
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
    public Set<ComponentNode> unscheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        // find all of the schedulable components (processors, reporting tasks) that refer to this Controller Service,
        // or a service that references this controller service, etc.
        final List<ProcessorNode> processors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);
        final List<ReportingTaskNode> reportingTasks = serviceNode.getReferences().findRecursiveReferences(ReportingTaskNode.class);

        final Set<ComponentNode> updated = new HashSet<>();

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
        serviceNode.reloadAdditionalResourcesIfNecessary();
        return processScheduler.enableControllerService(serviceNode);
    }

    @Override
    public void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes) {
        boolean shouldStart = true;

        Iterator<ControllerServiceNode> serviceIter = serviceNodes.iterator();
        while (serviceIter.hasNext() && shouldStart) {
            ControllerServiceNode controllerServiceNode = serviceIter.next();
            List<ControllerServiceNode> requiredServices = controllerServiceNode.getRequiredControllerServices();
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

    @Override
    public Future<Void> enableControllerServicesAsync(final Collection<ControllerServiceNode> serviceNodes) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        processScheduler.submitFrameworkTask(() -> {
            enableControllerServices(serviceNodes, future);
            future.complete(null);
        });

        return future;
    }

    private void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes, final CompletableFuture<Void> completableFuture) {
        // validate that we are able to start all of the services.
        for (final ControllerServiceNode controllerServiceNode : serviceNodes) {
            List<ControllerServiceNode> requiredServices = controllerServiceNode.getRequiredControllerServices();
            for (ControllerServiceNode requiredService : requiredServices) {
                if (!requiredService.isActive() && !serviceNodes.contains(requiredService)) {
                    logger.error("Cannot enable {} because it has a dependency on {}, which is not enabled", controllerServiceNode, requiredService);
                    completableFuture.completeExceptionally(new IllegalStateException("Cannot enable " + controllerServiceNode
                        + " because it has a dependency on " + requiredService + ", which is not enabled"));
                    return;
                }
            }
        }

        for (final ControllerServiceNode controllerServiceNode : serviceNodes) {
            if (completableFuture.isCancelled()) {
                return;
            }

            try {
                if (!controllerServiceNode.isActive()) {
                    final Future<Void> future = enableControllerServiceDependenciesFirst(controllerServiceNode);

                    while (true) {
                        try {
                            future.get(1, TimeUnit.SECONDS);
                            logger.debug("Successfully enabled {}; service state = {}", controllerServiceNode, controllerServiceNode.getState());
                            break;
                        } catch (final TimeoutException e) {
                            if (completableFuture.isCancelled()) {
                                return;
                            }
                        } catch (final Exception e) {
                            logger.warn("Failed to enable service {}", controllerServiceNode, e);
                            completableFuture.completeExceptionally(e);

                            if (this.bulletinRepo != null) {
                                this.bulletinRepo.addBulletin(BulletinFactory.createBulletin("Controller Service",
                                    Severity.ERROR.name(), "Could not enable " + controllerServiceNode + " due to " + e));
                            }

                            return;
                        }
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
            determineEnablingOrder(serviceNodeMap, node, branch, new HashSet<>());
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
    public Future<Void> disableControllerServicesAsync(final Collection<ControllerServiceNode> serviceNodes) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        processScheduler.submitFrameworkTask(() -> {
            disableControllerServices(serviceNodes, future);
            future.complete(null);
        });

        return future;
    }

    private void disableControllerServices(final Collection<ControllerServiceNode> serviceNodes, final CompletableFuture<Void> future) {
        final Set<ControllerServiceNode> serviceNodeSet = new HashSet<>(serviceNodes);

        // Verify that for each Controller Service given, any service that references it is either disabled or is also in the given collection
        for (final ControllerServiceNode serviceNode : serviceNodes) {
            final List<ControllerServiceNode> references = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
            for (final ControllerServiceNode reference : references) {
                if (reference.isActive()) {
                    try {
                        reference.verifyCanDisable(serviceNodeSet);
                    } catch (final Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            }
        }

        for (final ControllerServiceNode serviceNode : serviceNodes) {
            if (serviceNode.isActive()) {
                disableReferencingServices(serviceNode);
                final CompletableFuture<?> serviceFuture = disableControllerService(serviceNode);

                while (true) {
                    try {
                        serviceFuture.get(1, TimeUnit.SECONDS);
                        break;
                    } catch (final TimeoutException e) {
                        if (future.isCancelled()) {
                            return;
                        }

                        continue;
                    } catch (final Exception e) {
                        logger.error("Failed to disable {}", serviceNode, e);
                        future.completeExceptionally(e);
                    }
                }
            }
        }
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        final ControllerServiceNode node = getControllerServiceNode(serviceIdentifier);
        return node == null ? null : node.getProxiedControllerService();
    }

    private ProcessGroup getRootGroup() {
        return flowManager.getRootGroup();
    }

    @Override
    public ControllerService getControllerServiceForComponent(final String serviceIdentifier, final String componentId) {
        // Find the Process Group that owns the component.
        ProcessGroup groupOfInterest;

        final ProcessorNode procNode = flowManager.getProcessorNode(componentId);
        if (procNode == null) {
            final ControllerServiceNode serviceNode = getControllerServiceNode(componentId);
            if (serviceNode == null) {
                final ReportingTaskNode taskNode = flowController.getReportingTaskNode(componentId);
                if (taskNode == null) {
                    throw new IllegalStateException("Could not find any Processor, Reporting Task, or Controller Service with identifier " + componentId);
                }

                // we have confirmed that the component is a reporting task. We can only reference Controller Services
                // that are scoped at the FlowController level in this case.
                final ControllerServiceNode rootServiceNode = flowManager.getRootControllerService(serviceIdentifier);
                return (rootServiceNode == null) ? null : rootServiceNode.getProxiedControllerService();
            } else {
                groupOfInterest = serviceNode.getProcessGroup();
            }
        } else {
            groupOfInterest = procNode.getProcessGroup();
        }

        if (groupOfInterest == null) {
            final ControllerServiceNode rootServiceNode = flowManager.getRootControllerService(serviceIdentifier);
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
        final ControllerServiceNode rootServiceNode = flowManager.getRootControllerService(serviceIdentifier);
        if (rootServiceNode != null) {
            return rootServiceNode;
        }

        return serviceCache.get(serviceIdentifier);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType, final String groupId) {
        final Set<ControllerServiceNode> serviceNodes;
        if (groupId == null) {
            serviceNodes = flowManager.getRootControllerServices();
        } else {
            ProcessGroup group = getRootGroup();
            if (!FlowManager.ROOT_GROUP_ID_ALIAS.equals(groupId) && !group.getIdentifier().equals(groupId)) {
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
            flowManager.removeRootControllerService(serviceNode);
            return;
        }

        group.removeControllerService(serviceNode);
        LogRepositoryFactory.removeRepository(serviceNode.getIdentifier());
        final ExtensionManager extensionManager = flowController.getExtensionManager();
        extensionManager.removeInstanceClassLoader(serviceNode.getIdentifier());
        serviceCache.remove(serviceNode.getIdentifier());
    }

    @Override
    public Collection<ControllerServiceNode> getNonRootControllerServices() {
        return serviceCache.values();
    }


    @Override
    public Set<ComponentNode> enableReferencingServices(final ControllerServiceNode serviceNode) {
        final List<ControllerServiceNode> recursiveReferences = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
        logger.debug("Enabling the following Referencing Services for {}: {}", serviceNode, recursiveReferences);
        return enableReferencingServices(serviceNode, recursiveReferences);
    }

    private Set<ComponentNode> enableReferencingServices(final ControllerServiceNode serviceNode, final List<ControllerServiceNode> recursiveReferences) {
        if (!serviceNode.isActive()) {
            serviceNode.verifyCanEnable(new HashSet<>(recursiveReferences));
        }

        final Set<ComponentNode> updated = new HashSet<>();

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
                final Future<?> enableFuture = enableControllerService(nodeToEnable);
                try {
                    enableFuture.get();
                } catch (final ExecutionException ee) {
                    throw new IllegalStateException("Failed to enable Controller Service " + nodeToEnable, ee.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while enabling Controller Service");
                }

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

    @Override
    public ExtensionManager getExtensionManager() {
        return flowController.getExtensionManager();
    }
}
