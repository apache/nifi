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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandaloneProcessGroupLifecycle implements ProcessGroupLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneProcessGroupLifecycle.class);

    private final ProcessGroup processGroup;
    private final ControllerServiceProvider controllerServiceProvider;
    private final StatelessGroupLifecycle statelessGroupLifecycle;
    private final Function<String, ProcessGroupLifecycle> childGroupLifecycleFactory;

    public StandaloneProcessGroupLifecycle(final ProcessGroup processGroup, final ControllerServiceProvider controllerServiceProvider,
                final StatelessGroupLifecycle statelessGroupLifecycle, final Function<String, ProcessGroupLifecycle> childGroupLifecycleFactory) {

        this.processGroup = processGroup;
        this.controllerServiceProvider = controllerServiceProvider;
        this.statelessGroupLifecycle = statelessGroupLifecycle;
        this.childGroupLifecycleFactory = childGroupLifecycleFactory;
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final ControllerServiceReferenceScope scope, final ControllerServiceReferenceHierarchy hierarchy) {
        final boolean recursive = (hierarchy == ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS);
        final Set<ControllerServiceNode> controllerServices = (scope == ControllerServiceReferenceScope.INCLUDE_ALL) ? processGroup.findAllControllerServices() : findReferencedServices(recursive);
        return enableControllerServices(controllerServices);
    }

    public Set<ControllerServiceNode> findReferencedServices(final boolean recursive) {
        final Set<ControllerServiceNode> referencedServices = new HashSet<>();
        collectReferencedServices(processGroup, referencedServices, recursive);
        return referencedServices;
    }

    private void collectReferencedServices(final ProcessGroup group, final Set<ControllerServiceNode> referencedServices, final boolean recursive) {
        for (final ProcessorNode processor : group.getProcessors()) {
            for (final PropertyDescriptor descriptor : processor.getPropertyDescriptors()) {
                if (descriptor.getControllerServiceDefinition() == null) {
                    continue;
                }

                final String serviceId = processor.getProperty(descriptor).getEffectiveValue(group.getParameterContext());
                if (serviceId == null) {
                    continue;
                }

                final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(serviceId);
                if (serviceNode == null) {
                    continue;
                }

                logger.debug("Marking {} as a Referenced Controller Service because it is referenced by {} property of {}",
                    serviceNode, descriptor.getName(), processor);
                referencedServices.add(serviceNode);
            }
        }

        while (true) {
            final Set<ControllerServiceNode> newlyAddedServices = new HashSet<>();
            for (final ControllerServiceNode service : referencedServices) {
                for (final PropertyDescriptor descriptor : service.getPropertyDescriptors()) {
                    if (descriptor.getControllerServiceDefinition() == null) {
                        continue;
                    }

                    final String serviceId = service.getProperty(descriptor).getEffectiveValue(group.getParameterContext());
                    if (serviceId == null) {
                        continue;
                    }

                    final ControllerServiceNode referencedService = controllerServiceProvider.getControllerServiceNode(serviceId);
                    if (referencedService != null && !referencedServices.contains(referencedService)) {
                        logger.debug("Marking {} as a Referenced Controller Service because it is referenced by {} property of {}",
                            referencedService, descriptor.getName(), service);

                        newlyAddedServices.add(referencedService);
                    }
                }
            }

            referencedServices.addAll(newlyAddedServices);
            if (newlyAddedServices.isEmpty()) {
                break;
            }
        }

        if (recursive) {
            for (final ProcessGroup childGroup : group.getProcessGroups()) {
                collectReferencedServices(childGroup, referencedServices, true);
            }
        }
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final Collection<String> collection) {
        final Set<ControllerServiceNode> serviceNodes = findControllerServices(collection);
        return enableControllerServices(serviceNodes);
    }

    private CompletableFuture<Void> enableControllerServices(final Set<ControllerServiceNode> serviceNodes) {
        if (serviceNodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // If any service is not currently valid, perform validation again to ensure that the status is up to date.
        for (final ControllerServiceNode serviceNode : serviceNodes) {
            final ValidationStatus validationStatus = serviceNode.getValidationStatus();
            if (validationStatus != ValidationStatus.VALID) {
                serviceNode.performValidation();
            }
        }

        return controllerServiceProvider.enableControllerServicesAsync(serviceNodes);
    }

    private Set<ControllerServiceNode> findControllerServices(final Collection<String> serviceIds) {
        return processGroup.findAllControllerServices().stream()
            .filter(service -> service.getVersionedComponentId().isPresent())
            .filter(service -> serviceIds.contains(service.getVersionedComponentId().get()))
            .collect(Collectors.toSet());
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final ControllerServiceReferenceHierarchy hierarchy) {
        final boolean recursive = (hierarchy == ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS);
        final Set<ControllerServiceNode> controllerServices = recursive ? processGroup.findAllControllerServices() : processGroup.getControllerServices(false);
        return disableControllerServices(controllerServices);
    }

    private CompletableFuture<Void> disableControllerServices(final Set<ControllerServiceNode> serviceNodes) {
        if (serviceNodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return controllerServiceProvider.disableControllerServicesAsync(serviceNodes);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final Collection<String> collection) {
        final Set<ControllerServiceNode> serviceNodes = findControllerServices(collection);
        return disableControllerServices(serviceNodes);
    }

    @Override
    public CompletableFuture<Void> startProcessors(final boolean recursive) {
        final Collection<ProcessorNode> processors = recursive ? processGroup.findAllProcessors() : processGroup.getProcessors();
        final List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        for (final ProcessorNode processor : processors) {
            // If Processor is not valid, perform validation again to ensure that the status is up to date.
            final ValidationStatus validationStatus = processor.getValidationStatus();
            if (validationStatus != ValidationStatus.VALID) {
                processor.performValidation();
            }

            if (processor.getScheduledState() == ScheduledState.DISABLED) {
                logger.debug("Not starting Processor {} because it is disabled", processor);
                continue;
            }

            startFutures.add(processor.getProcessGroup().startProcessor(processor, true));
        }

        return CompletableFuture.allOf(startFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> start(final ControllerServiceReferenceScope serviceReferenceScope) {
        if (processGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            return statelessGroupLifecycle.start();
        }

        final CompletableFuture<Void> result = new CompletableFuture<>();

        Thread.startVirtualThread(() -> {
            try {
                enableControllerServices(serviceReferenceScope, ControllerServiceReferenceHierarchy.DIRECT_SERVICES_ONLY).get();
                startPorts(false).get();
                startRemoteProcessGroups(false).get();
                startProcessors(false).get();

                final List<CompletableFuture<Void>> childGroupFutures = new ArrayList<>();
                for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
                    final String childGroupId = childGroup.getVersionedComponentId().orElse(null);
                    if (childGroupId == null) {
                        logger.warn("Encountered child Process Group {} without a Versioned Component ID. Skipping start of child group.", childGroup.getIdentifier());
                        continue;
                    }

                    final ProcessGroupLifecycle childLifecycle = childGroupLifecycleFactory.apply(childGroupId);
                    childGroupFutures.add(childLifecycle.start(serviceReferenceScope));
                }

                CompletableFuture.allOf(childGroupFutures.toArray(new CompletableFuture[0])).get();
                result.complete(null);
            } catch (final Exception e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    @Override
    public CompletableFuture<Void> startPorts(final boolean recursive) {
        logger.debug("{} starting all ports", this);
        final Collection<Port> inputPorts = recursive ? processGroup.findAllInputPorts() : processGroup.getInputPorts();
        for (final Port inputPort : inputPorts) {
            inputPort.getProcessGroup().startInputPort(inputPort);
        }

        final Collection<Port> outputPorts = recursive ? processGroup.findAllOutputPorts() : processGroup.getOutputPorts();
        for (final Port outputPort : outputPorts) {
            outputPort.getProcessGroup().startOutputPort(outputPort);
        }

        logger.info("{} started all ports", this);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopPorts(final boolean recursive) {
        logger.debug("{} stopping all ports", this);

        final Collection<Port> inputPorts = recursive ? processGroup.findAllInputPorts() : processGroup.getInputPorts();
        for (final Port inputPort : inputPorts) {
            inputPort.getProcessGroup().stopInputPort(inputPort);
        }

        final Collection<Port> outputPorts = recursive ? processGroup.findAllOutputPorts() : processGroup.getOutputPorts();
        for (final Port outputPort : outputPorts) {
            outputPort.getProcessGroup().stopOutputPort(outputPort);
        }

        logger.info("{} stopped all ports", this);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> startRemoteProcessGroups(final boolean recursive) {
        logger.debug("{} starting all Remote Process Groups", this);

        final Collection<RemoteProcessGroup> rpgs = recursive ? processGroup.findAllRemoteProcessGroups() : processGroup.getRemoteProcessGroups();
        for (final RemoteProcessGroup rpg : rpgs) {
            rpg.startTransmitting();
        }

        logger.info("{} started all Remote Process Groups", this);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopRemoteProcessGroups(final boolean recursive) {
        logger.debug("{} stopping all Remote Process Groups", this);
        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();

        final Collection<RemoteProcessGroup> rpgs = recursive ? processGroup.findAllRemoteProcessGroups() : processGroup.getRemoteProcessGroups();
        for (final RemoteProcessGroup rpg : rpgs) {
            stopFutures.add(rpg.stopTransmitting());
        }

        logger.info("{} stopped all Remote Process Groups", this);
        return CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> startStatelessGroups(final boolean recursive) {
        logger.debug("{} starting all Stateless Process Groups", this);
        final List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        final Collection<ProcessGroup> processGroups = processGroup.getProcessGroups();
        for (final ProcessGroup childGroup : processGroups) {
            final String childGroupId = childGroup.getVersionedComponentId().orElse(null);
            if (childGroupId == null) {
                logger.warn("Encountered stateless child Process Group {} without a Versioned Component ID. Skipping start.", childGroup.getIdentifier());
                continue;
            }

            final ProcessGroupLifecycle childLifecycle = childGroupLifecycleFactory.apply(childGroupId);

            if (childGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
                startFutures.add(childLifecycle.start(ControllerServiceReferenceScope.INCLUDE_REFERENCED_SERVICES_ONLY));
            } else if (recursive) {
                startFutures.add(childLifecycle.startStatelessGroups(true));
            }
        }

        logger.info("{} started all Stateless Process Groups", this);
        return CompletableFuture.allOf(startFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> stopStatelessGroups(final boolean recursive) {
        logger.debug("{} stopping all Stateless Process Groups", this);
        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();

        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            final String childGroupId = childGroup.getVersionedComponentId().orElse(null);
            if (childGroupId == null) {
                logger.warn("Encountered stateless child Process Group {} without a Versioned Component ID. Skipping stop.", childGroup.getIdentifier());
                continue;
            }

            final ProcessGroupLifecycle childLifecycle = childGroupLifecycleFactory.apply(childGroupId);

            if (childGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
                stopFutures.add(childLifecycle.stop());
            } else if (recursive) {
                stopFutures.add(childLifecycle.stopStatelessGroups(true));
            }
        }

        logger.info("{} stopped all Stateless Process Groups", this);
        return CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> stop() {
        logger.debug("Stopping Process Group {}", processGroup.getIdentifier());
        if (processGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            return statelessGroupLifecycle.stop();
        }

        final CompletableFuture<Void> result = new CompletableFuture<>();

        Thread.startVirtualThread(() -> {
            try {
                stopProcessors(false).get();
                stopChildren().get();
                stopPorts(false).get();
                stopRemoteProcessGroups(false).get();
                disableControllerServices(ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS).get();

                logger.info("Stopped Process Group {}", processGroup.getIdentifier());
                result.complete(null);
            } catch (final Exception e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    private CompletableFuture<Void> stopChildren() {
        final List<CompletableFuture<Void>> childGroupFutures = new ArrayList<>();
        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            final String childGroupId = childGroup.getVersionedComponentId().orElse(null);
            if (childGroupId == null) {
                logger.warn("Encountered child Process Group {} without a Versioned Component ID. Skipping stop of child group.", childGroup.getIdentifier());
                continue;
            }

            final ProcessGroupLifecycle childLifecycle = childGroupLifecycleFactory.apply(childGroupId);
            final CompletableFuture<Void> childFuture = childLifecycle.stop();
            childGroupFutures.add(childFuture);
        }

        return CompletableFuture.allOf(childGroupFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> stopProcessors(final boolean recursive) {
        final Collection<ProcessorNode> processors = recursive ? processGroup.findAllProcessors() : processGroup.getProcessors();
        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        for (final ProcessorNode processor : processors) {
            final ScheduledState processorState = processor.getScheduledState();
            if (processorState == ScheduledState.DISABLED || processorState == ScheduledState.STOPPED) {
                logger.debug("Not stopping Processor {} because its state is {}", processor, processorState);
                continue;
            }

            final CompletableFuture<Void> stopFuture = processor.getProcessGroup().stopProcessor(processor);
            stopFutures.add(stopFuture);
        }

        return CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public int getActiveThreadCount() {
        return getActiveThreadCount(processGroup);
    }

    private int getActiveThreadCount(final ProcessGroup group) {
        int total = 0;
        for (final ProcessorNode processor : group.getProcessors()) {
            total += processor.getActiveThreadCount();
        }
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            total += getActiveThreadCount(childGroup);
        }
        return total;
    }

    @Override
    public String toString() {
        return "StandaloneProcessGroupLifecycle[" +
            "processGroup=" + processGroup.getIdentifier() +
            "]";
    }
}
