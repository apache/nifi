/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StandardNarComponentManager implements NarComponentManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardNarComponentManager.class);

    private static final Duration COMPONENT_STOP_TIMEOUT = Duration.ofSeconds(30);

    private final FlowManager flowManager;
    private final ReloadComponent reloadComponent;
    private final ControllerServiceProvider controllerServiceProvider;

    public StandardNarComponentManager(final FlowController flowController) {
        this.flowManager = flowController.getFlowManager();
        this.reloadComponent = flowController.getReloadComponent();
        this.controllerServiceProvider = flowController.getControllerServiceProvider();
    }

    @Override
    public boolean componentsExist(final BundleCoordinate coordinate) {
        if (componentsExist(coordinate, flowManager.getAllControllerServices())) {
            return true;
        }
        if (componentsExist(coordinate, flowManager.getAllReportingTasks())) {
            return true;
        }
        if (componentsExist(coordinate, flowManager.getAllFlowRegistryClients())) {
            return true;
        }
        if (componentsExist(coordinate, flowManager.getAllFlowAnalysisRules())) {
            return true;
        }
        if (componentsExist(coordinate, flowManager.getAllParameterProviders())) {
            return true;
        }

        return flowManager.findAllProcessors(processorNode -> processorNode.getBundleCoordinate().equals(coordinate)).stream()
                .findAny()
                .isPresent();
    }

    private <T extends ComponentNode> boolean componentsExist(final BundleCoordinate coordinate, final Set<T> componentNodes) {
        return componentNodes.stream().anyMatch(componentNode -> componentNode.getBundleCoordinate().equals(coordinate));
    }

    @Override
    public void loadMissingComponents(final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final Set<ComponentNode> componentNodes = getComponentsForBundle(bundleCoordinate, (ComponentNode::isExtensionMissing));
        LOGGER.debug("Found {} missing components to load from NAR [{}]", componentNodes.size(), bundleCoordinate);
        componentNodes.forEach(componentNode -> {
            // ghosted components could have a scheduled state of RUNNING/DISABLED, so they need to be STOPPED/DISABLED before reloading
            stopComponent(componentNode, bundleCoordinate, stoppedComponents);
            reloadComponent(componentNode, bundleCoordinate);
        });
    }

    @Override
    public void unloadComponents(final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final Set<ComponentNode> componentNodes = getComponentsForBundle(bundleCoordinate, (componentNode -> !componentNode.isExtensionMissing()));
        LOGGER.debug("Found {} components to unload from deleted NAR [{}]", componentNodes.size(), bundleCoordinate);
        componentNodes.forEach(componentNode -> {
            stopComponent(componentNode, bundleCoordinate, stoppedComponents);
            reloadComponent(componentNode, bundleCoordinate);
        });
    }

    private void stopComponent(final ComponentNode componentNode, final BundleCoordinate bundleCoordinate, final StoppedComponents stoppedComponents) {
        final String componentId = componentNode.getIdentifier();
        final String componentType = componentNode.getCanonicalClassName();
        LOGGER.debug("Stopping component [{}] of type [{}] from bundle [{}]", componentId, componentType, bundleCoordinate);

        switch (componentNode) {
            case ProcessorNode processorNode -> stopProcessor(processorNode, stoppedComponents);
            case ControllerServiceNode controllerServiceNode -> stopControllerService(controllerServiceNode, stoppedComponents);
            case ReportingTaskNode reportingTaskNode -> stopReportingTask(reportingTaskNode, stoppedComponents);
            case FlowAnalysisRuleNode flowAnalysisRuleNode -> stopFlowAnalysisRule(flowAnalysisRuleNode, stoppedComponents);
            default -> LOGGER.warn("Component of type [{}] from NAR [{}] does not need to be stopped", componentType, bundleCoordinate);
        }
    }

    private void stopProcessor(final ProcessorNode processorNode, final StoppedComponents stoppedComponents) {
        if (!processorNode.isRunning() && processorNode.getPhysicalScheduledState() != ScheduledState.STARTING) {
            return;
        }

        final Future<Void> future = processorNode.getProcessGroup().stopProcessor(processorNode);
        stoppedComponents.addProcessor(processorNode);
        try {
            future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to stop processor [{}], processor will be terminated", processorNode.getIdentifier(), e);
            processorNode.terminate();
        }
    }

    private void stopControllerService(final ControllerServiceNode controllerServiceNode, final StoppedComponents stoppedComponents) {
        if (!controllerServiceNode.isActive()) {
            return;
        }

        // Unscheduled components that reference the current controller service
        final Map<ComponentNode, Future<Void>> futures = controllerServiceProvider.unscheduleReferencingComponents(controllerServiceNode);
        for (final Map.Entry<ComponentNode, Future<Void>> entry : futures.entrySet()) {
            final ComponentNode component = entry.getKey();
            switch (component) {
                case ProcessorNode processorNode -> stoppedComponents.addProcessor(processorNode);
                case ReportingTaskNode reportingTaskNode -> stoppedComponents.addReportingTask(reportingTaskNode);
                case FlowAnalysisRuleNode flowAnalysisRuleNode -> stoppedComponents.addFlowAnalysisRule(flowAnalysisRuleNode);
                default -> LOGGER.warn("Unexpected stopped component of type {} with ID {}}", component.getCanonicalClassName(), component.getIdentifier());
            }

            final Future<Void> future = entry.getValue();
            try {
                future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                LOGGER.warn("Failed to stop controller service [{}]", component.getIdentifier(), e);
            }
        }

        // Find other controller services that are enabled and reference the current controller service
        final List<ControllerServiceNode> referencingServices = controllerServiceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class).stream()
                .filter(ControllerServiceNode::isActive)
                .toList();

        // Disable the current service and the referencing services
        final Set<ControllerServiceNode> servicesToDisable = new HashSet<>();
        servicesToDisable.add(controllerServiceNode);
        servicesToDisable.addAll(referencingServices);

        final Future<Void> future = controllerServiceProvider.disableControllerServicesAsync(servicesToDisable);
        stoppedComponents.addAllControllerServices(servicesToDisable);
        try {
            future.get(COMPONENT_STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to disable controller service [{}], or one of it's referencing services", controllerServiceNode.getIdentifier(), e);
        }
    }

    private void stopReportingTask(final ReportingTaskNode reportingTaskNode, final StoppedComponents stoppedComponents) {
        if (!reportingTaskNode.isRunning()) {
            return;
        }
        reportingTaskNode.stop();
        stoppedComponents.addReportingTask(reportingTaskNode);
    }

    private void stopFlowAnalysisRule(final FlowAnalysisRuleNode flowAnalysisRuleNode, final StoppedComponents stoppedComponents) {
        if (!flowAnalysisRuleNode.isEnabled()) {
            return;
        }
        flowAnalysisRuleNode.disable();
        stoppedComponents.addFlowAnalysisRule(flowAnalysisRuleNode);
    }

    private <T extends ComponentNode> void reloadComponent(final T componentNode, final BundleCoordinate bundleCoordinate) {
        final String componentId = componentNode.getIdentifier();
        final String componentType = componentNode.getCanonicalClassName();
        final boolean isMissing = componentNode.isExtensionMissing();
        LOGGER.info("Reloading component [{}] of type [{}] from bundle [{}], isExtensionMissing = {}", componentId, componentType, bundleCoordinate, isMissing);

        componentNode.pauseValidationTrigger();
        try {
            switch (componentNode) {
                case ProcessorNode processorNode -> reloadComponent.reload(processorNode, componentType, bundleCoordinate, Collections.emptySet());
                case ControllerServiceNode controllerServiceNode -> reloadComponent.reload(controllerServiceNode, componentType, bundleCoordinate, Collections.emptySet());
                case ReportingTaskNode reportingTaskNode -> reloadComponent.reload(reportingTaskNode, componentType, bundleCoordinate, Collections.emptySet());
                case FlowRegistryClientNode flowRegistryClientNode -> reloadComponent.reload(flowRegistryClientNode, componentType, bundleCoordinate, Collections.emptySet());
                case FlowAnalysisRuleNode flowAnalysisRuleNode -> reloadComponent.reload(flowAnalysisRuleNode, componentType, bundleCoordinate, Collections.emptySet());
                case ParameterProviderNode parameterProviderNode -> reloadComponent.reload(parameterProviderNode, componentType, bundleCoordinate, Collections.emptySet());
                default -> LOGGER.warn("Component of type [{}] from NAR [{}] is not reloadable", componentType, bundleCoordinate);
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to reload component [{}] of type [{}] from NAR [{}]", componentNode.getComponent().getIdentifier(), componentType, bundleCoordinate, e);
        } finally {
            componentNode.resumeValidationTrigger();
        }
    }

    private Set<ComponentNode> getComponentsForBundle(final BundleCoordinate bundleCoordinate, final Predicate<ComponentNode> componentFilter) {
        final Set<ComponentNode> componentNodes = new HashSet<>();
        componentNodes.addAll(flowManager.findAllProcessors(processorNode -> componentFilter.test(processorNode) && isComponentFromBundle(processorNode, bundleCoordinate)));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllControllerServices(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllReportingTasks(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllFlowRegistryClients(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllFlowAnalysisRules(), bundleCoordinate, componentFilter));
        componentNodes.addAll(getComponentsForBundle(flowManager.getAllParameterProviders(), bundleCoordinate, componentFilter));
        return componentNodes;
    }

    private <T extends ComponentNode> Set<T> getComponentsForBundle(final Set<T> componentNodes, final BundleCoordinate coordinate, final Predicate<ComponentNode> componentFilter) {
        return componentNodes.stream()
                .filter(componentFilter)
                .filter(componentNode -> isComponentFromBundle(componentNode, coordinate))
                .collect(Collectors.toSet());
    }

    private <T extends ComponentNode> boolean isComponentFromBundle(final T componentNode, final BundleCoordinate coordinate) {
        if (componentNode.isExtensionMissing()) {
            final BundleCoordinate componentBundleCoordinate = componentNode.getBundleCoordinate();
            return componentBundleCoordinate.getGroup().equals(coordinate.getGroup())
                    && componentBundleCoordinate.getId().equals(coordinate.getId());
        } else {
            return componentNode.getBundleCoordinate().equals(coordinate);
        }
    }

}
