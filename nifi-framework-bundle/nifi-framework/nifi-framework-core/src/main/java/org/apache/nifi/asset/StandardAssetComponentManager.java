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

package org.apache.nifi.asset;

import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StandardAssetComponentManager implements AssetComponentManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardAssetComponentManager.class);
    private static final Duration COMPONENT_WAIT_TIMEOUT = Duration.ofSeconds(30);

    private final FlowManager flowManager;
    private final ControllerServiceProvider controllerServiceProvider;

    public StandardAssetComponentManager(final FlowController flowController) {
        this.flowManager = flowController.getFlowManager();
        this.controllerServiceProvider = flowController.getControllerServiceProvider();
    }

    @Override
    public void restartReferencingComponentsAsync(final Asset asset) {
        final String assetName = asset.getName();
        final String paramContextId = asset.getParameterContextIdentifier();
        Thread.ofVirtual().name("Restart Components for Asset [%s] in ParameterContext [%s]".formatted(assetName, paramContextId)).start(() -> {
            try {
                restartReferencingComponents(asset);
            } catch (final Exception e) {
                logger.error("Failed to restart referencing components for Asset [{}] in ParameterContext [{}]", assetName, paramContextId);
            }
        });
    }

    @Override
    public void restartReferencingComponents(final Asset asset) {
        final ParameterContextManager parameterContextManager = flowManager.getParameterContextManager();
        final ParameterContext parameterContext = parameterContextManager.getParameterContext(asset.getParameterContextIdentifier());

        // Determine which parameters reference the given asset
        final Set<Parameter> parametersReferencingAsset = getParametersReferencingAsset(parameterContext, asset);
        if (parametersReferencingAsset.isEmpty()) {
            logger.info("Asset [{}] is not referenced by any parameters in ParameterContext [{}] ", asset.getName(), parameterContext.getIdentifier());
            return;
        }

        // Determine processors, controller services, and stateless PGs that reference a parameter that references the asset
        final Set<ProcessorNode> processorsReferencingParameters = getProcessorsReferencingParameters(parameterContext, parametersReferencingAsset);
        final Set<ControllerServiceNode> controllerServicesReferencingParameters = getControllerServicesReferencingParameters(parameterContext, parametersReferencingAsset);
        final Set<ProcessGroup> affectedStatelessGroups = getAffectedStatelessGroups(parameterContext);

        logger.info("Found {} stateless groups, {} processors, and {} controller services referencing Asset [{}]", affectedStatelessGroups.size(),
                processorsReferencingParameters.size(), controllerServicesReferencingParameters.size(), asset.getName());

        // Stop/disable the impacted components
        final Set<ProcessGroup> stoppedStatelessProcessGroups = new HashSet<>();
        affectedStatelessGroups.forEach(processGroup -> stopProcessGroup(processGroup, stoppedStatelessProcessGroups));

        final Set<ProcessorNode> stoppedProcessors = new HashSet<>();
        processorsReferencingParameters.forEach(processorNode -> stopProcessors(processorNode, stoppedProcessors));

        final Set<ControllerServiceNode> disabledControllerServices = new HashSet<>();
        controllerServicesReferencingParameters.forEach(controllerServiceNode -> disableControllerService(controllerServiceNode, disabledControllerServices, stoppedProcessors));

        // Start/enable the components that were previously stopped/disabled
        enableControllerServices(disabledControllerServices);
        stoppedProcessors.forEach(this::startProcessor);
        stoppedStatelessProcessGroups.forEach(ProcessGroup::startProcessing);
    }

    private void startProcessor(final ProcessorNode processorNode) {
        try {
            final Future<Void> future  = processorNode.getProcessGroup().startProcessor(processorNode, false);
            future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to start processor within the given time period", e);
        }
    }

    private void enableControllerServices(final Set<ControllerServiceNode> controllerServices) {
        try {
            final Future<Void> future  = controllerServiceProvider.enableControllerServicesAsync(controllerServices);
            future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to enable controller services within the given time period", e);
        }
    }

    private void stopProcessors(final ProcessorNode processorNode, final Set<ProcessorNode> stoppedProcessors) {
        if (!processorNode.isRunning() && processorNode.getPhysicalScheduledState() != ScheduledState.STARTING) {
            return;
        }

        try {
            final Future<Void> future = processorNode.getProcessGroup().stopProcessor(processorNode);
            stoppedProcessors.add(processorNode);
            future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to stop processor [{}], processor will be terminated", processorNode.getIdentifier(), e);
            processorNode.terminate();
        }
    }

    private void disableControllerService(final ControllerServiceNode controllerServiceNode, final Set<ControllerServiceNode> disabledControllerServices,
                                          final Set<ProcessorNode> stoppedProcessors) {
        if (!controllerServiceNode.isActive()) {
            return;
        }

        // Unscheduled components that reference the current controller service
        final Map<ComponentNode, Future<Void>> futures = controllerServiceProvider.unscheduleReferencingComponents(controllerServiceNode);
        for (final Map.Entry<ComponentNode, Future<Void>> entry : futures.entrySet()) {
            final ComponentNode component = entry.getKey();
            if (component instanceof ProcessorNode processorNode) {
                stoppedProcessors.add(processorNode);
            } else {
                logger.warn("Unexpected stopped component of type {} with ID {}}", component.getCanonicalClassName(), component.getIdentifier());
            }

            try {
                final Future<Void> future = entry.getValue();
                future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                logger.warn("Failed to unschedule referencing component [{}]", component.getIdentifier(), e);
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

        try {
            final Future<Void> future = controllerServiceProvider.disableControllerServicesAsync(servicesToDisable);
            disabledControllerServices.addAll(servicesToDisable);
            future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to disable controller service [{}], or one of it's referencing services", controllerServiceNode.getIdentifier(), e);
        }
    }

    private void stopProcessGroup(final ProcessGroup processGroup, final Set<ProcessGroup> stoppedProcessGroups) {
        if (!processGroup.isStatelessActive()) {
            return;
        }

        try {
            final Future<Void> future = processGroup.stopProcessing();
            stoppedProcessGroups.add(processGroup);
            future.get(COMPONENT_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to stop process group [{}]", processGroup.getIdentifier(), e);
        }
    }

    private Set<ProcessorNode> getProcessorsReferencingParameters(final ParameterContext parameterContext, final Set<Parameter> parameters) {
        final ParameterReferenceManager parameterReferenceManager = parameterContext.getParameterReferenceManager();
        final Set<ProcessorNode> allReferencingProcessors = new HashSet<>();
        for (final Parameter parameter : parameters) {
            final String parameterName = parameter.getDescriptor().getName();
            final Set<ProcessorNode> referencingProcessors = parameterReferenceManager.getProcessorsReferencing(parameterContext, parameterName);
            allReferencingProcessors.addAll(referencingProcessors);
        }
        return allReferencingProcessors;
    }

    private Set<ControllerServiceNode> getControllerServicesReferencingParameters(final ParameterContext parameterContext, final Set<Parameter> parameters) {
        final ParameterReferenceManager parameterReferenceManager = parameterContext.getParameterReferenceManager();

        final Set<ControllerServiceNode> allReferencingServices = new HashSet<>();
        for (final Parameter parameter : parameters) {
            final String parameterName = parameter.getDescriptor().getName();
            final Set<ControllerServiceNode> referencingServices = parameterReferenceManager.getControllerServicesReferencing(parameterContext, parameterName);
            allReferencingServices.addAll(referencingServices);
        }
        return allReferencingServices;
    }

    private Set<ProcessGroup> getAffectedStatelessGroups(final ParameterContext parameterContext) {
        final ParameterReferenceManager parameterReferenceManager = parameterContext.getParameterReferenceManager();
        final Set<ProcessGroup> boundProcessGroups = parameterReferenceManager.getProcessGroupsBound(parameterContext);

        final Set<ProcessGroup> affectedStatelessGroups = new HashSet<>();
        for (final ProcessGroup processGroup : boundProcessGroups) {
            final ProcessGroup statelessGroup = getStatelessParent(processGroup);
            if (statelessGroup != null && statelessGroup.isStatelessActive()) {
                affectedStatelessGroups.add(statelessGroup);
            }
        }
        return affectedStatelessGroups;
    }

    private ProcessGroup getStatelessParent(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final ExecutionEngine engine = group.getExecutionEngine();
        if (engine == ExecutionEngine.STATELESS) {
            return group;
        }

        return getStatelessParent(group.getParent());
    }

    private Set<Parameter> getParametersReferencingAsset(final ParameterContext parameterContext, final Asset asset) {
        final Set<Parameter> referencingParameters = new HashSet<>();
        for (final Parameter parameter : parameterContext.getParameters().values()) {
            for (final Asset parameterAsset : parameter.getReferencedAssets()) {
                if (parameterAsset.getIdentifier().equals(asset.getIdentifier())) {
                    referencingParameters.add(parameter);
                }
            }
        }
        return referencingParameters;
    }

}
