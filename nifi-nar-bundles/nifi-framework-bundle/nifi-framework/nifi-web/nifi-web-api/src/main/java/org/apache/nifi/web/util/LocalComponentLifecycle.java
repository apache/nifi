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

package org.apache.nifi.web.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LocalComponentLifecycle implements ComponentLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(LocalComponentLifecycle.class);

    private NiFiServiceFacade serviceFacade;
    private RevisionManager revisionManager;
    private DtoFactory dtoFactory;

    @Override
    public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components,
        final ScheduledState desiredState, final Pause pause) throws LifecycleManagementException {

        final Map<String, Revision> processorRevisions = components.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, entity -> revisionManager.getRevision(entity.getId())));

        final Map<String, AffectedComponentEntity> affectedComponentMap = components.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));

        if (desiredState == ScheduledState.RUNNING) {
            startComponents(groupId, processorRevisions, affectedComponentMap, pause);
        } else {
            stopComponents(groupId, processorRevisions, affectedComponentMap, pause);
        }

        final Set<AffectedComponentEntity> updatedEntities = components.stream()
            .map(component -> AffectedComponentUtils.updateEntity(component, serviceFacade, dtoFactory))
            .collect(Collectors.toSet());
        return updatedEntities;
    }

    @Override
    public Set<AffectedComponentEntity> activateControllerServices(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> services,
        final ControllerServiceState desiredState, final Pause pause) throws LifecycleManagementException {

        final Map<String, Revision> serviceRevisions = services.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, entity -> revisionManager.getRevision(entity.getId())));

        final Map<String, AffectedComponentEntity> affectedServiceMap = services.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));

        if (desiredState == ControllerServiceState.ENABLED) {
            enableControllerServices(groupId, serviceRevisions, affectedServiceMap, pause);
        } else {
            disableControllerServices(groupId, serviceRevisions, affectedServiceMap, pause);
        }

        return services.stream()
            .map(componentEntity -> serviceFacade.getControllerService(componentEntity.getId()))
            .map(dtoFactory::createAffectedComponentEntity)
            .collect(Collectors.toSet());
    }


    private void startComponents(final String processGroupId, final Map<String, Revision> componentRevisions, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause) {

        if (componentRevisions.isEmpty()) {
            return;
        }

        logger.debug("Starting components with ID's {} from Process Group {}", componentRevisions.keySet(), processGroupId);

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.RUNNING, componentRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.RUNNING, componentRevisions);

        // wait for all of the Processors to reach the desired state. We don't have to wait for other components because
        // Local and Remote Ports as well as funnels start immediately.
        waitForProcessorState(processGroupId, affectedComponents, ScheduledState.RUNNING, pause);
    }

    private void stopComponents(final String processGroupId, final Map<String, Revision> componentRevisions, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause) {

        if (componentRevisions.isEmpty()) {
            return;
        }

        logger.debug("Stopping components with ID's {} from Process Group {}", componentRevisions.keySet(), processGroupId);

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.STOPPED, componentRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.STOPPED, componentRevisions);

        // wait for all of the Processors to reach the desired state. We don't have to wait for other components because
        // Local and Remote Ports as well as funnels stop immediately.
        waitForProcessorState(processGroupId, affectedComponents, ScheduledState.STOPPED, pause);
    }

    /**
     * Waits for all of the given Processors to reach the given Scheduled State.
     *
     * @return <code>true</code> if all processors have reached the desired state, false if the given {@link Pause} indicates
     *         to give up before all of the processors have reached the desired state
     */
    private boolean waitForProcessorState(final String groupId, final Map<String, AffectedComponentEntity> affectedComponents,
        final ScheduledState desiredState, final Pause pause) {

        logger.debug("Waiting for {} processors to transition their states to {}", affectedComponents.size(), desiredState);

        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ProcessorEntity> processorEntities = serviceFacade.getProcessors(groupId, true);

            if (isProcessorActionComplete(processorEntities, affectedComponents, desiredState)) {
                logger.debug("All {} processors of interest now have the desired state of {}", affectedComponents.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private boolean isProcessorActionComplete(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents, final ScheduledState desiredState) {

        final String desiredStateName = desiredState.name();

        // update the affected processors
        processorEntities.stream()
            .filter(entity -> affectedComponents.containsKey(entity.getId()))
            .forEach(entity -> {
                final AffectedComponentEntity affectedComponentEntity = affectedComponents.get(entity.getId());
                affectedComponentEntity.setRevision(entity.getRevision());

                // only consider updating this component if the user had permissions to it
                if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                    final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                    affectedComponent.setState(entity.getStatus().getAggregateSnapshot().getRunStatus());
                    affectedComponent.setActiveThreadCount(entity.getStatus().getAggregateSnapshot().getActiveThreadCount());

                    if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                        affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                    }
                }
            });

        final boolean allProcessorsMatch = processorEntities.stream()
            .filter(entity -> affectedComponents.containsKey(entity.getId()))
            .allMatch(entity -> {
                final ProcessorStatusDTO status = entity.getStatus();

                final String runStatus = status.getAggregateSnapshot().getRunStatus();
                final boolean stateMatches = desiredStateName.equalsIgnoreCase(runStatus);
                if (!stateMatches) {
                    return false;
                }

                if (desiredState == ScheduledState.STOPPED && status.getAggregateSnapshot().getActiveThreadCount() != 0) {
                    return false;
                }

                return true;
            });

        if (!allProcessorsMatch) {
            return false;
        }

        return true;
    }


    private void enableControllerServices(final String processGroupId, final Map<String, Revision> serviceRevisions, final Map<String, AffectedComponentEntity> affectedServices, final Pause pause) {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        logger.debug("Enabling Controller Services with ID's {} from Process Group {}", serviceRevisions.keySet(), processGroupId);

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.ENABLED, affectedServices.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.ENABLED, serviceRevisions);
        waitForControllerServiceState(processGroupId, affectedServices, ControllerServiceState.ENABLED, pause);
    }

    private void disableControllerServices(final String processGroupId, final Map<String, Revision> serviceRevisions, final Map<String, AffectedComponentEntity> affectedServices, final Pause pause) {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        logger.debug("Disabling Controller Services with ID's {} from Process Group {}", serviceRevisions.keySet(), processGroupId);

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.DISABLED, affectedServices.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.DISABLED, serviceRevisions);
        waitForControllerServiceState(processGroupId, affectedServices, ControllerServiceState.DISABLED, pause);
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


    /**
     * Periodically polls the process group with the given ID, waiting for all controller services whose ID's are given to have the given Controller Service State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param affectedServices all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForControllerServiceState(final String groupId, final Map<String, AffectedComponentEntity> affectedServices, final ControllerServiceState desiredState, final Pause pause) {

        logger.debug("Waiting for {} Controller Services to transition their states to {}", affectedServices.size(), desiredState);

        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ControllerServiceEntity> serviceEntities = serviceFacade.getControllerServices(groupId, false, true);

            // update the affected controller services
            updateAffectedControllerServices(serviceEntities, affectedServices);

            final String desiredStateName = desiredState.name();
            final boolean allServicesMatch = serviceEntities.stream()
                .map(entity -> entity.getComponent())
                .filter(service -> affectedServices.containsKey(service.getId()))
                .map(service -> service.getState())
                .allMatch(state -> desiredStateName.equals(state));


            if (allServicesMatch) {
                logger.debug("All {} controller services of interest now have the desired state of {}", affectedServices.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }


    /**
     * Updates the affected controller services in the specified updateRequest with the serviceEntities.
     *
     * @param serviceEntities service entities
     * @param affectedServices all Controller Services whose state should be equal to the given desired state
     */
    private void updateAffectedControllerServices(final Set<ControllerServiceEntity> serviceEntities, final Map<String, AffectedComponentEntity> affectedServices) {
        // update the affected components
        serviceEntities.stream()
            .filter(entity -> affectedServices.containsKey(entity.getId()))
            .forEach(entity -> {
                final AffectedComponentEntity affectedComponentEntity = affectedServices.get(entity.getId());
                affectedComponentEntity.setRevision(entity.getRevision());

                // only consider update this component if the user had permissions to it
                if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                    final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                    affectedComponent.setState(entity.getComponent().getState());

                    if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                        affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                    }
                }
            });
    }


    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }
}
