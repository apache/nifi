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
package org.apache.nifi.web.dao.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.dao.ProcessGroupDAO;

public class StandardProcessGroupDAO extends ComponentDAO implements ProcessGroupDAO {

    private FlowController flowController;

    @Override
    public ProcessGroup createProcessGroup(String parentGroupId, ProcessGroupDTO processGroup) {
        if (processGroup.getParentGroupId() != null && !flowController.areGroupsSame(processGroup.getParentGroupId(), parentGroupId)) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Process Group is being added.");
        }

        // get the parent group
        ProcessGroup parentGroup = locateProcessGroup(flowController, parentGroupId);

        // create the process group
        ProcessGroup group = flowController.createProcessGroup(processGroup.getId());
        group.setName(processGroup.getName());
        if (processGroup.getPosition() != null) {
            group.setPosition(new Position(processGroup.getPosition().getX(), processGroup.getPosition().getY()));
        }

        // add the process group
        group.setParent(parentGroup);
        parentGroup.addProcessGroup(group);

        return group;
    }

    @Override
    public boolean hasProcessGroup(String groupId) {
        return flowController.getGroup(groupId) != null;
    }

    @Override
    public void verifyUpdate(final ProcessGroupDTO processGroup) {
    }

    @Override
    public ProcessGroup getProcessGroup(String groupId) {
        return locateProcessGroup(flowController, groupId);
    }

    @Override
    public Set<ProcessGroup> getProcessGroups(String parentGroupId) {
        ProcessGroup group = locateProcessGroup(flowController, parentGroupId);
        return group.getProcessGroups();
    }

    @Override
    public void verifyScheduleComponents(final String groupId, final ScheduledState state,final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final Set<Connectable> connectables = new HashSet<>(componentIds.size());
        for (final String componentId : componentIds) {
            final Connectable connectable = group.findLocalConnectable(componentId);
            if (connectable == null) {
                throw new ResourceNotFoundException("Unable to find component with id " + componentId);
            }

            connectables.add(connectable);
        }

        // verify as appropriate
        connectables.forEach(connectable -> {
            if (ScheduledState.RUNNING.equals(state)) {
                group.verifyCanStart(connectable);
            } else {
                group.verifyCanStop(connectable);
            }
        });
    }

    @Override
    public void verifyActivateControllerServices(final String groupId, final ControllerServiceState state, final Set<String> serviceIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        group.findAllControllerServices().stream()
            .filter(service -> serviceIds.contains(service.getIdentifier()))
            .forEach(service -> {
                if (state == ControllerServiceState.ENABLED) {
                    service.verifyCanEnable();
                } else {
                    service.verifyCanDisable();
                }
            });
    }

    @Override
    public CompletableFuture<Void> scheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

        for (final String componentId : componentIds) {
            final Connectable connectable = group.findLocalConnectable(componentId);
            if (ScheduledState.RUNNING.equals(state)) {
                if (ConnectableType.PROCESSOR.equals(connectable.getConnectableType())) {
                    final CompletableFuture<?> processorFuture = connectable.getProcessGroup().startProcessor((ProcessorNode) connectable);
                    future = CompletableFuture.allOf(future, processorFuture);
                } else if (ConnectableType.INPUT_PORT.equals(connectable.getConnectableType())) {
                    connectable.getProcessGroup().startInputPort((Port) connectable);
                } else if (ConnectableType.OUTPUT_PORT.equals(connectable.getConnectableType())) {
                    connectable.getProcessGroup().startOutputPort((Port) connectable);
                }
            } else {
                if (ConnectableType.PROCESSOR.equals(connectable.getConnectableType())) {
                    final CompletableFuture<?> processorFuture = connectable.getProcessGroup().stopProcessor((ProcessorNode) connectable);
                    future = CompletableFuture.allOf(future, processorFuture);
                } else if (ConnectableType.INPUT_PORT.equals(connectable.getConnectableType())) {
                    connectable.getProcessGroup().stopInputPort((Port) connectable);
                } else if (ConnectableType.OUTPUT_PORT.equals(connectable.getConnectableType())) {
                    connectable.getProcessGroup().stopOutputPort((Port) connectable);
                }
            }
        }

        return future;
    }

    @Override
    public Future<Void> activateControllerServices(final String groupId, final ControllerServiceState state, final Set<String> serviceIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (final String serviceId : serviceIds) {
            final ControllerServiceNode serviceNode = group.findControllerService(serviceId);
            if (ControllerServiceState.ENABLED.equals(state)) {
                final CompletableFuture<Void> serviceFuture = flowController.enableControllerService(serviceNode);
                future = CompletableFuture.allOf(future, serviceFuture);
            } else {
                final CompletableFuture<Void> serviceFuture = flowController.disableControllerService(serviceNode);
                future = CompletableFuture.allOf(future, serviceFuture);
            }
        }

        return future;
    }

    @Override
    public ProcessGroup updateProcessGroup(ProcessGroupDTO processGroupDTO) {
        final ProcessGroup group = locateProcessGroup(flowController, processGroupDTO.getId());

        final String name = processGroupDTO.getName();
        final String comments = processGroupDTO.getComments();

        if (isNotNull(name)) {
            group.setName(name);
        }
        if (isNotNull(processGroupDTO.getPosition())) {
            group.setPosition(new Position(processGroupDTO.getPosition().getX(), processGroupDTO.getPosition().getY()));
        }
        if (isNotNull(comments)) {
            group.setComments(comments);
        }

        return group;
    }

    @Override
    public ProcessGroup updateVariableRegistry(final VariableRegistryDTO variableRegistry) {
        final ProcessGroup group = locateProcessGroup(flowController, variableRegistry.getProcessGroupId());
        if (group == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + variableRegistry.getProcessGroupId());
        }

        final Map<String, String> variableMap = new HashMap<>();
        variableRegistry.getVariables().stream() // have to use forEach here instead of using Collectors.toMap because value may be null
            .map(VariableEntity::getVariable)
            .forEach(var -> variableMap.put(var.getName(), var.getValue()));

        group.setVariables(variableMap);
        return group;
    }

    @Override
    public void verifyDelete(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.verifyCanDelete();
    }

    @Override
    public void deleteProcessGroup(String processGroupId) {
        // get the group
        ProcessGroup group = locateProcessGroup(flowController, processGroupId);
        ProcessGroup parentGroup = group.getParent();

        // ensure this isn't the root group
        if (parentGroup == null) {
            throw new IllegalArgumentException("The Root Group cannot be removed");
        }

        // remove the group
        parentGroup.removeProcessGroup(group);
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
