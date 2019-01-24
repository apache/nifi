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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.dao.ProcessGroupDAO;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class StandardProcessGroupDAO extends ComponentDAO implements ProcessGroupDAO {

    private FlowController flowController;

    @Override
    public ProcessGroup createProcessGroup(String parentGroupId, ProcessGroupDTO processGroup) {
        final FlowManager flowManager = flowController.getFlowManager();
        if (processGroup.getParentGroupId() != null && !flowManager.areGroupsSame(processGroup.getParentGroupId(), parentGroupId)) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Process Group is being added.");
        }

        // get the parent group
        ProcessGroup parentGroup = locateProcessGroup(flowController, parentGroupId);

        // create the process group
        ProcessGroup group = flowManager.createProcessGroup(processGroup.getId());
        if (processGroup.getName() != null) {
            group.setName(processGroup.getName());
        }
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
        return flowController.getFlowManager().getGroup(groupId) != null;
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

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);
            if (connectable == null) {
                throw new ResourceNotFoundException("Unable to find component with id " + componentId);
            }

            if (connectable  instanceof RemoteGroupPort) {
                final RemoteGroupPort remotePort = (RemoteGroupPort) connectable;

                if (ScheduledState.RUNNING.equals(state)) {
                    remotePort.verifyCanStart();
                } else {
                    remotePort.verifyCanStop();
                }

                continue;
            }

            // verify as appropriate
            if (ScheduledState.RUNNING.equals(state)) {
                group.verifyCanStart(connectable);
            } else {
                group.verifyCanStop(connectable);
            }
        }
    }

    @Override
    public void verifyEnableComponents(String groupId, ScheduledState state, Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);
            if (ScheduledState.STOPPED.equals(state)) {
                connectable.verifyCanEnable();
            } else if (ScheduledState.DISABLED.equals(state)) {
                connectable.verifyCanDisable();
            }
        }
    }

    @Override
    public void verifyActivateControllerServices(final ControllerServiceState state, final Collection<String> serviceIds) {
        final FlowManager flowManager = flowController.getFlowManager();

        final Set<ControllerServiceNode> serviceNodes = serviceIds.stream()
            .map(flowManager::getControllerServiceNode)
            .collect(Collectors.toSet());

        for (final ControllerServiceNode serviceNode : serviceNodes) {
            if (state == ControllerServiceState.ENABLED) {
                serviceNode.verifyCanEnable(serviceNodes);
            } else {
                serviceNode.verifyCanDisable(serviceNodes);
            }
        }
    }

    private Connectable findConnectable(final String componentId, final String groupId, final Set<ProcessGroup> validProcessGroups) {
        // Get the component with the given ID and ensure that it belongs to the group that we are looking for.
        // We do this, rather than calling ProcessGroup.findLocalConnectable because for any component that is buried several
        // layers of Process Groups deep, that method becomes quite a bit more expensive than this method, due to all of the
        // Read Locks that must be obtained while recursing through the Process Group's descendant groups.
        final Connectable connectable = flowController.getFlowManager().findConnectable(componentId);
        if (connectable == null) {
            throw new ResourceNotFoundException("Could not find Component with ID " + componentId);
        }

        final ProcessGroup connectableGroup = connectable.getProcessGroup();
        if (!validProcessGroups.contains(connectableGroup)) {
            throw new ResourceNotFoundException("Component with ID " + componentId + " does not belong to Process Group " + groupId + " or any of its descendent groups");
        }

        return connectable;
    }

    @Override
    public Future<Void> scheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);

            if (ScheduledState.RUNNING.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        final CompletableFuture<?> processorFuture = connectable.getProcessGroup().startProcessor((ProcessorNode) connectable, true);
                        future = CompletableFuture.allOf(future, processorFuture);
                        break;
                    case INPUT_PORT:
                        connectable.getProcessGroup().startInputPort((Port) connectable);
                        break;
                    case OUTPUT_PORT:
                        connectable.getProcessGroup().startOutputPort((Port) connectable);
                        break;
                    case REMOTE_INPUT_PORT:
                    case REMOTE_OUTPUT_PORT:
                        final RemoteGroupPort remotePort = (RemoteGroupPort) connectable;
                        remotePort.getRemoteProcessGroup().startTransmitting(remotePort);
                        break;
                }
            } else if (ScheduledState.STOPPED.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        final CompletableFuture<?> processorFuture = connectable.getProcessGroup().stopProcessor((ProcessorNode) connectable);
                        future = CompletableFuture.allOf(future, processorFuture);
                        break;
                    case INPUT_PORT:
                        connectable.getProcessGroup().stopInputPort((Port) connectable);
                        break;
                    case OUTPUT_PORT:
                        connectable.getProcessGroup().stopOutputPort((Port) connectable);
                        break;
                    case REMOTE_INPUT_PORT:
                    case REMOTE_OUTPUT_PORT:
                        final RemoteGroupPort remotePort = (RemoteGroupPort) connectable;
                        remotePort.getRemoteProcessGroup().stopTransmitting(remotePort);
                        break;
                }
            }
        }

        return future;
    }

    @Override
    public void enableComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);

            if (ScheduledState.STOPPED.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        connectable.getProcessGroup().enableProcessor((ProcessorNode) connectable);
                        break;
                    case INPUT_PORT:
                        connectable.getProcessGroup().enableInputPort((Port) connectable);
                        break;
                    case OUTPUT_PORT:
                        connectable.getProcessGroup().enableOutputPort((Port) connectable);
                        break;
                }
            } else if (ScheduledState.DISABLED.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        connectable.getProcessGroup().disableProcessor((ProcessorNode) connectable);
                        break;
                    case INPUT_PORT:
                        connectable.getProcessGroup().disableInputPort((Port) connectable);
                        break;
                    case OUTPUT_PORT:
                        connectable.getProcessGroup().disableOutputPort((Port) connectable);
                        break;
                }
            }
        }
    }

    @Override
    public Future<Void> activateControllerServices(final String groupId, final ControllerServiceState state, final Collection<String> serviceIds) {
        final FlowManager flowManager = flowController.getFlowManager();
        final List<ControllerServiceNode> serviceNodes = serviceIds.stream()
            .map(flowManager::getControllerServiceNode)
            .collect(Collectors.toList());

        if (state == ControllerServiceState.ENABLED) {
            return flowController.getControllerServiceProvider().enableControllerServicesAsync(serviceNodes);
        } else {
            return flowController.getControllerServiceProvider().disableControllerServicesAsync(serviceNodes);
        }
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
            final ProcessGroup parent = group.getParent();
            if (parent != null) {
                parent.onComponentModified();
            }
        }
        if (isNotNull(comments)) {
            group.setComments(comments);
        }

        group.onComponentModified();
        return group;
    }

    @Override
    public ProcessGroup updateVersionControlInformation(final VersionControlInformationDTO versionControlInformation, final Map<String, String> versionedComponentMapping) {
        final String groupId = versionControlInformation.getGroupId();
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final String registryId = versionControlInformation.getRegistryId();
        final FlowRegistry flowRegistry = flowController.getFlowRegistryClient().getFlowRegistry(registryId);
        final String registryName = flowRegistry == null ? registryId : flowRegistry.getName();

        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(flowController.getExtensionManager());
        final VersionedProcessGroup flowSnapshot = mapper.mapProcessGroup(group, flowController.getControllerServiceProvider(), flowController.getFlowRegistryClient(), false);

        final StandardVersionControlInformation vci = StandardVersionControlInformation.Builder.fromDto(versionControlInformation)
            .registryName(registryName)
            .flowSnapshot(flowSnapshot)
            .build();

        group.setVersionControlInformation(vci, versionedComponentMapping);
        group.onComponentModified();

        return group;
    }

    @Override
    public ProcessGroup disconnectVersionControl(final String groupId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.disconnectVersionControl(true);
        group.onComponentModified();
        return group;
    }

    @Override
    public ProcessGroup updateProcessGroupFlow(final String groupId, final VersionedFlowSnapshot proposedSnapshot, final VersionControlInformationDTO versionControlInformation,
                                               final String componentIdSeed, final boolean verifyNotModified, final boolean updateSettings, final boolean updateDescendantVersionedFlows) {

        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.updateFlow(proposedSnapshot, componentIdSeed, verifyNotModified, updateSettings, updateDescendantVersionedFlows);
        group.findAllRemoteProcessGroups().stream().forEach(RemoteProcessGroup::initialize);

        final StandardVersionControlInformation svci = StandardVersionControlInformation.Builder.fromDto(versionControlInformation)
            .flowSnapshot(proposedSnapshot.getFlowContents())
            .build();

        group.setVersionControlInformation(svci, Collections.emptyMap());
        group.onComponentModified();

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
        group.onComponentModified();
        return group;
    }

    @Override
    public void verifyDelete(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.verifyCanDelete();
    }

    @Override
    public void verifyDeleteFlowRegistry(String registryId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();

        final VersionControlInformation versionControlInformation = rootGroup.getVersionControlInformation();
        if (versionControlInformation != null && versionControlInformation.getRegistryIdentifier().equals(registryId)) {
            throw new IllegalStateException("The Registry cannot be removed because a Process Group currently under version control is tracking to it.");
        }

        final Set<VersionControlInformation> trackedVersionControlInformation = rootGroup.findAllProcessGroups().stream()
                .map(group -> group.getVersionControlInformation())
                .filter(Objects::nonNull)
                .filter(vci -> vci.getRegistryIdentifier().equals(registryId))
                .collect(Collectors.toSet());

        if (!trackedVersionControlInformation.isEmpty()) {
            throw new IllegalStateException("The Registry cannot be removed because a Process Group currently under version control is tracking to it.");
        }
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
