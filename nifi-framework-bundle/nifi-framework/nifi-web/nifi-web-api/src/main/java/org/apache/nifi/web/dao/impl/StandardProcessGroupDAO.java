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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.WebApplicationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class StandardProcessGroupDAO extends ComponentDAO implements ProcessGroupDAO {
    private static final Logger logger = LoggerFactory.getLogger(StandardProcessGroupDAO.class);

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

        if (processGroup.getComments() != null) {
            group.setComments(processGroup.getComments());
        }

        final ParameterContextReferenceEntity parameterContextReference = processGroup.getParameterContext();
        if (parameterContextReference != null && parameterContextReference.getId() != null) {
            final ParameterContext parameterContext = flowController.getFlowManager().getParameterContextManager().getParameterContext(parameterContextReference.getId());
            group.setParameterContext(parameterContext);
        }

        if (processGroup.getStatelessFlowTimeout() != null) {
            group.setStatelessFlowTimeout(processGroup.getStatelessFlowTimeout());
        }
        if (processGroup.getMaxConcurrentTasks() != null) {
            group.setMaxConcurrentTasks(processGroup.getMaxConcurrentTasks());
        }
        if (processGroup.getExecutionEngine() != null) {
            group.setExecutionEngine(ExecutionEngine.valueOf(processGroup.getExecutionEngine()));
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
        final ProcessGroup group = locateProcessGroup(flowController, processGroup.getId());
        verifyCanSetParameterContext(processGroup, group);

        final String executionEngine = processGroup.getExecutionEngine();
        if (executionEngine != null) {
            group.verifyCanSetExecutionEngine(ExecutionEngine.valueOf(executionEngine));
        }

        final VersionControlInformationDTO versionControlInfoDTO = processGroup.getVersionControlInformation();
        final VersionControlInformation versionControlInformation = group.getVersionControlInformation();
        if (versionControlInfoDTO != null) {
            if (versionControlInformation != null) {
                throw new IllegalStateException("Cannot set Version Control Info because process group is already under version control");
            }
            verifyChildProcessGroupsNotUnderVersionControl(group.getProcessGroups());
        }
    }

    private void verifyChildProcessGroupsNotUnderVersionControl(final Set<ProcessGroup> childGroups) {
        if (childGroups == null || childGroups.isEmpty()) {
            return;
        }

        for (final ProcessGroup childGroup : childGroups) {
            final VersionControlInformation versionControlInformation = childGroup.getVersionControlInformation();
            if (versionControlInformation != null) {
                throw new IllegalStateException("Cannot set Version Control Info because a child process group is already under version control");
            }
            verifyChildProcessGroupsNotUnderVersionControl(childGroup.getProcessGroups());
        }
    }

    private void verifyCanSetParameterContext(final ProcessGroupDTO processGroup, final ProcessGroup group) {
        final ParameterContextReferenceEntity parameterContextReference = processGroup.getParameterContext();
        if (parameterContextReference == null) {
            return;
        }

        final ParameterContext parameterContext = locateParameterContext(parameterContextReference.getId());

        group.verifyCanSetParameterContext(parameterContext);
    }

    private ParameterContext locateParameterContext(final String id) {
        final ParameterContext parameterContext;
        if (id == null) {
            return null;
        } else {
            parameterContext = flowController.getFlowManager().getParameterContextManager().getParameterContext(id);
            if (parameterContext == null) {
                throw new IllegalStateException("Cannot update Process Group's Parameter Context because no Parameter Context exists with ID " + id);
            }

            return parameterContext;
        }
    }

    @Override
    public ProcessGroup getProcessGroup(String groupId) {
        return locateProcessGroup(flowController, groupId);
    }

    @Override
    public Set<ProcessGroup> getProcessGroups(final String parentGroupId, final ProcessGroupRecursivity processGroupRecursivity) {
        ProcessGroup group = locateProcessGroup(flowController, parentGroupId);
        if (processGroupRecursivity == ProcessGroupRecursivity.ALL_DESCENDANTS) {
            return new HashSet<>(group.findAllProcessGroups());
        } else {
            return group.getProcessGroups();
        }
    }

    @Override
    public Set<ProcessGroup> getProcessGroups(final String parentGroupId, final boolean includeDescendants) {
        final ProcessGroup group = locateProcessGroup(flowController, parentGroupId);
        return new HashSet<>(group.findAllProcessGroups());
    }

    @Override
    public void verifyScheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final ExecutionEngine executionEngine = group.resolveExecutionEngine();
        if (executionEngine == ExecutionEngine.STATELESS) {
            final ProcessGroup parentGroup = group.getParent();
            if (parentGroup != null && parentGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
                final String action = state == ScheduledState.STOPPED ? "stop" : "start";
                throw new IllegalStateException("Cannot " + action + " Process Group with ID " + groupId + " directly because it is part of a Stateless Process Group");
            }

            group.verifyCanStart();
            return;
        }

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);

            if (connectable instanceof RemoteGroupPort) {
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
    public void scheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final Set<ProcessGroup> validGroups = new HashSet<>();
        validGroups.add(group);
        validGroups.addAll(group.findAllProcessGroups());

        if (group.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            if (ScheduledState.RUNNING.equals(state)) {
                group.startProcessing();
                return;
            } else if (ScheduledState.STOPPED.equals(state)) {
                group.stopProcessing();
                return;
            }
        }

        for (final String componentId : componentIds) {
            final Connectable connectable = findConnectable(componentId, groupId, validGroups);

            if (ScheduledState.RUNNING.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        connectable.getProcessGroup().startProcessor((ProcessorNode) connectable, true);
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
                    case STATELESS_GROUP:
                        connectable.getProcessGroup().startProcessing();
                        break;
                }
            } else if (ScheduledState.STOPPED.equals(state)) {
                switch (connectable.getConnectableType()) {
                    case PROCESSOR:
                        connectable.getProcessGroup().stopProcessor((ProcessorNode) connectable);
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
                    case STATELESS_GROUP:
                        connectable.getProcessGroup().stopProcessing();
                        break;
                }
            }
        }
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
    public void activateControllerServices(final String groupId, final ControllerServiceState state, final Collection<String> serviceIds) {
        final FlowManager flowManager = flowController.getFlowManager();
        final List<ControllerServiceNode> serviceNodes = serviceIds.stream()
            .map(flowManager::getControllerServiceNode)
            .collect(Collectors.toList());

        final ProcessGroup group = flowManager.getGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Cannot activate Controller Services with IDs " + serviceIds + " because the associated Process Group (id=" + groupId + ") could not be found");
        }

        final ExecutionEngine executionEngine = group.resolveExecutionEngine();
        if (executionEngine == ExecutionEngine.STATELESS) {
            logger.info("Attempt was made to activate Controller Services for Process Group with ID {} and Controller Service State {} but group is configured to use the Stateless Engine so will " +
                "not change state of Controller Services", groupId, state);
            return;
        }

        if (state == ControllerServiceState.ENABLED) {
            flowController.getControllerServiceProvider().enableControllerServicesAsync(serviceNodes);
        } else {
            flowController.getControllerServiceProvider().disableControllerServicesAsync(serviceNodes);
        }
    }

    @Override
    public ProcessGroup updateProcessGroup(ProcessGroupDTO processGroupDTO) {
        final ProcessGroup group = locateProcessGroup(flowController, processGroupDTO.getId());

        final String name = processGroupDTO.getName();
        final String comments = processGroupDTO.getComments();
        final String concurrencyName = processGroupDTO.getFlowfileConcurrency();
        final FlowFileConcurrency flowFileConcurrency = concurrencyName == null ? null : FlowFileConcurrency.valueOf(concurrencyName);

        final String outboundPolicyName = processGroupDTO.getFlowfileOutboundPolicy();
        final FlowFileOutboundPolicy flowFileOutboundPolicy = outboundPolicyName == null ? null : FlowFileOutboundPolicy.valueOf(outboundPolicyName);

        final String defaultFlowFileExpiration = processGroupDTO.getDefaultFlowFileExpiration();
        final Long defaultBackPressureObjectThreshold = processGroupDTO.getDefaultBackPressureObjectThreshold();
        final String defaultBackPressureDataSizeThreshold = processGroupDTO.getDefaultBackPressureDataSizeThreshold();

        final String logFileSuffix = processGroupDTO.getLogFileSuffix();

        final ParameterContextReferenceEntity parameterContextReference = processGroupDTO.getParameterContext();
        if (parameterContextReference != null) {
            final String parameterContextId = parameterContextReference.getId();
            if (parameterContextId == null) {
                group.setParameterContext(null);
            } else {
                final ParameterContext parameterContext = flowController.getFlowManager().getParameterContextManager().getParameterContext(parameterContextId);
                if (parameterContext == null) {
                    throw new IllegalStateException("Cannot set Process Group's Parameter Context because no Parameter Context exists with ID " + parameterContextId);
                }

                group.setParameterContext(parameterContext);
            }
        }

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
        if (flowFileConcurrency != null) {
            group.setFlowFileConcurrency(flowFileConcurrency);
        }
        if (flowFileOutboundPolicy != null) {
            group.setFlowFileOutboundPolicy(flowFileOutboundPolicy);
        }

        if (defaultFlowFileExpiration != null) {
            group.setDefaultFlowFileExpiration(processGroupDTO.getDefaultFlowFileExpiration());
        }
        if (defaultBackPressureObjectThreshold != null) {
            group.setDefaultBackPressureObjectThreshold(processGroupDTO.getDefaultBackPressureObjectThreshold());
        }
        if (defaultBackPressureDataSizeThreshold != null) {
            group.setDefaultBackPressureDataSizeThreshold(processGroupDTO.getDefaultBackPressureDataSizeThreshold());
        }
        if (processGroupDTO.getExecutionEngine() != null) {
            group.setExecutionEngine(ExecutionEngine.valueOf(processGroupDTO.getExecutionEngine()));
        }
        if (processGroupDTO.getMaxConcurrentTasks() != null) {
            group.setMaxConcurrentTasks(processGroupDTO.getMaxConcurrentTasks());
        }
        if (processGroupDTO.getStatelessFlowTimeout() != null) {
            group.setStatelessFlowTimeout(processGroupDTO.getStatelessFlowTimeout());
        }

        if (logFileSuffix != null) {
            group.setLogFileSuffix(logFileSuffix);
        }

        group.onComponentModified();
        return group;
    }

    @Override
    public ProcessGroup setVersionControlInformation(final ProcessGroupDTO processGroup, final RegisteredFlowSnapshot flowSnapshot) {
        final ProcessGroup group = locateProcessGroup(flowController, processGroup.getId());
        final VersionControlInformationDTO versionControlInfo = processGroup.getVersionControlInformation();
        final VersionedProcessGroup versionedProcessGroup = flowSnapshot.getFlowContents();
        updateVersionControlInformation(group, versionedProcessGroup, versionControlInfo, Collections.emptyMap());
        return group;
    }

    @Override
    public ProcessGroup updateVersionControlInformation(final VersionControlInformationDTO versionControlInformation, final Map<String, String> versionedComponentMapping) {
        final String groupId = versionControlInformation.getGroupId();
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(flowController.getExtensionManager());
        final InstantiatedVersionedProcessGroup flowSnapshot = mapper.mapProcessGroup(group, flowController.getControllerServiceProvider(), flowController.getFlowManager(), false);

        updateVersionControlInformation(group, flowSnapshot, versionControlInformation, versionedComponentMapping);
        group.onComponentModified();

        return group;
    }

    private void updateVersionControlInformation(final ProcessGroup group, final VersionedProcessGroup flowSnapshot,
        final VersionControlInformationDTO versionControlInformation, final Map<String, String> versionedComponentMapping) {

        final String registryId = versionControlInformation.getRegistryId();
        final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
        final String registryName = flowRegistry == null ? registryId : flowRegistry.getName();

        final StandardVersionControlInformation vci = StandardVersionControlInformation.Builder.fromDto(versionControlInformation)
            .registryName(registryName)
            .flowSnapshot(flowSnapshot)
            .build();

        group.setVersionControlInformation(vci, versionedComponentMapping);
    }

    @Override
    public ProcessGroup disconnectVersionControl(final String groupId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.disconnectVersionControl();
        group.onComponentModified();
        return group;
    }

    @Override
    public ComponentAdditions addVersionedComponents(final String groupId, final VersionedComponentAdditions additions, final String componentIdSeed) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final ComponentAdditions componentAdditions = group.addVersionedComponents(additions, componentIdSeed);
        group.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);

        group.onComponentModified();

        return componentAdditions;
    }

    @Override
    public ProcessGroup updateProcessGroupFlow(final String groupId, final VersionedExternalFlow proposedSnapshot, final VersionControlInformationDTO versionControlInformation,
                                               final String componentIdSeed, final boolean verifyNotModified, final boolean updateSettings, final boolean updateDescendantVersionedFlows) {

        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        group.updateFlow(proposedSnapshot, componentIdSeed, verifyNotModified, updateSettings, updateDescendantVersionedFlows);
        group.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::initialize);

        // process group being updated may not be versioned
        if (versionControlInformation != null) {
            final StandardVersionControlInformation svci = StandardVersionControlInformation.Builder.fromDto(versionControlInformation)
                    .flowSnapshot(proposedSnapshot.getFlowContents())
                    .build();
            group.setVersionControlInformation(svci, Collections.emptyMap());
        }

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
    public DropFlowFileStatus createDropAllFlowFilesRequest(String processGroupId, String dropRequestId) {
        ProcessGroup processGroup = locateProcessGroup(flowController, processGroupId);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        return processGroup.dropAllFlowFiles(dropRequestId, user.getIdentity());
    }

    @Override
    public DropFlowFileStatus getDropAllFlowFilesRequest(String processGroupId, String dropRequestId) {
        ProcessGroup processGroup = locateProcessGroup(flowController, processGroupId);

        return processGroup.getDropAllFlowFilesStatus(dropRequestId);
    }

    @Override
    public DropFlowFileStatus deleteDropAllFlowFilesRequest(String processGroupId, String dropRequestId) {
        ProcessGroup processGroup = locateProcessGroup(flowController, processGroupId);

        return processGroup.cancelDropAllFlowFiles(dropRequestId);
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

    @Autowired
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
