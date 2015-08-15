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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardRemoteProcessGroupDAO extends ComponentDAO implements RemoteProcessGroupDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardRemoteProcessGroupDAO.class);
    private FlowController flowController;

    private RemoteProcessGroup locateRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        return locateRemoteProcessGroup(locateProcessGroup(flowController, groupId), remoteProcessGroupId);
    }

    private RemoteProcessGroup locateRemoteProcessGroup(ProcessGroup group, String remoteProcessGroupId) {
        RemoteProcessGroup remoteProcessGroup = group.getRemoteProcessGroup(remoteProcessGroupId);

        if (remoteProcessGroup == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group with id '%s'.", remoteProcessGroupId));
        }

        return remoteProcessGroup;
    }

    /**
     * Creates a remote process group reference.
     *
     * @param remoteProcessGroupDTO The remote process group
     * @return The remote process group
     */
    @Override
    public RemoteProcessGroup createRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        if (remoteProcessGroupDTO.getParentGroupId() != null && !flowController.areGroupsSame(groupId, remoteProcessGroupDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Remote Process Group is being added.");
        }

        final String rawTargetUri = remoteProcessGroupDTO.getTargetUri();
        if (rawTargetUri == null) {
            throw new IllegalArgumentException("Cannot add a Remote Process Group without specifying the Target URI");
        }

        // create the remote process group
        RemoteProcessGroup remoteProcessGroup = flowController.createRemoteProcessGroup(remoteProcessGroupDTO.getId(), rawTargetUri);

        // update the remote process group
        if (isNotNull(remoteProcessGroupDTO.getPosition())) {
            remoteProcessGroup.setPosition(new Position(remoteProcessGroupDTO.getPosition().getX(), remoteProcessGroupDTO.getPosition().getY()));
        }
        remoteProcessGroup.setComments(remoteProcessGroupDTO.getComments());

        // get the group to add the remote process group to
        group.addRemoteProcessGroup(remoteProcessGroup);

        return remoteProcessGroup;
    }

    /**
     * Gets the specified remote process group.
     *
     * @param remoteProcessGroupId The remote process group id
     * @return The remote process group
     */
    @Override
    public RemoteProcessGroup getRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(groupId, remoteProcessGroupId);

        return remoteProcessGroup;
    }

    /**
     * Determines if the specified remote process group exists.
     *
     * @param remoteProcessGroupId id
     * @return true if exists
     */
    @Override
    public boolean hasRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        ProcessGroup group = flowController.getGroup(groupId);

        if (group == null) {
            return false;
        }

        return group.getRemoteProcessGroup(remoteProcessGroupId) != null;
    }

    /**
     * Gets all of the remote process groups.
     *
     * @return The remote process groups
     */
    @Override
    public Set<RemoteProcessGroup> getRemoteProcessGroups(String groupId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final Set<RemoteProcessGroup> remoteProcessGroups = group.getRemoteProcessGroups();
        return remoteProcessGroups;
    }

    @Override
    public void verifyUpdate(String groupId, RemoteProcessGroupDTO remoteProcessGroup) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        verifyUpdate(locateRemoteProcessGroup(group, remoteProcessGroup.getId()), remoteProcessGroup);
    }

    /**
     * Verifies the specified remote group can be updated, if necessary.
     */
    private void verifyUpdate(RemoteProcessGroup remoteProcessGroup, RemoteProcessGroupDTO remoteProcessGroupDto) {
        // see if the remote process group can start/stop transmitting
        if (isNotNull(remoteProcessGroupDto.isTransmitting())) {
            if (!remoteProcessGroup.isTransmitting() && remoteProcessGroupDto.isTransmitting()) {
                remoteProcessGroup.verifyCanStartTransmitting();
            } else if (remoteProcessGroup.isTransmitting() && !remoteProcessGroupDto.isTransmitting()) {
                remoteProcessGroup.verifyCanStopTransmitting();
            }
        }

        // validate the proposed configuration
        validateProposedRemoteProcessGroupConfiguration(remoteProcessGroupDto);

        // if any remote group properties are changing, verify update
        if (isAnyNotNull(remoteProcessGroupDto.getYieldDuration(), remoteProcessGroupDto.getCommunicationsTimeout())) {
            remoteProcessGroup.verifyCanUpdate();
        }
    }

    @Override
    public void verifyUpdateInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(group, remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getInputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group input port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        verifyUpdatePort(port, remoteProcessGroupPortDto);
    }

    @Override
    public void verifyUpdateOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(group, remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getOutputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group output port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        verifyUpdatePort(port, remoteProcessGroupPortDto);
    }

    /**
     * Verified the specified remote port can be updated, if necessary.
     */
    private void verifyUpdatePort(RemoteGroupPort port, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        // see if the remote process group can start/stop transmitting
        if (isNotNull(remoteProcessGroupPortDto.isTransmitting())) {
            if (!port.isRunning() && remoteProcessGroupPortDto.isTransmitting()) {
                port.verifyCanStart();
            } else if (port.isRunning() && !remoteProcessGroupPortDto.isTransmitting()) {
                port.verifyCanStop();
            }
        }

        // validate the proposed configuration
        validateProposedRemoteProcessGroupPortConfiguration(port, remoteProcessGroupPortDto);

        // verify update when appropriate
        if (isAnyNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount(), remoteProcessGroupPortDto.getUseCompression())) {
            port.verifyCanUpdate();
        }
    }

    /**
     * Validates the proposed configuration for the specified remote port.
     */
    private List<String> validateProposedRemoteProcessGroupPortConfiguration(RemoteGroupPort remoteGroupPort, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        final List<String> validationErrors = new ArrayList<>();

        // ensure the proposed port configuration is valid
        if (isNotNull(remoteProcessGroupPortDTO.getConcurrentlySchedulableTaskCount()) && remoteProcessGroupPortDTO.getConcurrentlySchedulableTaskCount() <= 0) {
            validationErrors.add(String.format("Concurrent tasks for port '%s' must be a positive integer.", remoteGroupPort.getName()));
        }

        return validationErrors;
    }

    /**
     * Validates the proposed configuration for the specified remote group.
     */
    private List<String> validateProposedRemoteProcessGroupConfiguration(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final List<String> validationErrors = new ArrayList<>();

        if (isNotNull(remoteProcessGroupDTO.getCommunicationsTimeout())) {
            Matcher yieldMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(remoteProcessGroupDTO.getCommunicationsTimeout());
            if (!yieldMatcher.matches()) {
                validationErrors.add("Communications timeout is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        if (isNotNull(remoteProcessGroupDTO.getYieldDuration())) {
            Matcher yieldMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(remoteProcessGroupDTO.getYieldDuration());
            if (!yieldMatcher.matches()) {
                validationErrors.add("Yield duration is not a valid time duration (ie 30 sec, 5 min)");
            }
        }

        return validationErrors;
    }

    @Override
    public RemoteGroupPort updateRemoteProcessGroupInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(groupId, remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getInputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group input port with id '%s'.", remoteProcessGroupPortDto.getId()));
        }

        // verify the update
        verifyUpdatePort(port, remoteProcessGroupPortDto);

        // perform the update
        if (isNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount())) {
            port.setMaxConcurrentTasks(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount());
        }
        if (isNotNull(remoteProcessGroupPortDto.getUseCompression())) {
            port.setUseCompression(remoteProcessGroupPortDto.getUseCompression());
        }

        final Boolean isTransmitting = remoteProcessGroupPortDto.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!port.isRunning() && isTransmitting) {
                remoteProcessGroup.startTransmitting(port);
            } else if (port.isRunning() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting(port);
            }
        }

        return port;
    }

    @Override
    public RemoteGroupPort updateRemoteProcessGroupOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDto) {
        final RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(groupId, remoteProcessGroupId);
        final RemoteGroupPort port = remoteProcessGroup.getOutputPort(remoteProcessGroupPortDto.getId());

        if (port == null) {
            throw new ResourceNotFoundException(
                    String.format("Unable to find remote process group output port with id '%s'.", remoteProcessGroupId));
        }

        // verify the update
        verifyUpdatePort(port, remoteProcessGroupPortDto);

        // perform the update
        if (isNotNull(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount())) {
            port.setMaxConcurrentTasks(remoteProcessGroupPortDto.getConcurrentlySchedulableTaskCount());
        }
        if (isNotNull(remoteProcessGroupPortDto.getUseCompression())) {
            port.setUseCompression(remoteProcessGroupPortDto.getUseCompression());
        }

        final Boolean isTransmitting = remoteProcessGroupPortDto.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!port.isRunning() && isTransmitting) {
                remoteProcessGroup.startTransmitting(port);
            } else if (port.isRunning() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting(port);
            }
        }

        return port;
    }

    @Override
    public RemoteProcessGroup updateRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(group, remoteProcessGroupDTO.getId());

        // verify the update request
        verifyUpdate(remoteProcessGroup, remoteProcessGroupDTO);

        // configure the remote process group
        final String name = remoteProcessGroupDTO.getName();
        final String comments = remoteProcessGroupDTO.getComments();
        final String communicationsTimeout = remoteProcessGroupDTO.getCommunicationsTimeout();
        final String yieldDuration = remoteProcessGroupDTO.getYieldDuration();

        if (isNotNull(name)) {
            remoteProcessGroup.setName(name);
        }
        if (isNotNull(comments)) {
            remoteProcessGroup.setComments(comments);
        }
        if (isNotNull(communicationsTimeout)) {
            remoteProcessGroup.setCommunicationsTimeout(communicationsTimeout);
        }
        if (isNotNull(yieldDuration)) {
            remoteProcessGroup.setYieldDuration(yieldDuration);
        }
        if (isNotNull(remoteProcessGroupDTO.getPosition())) {
            remoteProcessGroup.setPosition(new Position(remoteProcessGroupDTO.getPosition().getX(), remoteProcessGroupDTO.getPosition().getY()));
        }

        final Boolean isTransmitting = remoteProcessGroupDTO.isTransmitting();
        if (isNotNull(isTransmitting)) {
            // start or stop as necessary
            if (!remoteProcessGroup.isTransmitting() && isTransmitting) {
                remoteProcessGroup.startTransmitting();
            } else if (remoteProcessGroup.isTransmitting() && !isTransmitting) {
                remoteProcessGroup.stopTransmitting();
            }
        }

        return remoteProcessGroup;
    }

    @Override
    public void verifyDelete(String groupId, String remoteProcessGroupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(group, remoteProcessGroupId);
        remoteProcessGroup.verifyCanDelete();
    }

    @Override
    public void deleteRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        RemoteProcessGroup remoteProcessGroup = locateRemoteProcessGroup(group, remoteProcessGroupId);
        group.removeRemoteProcessGroup(remoteProcessGroup);
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
