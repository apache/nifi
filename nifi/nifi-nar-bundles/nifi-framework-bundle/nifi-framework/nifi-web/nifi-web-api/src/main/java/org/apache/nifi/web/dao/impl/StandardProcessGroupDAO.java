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

import java.util.Set;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
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
    public ProcessGroup getProcessGroup(String groupId) {
        return locateProcessGroup(flowController, groupId);
    }

    @Override
    public boolean hasProcessGroup(String groupId) {
        return flowController.getGroup(groupId) != null;
    }

    @Override
    public Set<ProcessGroup> getProcessGroups(String parentGroupId) {
        ProcessGroup group = locateProcessGroup(flowController, parentGroupId);
        return group.getProcessGroups();
    }

    @Override
    public void verifyUpdate(ProcessGroupDTO processGroupDTO) {
        final ProcessGroup group = locateProcessGroup(flowController, processGroupDTO.getId());

        // determine if any action is required
        if (isNotNull(processGroupDTO.isRunning())) {
            if (processGroupDTO.isRunning()) {
                group.verifyCanStart();
            } else {
                group.verifyCanStop();
            }
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
        }
        if (isNotNull(comments)) {
            group.setComments(comments);
        }

        // determine if any action is required
        if (isNotNull(processGroupDTO.isRunning())) {
            if (processGroupDTO.isRunning()) {
                group.startProcessing();
            } else {
                group.stopProcessing();
            }
        }

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
