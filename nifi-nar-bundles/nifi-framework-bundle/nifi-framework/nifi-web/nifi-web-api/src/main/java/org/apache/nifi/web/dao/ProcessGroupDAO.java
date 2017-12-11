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
package org.apache.nifi.web.dao;

import java.util.Set;
import java.util.concurrent.Future;

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;

public interface ProcessGroupDAO {

    /**
     * Determines if the specified remote process group exists.
     *
     * @return true if group exists
     */
    boolean hasProcessGroup(String groupId);

    /**
     * Creates a process group reference.
     *
     * @param parentGroupId The parent process group id
     * @param processGroup The process group
     * @return The process group
     */
    ProcessGroup createProcessGroup(String parentGroupId, ProcessGroupDTO processGroup);

    /**
     * Gets the specified process group.
     *
     * @param groupId The process group id
     * @return The process group
     */
    ProcessGroup getProcessGroup(String groupId);

    /**
     * Gets all of the process groups.
     *
     * @param parentGroupId The parent group id
     * @return The process groups
     */
    Set<ProcessGroup> getProcessGroups(String parentGroupId);

    /**
     * Verifies the specified process group can be modified.
     *
     * @param groupId id
     * @param state scheduled state
     */
    void verifyScheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Verifies the specified controller services can be modified
     *
     * @param groupId the ID of the process group
     * @param state the desired state
     * @param serviceIds the ID's of the controller services
     */
    void verifyActivateControllerServices(String groupId, ControllerServiceState state, Set<String> serviceIds);

    /**
     * Schedules the components in the specified process group.
     *
     * @param groupId id
     * @param state scheduled state
     *
     * @return a Future that can be used to wait for the services to finish starting or stopping
     */
    Future<Void> scheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Enables or disables the controller services in the specified process group
     *
     * @param groupId the id of the group
     * @param state the desired state
     * @param serviceIds the ID's of the services to enable or disable
     */
    Future<Void> activateControllerServices(String groupId, ControllerServiceState state, Set<String> serviceIds);

    /**
     * Updates the specified process group.
     *
     * @param processGroup The process group
     * @return The process group
     */
    ProcessGroup updateProcessGroup(ProcessGroupDTO processGroup);

    /**
     * Updates the specified variable registry
     *
     * @param variableRegistry the Variable Registry
     * @return the Process Group that was updated
     */
    ProcessGroup updateVariableRegistry(VariableRegistryDTO variableRegistry);

    /**
     * Verifies that the specified updates to a current Process Group can be applied at this time
     *
     * @param processGroup the DTO That describes the changes to occur
     */
    void verifyUpdate(ProcessGroupDTO processGroup);

    /**
     * Verifies the specified process group can be removed.
     *
     * @param groupId id
     */
    void verifyDelete(String groupId);

    /**
     * Deletes the specified process group.
     *
     * @param groupId The process group id
     */
    void deleteProcessGroup(String groupId);
}
