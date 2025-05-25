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

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
     * @param processGroupRecursivity how far into child/descendant groups to recurse
     * @return The process groups
     */
    Set<ProcessGroup> getProcessGroups(String parentGroupId, ProcessGroupRecursivity processGroupRecursivity);

    /**
     * Gets all of the process groups.
     *
     * @param parentGroupId The parent group id
     * @param includeDescendants Whether or not to include descendant groups
     * @return The process groups
     */
    Set<ProcessGroup> getProcessGroups(String parentGroupId, boolean includeDescendants);

    /**
     * Verifies the specified process group can be modified.
     *
     * @param groupId id
     * @param state scheduled state
     * @param componentIds component ids
     */
    void verifyScheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Verifies the specified process group can be enabled.
     *
     * @param groupId id
     * @param state scheduled state
     * @param componentIds component ids
     */
    void verifyEnableComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Verifies the specified controller services can be modified
     *
     * @param state the desired state
     * @param serviceIds the ID's of the controller services
     */
    void verifyActivateControllerServices(ControllerServiceState state, Collection<String> serviceIds);

    /**
     * Schedules the components in the specified process group.
     *
     * @param groupId id
     * @param state scheduled state
     * @param componentIds components
     */
    void scheduleComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Enables or disabled the components in the specified process group.
     *
     * @param groupId id
     * @param state scheduled state
     * @param componentIds components
     */
    void enableComponents(String groupId, ScheduledState state, Set<String> componentIds);

    /**
     * Enables or disables the controller services in the specified process group
     *
     * @param groupId id
     * @param state the desired state
     * @param serviceIds the ID's of the services to enable or disable
     */
    void activateControllerServices(String groupId, ControllerServiceState state, Collection<String> serviceIds);

    /**
     * Updates the specified process group.
     *
     * @param processGroup The process group
     * @return The process group
     */
    ProcessGroup updateProcessGroup(ProcessGroupDTO processGroup);

    /**
     * Sets the version control info on an unversioned process group.
     *
     * @param processGroup the process group with the version control info specified
     * @param flowSnapshot the flow snapshot for the given version control info
     * @return the updated process group
     */
    ProcessGroup setVersionControlInformation(ProcessGroupDTO processGroup, RegisteredFlowSnapshot flowSnapshot);

    /**
     * Updates the process group so that it matches the proposed flow
     *
     * @param groupId the ID of the process group
     * @param proposedSnapshot Flow the new version of the flow
     * @param versionControlInformation the new Version Control Information
     * @param componentIdSeed the seed value to use for generating ID's for new components
     * @param updateSettings whether or not to update the process group's name and position
     * @param updateDescendantVersionedFlows if a child/descendant Process Group is under Version Control, specifies whether or not to
     *            update the contents of that Process Group
     * @return the process group
     */
    ProcessGroup updateProcessGroupFlow(String groupId, VersionedExternalFlow proposedSnapshot, VersionControlInformationDTO versionControlInformation, String componentIdSeed,
                                        boolean verifyNotModified, boolean updateSettings, boolean updateDescendantVersionedFlows);

    /**
     * Adds versioned components to a Process Group
     *
     * @param groupId the ID of the process group
     * @param additions the additions to add to this Process Group
     * @param componentIdSeed the seed value to use for generating ID's for new components
     * @return the component additions
     */
    ComponentAdditions addVersionedComponents(String groupId, VersionedComponentAdditions additions, String componentIdSeed);

    /**
     * Applies the given Version Control Information to the Process Group
     *
     * @param versionControlInformation the Version Control Information to apply
     * @param versionedComponentMapping a mapping of Component ID to Versioned Component ID
     * @return the Process Group
     */
    ProcessGroup updateVersionControlInformation(VersionControlInformationDTO versionControlInformation, Map<String, String> versionedComponentMapping);

    /**
     * Disconnects the specified group from version control.
     *
     * @param groupId group id
     * @return the corresponding Process Group
     */
    ProcessGroup disconnectVersionControl(String groupId);

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
     * Verifies the specified registry can be removed.
     *
     * @param registryId registry id
     */
    void verifyDeleteFlowRegistry(String registryId);

    /**
     * Creates a request to drop flowfiles in all connections (recursively).
     *
     * @param processGroupId process group id
     * @param dropRequestId drop request id
     * @return The drop request status
     */
    DropFlowFileStatus createDropAllFlowFilesRequest(String processGroupId, String dropRequestId);

    /**
     * Gets the specified request for dropping all flowfiles in all connections (recursively).
     * @param processGroupId The id of the process group
     * @param dropRequestId The drop request id
     * @return The drop request status
     */
    DropFlowFileStatus getDropAllFlowFilesRequest(String processGroupId, String dropRequestId);

    /**
     * Deletes the specified request for dropping all flowfiles in all connections (recursively).
     *
     * @param processGroupId The id of the process group
     * @param dropRequestId The drop request id
     * @return The drop request status
     */
    DropFlowFileStatus deleteDropAllFlowFilesRequest(String processGroupId, String dropRequestId);

    /**
     * Deletes the specified process group.
     *
     * @param groupId The process group id
     */
    void deleteProcessGroup(String groupId);
}
