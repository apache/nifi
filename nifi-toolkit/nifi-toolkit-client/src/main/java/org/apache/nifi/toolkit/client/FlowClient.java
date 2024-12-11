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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.ClusterSummaryEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBranchesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBucketsEntity;
import org.apache.nifi.web.api.entity.ParameterProvidersEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.ReportingTaskTypesEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;
import org.apache.nifi.web.api.entity.VersionedFlowsEntity;

import java.io.IOException;

/**
 * Client for FlowResource.
 */
public interface FlowClient {

    /**
     * @return the entity representing the current user accessing the NiFi instance
     */
    CurrentUserEntity getCurrentUser() throws NiFiClientException, IOException;

    /**
     * @return the id of the root process group
     */
    String getRootGroupId() throws NiFiClientException, IOException;

    /**
     * Retrieves the process group with the given id.
     *
     * Passing in "root" as the id will retrieve the root group.
     *
     * @param id the id of the process group to retrieve
     * @return the process group entity
     */
    ProcessGroupFlowEntity getProcessGroup(String id) throws NiFiClientException, IOException;

    /**
     * Suggest a location for the new process group on a canvas, within a given
     * process group. Will locate to the right and then bottom and offset for the
     * size of the PG box.
     *
     * @param parentId the id of the parent process group where the new process
     *                 group will be located
     * @return the process group box where a new process group can be placed
     */
    ProcessGroupBox getSuggestedProcessGroupCoordinates(String parentId) throws NiFiClientException, IOException;

    /**
     * Schedules the components of a process group.
     *
     * @param processGroupId           the id of a process group
     * @param scheduleComponentsEntity the scheduled state to update to
     * @return the entity representing the scheduled state
     */
    ScheduleComponentsEntity scheduleProcessGroupComponents(
            String processGroupId, ScheduleComponentsEntity scheduleComponentsEntity) throws NiFiClientException, IOException;

    /**
     * Gets the possible versions for the given flow in the given bucket in the
     * given registry.
     *
     * @param registryId the id of the registry client
     * @param bucketId   the bucket id
     * @param flowId     the flow id
     * @return the set of snapshot metadata entities
     */
    VersionedFlowSnapshotMetadataSetEntity getVersions(String registryId, String bucketId, String flowId)
            throws NiFiClientException, IOException;

    /**
     * Gets the possible versions for the given flow in the given bucket/branch in the given registry.
     *
     * @param registryId the id of the registry client
     * @param bucketId the bucket id
     * @param flowId the flow id
     * @param branch the branch name
     * @return the set of snapshot metadata entities
     */
    VersionedFlowSnapshotMetadataSetEntity getVersions(String registryId, String bucketId, String flowId, String branch)
            throws NiFiClientException, IOException;

    /**
     * Retrieves the controller services for the given group.
     *
     * @param groupId the process group id
     * @return the controller services entity
     */
    ControllerServicesEntity getControllerServices(String groupId) throws NiFiClientException, IOException;

    /**
     * Enable or disable controller services in the given process group.
     *
     * @param activateControllerServicesEntity the entity indicating the process
     *                                         group to enable/disable services for
     * @return the activate response
     */
    ActivateControllerServicesEntity activateControllerServices(ActivateControllerServicesEntity activateControllerServicesEntity) throws NiFiClientException, IOException;

    /**
     *
     * @return cluster summary response
     */
    ClusterSummaryEntity getClusterSummary() throws NiFiClientException, IOException;

    /**
     * Retrieves the controller services for the reporting tasks.
     *
     * @return the controller services entity
     */
    ControllerServicesEntity getControllerServices() throws NiFiClientException, IOException;

    /**
     * Retrieves the reporting tasks.
     *
     * @return the reporting tasks entity
     */
    ReportingTasksEntity getReportingTasks() throws NiFiClientException, IOException;

    /**
     * Retrieves the snapshot of all reporting tasks and their respective controller
     * services.
     *
     * @return the snapshot
     */
    VersionedReportingTaskSnapshot getReportingTaskSnapshot() throws NiFiClientException, IOException;

    /**
     * Retrieves the snapshot of the given reporting task and it's respective
     * controller services.
     *
     * @return the snapshot
     */
    VersionedReportingTaskSnapshot getReportingTaskSnapshot(String reportingTaskId) throws NiFiClientException, IOException;

    /**
     * Retrieves the parameter providers.
     *
     * @return the parameter providers entity
     */
    ParameterProvidersEntity getParamProviders() throws NiFiClientException, IOException;

    /**
     * Retrieves the status for the connection with the given ID
     *
     * @param connectionId the id of the connection
     * @param nodewise     whether or not node-wise information should be returned
     * @return the status for the connection
     */
    ConnectionStatusEntity getConnectionStatus(String connectionId, boolean nodewise) throws NiFiClientException, IOException;

    /**
     * Retrieves the status for the process group with the given ID
     *
     * @param groupId   the id of the process group
     * @param recursive whether or not to recurse into sub groups
     * @return the status for the process group
     */
    ProcessGroupStatusEntity getProcessGroupStatus(String groupId, boolean recursive) throws NiFiClientException, IOException;

    /**
     * Retrieves the available processor types.
     *
     * @return the processor types
     */
    ProcessorTypesEntity getProcessorTypes() throws NiFiClientException, IOException;

    /**
     * Retrieves the available controller service types.
     *
     * @return the controller service types
     */
    ControllerServiceTypesEntity getControllerServiceTypes() throws NiFiClientException, IOException;

    /**
     * Retrieves the available reporting task types.
     *
     * @return the reporting task types
     */
    ReportingTaskTypesEntity getReportingTaskTypes() throws NiFiClientException, IOException;

    /**
     * Returns the list of branches for the specified registry client ID
     * @param registryClientId Registry Client ID
     * @return list of branches
     */
    FlowRegistryBranchesEntity getFlowRegistryBranches(String registryClientId) throws NiFiClientException, IOException;

    /**
     * Returns the list of buckets in a given branch for the specified registry client ID
     * @param registryClientId Registry Client ID
     * @param branch Name of the branch
     * @return list of buckets
     */
    FlowRegistryBucketsEntity getFlowRegistryBuckets(String registryClientId, String branch) throws NiFiClientException, IOException;

    /**
     * Returns the list of flows in a given branch and bucket for the specified registry client ID
     * @param registryClientId Registry Client ID
     * @param branch Name of the branch
     * @param bucket ID of the bucket
     * @return list of flows
     */
    VersionedFlowsEntity getFlowRegistryFlows(String registryClientId, String branch, String bucket) throws NiFiClientException, IOException;
}
