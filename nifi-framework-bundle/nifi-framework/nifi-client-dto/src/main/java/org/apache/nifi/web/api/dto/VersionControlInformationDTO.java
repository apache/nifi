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

package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

@XmlType(name = "versionControlInformation")
public class VersionControlInformationDTO {
    public static final String LOCALLY_MODIFIED = "LOCALLY_MODIFIED";
    public static final String STALE = "STALE";
    public static final String LOCALLY_MODIFIED_AND_STALE = "LOCALLY_MODIFIED_AND_STALE";
    public static final String UP_TO_DATE = "UP_TO_DATE";
    public static final String SYNC_FAILURE = "SYNC_FAILURE";
    private static final String ALLOWABLE_STATES = String.join(", ", LOCALLY_MODIFIED, STALE, LOCALLY_MODIFIED_AND_STALE, UP_TO_DATE, SYNC_FAILURE);

    private String groupId;
    private String registryId;
    private String registryName;
    private String branch;
    private String bucketId;
    private String bucketName;
    private String flowId;
    private String flowName;
    private String flowDescription;
    private String version;
    private String storageLocation;
    private String state;
    private String stateExplanation;

    @Schema(description = "The ID of the Process Group that is under version control")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The ID of the registry that the flow is stored in")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(final String registryId) {
        this.registryId = registryId;
    }

    @Schema(description = "The name of the registry that the flow is stored in", accessMode = Schema.AccessMode.READ_ONLY)
    public String getRegistryName() {
        return registryName;
    }

    public void setRegistryName(final String registryName) {
        this.registryName = registryName;
    }

    @Schema(description = "The ID of the branch that the flow is stored in")
    public String getBranch() {
        return branch;
    }

    public void setBranch(final String branch) {
        this.branch = branch;
    }

    @Schema(description = "The ID of the bucket that the flow is stored in")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    @Schema(description = "The name of the bucket that the flow is stored in", accessMode = Schema.AccessMode.READ_ONLY)
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Schema(description = "The ID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(final String flowId) {
        this.flowId = flowId;
    }

    @Schema(description = "The name of the flow")
    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @Schema(description = "The description of the flow")
    public String getFlowDescription() {
        return flowDescription;
    }

    public void setFlowDescription(String flowDescription) {
        this.flowDescription = flowDescription;
    }

    @Schema(description = "The version of the flow")
    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Schema(description = "The storage location")
    public String getStorageLocation() {
        return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    @Schema(accessMode = Schema.AccessMode.READ_ONLY,
        description = "The current state of the Process Group, as it relates to the Versioned Flow",
        allowableValues = {LOCALLY_MODIFIED, STALE, LOCALLY_MODIFIED_AND_STALE, UP_TO_DATE, SYNC_FAILURE})
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @Schema(description = "Explanation of why the group is in the specified state", accessMode = Schema.AccessMode.READ_ONLY)
    public String getStateExplanation() {
        return stateExplanation;
    }

    public void setStateExplanation(String explanation) {
        this.stateExplanation = explanation;
    }
}
