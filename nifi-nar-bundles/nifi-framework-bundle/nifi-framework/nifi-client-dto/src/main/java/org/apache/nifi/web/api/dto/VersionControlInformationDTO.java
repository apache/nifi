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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "versionControlInformation")
public class VersionControlInformationDTO {
    private String groupId;
    private String registryId;
    private String registryName;
    private String bucketId;
    private String bucketName;
    private String flowId;
    private String flowName;
    private String flowDescription;
    private Integer version;
    private String state;
    private String stateExplanation;

    @ApiModelProperty("The ID of the Process Group that is under version control")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The ID of the registry that the flow is stored in")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(final String registryId) {
        this.registryId = registryId;
    }

    @ApiModelProperty(value = "The name of the registry that the flow is stored in", readOnly = true)
    public String getRegistryName() {
        return registryName;
    }

    public void setRegistryName(final String registryName) {
        this.registryName = registryName;
    }

    @ApiModelProperty("The ID of the bucket that the flow is stored in")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    @ApiModelProperty(value = "The name of the bucket that the flow is stored in", readOnly = true)
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @ApiModelProperty("The ID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(final String flowId) {
        this.flowId = flowId;
    }

    @ApiModelProperty("The name of the flow")
    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @ApiModelProperty("The description of the flow")
    public String getFlowDescription() {
        return flowDescription;
    }

    public void setFlowDescription(String flowDescription) {
        this.flowDescription = flowDescription;
    }

    @ApiModelProperty("The version of the flow")
    public Integer getVersion() {
        return version;
    }

    public void setVersion(final Integer version) {
        this.version = version;
    }

    @ApiModelProperty(readOnly = true,
        value = "The current state of the Process Group, as it relates to the Versioned Flow",
        allowableValues = "LOCALLY_MODIFIED, STALE, LOCALLY_MODIFIED_AND_STALE, UP_TO_DATE, SYNC_FAILURE")
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @ApiModelProperty(readOnly = true, value = "Explanation of why the group is in the specified state")
    public String getStateExplanation() {
        return stateExplanation;
    }

    public void setStateExplanation(String explanation) {
        this.stateExplanation = explanation;
    }
}
