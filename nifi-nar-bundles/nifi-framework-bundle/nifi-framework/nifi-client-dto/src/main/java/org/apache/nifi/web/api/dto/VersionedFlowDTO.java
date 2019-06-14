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

@XmlType(name = "versionedFlow")
public class VersionedFlowDTO {
    public static final String COMMIT_ACTION = "COMMIT";
    public static final String FORCE_COMMIT_ACTION = "FORCE_COMMIT";

    private String registryId = "default"; // placeholder for now.
    private String bucketId;
    private String flowId;
    private String flowName;
    private String description;
    private String comments;
    private String action;

    @ApiModelProperty("The ID of the registry that the flow is tracked to")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(String registryId) {
        this.registryId = registryId;
    }

    @ApiModelProperty("The ID of the bucket where the flow is stored")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @ApiModelProperty(value = "The ID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    @ApiModelProperty("The name of the flow")
    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @ApiModelProperty("A description of the flow")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty("Comments for the changeset")
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @ApiModelProperty(value = "The action being performed", allowableValues = COMMIT_ACTION + ", " + FORCE_COMMIT_ACTION)
    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
