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
    private String bucketId;
    private String flowId;
    private Integer version;
    private Boolean modified;
    private Boolean current;

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

    @ApiModelProperty("The ID of the bucket that the flow is stored in")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    @ApiModelProperty("The ID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(final String flowId) {
        this.flowId = flowId;
    }

    @ApiModelProperty("The version of the flow")
    public Integer getVersion() {
        return version;
    }

    public void setVersion(final Integer version) {
        this.version = version;
    }

    @ApiModelProperty(readOnly=true,
        value = "Whether or not the flow has been modified since it was last synced to the Flow Registry. The value will be null if this information is not yet known.")
    public Boolean getModified() {
        return modified;
    }

    public void setModified(Boolean modified) {
        this.modified = modified;
    }

    @ApiModelProperty(readOnly=true,
        value = "Whether or not this is the most recent version of the flow in the Flow Registry. The value will be null if this information is not yet known.")
    public Boolean getCurrent() {
        return current;
    }

    public void setCurrent(Boolean current) {
        this.current = current;
    }
}
