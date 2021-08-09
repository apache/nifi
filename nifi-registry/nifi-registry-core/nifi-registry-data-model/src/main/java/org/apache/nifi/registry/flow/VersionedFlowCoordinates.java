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

package org.apache.nifi.registry.flow;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;

public class VersionedFlowCoordinates {
    private String registryUrl;
    private String bucketId;
    private String flowId;
    private int version;
    private Boolean latest;

    @ApiModelProperty("The URL of the Flow Registry that contains the flow")
    public String getRegistryUrl() {
        return registryUrl;
    }

    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @ApiModelProperty("The UUID of the bucket that the flow resides in")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @ApiModelProperty("The UUID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    @ApiModelProperty("The version of the flow")
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @ApiModelProperty("Whether or not these coordinates point to the latest version of the flow")
    public Boolean getLatest() {
        return latest;
    }

    public void setLatest(Boolean latest) {
        this.latest = latest;
    }

    @Override
    public int hashCode() {
        return Objects.hash(registryUrl, bucketId, flowId, version);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof VersionedFlowCoordinates)) {
            return false;
        }

        final VersionedFlowCoordinates other = (VersionedFlowCoordinates) obj;
        return Objects.equals(registryUrl, other.registryUrl) && Objects.equals(bucketId, other.bucketId) && Objects.equals(flowId, other.flowId) && Objects.equals(version, other.version);
    }

    @Override
    public String toString() {
        return "VersionedFlowCoordinates[bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version + ", registryUrl=" + registryUrl + "]";
    }
}
