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

package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

public class VersionedFlowCoordinates {
    private String registryId;
    private String storageLocation;
    private String registryUrl;
    private String bucketId;
    private String flowId;
    private int version;
    private Boolean latest;

    @ApiModelProperty("The identifier of the Flow Registry that contains the flow")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(String registryId) {
        this.registryId = registryId;
    }

    @ApiModelProperty("The location of the Flow Registry that stores the flow")
    public String getStorageLocation() {
        return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    @Deprecated
    @ApiModelProperty("The URL of the Flow Registry that contains the flow")
    public String getRegistryUrl() {
        return registryUrl;
    }

    @Deprecated
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
        return Objects.hash(registryId, storageLocation, registryUrl, bucketId, flowId, version);
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
