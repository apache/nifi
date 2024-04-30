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

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;

public class VersionedFlowCoordinates {
    private String registryId;
    private String storageLocation;
    private String branch;
    private String bucketId;
    private String flowId;
    private String version;
    private Boolean latest;

    @Schema(description = "The identifier of the Flow Registry that contains the flow")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(String registryId) {
        this.registryId = registryId;
    }

    @Schema(description = "The location of the Flow Registry that stores the flow")
    public String getStorageLocation() {
        return storageLocation;
    }

    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    @Schema(description = "The name of the branch that the flow resides in")
    public String getBranch() {
        return branch;
    }

    public void setBranch(final String branch) {
        this.branch = branch;
    }

    @Schema(description = "The UUID of the bucket that the flow resides in")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @Schema(description = "The UUID of the flow")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    @Schema(description = "The version of the flow")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Schema(description = "Whether or not these coordinates point to the latest version of the flow")
    public Boolean getLatest() {
        return latest;
    }

    public void setLatest(Boolean latest) {
        this.latest = latest;
    }

    @Override
    public int hashCode() {
        return Objects.hash(registryId, storageLocation, branch, bucketId, flowId, version);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof final VersionedFlowCoordinates other)) {
            return false;
        }

        return Objects.equals(storageLocation, other.storageLocation)
                && Objects.equals(branch, other.branch)
                && Objects.equals(bucketId, other.bucketId)
                && Objects.equals(flowId, other.flowId)
                && Objects.equals(version, other.version);
    }

    @Override
    public String toString() {
        return "VersionedFlowCoordinates[branch=" + branch + ", bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version + ", storageLocation=" + storageLocation + "]";
    }
}
