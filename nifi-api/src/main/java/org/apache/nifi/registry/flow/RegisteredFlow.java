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

public class RegisteredFlow {
    private String identifier;
    private String name;
    private String description;
    private String branch;
    private String bucketIdentifier;
    private String bucketName;
    private long createdTimestamp;
    private long lastModifiedTimestamp;
    private FlowRegistryPermissions permissions;
    private long versionCount;
    private RegisteredFlowVersionInfo versionInfo;

    public String getIdentifier() {
        return identifier;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(final String branch) {
        this.branch = branch;
    }

    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    public String getBucketName() {
        return bucketName;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    public FlowRegistryPermissions getPermissions() {
        return permissions;
    }

    public long getVersionCount() {
        return versionCount;
    }

    public RegisteredFlowVersionInfo getVersionInfo() {
        return versionInfo;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setBucketIdentifier(String bucketIdentifier) {
        this.bucketIdentifier = bucketIdentifier;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public void setLastModifiedTimestamp(long lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    public void setPermissions(FlowRegistryPermissions permissions) {
        this.permissions = permissions;
    }

    public void setVersionCount(long versionCount) {
        this.versionCount = versionCount;
    }

    public void setVersionInfo(RegisteredFlowVersionInfo versionInfo) {
        this.versionInfo = versionInfo;
    }

}
