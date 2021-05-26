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
package org.apache.nifi.registry.db.entity;

import java.util.Date;

public class BundleVersionEntity {

    // Database id for this specific version of an extension bundle
    private String id;

    // Foreign key to the extension bundle this version goes with
    private String bundleId;

    // The bucket id where the bundle is located
    private String bucketId;

    // The group and artifact id for the bundle this version belongs to
    private String groupId;
    private String artifactId;

    // The version of this bundle
    private String version;

    // General info about this version of the bundle
    private Date created;
    private String createdBy;
    private String description;

    // The hex representation of the SHA-256 digest for the binary content of this version
    private String sha256Hex;

    // Indicates whether the SHA-256 was supplied by the client, which means it matched the server's calculation, or was not supplied by the client
    private boolean sha256Supplied;

    // The size of binary content in bytes
    private long contentSize;

    // The version of the system API that the bundle was built against (i.e. nifi-api)
    private String systemApiVersion;

    // Build information
    private String buildTool;
    private String buildFlags;

    private String buildBranch;
    private String buildTag;
    private String buildRevision;

    private Date built;
    private String builtBy;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSha256Hex() {
        return sha256Hex;
    }

    public void setSha256Hex(String sha256Hex) {
        this.sha256Hex = sha256Hex;
    }

    public boolean getSha256Supplied() {
        return sha256Supplied;
    }

    public void setSha256Supplied(boolean sha256Supplied) {
        this.sha256Supplied = sha256Supplied;
    }

    public long getContentSize() {
        return contentSize;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    public String getSystemApiVersion() {
        return systemApiVersion;
    }

    public void setSystemApiVersion(String systemApiVersion) {
        this.systemApiVersion = systemApiVersion;
    }

    public String getBuildTool() {
        return buildTool;
    }

    public void setBuildTool(String buildTool) {
        this.buildTool = buildTool;
    }

    public String getBuildFlags() {
        return buildFlags;
    }

    public void setBuildFlags(String buildFlags) {
        this.buildFlags = buildFlags;
    }

    public String getBuildBranch() {
        return buildBranch;
    }

    public void setBuildBranch(String buildBranch) {
        this.buildBranch = buildBranch;
    }

    public String getBuildTag() {
        return buildTag;
    }

    public void setBuildTag(String buildTag) {
        this.buildTag = buildTag;
    }

    public String getBuildRevision() {
        return buildRevision;
    }

    public void setBuildRevision(String buildRevision) {
        this.buildRevision = buildRevision;
    }

    public Date getBuilt() {
        return built;
    }

    public void setBuilt(Date built) {
        this.built = built;
    }

    public String getBuiltBy() {
        return builtBy;
    }

    public void setBuiltBy(String builtBy) {
        this.builtBy = builtBy;
    }

}
