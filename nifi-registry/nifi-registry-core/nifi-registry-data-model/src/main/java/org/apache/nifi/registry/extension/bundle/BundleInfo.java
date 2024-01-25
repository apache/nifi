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
package org.apache.nifi.registry.extension.bundle;

import io.swagger.v3.oas.annotations.media.Schema;

public class BundleInfo {

    private String bucketId;
    private String bucketName;

    private String bundleId;
    private BundleType bundleType;

    private String groupId;
    private String artifactId;
    private String version;

    private String systemApiVersion;

    @Schema(description = "The id of the bucket where the bundle is located")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    @Schema(description = "The name of the bucket where the bundle is located")
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Schema(description = "The id of the bundle")
    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    @Schema(description = "The type of bundle (i.e. a NiFi NAR vs MiNiFi CPP)")
    public BundleType getBundleType() {
        return bundleType;
    }

    public void setBundleType(BundleType bundleType) {
        this.bundleType = bundleType;
    }

    @Schema(description = "The group id of the bundle")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The artifact id of the bundle")
    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    @Schema(description = "The version of the bundle")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Schema(description = "The version of the system API the bundle was built against")
    public String getSystemApiVersion() {
        return systemApiVersion;
    }

    public void setSystemApiVersion(String systemApiVersion) {
        this.systemApiVersion = systemApiVersion;
    }
}
