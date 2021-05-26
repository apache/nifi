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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.link.LinkableEntity;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

@ApiModel
@XmlRootElement
public class BundleVersionMetadata extends LinkableEntity implements Comparable<BundleVersionMetadata> {

    @NotBlank
    private String id;

    @NotBlank
    private String bundleId;

    @NotBlank
    private String bucketId;

    // read-only, populated on response
    private String groupId;

    // read-only, populated on response
    private String artifactId;

    @NotBlank
    private String version;

    @Min(1)
    private long timestamp;

    @NotBlank
    private String author;

    private String description;

    @NotBlank
    private String sha256;

    @NotNull
    private Boolean sha256Supplied;

    @NotNull
    @Min(0)
    private long contentSize;

    @NotBlank
    private String systemApiVersion;

    @Valid
    @NotNull
    private BuildInfo buildInfo;


    @ApiModelProperty(value = "The id of this version of the extension bundle")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(value = "The id of the extension bundle this version is for")
    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    @ApiModelProperty(value = "The id of the bucket the extension bundle belongs to", required = true)
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

    @ApiModelProperty(value = "The version of the extension bundle")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @ApiModelProperty(value = "The timestamp of the create date of this version")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty(value = "The identity that created this version")
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @ApiModelProperty(value = "The description for this version")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "The hex representation of the SHA-256 digest of the binary content for this version")
    public String getSha256() {
        return sha256;
    }

    public void setSha256(String sha256) {
        this.sha256 = sha256;
    }

    @ApiModelProperty(value = "Whether or not the client supplied a SHA-256 when uploading the bundle")
    public Boolean getSha256Supplied() {
        return sha256Supplied;
    }

    public void setSha256Supplied(Boolean sha256Supplied) {
        this.sha256Supplied = sha256Supplied;
    }

    @ApiModelProperty(value = "The size of the binary content for this version in bytes")
    public long getContentSize() {
        return contentSize;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    @ApiModelProperty(value = "The version of the system API that this bundle version was built against")
    public String getSystemApiVersion() {
        return systemApiVersion;
    }

    public void setSystemApiVersion(String systemApiVersion) {
        this.systemApiVersion = systemApiVersion;
    }

    @ApiModelProperty(value = "The build information about this version")
    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    public void setBuildInfo(BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
    }

    @Override
    public int compareTo(final BundleVersionMetadata o) {
        return o == null ? -1 : version.compareTo(o.version);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final BundleVersionMetadata other = (BundleVersionMetadata) obj;
        return Objects.equals(this.id, other.id);
    }
}
