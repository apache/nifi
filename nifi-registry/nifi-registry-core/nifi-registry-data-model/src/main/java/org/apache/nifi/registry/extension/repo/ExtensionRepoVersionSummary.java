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
package org.apache.nifi.registry.extension.repo;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.registry.link.LinkableEntity;

import java.util.Comparator;
import java.util.Objects;

@XmlRootElement
public class ExtensionRepoVersionSummary extends LinkableEntity implements Comparable<ExtensionRepoVersionSummary> {

    private String bucketName;

    private String groupId;
    private String artifactId;
    private String version;

    private String author;
    private Long timestamp;

    @Schema(description = "The bucket name")
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(final String bucketName) {
        this.bucketName = bucketName;
    }

    @Schema(description = "The group id")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The artifact id")
    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(final String artifactId) {
        this.artifactId = artifactId;
    }

    @Schema(description = "The version")
    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Schema(description = "The identity of the user that created this version")
    public String getAuthor() {
        return author;
    }

    public void setAuthor(final String author) {
        this.author = author;
    }

    @Schema(description = "The timestamp of when this version was created")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(final ExtensionRepoVersionSummary o) {
        return Comparator.comparing(ExtensionRepoVersionSummary::getVersion)
                .thenComparing(ExtensionRepoVersionSummary::getArtifactId)
                .thenComparing(ExtensionRepoVersionSummary::getGroupId)
                .thenComparing(ExtensionRepoVersionSummary::getBucketName)
                .compare(this, o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.bucketName, this.groupId, this.artifactId, this.version, this.author, this.timestamp);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final ExtensionRepoVersionSummary other = (ExtensionRepoVersionSummary) obj;

        return Objects.equals(this.getBucketName(), other.getBucketName())
                && Objects.equals(this.getGroupId(), other.getGroupId())
                && Objects.equals(this.getArtifactId(), other.getArtifactId())
                && Objects.equals(this.getVersion(), other.getVersion())
                && Objects.equals(this.getAuthor(), other.getAuthor())
                && Objects.equals(this.getVersion(), other.getVersion());
    }

}
