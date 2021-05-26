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
package org.apache.nifi.registry.provider.extension;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;

import java.util.Objects;

public class StandardBundleVersionCoordinate implements BundleVersionCoordinate {

    private final String bucketId;
    private final String groupId;
    private final String artifactId;
    private final String version;
    private final BundleVersionType type;

    private StandardBundleVersionCoordinate(final Builder builder) {
        this.bucketId = builder.bucketId;
        this.groupId = builder.groupId;
        this.artifactId = builder.artifactId;
        this.version = builder.version;
        this.type = builder.type;
        Validate.notBlank(this.bucketId, "Bucket Id is required");
        Validate.notBlank(this.groupId, "Group Id is required");
        Validate.notBlank(this.artifactId, "Artifact Id is required");
        Validate.notBlank(this.version, "Version is required");
        Validate.notNull(this.type, "BundleVersionType is required");
    }

    @Override
    public String getBucketId() {
        return bucketId;
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public String getArtifactId() {
        return artifactId;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public BundleVersionType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StandardBundleVersionCoordinate that = (StandardBundleVersionCoordinate) o;
        return bucketId.equals(that.bucketId)
                && groupId.equals(that.groupId)
                && artifactId.equals(that.artifactId)
                && version.equals(that.version)
                && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, groupId, artifactId, version, type);
    }

    @Override
    public String toString() {
        return "BundleVersionCoordinate [" +
                "bucketId='" + bucketId + '\'' +
                ", groupId='" + groupId + '\'' +
                ", artifactId='" + artifactId + '\'' +
                ", version='" + version + '\'' +
                ", type=" + type +
                ']';
    }

    public static class Builder {

        private String bucketId;
        private String groupId;
        private String artifactId;
        private String version;
        private BundleVersionType type;

        public Builder bucketId(final String bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public Builder groupId(final String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder artifactId(final String artifactId) {
            this.artifactId = artifactId;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder type(final BundleVersionType type) {
            this.type = type;
            return this;
        }

        public StandardBundleVersionCoordinate build() {
            return new StandardBundleVersionCoordinate(this);
        }

    }
}
