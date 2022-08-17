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
import org.apache.nifi.registry.extension.BundleCoordinate;

import java.util.Objects;

public class StandardBundleCoordinate implements BundleCoordinate {

    private final String bucketId;
    private final String groupId;
    private final String artifactId;

    private StandardBundleCoordinate(final Builder builder) {
        this.bucketId = builder.bucketId;
        this.groupId = builder.groupId;
        this.artifactId = builder.artifactId;
        Validate.notBlank(this.bucketId, "Bucket Id is required");
        Validate.notBlank(this.groupId, "Group Id is required");
        Validate.notBlank(this.artifactId, "Artifact Id is required");
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StandardBundleCoordinate that = (StandardBundleCoordinate) o;
        return bucketId.equals(that.bucketId)
                && groupId.equals(that.groupId)
                && artifactId.equals(that.artifactId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, groupId, artifactId);
    }

    public static class Builder {

        private String bucketId;
        private String groupId;
        private String artifactId;

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


        public StandardBundleCoordinate build() {
            return new StandardBundleCoordinate(this);
        }

    }
}
