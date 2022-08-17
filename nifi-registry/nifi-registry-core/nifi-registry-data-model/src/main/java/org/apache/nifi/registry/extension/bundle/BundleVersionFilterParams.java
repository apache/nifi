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

/**
 * Filter parameters for extension bundle versions.
 *
 * Any combination of fields may be populated to filter on the provided values.
 *
 * Note: This class is currently not part of the REST API so it doesn't not have the Swagger annotations, but it is used
 * in the service layer and client to pass around params.
 */
public class BundleVersionFilterParams {

    private static final BundleVersionFilterParams EMPTY_PARAMS = new Builder().build();

    private final String groupId;
    private final String artifactId;
    private final String version;

    private BundleVersionFilterParams(final Builder builder) {
        this.groupId = builder.groupId;
        this.artifactId = builder.artifactId;
        this.version = builder.version;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public static BundleVersionFilterParams of(final String groupId, final String artifactId, final String version) {
        return new Builder().group(groupId).artifact(artifactId).version(version).build();
    }

    public static BundleVersionFilterParams empty() {
        return EMPTY_PARAMS;
    }

    public static class Builder {

        private String groupId;
        private String artifactId;
        private String version;

        public Builder group(final String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder artifact(final String artifactId) {
            this.artifactId = artifactId;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public BundleVersionFilterParams build() {
            return new BundleVersionFilterParams(this);
        }
    }

}
