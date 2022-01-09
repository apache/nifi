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
package org.apache.nifi.registry.bundle.model;

import static org.apache.nifi.registry.bundle.util.BundleUtils.validateNotBlank;

/**
 * The identifier of an extension bundle (i.e group + artifact + version).
 */
public class BundleIdentifier {

    private final String groupId;
    private final String artifactId;
    private final String version;

    private final String identifier;

    public BundleIdentifier(final String groupId, final String artifactId, final String version) {
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.version = version;
        validateNotBlank("Group Id", this.groupId);
        validateNotBlank("Artifact Id", this.artifactId);
        validateNotBlank("Version", this.version);

        this.identifier = this.groupId + ":" + this.artifactId + ":" + this.version;
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

    public final String getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return identifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof BundleIdentifier)) {
            return false;
        }

        final BundleIdentifier other = (BundleIdentifier) obj;
        return getIdentifier().equals(other.getIdentifier());
    }

    @Override
    public int hashCode() {
        return 37 * this.identifier.hashCode();
    }

}
