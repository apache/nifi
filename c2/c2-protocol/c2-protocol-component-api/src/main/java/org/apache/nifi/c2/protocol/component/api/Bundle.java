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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.Objects;

public class Bundle implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_GROUP = "default";
    public static final String DEFAULT_ARTIFACT = "unknown";
    public static final String DEFAULT_VERSION = "unversioned";

    private String group;
    private String artifact;
    private String version;
    private ComponentManifest componentManifest;

    public Bundle() {
    }

    public Bundle(String group, String artifact, String version) {
        this.group = group;
        this.artifact = artifact;
        this.version = version;
    }

    public static Bundle defaultBundle() {
        return new Bundle(DEFAULT_GROUP, DEFAULT_ARTIFACT, DEFAULT_VERSION);
    }

    @Schema(description = "The group id of the bundle")
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Schema(description = "The artifact id of the bundle")
    public String getArtifact() {
        return artifact;
    }

    public void setArtifact(String artifact) {
        this.artifact = artifact;
    }

    @Schema(description = "The version of the bundle artifact")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Schema(description = "The full specification of the bundle contents")
    public ComponentManifest getComponentManifest() {
        return componentManifest;
    }

    public void setComponentManifest(ComponentManifest componentManifest) {
        this.componentManifest = componentManifest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Bundle bundle = (Bundle) o;

        return Objects.equals(group, bundle.group)
                && Objects.equals(artifact, bundle.artifact)
                && Objects.equals(version, bundle.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version);
    }
}
