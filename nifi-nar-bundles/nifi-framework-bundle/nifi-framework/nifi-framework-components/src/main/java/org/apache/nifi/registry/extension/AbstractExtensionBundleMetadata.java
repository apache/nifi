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
package org.apache.nifi.registry.extension;

import org.apache.commons.lang3.Validate;

import java.util.Objects;

/**
 * Base class for implementations of ExtensionBundleMetadata.
 */
public class AbstractExtensionBundleMetadata implements ExtensionBundleMetadata {

    private final String registryIdentifier;
    private final String group;
    private final String artifact;
    private final String version;
    private final long timestamp;

    public AbstractExtensionBundleMetadata(final String registryIdentifier, final String group, final String artifact, final String version, final long timestamp) {
        this.registryIdentifier = Validate.notBlank(registryIdentifier);
        this.group = Validate.notBlank(group);
        this.artifact = Validate.notBlank(artifact);
        this.version = Validate.notBlank(version);
        this.timestamp = timestamp;
    }

    @Override
    public String getRegistryIdentifier() {
        return registryIdentifier;
    }

    @Override
    public String getGroup() {
        return group;
    }

    @Override
    public String getArtifact() {
        return artifact;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, artifact, version, registryIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AbstractExtensionBundleMetadata otherMetadata = (AbstractExtensionBundleMetadata) o;
        return Objects.equals(group, otherMetadata.group)
                && Objects.equals(artifact, otherMetadata.artifact)
                && Objects.equals(version, otherMetadata.version)
                && Objects.equals(registryIdentifier, otherMetadata.registryIdentifier);
    }

}