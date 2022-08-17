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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * NiFi Registry implementation of ExtensionBundleMetadata which adds bundleIdentifier to the fields.
 */
public class NiFiRegistryExtensionBundleMetadata extends AbstractExtensionBundleMetadata {

    private static final String SEPARATOR = "::";
    private static final String LOCATION_FORMAT = String.join(SEPARATOR, "%s", "%s", "%s", "%s.nar");

    private final String bundleIdentifier;

    private NiFiRegistryExtensionBundleMetadata(final Builder builder) {
        super(builder.registryIdentifier, builder.group, builder.artifact, builder.version, builder.timestamp);
        this.bundleIdentifier = Validate.notBlank(builder.bundleIdentifier);
    }

    public String getBundleIdentifier() {
        return bundleIdentifier;
    }

    /**
     * @return a location string that will be returned from a NarProvider and passed back to the fetch method,
     *          also serves as the filename used by the NarProvider
     */
    public String toLocationString() {
        return String.format(LOCATION_FORMAT, getGroup(), getArtifact(), getVersion(), getBundleIdentifier());
    }

    /**
     * Creates a new Builder from parsing a location string.
     *
     * @param location the location string
     * @return a builder populated from the location string
     */
    public static Builder fromLocationString(final String location) {
        if (StringUtils.isBlank(location)) {
            throw new IllegalArgumentException("Location is null or blank");
        }

        final String[] locationParts = location.split(SEPARATOR);
        if (locationParts.length != 4) {
            throw new IllegalArgumentException("Invalid location: " + location);
        }

        return new Builder()
                .group(locationParts[0])
                .artifact(locationParts[1])
                .version(locationParts[2])
                .bundleIdentifier(locationParts[3].replace(".nar", ""));
    }

    public static class Builder {

        private String registryIdentifier;
        private String group;
        private String artifact;
        private String version;
        private String bundleIdentifier;
        private long timestamp;

        public Builder registryIdentifier(final String registryIdentifier) {
            this.registryIdentifier = registryIdentifier;
            return this;
        }

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder artifact(final String artifact) {
            this.artifact = artifact;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder bundleIdentifier(final String bundleIdentifier) {
            this.bundleIdentifier = bundleIdentifier;
            return this;
        }

        public Builder timestamp(final long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public NiFiRegistryExtensionBundleMetadata build() {
            return new NiFiRegistryExtensionBundleMetadata(this);
        }
    }
}
