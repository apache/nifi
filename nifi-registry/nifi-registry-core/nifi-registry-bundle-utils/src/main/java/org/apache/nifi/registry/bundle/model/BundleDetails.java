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


import org.apache.nifi.registry.bundle.extract.BundleExtractor;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.apache.nifi.registry.extension.component.manifest.Extension;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.registry.bundle.util.BundleUtils.validateNotNull;

/**
 * Details for a given bundle which are obtained from a given {@link BundleExtractor}.
 */
public class BundleDetails {

    private final BundleIdentifier bundleIdentifier;
    private final Set<BundleIdentifier> dependencies;

    private final String systemApiVersion;

    private final Set<Extension> extensions;
    private final Map<String,String> additionalDetails;

    private final BuildInfo buildInfo;

    private BundleDetails(final Builder builder) {
        this.bundleIdentifier = builder.bundleIdentifier;
        this.dependencies = Collections.unmodifiableSet(new HashSet<>(builder.dependencies));
        this.extensions = Collections.unmodifiableSet(new HashSet<>(builder.extensions));
        this.additionalDetails = Collections.unmodifiableMap(new HashMap<>(builder.additionalDetails));
        this.systemApiVersion = builder.systemApiVersion;
        this.buildInfo = builder.buildInfo;

        validateNotNull("Bundle Coordinate", this.bundleIdentifier);
        validateNotNull("Dependency Coordinates", this.dependencies);
        validateNotNull("Extension Details", this.extensions);
        validateNotNull("System API Version", this.systemApiVersion);
        validateNotNull("Build Details", this.buildInfo);
    }

    public BundleIdentifier getBundleIdentifier() {
        return bundleIdentifier;
    }

    public Set<BundleIdentifier> getDependencies() {
        return dependencies;
    }

    public String getSystemApiVersion() {
        return systemApiVersion;
    }

    public Set<Extension> getExtensions() {
        return extensions;
    }

    public Map<String, String> getAdditionalDetails() {
        return additionalDetails;
    }

    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    /**
     * Builder for creating instances of BundleDetails.
     */
    public static class Builder {

        private BundleIdentifier bundleIdentifier;
        private Set<BundleIdentifier> dependencies = new HashSet<>();
        private Set<Extension> extensions = new HashSet<>();
        private Map<String,String> additionalDetails = new HashMap<>();
        private BuildInfo buildInfo;
        private String systemApiVersion;

        public Builder coordinate(final BundleIdentifier bundleIdentifier) {
            this.bundleIdentifier = bundleIdentifier;
            return this;
        }

        public Builder addDependencyCoordinate(final BundleIdentifier dependencyCoordinate) {
            if (dependencyCoordinate != null) {
                this.dependencies.add(dependencyCoordinate);
            }
            return this;
        }

        public Builder systemApiVersion(final String systemApiVersion) {
            this.systemApiVersion = systemApiVersion;
            return this;
        }

        public Builder addExtension(final Extension extension) {
            if (extension != null) {
                this.extensions.add(extension);
            }
            return this;
        }

        public Builder addExtensions(final List<Extension> extensions) {
            if (extensions != null) {
                this.extensions.addAll(extensions);
            }
            return this;
        }

        public Builder addAdditionalDetails(final String extensionName, final String additionalDetails) {
            this.additionalDetails.put(extensionName, additionalDetails);
            return this;
        }

        public Builder buildInfo(final BuildInfo buildInfo) {
            this.buildInfo = buildInfo;
            return this;
        }

        public BundleDetails build() {
            return new BundleDetails(this);
        }
    }

}
