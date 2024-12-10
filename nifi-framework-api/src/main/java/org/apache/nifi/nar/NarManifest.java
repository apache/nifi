/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Values from a NAR MANIFEST file.
 */
public class NarManifest {

    private final String group;
    private final String id;
    private final String version;

    private final String dependencyGroup;
    private final String dependencyId;
    private final String dependencyVersion;

    private final String buildTag;
    private final String buildRevision;
    private final String buildBranch;
    private final String buildTimestamp;
    private final String buildJdk;
    private final String builtBy;

    private final String createdBy;

    private NarManifest(final Builder builder) {
        this.group = Objects.requireNonNull(builder.group);
        this.id = Objects.requireNonNull(builder.id);
        this.version = Objects.requireNonNull(builder.version);

        this.dependencyGroup = builder.dependencyGroup;
        this.dependencyId = builder.dependencyId;
        this.dependencyVersion = builder.dependencyVersion;

        this.buildTag = builder.buildTag;
        this.buildRevision = builder.buildRevision;
        this.buildBranch = builder.buildBranch;
        this.buildTimestamp = builder.buildTimestamp;
        this.buildJdk = builder.buildJdk;
        this.builtBy = builder.builtBy;

        this.createdBy = builder.createdBy;
    }

    public String getGroup() {
        return group;
    }

    public String getId() {
        return id;
    }

    public String getVersion() {
        return version;
    }

    public String getDependencyGroup() {
        return dependencyGroup;
    }

    public String getDependencyId() {
        return dependencyId;
    }

    public String getDependencyVersion() {
        return dependencyVersion;
    }

    public String getBuildTag() {
        return buildTag;
    }

    public String getBuildRevision() {
        return buildRevision;
    }

    public String getBuildBranch() {
        return buildBranch;
    }

    public String getBuildTimestamp() {
        return buildTimestamp;
    }

    public String getBuildJdk() {
        return buildJdk;
    }

    public String getBuiltBy() {
        return builtBy;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public BundleCoordinate getCoordinate() {
        return new BundleCoordinate(group, id, version);
    }

    public BundleCoordinate getDependencyCoordinate() {
        if (dependencyId == null) {
            return null;
        }
        return new BundleCoordinate(dependencyGroup, dependencyId, dependencyVersion);
    }

    /**
     * Creates a NarManifest from reading a File containing the contents of a NAR with a META-INF/MANIFEST entry.
     *
     * @param narFile the NAR file
     * @return the manifest instance
     * @throws IOException if unable to read the NAR file
     */
    public static NarManifest fromNarFile(final File narFile) throws IOException {
        try (final InputStream inputStream = new FileInputStream(narFile)) {
            return fromInputStream(inputStream);
        }
    }

    /**
     * Creates a NarManifest from reading an InputStream containing the contents of a NAR with a META-INF/MANIFEST entry.
     *
     * @param narInputStream the input stream containing the NAR contents
     * @return the manifest instance
     * @throws IOException if unable to read the NAR file
     */
    static NarManifest fromInputStream(final InputStream narInputStream) throws IOException {
        try (final JarInputStream jarInputStream = new JarInputStream(narInputStream)) {
            final Manifest manifest = jarInputStream.getManifest();
            return fromManifest(manifest);
        }
    }

    /**
     * Creates a NarManifest from a Manifest instance that was extracted from a NAR File or InputStream.
     *
     * @param manifest the manifest from the NAR
     * @return the manifest instance
     */
    static NarManifest fromManifest(final Manifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("NAR content is missing required META-INF/MANIFEST entry");
        }

        final Attributes attributes = manifest.getMainAttributes();

        return builder()
                .group(attributes.getValue(NarManifestEntry.NAR_GROUP.getEntryName()))
                .id(attributes.getValue(NarManifestEntry.NAR_ID.getEntryName()))
                .version(attributes.getValue(NarManifestEntry.NAR_VERSION.getEntryName()))
                .dependencyGroup(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_GROUP.getEntryName()))
                .dependencyId(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_ID.getEntryName()))
                .dependencyVersion(attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_VERSION.getEntryName()))
                .buildTag(attributes.getValue(NarManifestEntry.BUILD_TAG.getEntryName()))
                .buildRevision(attributes.getValue(NarManifestEntry.BUILD_REVISION.getEntryName()))
                .buildBranch(attributes.getValue(NarManifestEntry.BUILD_BRANCH.getEntryName()))
                .buildTimestamp(attributes.getValue(NarManifestEntry.BUILD_TIMESTAMP.getEntryName()))
                .buildJdk(attributes.getValue(NarManifestEntry.BUILD_JDK.getEntryName()))
                .builtBy(attributes.getValue(NarManifestEntry.BUILT_BY.getEntryName()))
                .createdBy(attributes.getValue(NarManifestEntry.CREATED_BY.getEntryName()))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String group;
        private String id;
        private String version;
        private String dependencyGroup;
        private String dependencyId;
        private String dependencyVersion;
        private String buildTag;
        private String buildRevision;
        private String buildBranch;
        private String buildTimestamp;
        private String buildJdk;
        private String builtBy;
        private String createdBy;

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        public Builder version(final String version) {
            this.version = version;
            return this;
        }

        public Builder dependencyGroup(final String dependencyGroup) {
            this.dependencyGroup = dependencyGroup;
            return this;
        }

        public Builder dependencyId(final String dependencyId) {
            this.dependencyId = dependencyId;
            return this;
        }

        public Builder dependencyVersion(final String dependencyVersion) {
            this.dependencyVersion = dependencyVersion;
            return this;
        }

        public Builder buildTag(final String buildTag) {
            this.buildTag = buildTag;
            return this;
        }

        public Builder buildRevision(final String buildRevision) {
            this.buildRevision = buildRevision;
            return this;
        }

        public Builder buildBranch(final String buildBranch) {
            this.buildBranch = buildBranch;
            return this;
        }

        public Builder buildTimestamp(final String buildTimestamp) {
            this.buildTimestamp = buildTimestamp;
            return this;
        }

        public Builder buildJdk(final String buildJdk) {
            this.buildJdk = buildJdk;
            return this;
        }

        public Builder builtBy(final String builtBy) {
            this.builtBy = builtBy;
            return this;
        }

        public Builder createdBy(final String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public NarManifest build() {
            return new NarManifest(this);
        }
    }
}
