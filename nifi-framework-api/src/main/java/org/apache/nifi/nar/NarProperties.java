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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Properties about a NAR that are persisted by the {@link NarPersistenceProvider}.
 */
public class NarProperties {

    private final String sourceType;
    private final String sourceId;

    private final String narGroup;
    private final String narId;
    private final String narVersion;

    private final String narDependencyGroup;
    private final String narDependencyId;
    private final String narDependencyVersion;

    private final Instant installed;

    private NarProperties(final Builder builder) {
        this.sourceType = Objects.requireNonNull(builder.sourceType);
        this.sourceId = builder.sourceId;

        this.narGroup = Objects.requireNonNull(builder.narGroup);
        this.narId = Objects.requireNonNull(builder.narId);
        this.narVersion = Objects.requireNonNull(builder.narVersion);

        this.narDependencyGroup = builder.narDependencyGroup;
        this.narDependencyId = builder.narDependencyId;
        this.narDependencyVersion = builder.narDependencyVersion;

        if (!((narDependencyGroup == null && narDependencyId == null && narDependencyVersion == null)
                || (narDependencyGroup != null && narDependencyId != null && narDependencyVersion != null))) {
            throw new IllegalArgumentException("Must provided all NAR dependency values, or no dependency values");
        }

        this.installed = Objects.requireNonNull(builder.installed);
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getNarGroup() {
        return narGroup;
    }

    public String getNarId() {
        return narId;
    }

    public String getNarVersion() {
        return narVersion;
    }

    public String getNarDependencyGroup() {
        return narDependencyGroup;
    }

    public String getNarDependencyId() {
        return narDependencyId;
    }

    public String getNarDependencyVersion() {
        return narDependencyVersion;
    }

    public Instant getInstalled() {
        return installed;
    }

    public BundleCoordinate getCoordinate() {
        return new BundleCoordinate(narGroup, narId, narVersion);
    }

    public Optional<BundleCoordinate> getDependencyCoordinate() {
        if (narDependencyGroup == null) {
            return Optional.empty();
        }
        return Optional.of(new BundleCoordinate(narDependencyGroup, narDependencyId, narDependencyVersion));
    }

    /**
     * @return a Properties instance containing the key/value pairs of the NarProperties
     */
    public Properties toProperties() {
        final Properties properties = new Properties();
        properties.put(NarProperty.SOURCE_TYPE.getKey(), sourceType);
        if (sourceId != null) {
            properties.put(NarProperty.SOURCE_ID.getKey(), sourceId);
        }

        properties.put(NarProperty.NAR_GROUP.getKey(), narGroup);
        properties.put(NarProperty.NAR_ID.getKey(), narId);
        properties.put(NarProperty.NAR_VERSION.getKey(), narVersion);

        if (narDependencyGroup != null) {
            properties.put(NarProperty.NAR_DEPENDENCY_GROUP.getKey(), narDependencyGroup);
            properties.put(NarProperty.NAR_DEPENDENCY_ID.getKey(), narDependencyId);
            properties.put(NarProperty.NAR_DEPENDENCY_VERSION.getKey(), narDependencyVersion);
        }

        properties.put(NarProperty.INSTALLED.getKey(), installed.toString());
        return properties;
    }

    /**
     * Reads an InputStream containing the Properties representation of the NAR Properties and returns the NarProperties instance.
     *
     * @param inputStream the input stream
     * @return the NAR properties instance
     * @throws IOException if unable to read the input stream
     */
    public static NarProperties parse(final InputStream inputStream) throws IOException {
        final Properties properties = new Properties();
        properties.load(inputStream);

        final String installedValue = properties.getProperty(NarProperty.INSTALLED.getKey());
        final Instant installed = Instant.parse(installedValue);

        return builder()
                .sourceType(properties.getProperty(NarProperty.SOURCE_TYPE.getKey()))
                .sourceId(properties.getProperty(NarProperty.SOURCE_ID.getKey()))
                .narGroup(properties.getProperty(NarProperty.NAR_GROUP.getKey()))
                .narId(properties.getProperty(NarProperty.NAR_ID.getKey()))
                .narVersion(properties.getProperty(NarProperty.NAR_VERSION.getKey()))
                .narDependencyGroup(properties.getProperty(NarProperty.NAR_DEPENDENCY_GROUP.getKey()))
                .narDependencyId(properties.getProperty(NarProperty.NAR_DEPENDENCY_ID.getKey()))
                .narDependencyVersion(properties.getProperty(NarProperty.NAR_DEPENDENCY_VERSION.getKey()))
                .installed(installed)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String sourceType;
        private String sourceId;
        private String narGroup;
        private String narId;
        private String narVersion;
        private String narDependencyGroup;
        private String narDependencyId;
        private String narDependencyVersion;
        private Instant installed;

        public Builder sourceType(final String sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder sourceId(final String sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        public Builder narGroup(final String narGroup) {
            this.narGroup = narGroup;
            return this;
        }

        public Builder narId(final String narId) {
            this.narId = narId;
            return this;
        }

        public Builder narVersion(final String narVersion) {
            this.narVersion = narVersion;
            return this;
        }

        public Builder narDependencyGroup(final String narDependencyGroup) {
            this.narDependencyGroup = narDependencyGroup;
            return this;
        }

        public Builder narDependencyId(final String narDependencyId) {
            this.narDependencyId = narDependencyId;
            return this;
        }

        public Builder narDependencyVersion(final String narDependencyVersion) {
            this.narDependencyVersion = narDependencyVersion;
            return this;
        }

        public Builder installed(final Instant installed) {
            this.installed = installed;
            return this;
        }

        public NarProperties build() {
            return new NarProperties(this);
        }
    }
}
