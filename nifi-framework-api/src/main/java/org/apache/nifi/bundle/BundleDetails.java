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
package org.apache.nifi.bundle;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Metadata about a bundle.
 */
public class BundleDetails {

    private final File workingDirectory;

    private final BundleCoordinate coordinate;
    private final BundleCoordinate dependencyCoordinate;

    private final String buildTag;
    private final String buildRevision;
    private final String buildBranch;
    private final String buildTimestamp;
    private final String buildJdk;
    private final String builtBy;

    private BundleDetails(final Builder builder) {
        this.workingDirectory = builder.workingDirectory;
        this.coordinate = builder.coordinate;
        this.dependencyCoordinate = builder.dependencyCoordinate;

        this.buildTag = builder.buildTag;
        this.buildRevision = builder.buildRevision;
        this.buildBranch = builder.buildBranch;
        this.buildTimestamp = builder.buildTimestamp;
        this.buildJdk = builder.buildJdk;
        this.builtBy = builder.builtBy;

        if (this.coordinate == null) {
            if (this.workingDirectory == null) {
                throw new IllegalStateException("Coordinate cannot be null");
            } else {
                throw new IllegalStateException("Coordinate cannot be null for " + this.workingDirectory.getAbsolutePath());
            }
        }

        if (this.workingDirectory == null) {
            throw new IllegalStateException("Working directory cannot be null for " + this.coordinate.getId());
        }
    }

    public File getWorkingDirectory() {
        return workingDirectory;
    }

    public BundleCoordinate getCoordinate() {
        return coordinate;
    }

    public BundleCoordinate getDependencyCoordinate() {
        return dependencyCoordinate;
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

    @Override
    public String toString() {
        return coordinate.toString();
    }

    public Date getBuildTimestampDate() {
        if (buildTimestamp != null && !buildTimestamp.isEmpty()) {
            try {
                SimpleDateFormat buildTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                Date buildTimestampDate = buildTimestampFormat.parse(buildTimestamp);
                return buildTimestampDate;
            } catch (ParseException parseEx) {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Builder for NarDetails.
     */
    public static class Builder {

        private File workingDirectory;

        private BundleCoordinate coordinate;
        private BundleCoordinate dependencyCoordinate;

        private String buildTag;
        private String buildRevision;
        private String buildBranch;
        private String buildTimestamp;
        private String buildJdk;
        private String builtBy;

        public Builder workingDir(final File workingDirectory) {
            this.workingDirectory = workingDirectory;
            return this;
        }

        public Builder coordinate(final BundleCoordinate coordinate) {
            this.coordinate = coordinate;
            return this;
        }

        public Builder dependencyCoordinate(final BundleCoordinate dependencyCoordinate) {
            this.dependencyCoordinate = dependencyCoordinate;
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

        public BundleDetails build() {
            return new BundleDetails(this);
        }
    }

}
