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

import java.util.Objects;

/**
 * Contextual information given to a {@link NarPersistenceProvider} when saving a NAR.
 */
public class NarPersistenceContext {

    private final NarManifest manifest;
    private final NarSource source;
    private final String sourceIdentifier;
    private final boolean clusterCoordinator;

    private NarPersistenceContext(final Builder builder) {
        this.manifest = Objects.requireNonNull(builder.manifest, "NAR Manifest is required");
        this.source = Objects.requireNonNull(builder.source, "NAR Source is required");
        this.sourceIdentifier = Objects.requireNonNull(builder.sourceIdentifier, "Source Identifier is required");
        this.clusterCoordinator = builder.clusterCoordinator;
    }

    public NarManifest getManifest() {
        return manifest;
    }

    public NarSource getSource() {
        return source;
    }

    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public boolean isClusterCoordinator() {
        return clusterCoordinator;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private NarManifest manifest;
        private NarSource source;
        private String sourceIdentifier;
        private boolean clusterCoordinator;

        public Builder manifest(final NarManifest manifest) {
            this.manifest = manifest;
            return this;
        }

        public Builder source(final NarSource source) {
            this.source = source;
            return this;
        }

        public Builder sourceIdentifier(final String sourceIdentifier) {
            this.sourceIdentifier = sourceIdentifier;
            return this;
        }

        public Builder clusterCoordinator(final boolean clusterCoordinator) {
            this.clusterCoordinator = clusterCoordinator;
            return this;
        }

        public NarPersistenceContext build() {
            return new NarPersistenceContext(this);
        }
    }
}
