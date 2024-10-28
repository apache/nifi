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

import java.io.File;
import java.util.Objects;

/**
 * Represents a NAR that exists in the NAR Manager.
 */
public class NarNode {

    private final String identifier;
    private final File narFile;
    private final String narFileDigest;
    private final NarManifest manifest;

    private final NarSource source;
    private final String sourceIdentifier;

    private volatile NarState state;
    private volatile String failureMessage;

    private NarNode(final Builder builder) {
        this.identifier = Objects.requireNonNull(builder.identifier);
        this.narFile = Objects.requireNonNull(builder.narFile);
        this.narFileDigest = Objects.requireNonNull(builder.narFileDigest);
        this.manifest = Objects.requireNonNull(builder.manifest);
        this.source = Objects.requireNonNull(builder.source);
        this.sourceIdentifier = Objects.requireNonNull(builder.sourceIdentifier);
        this.state = Objects.requireNonNull(builder.state);
    }

    public String getIdentifier() {
        return identifier;
    }

    public File getNarFile() {
        return narFile;
    }

    public String getNarFileDigest() {
        return narFileDigest;
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

    public NarState getState() {
        return state;
    }

    public void setState(final NarState state) {
        if (state == null) {
            throw new IllegalArgumentException("NAR State cannot be null");
        }
        this.state = state;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailure(final Throwable failure) {
        this.state = NarState.FAILED;
        this.failureMessage = "%s - %s".formatted(failure.getClass().getSimpleName(), failure.getMessage());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarNode narNode = (NarNode) o;
        return Objects.equals(identifier, narNode.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String identifier;
        private File narFile;
        private String narFileDigest;
        private NarManifest manifest;
        private NarSource source;
        private String sourceIdentifier;
        private NarState state;

        public Builder identifier(final String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder narFile(final File narFile) {
            this.narFile = narFile;
            return this;
        }

        public Builder narFileDigest(final String narFileDigest) {
            this.narFileDigest = narFileDigest;
            return this;
        }

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

        public Builder state(final NarState state) {
            this.state = state;
            return this;
        }

        public NarNode build() {
            return new NarNode(this);
        }
    }
}
