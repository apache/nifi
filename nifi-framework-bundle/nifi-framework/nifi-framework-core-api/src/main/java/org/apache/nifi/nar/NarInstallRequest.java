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

import java.io.InputStream;
import java.util.Objects;

/**
 * Request to install a NAR in the {@link NarManager}.
 */
public class NarInstallRequest {

    private final NarSource source;
    private final String sourceIdentifier;
    private final InputStream inputStream;

    private NarInstallRequest(final Builder builder) {
        this.source = Objects.requireNonNull(builder.source);
        this.sourceIdentifier = Objects.requireNonNull(builder.sourceIdentifier);
        this.inputStream = Objects.requireNonNull(builder.inputStream);
    }

    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public NarSource getSource() {
        return source;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private NarSource source;
        private String sourceIdentifier;
        private InputStream inputStream;

        public Builder source(final NarSource source) {
            this.source = source;
            return this;
        }

        public Builder sourceIdentifier(final String sourceIdentifier) {
            this.sourceIdentifier = sourceIdentifier;
            return this;
        }

        public Builder inputStream(final InputStream inputStream) {
            this.inputStream = inputStream;
            return this;
        }

        public NarInstallRequest build() {
            return new NarInstallRequest(this);
        }
    }
}
