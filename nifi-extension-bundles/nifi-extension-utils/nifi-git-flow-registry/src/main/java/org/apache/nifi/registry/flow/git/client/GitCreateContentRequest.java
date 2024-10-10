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

package org.apache.nifi.registry.flow.git.client;

import java.util.Objects;

public class GitCreateContentRequest {

    private final String branch;
    private final String path;
    private final String content;
    private final String message;
    private final String existingContentSha;

    private GitCreateContentRequest(final Builder builder) {
        this.branch = Objects.requireNonNull(builder.branch);
        this.path = Objects.requireNonNull(builder.path);
        this.content = Objects.requireNonNull(builder.content);
        this.message = Objects.requireNonNull(builder.message);
        // Will be null for create, and populated for update
        this.existingContentSha = builder.existingContentSha;
    }

    public String getBranch() {
        return branch;
    }

    public String getPath() {
        return path;
    }

    public String getContent() {
        return content;
    }

    public String getMessage() {
        return message;
    }

    public String getExistingContentSha() {
        return existingContentSha;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String branch;
        private String path;
        private String content;
        private String message;
        private String existingContentSha;

        public Builder branch(final String branch) {
            this.branch = branch;
            return this;
        }

        public Builder path(final String path) {
            this.path = path;
            return this;
        }

        public Builder content(final String content) {
            this.content = content;
            return this;
        }

        public Builder message(final String message) {
            this.message = message;
            return this;
        }

        public Builder existingContentSha(final String existingSha) {
            this.existingContentSha = existingSha;
            return this;
        }

        public GitCreateContentRequest build() {
            return new GitCreateContentRequest(this);
        }
    }
}
