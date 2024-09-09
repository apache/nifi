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

import java.util.Date;
import java.util.Objects;

public class GitCommit {

    private final String id;
    private final String author;
    private final String message;
    private final Date commitDate;

    private GitCommit(final Builder builder) {
        this.id = Objects.requireNonNull(builder.id, "Id is required");
        this.author = Objects.requireNonNull(builder.author, "Author is required");
        this.commitDate = Objects.requireNonNull(builder.commitDate, "Commit Date is required");
        this.message = builder.message;
    }

    public String getId() {
        return id;
    }

    public String getAuthor() {
        return author;
    }

    public String getMessage() {
        return message;
    }

    public Date getCommitDate() {
        return commitDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private String author;
        private String message;
        private Date commitDate;

        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        public Builder author(final String author) {
            this.author = author;
            return this;
        }

        public Builder message(final String message) {
            this.message = message;
            return this;
        }

        public Builder commitDate(final Date commitDate) {
            this.commitDate = commitDate;
            return this;
        }

        public GitCommit build() {
            return new GitCommit(this);
        }
    }
}
