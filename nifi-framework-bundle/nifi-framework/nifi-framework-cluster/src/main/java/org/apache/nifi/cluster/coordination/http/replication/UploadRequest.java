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

package org.apache.nifi.cluster.coordination.http.replication;

import org.apache.nifi.authorization.user.NiFiUser;

import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

/**
 * Upload request to be replicated.
 *
 * @param <T> the response entity
 */
public class UploadRequest<T> {

    private final NiFiUser user;
    private final String filename;
    private final String identifier;
    private final InputStream contents;
    private final URI exampleRequestUri;
    private final Class<T> responseClass;

    private UploadRequest(final Builder<T> builder) {
        this.user = Objects.requireNonNull(builder.user);
        this.filename = Objects.requireNonNull(builder.filename);
        this.identifier = Objects.requireNonNull(builder.identifier);
        this.contents = Objects.requireNonNull(builder.contents);
        this.exampleRequestUri = Objects.requireNonNull(builder.exampleRequestUri);
        this.responseClass = Objects.requireNonNull(builder.responseClass);
    }

    public NiFiUser getUser() {
        return user;
    }

    public String getFilename() {
        return filename;
    }

    public String getIdentifier() {
        return identifier;
    }

    public InputStream getContents() {
        return contents;
    }

    public URI getExampleRequestUri() {
        return exampleRequestUri;
    }

    public Class<T> getResponseClass() {
        return responseClass;
    }

    public static final class Builder<T> {
        private NiFiUser user;
        private String filename;
        private String identifier;
        private InputStream contents;
        private URI exampleRequestUri;
        private Class<T> responseClass;

        public Builder<T> user(NiFiUser user) {
            this.user = user;
            return this;
        }

        public Builder<T> filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder<T> identifier(String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder<T> contents(InputStream contents) {
            this.contents = contents;
            return this;
        }

        public Builder<T> exampleRequestUri(URI exampleRequestUri) {
            this.exampleRequestUri = exampleRequestUri;
            return this;
        }

        public Builder<T> responseClass(Class<T> responseClass) {
            this.responseClass = responseClass;
            return this;
        }

        public UploadRequest<T> build() {
            return new UploadRequest<>(this);
        }
    }
}
