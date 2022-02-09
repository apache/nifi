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
package org.apache.nifi.minifi.c2.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class C2ProtocolContext {
    private static final Logger logger = LoggerFactory.getLogger(C2ProtocolContext.class);

    private static final C2ProtocolContext EMPTY = builder().build();

    private final URI baseUri;
    private final Long contentLength;
    private final String sha256;

    C2ProtocolContext(final Builder builder) {
        this.baseUri = builder.baseUri;
        this.contentLength = builder.contentLength;
        this.sha256 = builder.sha256;
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public Long getContentLength() {
        return contentLength;
    }

    public String getSha256() {
        return sha256;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static C2ProtocolContext empty() {
        return EMPTY;
    }

    public static class Builder {

        private URI baseUri;
        private Long contentLength;
        private String sha256;

        private Builder() {
        }

        public Builder baseUri(final URI baseUri) {
            this.baseUri = baseUri;
            return this;
        }

        public Builder contentLength(final Long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Builder contentLength(final String contentLength) {
            try {
                this.contentLength = Long.valueOf(contentLength);
            } catch (final NumberFormatException e) {
                logger.debug("Could not parse content length string: " + contentLength, e);
            }
            return this;
        }

        public Builder sha256(final String sha256) {
            this.sha256 = sha256;
            return this;
        }

        public C2ProtocolContext build() {
            return new C2ProtocolContext(this);
        }
    }
}
