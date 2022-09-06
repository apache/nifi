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
package org.apache.nifi.web.client;

import okhttp3.HttpUrl;
import org.apache.nifi.web.client.api.HttpUriBuilder;

import java.net.URI;
import java.util.Objects;

/**
 * Standard HTTP URI Builder based on OkHttp HttpUrl
 */
public class StandardHttpUriBuilder implements HttpUriBuilder {
    private final HttpUrl.Builder builder;

    public StandardHttpUriBuilder() {
        this.builder = new HttpUrl.Builder();
    }

    @Override
    public URI build() {
        final HttpUrl httpUrl = builder.build();
        return httpUrl.uri();
    }

    @Override
    public HttpUriBuilder scheme(final String scheme) {
        Objects.requireNonNull(scheme, "Scheme required");
        builder.scheme(scheme);
        return this;
    }

    @Override
    public HttpUriBuilder host(final String host) {
        Objects.requireNonNull(host, "Host required");
        builder.host(host);
        return this;
    }

    @Override
    public HttpUriBuilder port(int port) {
        builder.port(port);
        return this;
    }

    @Override
    public HttpUriBuilder encodedPath(final String encodedPath) {
        builder.encodedPath(encodedPath);
        return this;
    }

    @Override
    public HttpUriBuilder addPathSegment(final String pathSegment) {
        Objects.requireNonNull(pathSegment, "Path segment required");
        builder.addPathSegment(pathSegment);
        return this;
    }

    @Override
    public HttpUriBuilder addQueryParameter(final String name, final String value) {
        Objects.requireNonNull(name, "Parameter name required");
        builder.addQueryParameter(name, value);
        return this;
    }
}
