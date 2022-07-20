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

import org.apache.nifi.web.client.api.HttpEntityHeaders;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of HTTP Entity Headers for Standard Web Client Service
 */
class StandardHttpEntityHeaders implements HttpEntityHeaders {
    private final Map<String, List<String>> headers;

    StandardHttpEntityHeaders(final Map<String, List<String>> headers) {
        this.headers = Collections.unmodifiableMap(headers);
    }

    @Override
    public Optional<String> getFirstHeader(final String headerName) {
        final List<String> values = getHeader(headerName);
        return values.stream().findFirst();
    }

    @Override
    public List<String> getHeader(final String headerName) {
        Objects.requireNonNull(headerName, "Header Name required");
        final List<String> values = headers.get(headerName);
        return values == null ? Collections.emptyList() : Collections.unmodifiableList(values);
    }

    @Override
    public Collection<String> getHeaderNames() {
        return Collections.unmodifiableSet(headers.keySet());
    }
}
