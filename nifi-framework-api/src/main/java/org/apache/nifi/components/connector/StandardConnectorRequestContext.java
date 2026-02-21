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

package org.apache.nifi.components.connector;

import org.apache.nifi.authorization.user.NiFiUser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Standard implementation of {@link ConnectorRequestContext} that stores an authenticated
 * {@link NiFiUser} and HTTP headers in a case-insensitive map.
 *
 * <p>Header name lookups via {@link #hasHeader}, {@link #getHeaderValues}, and
 * {@link #getFirstHeaderValue} are case-insensitive per the HTTP specification.
 * The backing map uses {@link String#CASE_INSENSITIVE_ORDER} to guarantee this.</p>
 */
public class StandardConnectorRequestContext implements ConnectorRequestContext {

    private final NiFiUser niFiUser;
    private final Map<String, List<String>> requestHeaders;

    /**
     * Creates a new context with the given user and headers. The provided headers map is
     * copied into a case-insensitive map; the original map is not retained.
     *
     * @param niFiUser the authenticated NiFi user, or {@code null} if not available
     * @param requestHeaders the HTTP headers from the request; may be {@code null} or empty
     */
    public StandardConnectorRequestContext(final NiFiUser niFiUser, final Map<String, List<String>> requestHeaders) {
        this.niFiUser = niFiUser;
        if (requestHeaders == null || requestHeaders.isEmpty()) {
            this.requestHeaders = Map.of();
        } else {
            final Map<String, List<String>> caseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            caseInsensitive.putAll(requestHeaders);
            this.requestHeaders = Collections.unmodifiableMap(caseInsensitive);
        }
    }

    @Override
    public NiFiUser getNiFiUser() {
        return niFiUser;
    }

    @Override
    public Map<String, List<String>> getRequestHeaders() {
        return requestHeaders;
    }

    @Override
    public boolean hasHeader(final String headerName) {
        return requestHeaders.containsKey(headerName);
    }

    @Override
    public List<String> getHeaderValues(final String headerName) {
        final List<String> values = requestHeaders.get(headerName);
        return values != null ? values : List.of();
    }

    @Override
    public String getFirstHeaderValue(final String headerName) {
        final List<String> values = getHeaderValues(headerName);
        return values.isEmpty() ? null : values.getFirst();
    }
}
