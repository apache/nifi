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

import java.util.List;
import java.util.Map;

/**
 * Provides request-scoped context from an HTTP request to a {@link ConnectorConfigurationProvider}.
 *
 * <p>When a connector operation is triggered by an HTTP request (e.g., from the Runtime UI),
 * the framework populates this context with the authenticated NiFi user and all HTTP headers
 * from the original request. Provider implementations can use this to make decisions based on
 * who is making the request and to extract forwarded credentials (e.g., OAuth tokens relayed
 * by a gateway).</p>
 *
 * <p>This context is available via {@link ConnectorRequestContextHolder#getContext()} on the
 * request thread. When no HTTP request is in scope (e.g., background operations), the holder
 * returns {@code null}.</p>
 */
public interface ConnectorRequestContext {

    /**
     * Returns the authenticated NiFi user who initiated the request. For proxied requests
     * (e.g., through a gateway), this is the end-user with the proxy chain accessible
     * via {@link NiFiUser#getChain()}.
     *
     * @return the authenticated NiFi user, or {@code null} if not available
     */
    NiFiUser getNiFiUser();

    /**
     * Returns all HTTP headers from the original request as an immutable, case-insensitive
     * multi-valued map. Each header name maps to an unmodifiable list of its values.
     *
     * <p>Provider implementations should read the specific headers they need and ignore
     * the rest. This allows new headers to be forwarded by a gateway without requiring
     * changes to the NiFi framework.</p>
     *
     * @return an immutable map of header names to their values
     */
    Map<String, List<String>> getRequestHeaders();

    /**
     * Returns whether the request contains a header with the given name.
     * Header name matching is case-insensitive per the HTTP specification.
     *
     * @param headerName the header name to check
     * @return {@code true} if the header is present, {@code false} otherwise
     */
    boolean hasHeader(String headerName);

    /**
     * Returns all values for the given header name, or an empty list if the header is not present.
     * Header name matching is case-insensitive per the HTTP specification.
     *
     * @param headerName the header name to look up
     * @return an unmodifiable list of header values, or an empty list if not present
     */
    List<String> getHeaderValues(String headerName);

    /**
     * Returns the first value for the given header name, or {@code null} if the header is
     * not present or has no values. Header name matching is case-insensitive per the HTTP
     * specification.
     *
     * @param headerName the header name to look up
     * @return the first header value, or {@code null} if not present
     */
    String getFirstHeaderValue(String headerName);
}
