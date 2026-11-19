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

import org.apache.nifi.web.client.api.HttpUriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Standard HTTP URI Builder based using java.net.URI
 */
public class StandardHttpUriBuilder implements HttpUriBuilder {
    private static final String HTTP_SCHEME = "http";

    private static final String HTTPS_SCHEME = "https";

    private static final int UNKNOWN_PORT = -1;

    private static final int MAXIMUM_PORT = 65535;

    private static final char PATH_SEGMENT_SEPARATOR_CHARACTER = '/';

    private static final String PATH_SEGMENT_SEPARATOR = "/";

    private static final String QUERY_PARAMETER_SEPARATOR = "&";

    private static final String QUERY_PARAMETER_VALUE_SEPARATOR = "=";

    private static final String QUERY_SEPARATOR = "?";

    private static final String FORM_ENCODED_SPACE = "+";

    private static final String PERCENT_ENCODED_SPACE = "%20";

    private String scheme = HTTP_SCHEME;

    private String host;

    private int port = UNKNOWN_PORT;

    private String encodedPath;

    private final List<String> pathSegments = new ArrayList<>();

    private final Map<String, List<String>> queryParameters = new LinkedHashMap<>();

    @Override
    public URI build() {
        if (host == null) {
            throw new IllegalStateException("Host not specified");
        }

        final String resolvedPath = getResolvedPath();
        final String resolvedQuery = getResolvedQuery();

        try {
            // Query excluded from java.net.URI construction to avoid quoting the percent character of resolved query parameters
            final URI uri = new URI(scheme, null, host, port, resolvedPath, null, null);
            if (resolvedQuery == null) {
                return uri;
            }
            return new URI(uri + QUERY_SEPARATOR + resolvedQuery);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("URI construction failed", e);
        }
    }

    @Override
    public HttpUriBuilder scheme(final String scheme) {
        Objects.requireNonNull(scheme, "Scheme required");
        if (HTTP_SCHEME.equals(scheme) || HTTPS_SCHEME.equals(scheme)) {
            this.scheme = scheme;
        } else {
            throw new IllegalArgumentException("Scheme [%s] not supported".formatted(scheme));
        }
        return this;
    }

    @Override
    public HttpUriBuilder host(final String host) {
        Objects.requireNonNull(host, "Host required");
        this.host = host;
        return this;
    }

    @Override
    public HttpUriBuilder port(int port) {
        if (port < UNKNOWN_PORT || port > MAXIMUM_PORT) {
            throw new IllegalArgumentException("Port [%d] not valid".formatted(port));
        }
        this.port = port;
        return this;
    }

    @Override
    public HttpUriBuilder encodedPath(final String encodedPath) {
        this.encodedPath = encodedPath;
        return this;
    }

    @Override
    public HttpUriBuilder addPathSegment(final String pathSegment) {
        Objects.requireNonNull(pathSegment, "Path segment required");
        pathSegments.add(pathSegment);
        return this;
    }

    @Override
    public HttpUriBuilder addQueryParameter(final String name, final String value) {
        Objects.requireNonNull(name, "Parameter name required");
        final String quotedValue = value == null ? null : getQuotedQueryComponent(value);
        addResolvedQueryParameter(getQuotedQueryComponent(name), quotedValue);
        return this;
    }

    @Override
    public HttpUriBuilder addUnencodedQueryParameter(final String name, final String value) {
        Objects.requireNonNull(name, "Parameter name required");
        final String encodedValue = value == null ? null : getEncodedQueryComponent(value);
        addResolvedQueryParameter(getEncodedQueryComponent(name), encodedValue);
        return this;
    }

    private void addResolvedQueryParameter(final String name, final String value) {
        final List<String> parameters = queryParameters.computeIfAbsent(name, parameterName -> new ArrayList<>());
        parameters.add(value);
    }

    private String getQuotedQueryComponent(final String component) {
        try {
            // Quote characters not legal in a URI query, preserving reserved characters as provided
            return new URI(null, null, null, UNKNOWN_PORT, null, component, null).getRawQuery();
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Query parameter component [%s] not valid".formatted(component), e);
        }
    }

    private String getEncodedQueryComponent(final String component) {
        final String formEncoded = URLEncoder.encode(component, StandardCharsets.UTF_8);
        // Percent encoding of spaces avoids ambiguous handling of plus characters in query components
        return formEncoded.replace(FORM_ENCODED_SPACE, PERCENT_ENCODED_SPACE);
    }

    private String getResolvedPath() {
        final StringBuilder pathBuilder = new StringBuilder();
        if (encodedPath != null) {
            // Decoded path required for subsequent encoding in java.net.URI construction
            final String decodedPath = URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
            pathBuilder.append(decodedPath);
        }

        if (!pathSegments.isEmpty()) {
            final int pathBuilderLength = pathBuilder.length();
            if (pathBuilderLength > 0) {
                // Append Path Segment Separator after encodedPath and before pathSegments when not found in encodedPath
                final int lastIndex = pathBuilderLength - 1;
                final char lastCharacter = pathBuilder.charAt(lastIndex);
                if (PATH_SEGMENT_SEPARATOR_CHARACTER != lastCharacter) {
                    pathBuilder.append(PATH_SEGMENT_SEPARATOR_CHARACTER);
                }
            }

            final String separatedPath = String.join(PATH_SEGMENT_SEPARATOR, pathSegments);
            pathBuilder.append(separatedPath);
        }
        final String path = pathBuilder.toString();

        final String resolvedPath;
        if (path.startsWith(PATH_SEGMENT_SEPARATOR)) {
            resolvedPath = path;
        } else {
            resolvedPath = PATH_SEGMENT_SEPARATOR + path;
        }
        return resolvedPath;
    }

    private String getResolvedQuery() {
        final StringBuilder queryBuilder = new StringBuilder();

        final Iterator<Map.Entry<String, List<String>>> parameters = queryParameters.entrySet().iterator();
        while (parameters.hasNext()) {
            final Map.Entry<String, List<String>> parameter = parameters.next();

            final String name = parameter.getKey();
            queryBuilder.append(name);

            final Iterator<String> values = parameter.getValue().iterator();
            while (values.hasNext()) {
                final String value = values.next();
                if (value != null) {
                    queryBuilder.append(QUERY_PARAMETER_VALUE_SEPARATOR);
                    queryBuilder.append(value);
                }

                if (values.hasNext()) {
                    queryBuilder.append(QUERY_PARAMETER_SEPARATOR);
                    queryBuilder.append(name);
                }
            }

            if (parameters.hasNext()) {
                queryBuilder.append(QUERY_PARAMETER_SEPARATOR);
            }
        }

        final String query;
        if (queryBuilder.isEmpty()) {
            query = null;
        } else {
            query = queryBuilder.toString();
        }
        return query;
    }
}
