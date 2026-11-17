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
package org.apache.nifi.github;

import org.kohsuke.github.connector.GitHubConnector;
import org.kohsuke.github.connector.GitHubConnectorRequest;
import org.kohsuke.github.connector.GitHubConnectorResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sends each request for a branch reference as a conditional request carrying the entity tag of the previous answer.
 * While the branch head is unchanged GitHub answers Not Modified, which does not count against the primary rate limit,
 * and the previous answer is returned instead. Checking the branch head before each cached read is then free while the
 * head is unchanged. Other requests are sent unchanged.
 */
final class ConditionalBranchReferenceConnector implements GitHubConnector {

    private static final String GET_METHOD = "GET";
    private static final String BRANCH_REFERENCE_PATH = "/git/refs/heads/";
    private static final String ENTITY_TAG_HEADER = "ETag";
    private static final String IF_NONE_MATCH_HEADER = "If-None-Match";
    // The stored body is already decoded, so headers describing the encoded body no longer apply
    private static final Set<String> ENCODED_BODY_HEADERS = Set.of("content-encoding", "content-length");

    private final GitHubConnector connector;
    private final Map<String, StoredResponse> storedResponses = new ConcurrentHashMap<>();

    ConditionalBranchReferenceConnector(final GitHubConnector connector) {
        this.connector = Objects.requireNonNull(connector, "Connector required");
    }

    @Override
    public GitHubConnectorResponse send(final GitHubConnectorRequest request) throws IOException {
        if (!isBranchReferenceRequest(request)) {
            return connector.send(request);
        }

        final String url = request.url().toString();
        final StoredResponse storedResponse = storedResponses.get(url);
        final GitHubConnectorRequest sentRequest = storedResponse == null ? request : new ConditionalRequest(request, storedResponse.entityTag());

        final GitHubConnectorResponse response = connector.send(sentRequest);
        final int statusCode = response.statusCode();
        if (storedResponse != null && statusCode == HttpURLConnection.HTTP_NOT_MODIFIED) {
            try (response) {
                return new StoredBodyResponse(request, HttpURLConnection.HTTP_OK, getDecodedBodyHeaders(response), storedResponse.body());
            }
        }

        final String entityTag = response.header(ENTITY_TAG_HEADER);
        if (statusCode != HttpURLConnection.HTTP_OK || entityTag == null) {
            // Keep the stored answer through transient failures such as rate limiting, so the next check stays conditional
            if (statusCode == HttpURLConnection.HTTP_NOT_FOUND || statusCode == HttpURLConnection.HTTP_GONE) {
                storedResponses.remove(url);
            }
            return response;
        }

        final byte[] body;
        try (response; InputStream bodyStream = response.bodyStream()) {
            body = bodyStream.readAllBytes();
        }
        storedResponses.put(url, new StoredResponse(entityTag, body));
        return new StoredBodyResponse(request, statusCode, getDecodedBodyHeaders(response), body);
    }

    private static boolean isBranchReferenceRequest(final GitHubConnectorRequest request) {
        return GET_METHOD.equals(request.method()) && request.url().getPath().contains(BRANCH_REFERENCE_PATH);
    }

    private static Map<String, List<String>> getDecodedBodyHeaders(final GitHubConnectorResponse response) {
        final Map<String, List<String>> headers = new LinkedHashMap<>();
        response.allHeaders().forEach((name, values) -> {
            if (name != null && !ENCODED_BODY_HEADERS.contains(name.toLowerCase())) {
                headers.put(name, values);
            }
        });
        return headers;
    }

    private record StoredResponse(String entityTag, byte[] body) {
    }

    private static final class ConditionalRequest implements GitHubConnectorRequest {
        private final GitHubConnectorRequest request;
        private final Map<String, List<String>> headers;

        private ConditionalRequest(final GitHubConnectorRequest request, final String entityTag) {
            this.request = request;
            final Map<String, List<String>> conditionalHeaders = new LinkedHashMap<>(request.allHeaders());
            conditionalHeaders.put(IF_NONE_MATCH_HEADER, List.of(entityTag));
            this.headers = conditionalHeaders;
        }

        @Override
        public String method() {
            return request.method();
        }

        @Override
        public Map<String, List<String>> allHeaders() {
            return headers;
        }

        @Override
        public String header(final String name) {
            return IF_NONE_MATCH_HEADER.equalsIgnoreCase(name) ? headers.get(IF_NONE_MATCH_HEADER).getFirst() : request.header(name);
        }

        @Override
        public String contentType() {
            return request.contentType();
        }

        @Override
        public InputStream body() {
            return request.body();
        }

        @Override
        public URL url() {
            return request.url();
        }

        @Override
        public boolean hasBody() {
            return request.hasBody();
        }
    }

    private static final class StoredBodyResponse extends GitHubConnectorResponse {
        private final byte[] body;

        private StoredBodyResponse(final GitHubConnectorRequest request, final int statusCode, final Map<String, List<String>> headers, final byte[] body) {
            super(request, statusCode, headers);
            this.body = body;
        }

        @Override
        protected InputStream rawBodyStream() {
            return new ByteArrayInputStream(body);
        }
    }
}
