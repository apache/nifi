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

import org.junit.jupiter.api.Test;
import org.kohsuke.github.connector.GitHubConnector;
import org.kohsuke.github.connector.GitHubConnectorRequest;
import org.kohsuke.github.connector.GitHubConnectorResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ConditionalBranchReferenceConnectorTest {
    private static final String BRANCH_REFERENCE_URL = "https://api.github.com/repos/owner/repo/git/refs/heads/main";
    private static final String CONTENTS_URL = "https://api.github.com/repos/owner/repo/contents/bucket/flow.json";
    private static final String GET_METHOD = "GET";
    private static final String DELETE_METHOD = "DELETE";
    private static final String ENTITY_TAG_HEADER = "ETag";
    private static final String IF_NONE_MATCH_HEADER = "If-None-Match";
    private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
    private static final String GZIP_ENCODING = "gzip";
    private static final String HEAD_1_ENTITY_TAG = "W/\"head-1\"";
    private static final String HEAD_2_ENTITY_TAG = "W/\"head-2\"";
    private static final String HEAD_1_BODY = "{\"object\":{\"sha\":\"head-1\"}}";
    private static final String HEAD_2_BODY = "{\"object\":{\"sha\":\"head-2\"}}";

    private final RecordingConnector recordingConnector = new RecordingConnector();
    private final ConditionalBranchReferenceConnector connector = new ConditionalBranchReferenceConnector(recordingConnector);

    @Test
    void testUnchangedReferenceAnsweredWithStoredBody() throws IOException {
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG), CONTENT_ENCODING_HEADER, List.of(GZIP_ENCODING)), gzip(HEAD_1_BODY));
        recordingConnector.respond(HttpURLConnection.HTTP_NOT_MODIFIED, Map.of(), "");

        final GitHubConnectorResponse first = connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL));
        assertEquals(HttpURLConnection.HTTP_OK, first.statusCode());
        assertNull(first.header(CONTENT_ENCODING_HEADER));
        assertEquals(HEAD_1_BODY, readBody(first));

        final GitHubConnectorResponse second = connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL));
        assertEquals(HttpURLConnection.HTTP_OK, second.statusCode());
        assertEquals(HEAD_1_BODY, readBody(second));

        assertNull(recordingConnector.getRequest(0).header(IF_NONE_MATCH_HEADER));
        assertEquals(HEAD_1_ENTITY_TAG, recordingConnector.getRequest(1).header(IF_NONE_MATCH_HEADER));
        assertEquals(List.of(HEAD_1_ENTITY_TAG), recordingConnector.getRequest(1).allHeaders().get(IF_NONE_MATCH_HEADER));
    }

    @Test
    void testChangedReferenceReplacesStoredBody() throws IOException {
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG)), HEAD_1_BODY);
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_2_ENTITY_TAG)), HEAD_2_BODY);
        recordingConnector.respond(HttpURLConnection.HTTP_NOT_MODIFIED, Map.of(), "");

        assertEquals(HEAD_1_BODY, readBody(connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL))));
        assertEquals(HEAD_2_BODY, readBody(connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL))));
        assertEquals(HEAD_2_BODY, readBody(connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL))));

        assertEquals(HEAD_1_ENTITY_TAG, recordingConnector.getRequest(1).header(IF_NONE_MATCH_HEADER));
        assertEquals(HEAD_2_ENTITY_TAG, recordingConnector.getRequest(2).header(IF_NONE_MATCH_HEADER));
    }

    @Test
    void testFailedReferenceRequestForgetsStoredBody() throws IOException {
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG)), HEAD_1_BODY);
        recordingConnector.respond(HttpURLConnection.HTTP_NOT_FOUND, Map.of(), "");
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG)), HEAD_1_BODY);

        readBody(connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL)));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL)).statusCode());
        readBody(connector.send(new TestRequest(GET_METHOD, BRANCH_REFERENCE_URL)));

        assertNull(recordingConnector.getRequest(2).header(IF_NONE_MATCH_HEADER));
    }

    @Test
    void testOtherRequestsSentUnchanged() throws IOException {
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG)), HEAD_1_BODY);
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(ENTITY_TAG_HEADER, List.of(HEAD_1_ENTITY_TAG)), HEAD_1_BODY);
        recordingConnector.respond(HttpURLConnection.HTTP_OK, Map.of(), "");
        final TestRequest contentsRequest = new TestRequest(GET_METHOD, CONTENTS_URL);
        final TestRequest deleteReferenceRequest = new TestRequest(DELETE_METHOD, BRANCH_REFERENCE_URL);

        // A stored answer for the contents would make the second request conditional
        connector.send(contentsRequest);
        connector.send(contentsRequest);
        connector.send(deleteReferenceRequest);

        assertSame(contentsRequest, recordingConnector.getRequest(0));
        assertSame(contentsRequest, recordingConnector.getRequest(1));
        assertSame(deleteReferenceRequest, recordingConnector.getRequest(2));
    }

    private static String readBody(final GitHubConnectorResponse response) throws IOException {
        try (response; InputStream bodyStream = response.bodyStream()) {
            return new String(bodyStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static byte[] gzip(final String body) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            gzipOutputStream.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return outputStream.toByteArray();
    }

    private record ResponseDefinition(int statusCode, Map<String, List<String>> headers, byte[] body) {
    }

    private static final class RecordingConnector implements GitHubConnector {
        private final List<GitHubConnectorRequest> requests = new ArrayList<>();
        private final Deque<ResponseDefinition> responseDefinitions = new ArrayDeque<>();

        void respond(final int statusCode, final Map<String, List<String>> headers, final String body) {
            respond(statusCode, headers, body.getBytes(StandardCharsets.UTF_8));
        }

        void respond(final int statusCode, final Map<String, List<String>> headers, final byte[] body) {
            responseDefinitions.add(new ResponseDefinition(statusCode, headers, body));
        }

        GitHubConnectorRequest getRequest(final int index) {
            return requests.get(index);
        }

        @Override
        public GitHubConnectorResponse send(final GitHubConnectorRequest request) {
            requests.add(request);
            final ResponseDefinition definition = responseDefinitions.removeFirst();
            return new TestResponse(request, definition.statusCode(), definition.headers(), definition.body());
        }
    }

    private static final class TestResponse extends GitHubConnectorResponse {
        private final byte[] body;

        private TestResponse(final GitHubConnectorRequest request, final int statusCode, final Map<String, List<String>> headers, final byte[] body) {
            super(request, statusCode, headers);
            this.body = body;
        }

        @Override
        protected InputStream rawBodyStream() {
            return new ByteArrayInputStream(body);
        }
    }

    private record TestRequest(String method, String address) implements GitHubConnectorRequest {
        @Override
        public Map<String, List<String>> allHeaders() {
            return Map.of("Accept", List.of("application/vnd.github+json"));
        }

        @Override
        public String header(final String name) {
            final List<String> values = allHeaders().get(name);
            return values == null ? null : values.getFirst();
        }

        @Override
        public String contentType() {
            return null;
        }

        @Override
        public InputStream body() {
            return null;
        }

        @Override
        public URL url() {
            try {
                return URI.create(address).toURL();
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean hasBody() {
            return false;
        }
    }
}
