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
package org.apache.nifi.cluster.coordination.http.replication;

import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestStandardUploadRequestReplicatorHeaders {

    private static final String TEST_USER_IDENTITY = "test-user";
    private static final String REAL_USER_IDENTITY = "real-user";
    private static final String EVIL_USER_IDENTITY = "evil-user";

    private static final String TEST_FILENAME = "test.txt";
    private static final String TEST_IDENTIFIER = "id-1";
    private static final URI TEST_REQUEST_URI = URI.create("https://localhost:8443/nifi-api/test");
    private static final int SUCCESS_STATUS = 200;

    private static final String FILENAME_HEADER = "Filename";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String CONTENT_TYPE_VALUE = "application/octet-stream";

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String AUTHORIZATION_VALUE = "Bearer secret-token";

    private static final String CUSTOM_TOKEN_HEADER = "X-Custom-Token";
    private static final String CUSTOM_TOKEN_VALUE = "custom-token-abc";
    private static final String CUSTOM_TOKEN_VALUE_ALT = "keep-this";
    private static final String CUSTOM_TOKEN_VALUE_SURVIVE = "keep-me";

    private static final String CUSTOM_HEADER = "X-Custom-Header";
    private static final String CUSTOM_HEADER_VALUE = "custom-value";

    private static final String ACCEPT_ENCODING_HEADER = "Accept-Encoding";
    private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
    private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    private static final String HOST_HEADER = "Host";
    private static final String TE_HEADER = "TE";
    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
    private static final String CONNECTION_HEADER = "Connection";

    private static final String COOKIE_HEADER = "Cookie";
    private static final String COOKIE_VALUE_WITH_AUTH = "__Secure-Authorization-Bearer=token123; __Secure-Request-Token=rt456; custom=value";
    private static final String COOKIE_CUSTOM_SEGMENT = "custom=value";

    private static final String SPOOFED_VALUE = "spoofed";
    private static final String SPOOFED_TX_VALUE = "spoofed-tx";

    private StandardUploadRequestReplicator replicator;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        final Properties props = new Properties();
        props.setProperty(NiFiProperties.UPLOAD_WORKING_DIRECTORY, tempDir.toString());
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties((String) null, props);
        replicator = new StandardUploadRequestReplicator(
                mock(ClusterCoordinator.class),
                mock(WebClientService.class),
                nifiProperties
        );
    }

    @Test
    void testCustomHeaderPreservedFromForwardedHeaders() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(CUSTOM_TOKEN_HEADER, CUSTOM_TOKEN_VALUE);
        forwarded.put(CUSTOM_HEADER, CUSTOM_HEADER_VALUE);

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals(CUSTOM_TOKEN_VALUE, result.get(CUSTOM_TOKEN_HEADER));
        assertEquals(CUSTOM_HEADER_VALUE, result.get(CUSTOM_HEADER));
    }

    @Test
    void testReplicationHeadersFromForwardedInputAreStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(RequestReplicationHeader.REQUEST_REPLICATED.getHeader(), SPOOFED_VALUE);
        forwarded.put(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader(), SPOOFED_VALUE);
        forwarded.put(RequestReplicationHeader.REQUEST_TRANSACTION_ID.getHeader(), SPOOFED_TX_VALUE);

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.REQUEST_REPLICATED.getHeader()));
        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader()));
        assertNull(result.get(RequestReplicationHeader.REQUEST_TRANSACTION_ID.getHeader()));
    }

    @Test
    void testAuthorizationHeaderStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(AUTHORIZATION_HEADER, AUTHORIZATION_VALUE);
        forwarded.put(CUSTOM_TOKEN_HEADER, CUSTOM_TOKEN_VALUE_ALT);

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNull(result.get(AUTHORIZATION_HEADER));
        assertEquals(CUSTOM_TOKEN_VALUE_ALT, result.get(CUSTOM_TOKEN_HEADER));
    }

    @Test
    void testHopByHopHeadersStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(ACCEPT_ENCODING_HEADER, "gzip, deflate");
        forwarded.put(CONTENT_ENCODING_HEADER, "gzip");
        forwarded.put(CONTENT_LENGTH_HEADER, "99999");
        forwarded.put(HOST_HEADER, "original-host:443");
        forwarded.put(TE_HEADER, "trailers, deflate");
        forwarded.put(TRANSFER_ENCODING_HEADER, "chunked");
        forwarded.put(CONNECTION_HEADER, "keep-alive");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNull(result.get(ACCEPT_ENCODING_HEADER));
        assertNull(result.get(CONTENT_ENCODING_HEADER));
        assertNull(result.get(CONTENT_LENGTH_HEADER));
        assertNull(result.get(TE_HEADER));
        assertNull(result.get(TRANSFER_ENCODING_HEADER));
        assertNull(result.get(CONNECTION_HEADER));
        assertNull(result.get(HOST_HEADER));
    }

    @Test
    void testExplicitBuilderHeadersWinOverForwarded() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(FILENAME_HEADER, "forwarded-name.txt");

        final UploadRequest<String> request = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(forwarded)
                .header(FILENAME_HEADER, "explicit-name.txt")
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);
        assertEquals("explicit-name.txt", result.get(FILENAME_HEADER));
    }

    @Test
    void testProxiedEntitiesSetFromUser() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(CUSTOM_TOKEN_HEADER, CUSTOM_TOKEN_VALUE);

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNotNull(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertTrue(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(TEST_USER_IDENTITY));
        assertNotNull(result.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
    }

    @Test
    void testReplicationFlagsAlwaysSet() {
        final UploadRequest<String> request = buildUploadRequest(new HashMap<>());
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.REQUEST_REPLICATED.getHeader()));
        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader()));
    }

    @Test
    void testNoForwardedHeadersStillWorks() {
        final UploadRequest<String> request = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .header(FILENAME_HEADER, TEST_FILENAME)
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals(TEST_FILENAME, result.get(FILENAME_HEADER));
        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.REQUEST_REPLICATED.getHeader()));
        assertNotNull(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
    }

    @Test
    void testBuilderCannotOverrideProxiedEntities() {
        final UploadRequest<String> request = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(REAL_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, "<" + EVIL_USER_IDENTITY + ">")
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertFalse(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(EVIL_USER_IDENTITY));
        assertTrue(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(REAL_USER_IDENTITY));
    }

    @Test
    void testAuthCookiesStrippedFromForwardedHeaders() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(COOKIE_HEADER, COOKIE_VALUE_WITH_AUTH);
        forwarded.put(CUSTOM_TOKEN_HEADER, CUSTOM_TOKEN_VALUE_SURVIVE);

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        final String cookies = result.get(COOKIE_HEADER);
        assertNotNull(cookies);
        assertFalse(cookies.contains("__Secure-Authorization-Bearer"));
        assertFalse(cookies.contains("__Secure-Request-Token"));
        assertTrue(cookies.contains(COOKIE_CUSTOM_SEGMENT));
        assertEquals(CUSTOM_TOKEN_VALUE_SURVIVE, result.get(CUSTOM_TOKEN_HEADER));
    }

    @Test
    void testForwardRequestHeadersNullProducesSameAsOmitted() {
        final UploadRequest<String> withNull = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(null)
                .header(FILENAME_HEADER, TEST_FILENAME)
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();

        final UploadRequest<String> withoutCall = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .header(FILENAME_HEADER, TEST_FILENAME)
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();

        final Map<String, String> resultNull = replicator.buildOutboundHeaders(withNull);
        final Map<String, String> resultOmitted = replicator.buildOutboundHeaders(withoutCall);

        assertEquals(resultOmitted, resultNull);
    }

    private UploadRequest<String> buildUploadRequest(final Map<String, String> forwardedHeaders) {
        return new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build())
                .filename(TEST_FILENAME)
                .identifier(TEST_IDENTIFIER)
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(forwardedHeaders)
                .header(FILENAME_HEADER, TEST_FILENAME)
                .header(CONTENT_TYPE_HEADER, CONTENT_TYPE_VALUE)
                .exampleRequestUri(TEST_REQUEST_URI)
                .responseClass(String.class)
                .successfulResponseStatus(SUCCESS_STATUS)
                .build();
    }
}
