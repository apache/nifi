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
        forwarded.put("Snowflake-Authorization-Token", "sf-token-abc");
        forwarded.put("X-Custom-Header", "custom-value");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals("sf-token-abc", result.get("Snowflake-Authorization-Token"));
        assertEquals("custom-value", result.get("X-Custom-Header"));
    }

    @Test
    void testReplicationHeadersFromForwardedInputAreStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put(RequestReplicationHeader.REQUEST_REPLICATED.getHeader(), "spoofed");
        forwarded.put(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader(), "spoofed");
        forwarded.put(RequestReplicationHeader.REQUEST_TRANSACTION_ID.getHeader(), "spoofed-tx");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.REQUEST_REPLICATED.getHeader()));
        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader()));
        assertNull(result.get(RequestReplicationHeader.REQUEST_TRANSACTION_ID.getHeader()));
    }

    @Test
    void testAuthorizationHeaderStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put("Authorization", "Bearer secret-token");
        forwarded.put("Snowflake-Authorization-Token", "keep-this");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNull(result.get("Authorization"));
        assertEquals("keep-this", result.get("Snowflake-Authorization-Token"));
    }

    @Test
    void testHopByHopHeadersStripped() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put("Content-Length", "99999");
        forwarded.put("Host", "original-host:443");
        forwarded.put("Transfer-Encoding", "chunked");
        forwarded.put("Connection", "keep-alive");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNull(result.get("Content-Length"));
        assertNull(result.get("Transfer-Encoding"));
        assertNull(result.get("Connection"));
        assertNull(result.get("Host"));
    }

    @Test
    void testExplicitBuilderHeadersWinOverForwarded() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put("Filename", "forwarded-name.txt");

        final UploadRequest<String> request = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity("test-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(forwarded)
                .header("Filename", "explicit-name.txt")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);
        assertEquals("explicit-name.txt", result.get("Filename"));
    }

    @Test
    void testProxiedEntitiesSetFromUser() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put("Snowflake-Authorization-Token", "token");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertNotNull(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertTrue(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains("test-user"));
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
                .user(new StandardNiFiUser.Builder().identity("test-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .header("Filename", "test.txt")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertEquals("test.txt", result.get("Filename"));
        assertEquals(Boolean.TRUE.toString(), result.get(RequestReplicationHeader.REQUEST_REPLICATED.getHeader()));
        assertNotNull(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
    }

    @Test
    void testBuilderCannotOverrideProxiedEntities() {
        final UploadRequest<String> request = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity("real-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, "<evil-user>")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();

        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        assertFalse(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains("evil-user"));
        assertTrue(result.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains("real-user"));
    }

    @Test
    void testAuthCookiesStrippedFromForwardedHeaders() {
        final Map<String, String> forwarded = new HashMap<>();
        forwarded.put("Cookie", "__Secure-Authorization-Bearer=token123; __Secure-Request-Token=rt456; custom=value");
        forwarded.put("Snowflake-Authorization-Token", "keep-me");

        final UploadRequest<String> request = buildUploadRequest(forwarded);
        final Map<String, String> result = replicator.buildOutboundHeaders(request);

        final String cookies = result.get("Cookie");
        assertNotNull(cookies);
        assertFalse(cookies.contains("__Secure-Authorization-Bearer"));
        assertFalse(cookies.contains("__Secure-Request-Token"));
        assertTrue(cookies.contains("custom=value"));
        assertEquals("keep-me", result.get("Snowflake-Authorization-Token"));
    }

    @Test
    void testForwardRequestHeadersNullProducesSameAsOmitted() {
        final UploadRequest<String> withNull = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity("test-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(null)
                .header("Filename", "test.txt")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();

        final UploadRequest<String> withoutCall = new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity("test-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .header("Filename", "test.txt")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();

        final Map<String, String> resultNull = replicator.buildOutboundHeaders(withNull);
        final Map<String, String> resultOmitted = replicator.buildOutboundHeaders(withoutCall);

        assertEquals(resultOmitted, resultNull);
    }

    private UploadRequest<String> buildUploadRequest(final Map<String, String> forwardedHeaders) {
        return new UploadRequest.Builder<String>()
                .user(new StandardNiFiUser.Builder().identity("test-user").build())
                .filename("test.txt")
                .identifier("id-1")
                .contents(new ByteArrayInputStream(new byte[0]))
                .forwardRequestHeaders(forwardedHeaders)
                .header("Filename", "test.txt")
                .header("Content-Type", "application/octet-stream")
                .exampleRequestUri(URI.create("https://localhost:8443/nifi-api/test"))
                .responseClass(String.class)
                .successfulResponseStatus(200)
                .build();
    }
}
