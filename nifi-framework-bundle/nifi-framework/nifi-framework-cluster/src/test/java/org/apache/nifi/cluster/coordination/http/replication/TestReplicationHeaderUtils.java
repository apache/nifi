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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.cluster.coordination.http.ReplicationHeader;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestReplicationHeaderUtils {

    private static final String TEST_USER_IDENTITY = "alice";

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String AUTHORIZATION_HEADER_UPPER = "AUTHORIZATION";
    private static final String AUTHORIZATION_VALUE = "Bearer secret";

    private static final String CUSTOM_HEADER = "X-Custom-Token";
    private static final String CUSTOM_HEADER_VALUE = "custom-token-123";

    private static final String COOKIE_HEADER = "Cookie";
    private static final String COOKIE_HEADER_LOWER = "cookie";
    private static final String HOST_HEADER = "Host";
    private static final String HOST_VALUE = "original-host:8080";

    private static final String ACCEPT_ENCODING_HEADER = "Accept-Encoding";
    private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
    private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    private static final String TE_HEADER = "TE";
    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
    private static final String CONNECTION_HEADER = "Connection";

    private static final String SPOOFED_VALUE = "spoofed";
    private static final String SHOULD_SURVIVE_VALUE = "should-survive";

    private static final String PROXIED_ENTITIES_CHAIN_UPPER = "X-PROXIEDENTITIESCHAIN";
    private static final String PROXIED_ENTITIES_CHAIN_LOWER = "x-proxiedentitieschain";
    private static final String PROXIED_ENTITY_GROUPS_UPPER = "X-PROXIEDENTITYGROUPS";
    private static final String PROXIED_ENTITY_GROUPS_LOWER = "x-proxiedentitygroups";

    private static final String OTHER_IDENTITY = "other-user";
    private static final String ANOTHER_IDENTITY = "another-user";
    private static final String IDENTITY_PROVIDER_GROUP = "analysts";
    private static final String OTHER_GROUP = "other-group";
    private static final String ANOTHER_GROUP = "another-group";

    @Test
    void testApplyUserProxyAndStripCredentialsSetsProxiedEntities() {
        final NiFiUser user = new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build();
        final Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, AUTHORIZATION_VALUE);
        headers.put(CUSTOM_HEADER, CUSTOM_HEADER_VALUE);

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, user);

        assertNotNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertTrue(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(TEST_USER_IDENTITY));
        assertNotNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
        assertNull(headers.get(AUTHORIZATION_HEADER));
        assertEquals(CUSTOM_HEADER_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testApplyUserProxyWithNullUserOmitsProxiedEntities() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, AUTHORIZATION_VALUE);

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
        assertNull(headers.get(AUTHORIZATION_HEADER));
    }

    @Test
    void testApplyUserProxyReplacesCaseVariantProxiedEntityHeaders() {
        final NiFiUser user = new StandardNiFiUser.Builder()
                .identity(TEST_USER_IDENTITY)
                .identityProviderGroups(Set.of(IDENTITY_PROVIDER_GROUP))
                .build();
        final Map<String, String> headers = new HashMap<>();
        headers.put(PROXIED_ENTITIES_CHAIN_UPPER, formatEntities(OTHER_IDENTITY));
        headers.put(PROXIED_ENTITIES_CHAIN_LOWER, formatEntities(ANOTHER_IDENTITY));
        headers.put(PROXIED_ENTITY_GROUPS_UPPER, formatEntities(OTHER_GROUP));
        headers.put(PROXIED_ENTITY_GROUPS_LOWER, formatEntities(ANOTHER_GROUP));

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, user);

        assertEquals(Set.of(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN), matchingHeaderNames(headers, ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertEquals(Set.of(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS), matchingHeaderNames(headers, ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
        assertTrue(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(TEST_USER_IDENTITY));
        assertTrue(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS).contains(IDENTITY_PROVIDER_GROUP));
        assertFalse(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(OTHER_IDENTITY));
        assertFalse(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains(ANOTHER_IDENTITY));
        assertFalse(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS).contains(OTHER_GROUP));
        assertFalse(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS).contains(ANOTHER_GROUP));
    }

    @Test
    void testApplyUserProxyWithNullUserRemovesExistingProxiedEntityHeaders() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(PROXIED_ENTITIES_CHAIN_UPPER, formatEntities(OTHER_IDENTITY));
        headers.put(PROXIED_ENTITY_GROUPS_LOWER, formatEntities(OTHER_GROUP));

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertTrue(matchingHeaderNames(headers, ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).isEmpty());
        assertTrue(matchingHeaderNames(headers, ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS).isEmpty());
    }

    @Test
    void testApplyUserProxyRemovesAllAuthorizationCaseVariants() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(AUTHORIZATION_HEADER, AUTHORIZATION_VALUE);
        headers.put(AUTHORIZATION_HEADER_UPPER, AUTHORIZATION_VALUE);

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertTrue(matchingHeaderNames(headers, AUTHORIZATION_HEADER).isEmpty());
    }

    @Test
    void testStripRequestReplicationHeadersRemovesAllProtocolHeaders() {
        final Map<String, String> headers = new HashMap<>();
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            headers.put(rh.getHeader(), SPOOFED_VALUE);
        }
        for (final ReplicationHeader rh : ReplicationHeader.values()) {
            headers.put(rh.getHeader(), SPOOFED_VALUE);
        }
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            assertNull(headers.get(rh.getHeader()), "Replication header should have been stripped: " + rh.getHeader());
        }
        for (final ReplicationHeader rh : ReplicationHeader.values()) {
            assertNull(headers.get(rh.getHeader()), "Replication header should have been stripped: " + rh.getHeader());
        }
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testStripReplicationMarkerHeadersRemovesOnlyMarkers() {
        final Map<String, String> headers = new HashMap<>();
        for (final ReplicationHeader rh : ReplicationHeader.values()) {
            headers.put(rh.getHeader(), SPOOFED_VALUE);
        }
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            headers.put(rh.getHeader(), SHOULD_SURVIVE_VALUE);
        }
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripReplicationMarkerHeaders(headers);

        for (final ReplicationHeader rh : ReplicationHeader.values()) {
            assertNull(headers.get(rh.getHeader()), "Replication marker header should have been stripped: " + rh.getHeader());
        }
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            assertEquals(SHOULD_SURVIVE_VALUE, headers.get(rh.getHeader()), "Request replication header should have been preserved: " + rh.getHeader());
        }
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testStripReplicationMarkerHeadersCaseInsensitive() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Request-Replicated", Boolean.TRUE.toString());
        headers.put("Request-Forwarded-To-Coordinator", Boolean.TRUE.toString());
        headers.put("Replication-Target-Id", SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripReplicationMarkerHeaders(headers);

        assertFalse(headers.containsKey("Request-Replicated"));
        assertFalse(headers.containsKey("Request-Forwarded-To-Coordinator"));
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get("Replication-Target-Id"));
    }

    @Test
    void testStripRequestReplicationHeadersCaseInsensitive() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Request-Replicated", Boolean.TRUE.toString());
        headers.put("EXECUTION-CONTINUE", Boolean.TRUE.toString());

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        assertFalse(headers.containsKey("Request-Replicated"));
        assertFalse(headers.containsKey("EXECUTION-CONTINUE"));
    }

    @Test
    void testStripRequestReplicationHeadersRemovesEveryCaseVariant() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Request-Replicated", Boolean.TRUE.toString());
        headers.put("request-replicated", Boolean.FALSE.toString());
        headers.put("EXECUTION-CONTINUE", Boolean.TRUE.toString());
        headers.put("execution-continue", Boolean.FALSE.toString());

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        assertTrue(headers.isEmpty());
    }

    @Test
    void testStripHopByHopHeaders() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(ACCEPT_ENCODING_HEADER, "gzip, deflate");
        headers.put(CONTENT_ENCODING_HEADER, "gzip");
        headers.put(CONTENT_LENGTH_HEADER, "12345");
        headers.put(HOST_HEADER, HOST_VALUE);
        headers.put(TE_HEADER, "trailers, deflate");
        headers.put(TRANSFER_ENCODING_HEADER, "chunked");
        headers.put(CONNECTION_HEADER, "keep-alive");
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripHopByHopHeaders(headers);

        assertNull(headers.get(ACCEPT_ENCODING_HEADER));
        assertNull(headers.get(CONTENT_ENCODING_HEADER));
        assertNull(headers.get(CONTENT_LENGTH_HEADER));
        assertNull(headers.get(HOST_HEADER));
        assertNull(headers.get(TE_HEADER));
        assertNull(headers.get(TRANSFER_ENCODING_HEADER));
        assertNull(headers.get(CONNECTION_HEADER));
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testStripAuthCookies() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(COOKIE_HEADER, "__Secure-Authorization-Bearer=token123; __Secure-Request-Token=rt456; other=value");
        headers.put(COOKIE_HEADER_LOWER, "__Secure-Authorization-Bearer=other-token; keep=yes");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        final String remaining = headers.get(COOKIE_HEADER);
        assertNotNull(remaining);
        assertFalse(remaining.contains("__Secure-Authorization-Bearer"));
        assertFalse(remaining.contains("__Secure-Request-Token"));
        assertTrue(remaining.contains("other=value"));

        final String lowerCaseCookies = headers.get(COOKIE_HEADER_LOWER);
        assertNotNull(lowerCaseCookies);
        assertFalse(lowerCaseCookies.contains("__Secure-Authorization-Bearer"));
        assertTrue(lowerCaseCookies.contains("keep=yes"));
    }

    @Test
    void testRemoveHostHeader() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(HOST_HEADER, HOST_VALUE);
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertNull(headers.get(HOST_HEADER));
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    private static Set<String> matchingHeaderNames(final Map<String, String> headers, final String headerName) {
        return headers.keySet().stream()
                .filter(headerName::equalsIgnoreCase)
                .collect(Collectors.toSet());
    }

    private static String formatEntities(final String... entities) {
        final StringBuilder formattedEntities = new StringBuilder();
        for (final String entity : entities) {
            formattedEntities.append('<').append(entity).append('>');
        }
        return formattedEntities.toString();
    }
}
