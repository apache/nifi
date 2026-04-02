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
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestReplicationHeaderUtils {

    private static final String TEST_USER_IDENTITY = "alice";

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String AUTHORIZATION_VALUE = "Bearer secret";

    private static final String CUSTOM_HEADER = "X-Custom-Token";
    private static final String CUSTOM_HEADER_VALUE = "custom-token-123";

    private static final String COOKIE_HEADER = "Cookie";
    private static final String HOST_HEADER = "Host";
    private static final String HOST_VALUE = "original-host:8080";

    private static final String CONTENT_LENGTH_HEADER = "Content-Length";
    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
    private static final String CONNECTION_HEADER = "Connection";

    private static final String SPOOFED_VALUE = "spoofed";
    private static final String SHOULD_SURVIVE_VALUE = "should-survive";

    @Test
    void testApplyUserProxyAndStripCredentialsSetsProxiedEntities() {
        final var user = new StandardNiFiUser.Builder().identity(TEST_USER_IDENTITY).build();
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
    void testStripRequestReplicationHeadersRemovesAllProtocolHeaders() {
        final Map<String, String> headers = new HashMap<>();
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            headers.put(rh.getHeader(), SPOOFED_VALUE);
        }
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            assertNull(headers.get(rh.getHeader()), "Replication header should have been stripped: " + rh.getHeader());
        }
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testStripRequestReplicationHeadersCaseInsensitive() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Request-Replicated", "true");
        headers.put("EXECUTION-CONTINUE", "true");

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        assertFalse(headers.containsKey("Request-Replicated"));
        assertFalse(headers.containsKey("EXECUTION-CONTINUE"));
    }

    @Test
    void testStripHopByHopHeaders() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(CONTENT_LENGTH_HEADER, "12345");
        headers.put(HOST_HEADER, HOST_VALUE);
        headers.put(TRANSFER_ENCODING_HEADER, "chunked");
        headers.put(CONNECTION_HEADER, "keep-alive");
        headers.put(CUSTOM_HEADER, SHOULD_SURVIVE_VALUE);

        ReplicationHeaderUtils.stripHopByHopHeaders(headers);

        assertNull(headers.get(CONTENT_LENGTH_HEADER));
        assertNull(headers.get(HOST_HEADER));
        assertNull(headers.get(TRANSFER_ENCODING_HEADER));
        assertNull(headers.get(CONNECTION_HEADER));
        assertEquals(SHOULD_SURVIVE_VALUE, headers.get(CUSTOM_HEADER));
    }

    @Test
    void testStripAuthCookies() {
        final Map<String, String> headers = new HashMap<>();
        headers.put(COOKIE_HEADER, "__Secure-Authorization-Bearer=token123; __Secure-Request-Token=rt456; other=value");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        final String remaining = headers.get(COOKIE_HEADER);
        assertNotNull(remaining);
        assertFalse(remaining.contains("__Secure-Authorization-Bearer"));
        assertFalse(remaining.contains("__Secure-Request-Token"));
        assertTrue(remaining.contains("other=value"));
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
}
