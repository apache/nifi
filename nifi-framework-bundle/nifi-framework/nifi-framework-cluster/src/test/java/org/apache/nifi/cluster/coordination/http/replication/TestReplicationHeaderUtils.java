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

    @Test
    void testApplyUserProxyAndStripCredentialsSetsProxiedEntities() {
        final var user = new StandardNiFiUser.Builder().identity("alice").build();
        final Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer secret");
        headers.put("Snowflake-Authorization-Token", "sf-token-123");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, user);

        assertNotNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertTrue(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN).contains("alice"));
        assertNotNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
        assertNull(headers.get("Authorization"));
        assertEquals("sf-token-123", headers.get("Snowflake-Authorization-Token"));
    }

    @Test
    void testApplyUserProxyWithNullUserOmitsProxiedEntities() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer secret");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN));
        assertNull(headers.get(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS));
        assertNull(headers.get("Authorization"));
    }

    @Test
    void testStripRequestReplicationHeadersRemovesAllProtocolHeaders() {
        final Map<String, String> headers = new HashMap<>();
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            headers.put(rh.getHeader(), "spoofed");
        }
        headers.put("Snowflake-Authorization-Token", "should-survive");

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);

        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            assertNull(headers.get(rh.getHeader()), "Replication header should have been stripped: " + rh.getHeader());
        }
        assertEquals("should-survive", headers.get("Snowflake-Authorization-Token"));
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
        headers.put("Content-Length", "12345");
        headers.put("Host", "original-host:8080");
        headers.put("Transfer-Encoding", "chunked");
        headers.put("Connection", "keep-alive");
        headers.put("Snowflake-Authorization-Token", "keep-me");

        ReplicationHeaderUtils.stripHopByHopHeaders(headers);

        assertNull(headers.get("Content-Length"));
        assertNull(headers.get("Host"));
        assertNull(headers.get("Transfer-Encoding"));
        assertNull(headers.get("Connection"));
        assertEquals("keep-me", headers.get("Snowflake-Authorization-Token"));
    }

    @Test
    void testStripAuthCookies() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Cookie", "__Secure-Authorization-Bearer=token123; __Secure-Request-Token=rt456; other=value");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        final String remaining = headers.get("Cookie");
        assertNotNull(remaining);
        assertFalse(remaining.contains("__Secure-Authorization-Bearer"));
        assertFalse(remaining.contains("__Secure-Request-Token"));
        assertTrue(remaining.contains("other=value"));
    }

    @Test
    void testRemoveHostHeader() {
        final Map<String, String> headers = new HashMap<>();
        headers.put("Host", "original-host:8080");
        headers.put("X-Custom", "keep");

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, null);

        assertNull(headers.get("Host"));
        assertEquals("keep", headers.get("X-Custom"));
    }
}
