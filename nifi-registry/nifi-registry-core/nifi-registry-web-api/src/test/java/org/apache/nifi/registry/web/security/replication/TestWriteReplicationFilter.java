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
package org.apache.nifi.registry.web.security.replication;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.cluster.NodeAddress;
import org.apache.nifi.registry.cluster.NodeRegistry;
import org.apache.nifi.registry.cluster.ReplicationClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure unit tests for {@link WriteReplicationFilter}.
 *
 * <p>Uses Spring {@link MockHttpServletRequest} / {@link MockHttpServletResponse}
 * so no servlet container is needed. All cluster dependencies are Mockito mocks.
 */
@ExtendWith(MockitoExtension.class)
public class TestWriteReplicationFilter {

    private static final String EXPECTED_AUTH_TOKEN = "test-token-abc";

    @Mock
    private LeaderElectionManager leaderElectionManager;

    @Mock
    private NodeRegistry nodeRegistry;

    @Mock
    private ReplicationClient replicationClient;

    private WriteReplicationFilter filter;

    @BeforeEach
    public void setup() {
        filter = new WriteReplicationFilter(
                leaderElectionManager, nodeRegistry, replicationClient, EXPECTED_AUTH_TOKEN);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * GET requests should pass through without replication logic.
     */
    @Test
    public void testGetRequestPassesThrough() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest("GET", "/nifi-registry-api/buckets");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertNotNull(chain.getRequest(), "Filter chain should have been called for GET");
        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * POST to an actuator path should bypass replication entirely.
     */
    @Test
    public void testActuatorPathPassesThrough() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/actuator/health");
        request.setServletPath("/actuator/health");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertNotNull(chain.getRequest(), "Filter chain should have been called for actuator path");
        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * When nodeRegistry is null (standalone or database-coordination mode)
     * a POST request should pass through unchanged.
     */
    @Test
    public void testNullNodeRegistryPassesThrough() throws Exception {
        final WriteReplicationFilter standaloneFilter = new WriteReplicationFilter(
                leaderElectionManager, null, null, EXPECTED_AUTH_TOKEN);

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        standaloneFilter.doFilter(request, response, chain);

        assertNotNull(chain.getRequest(), "Filter chain should have been called when nodeRegistry is null");
    }

    /**
     * A replicated request with the correct auth token should pass through
     * without being forwarded again (prevents forwarding loops).
     */
    @Test
    public void testReplicatedRequestValidTokenPassesThrough() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.addHeader(WriteReplicationFilter.REPLICATION_HEADER, "true");
        request.addHeader(WriteReplicationFilter.INTERNAL_AUTH_HEADER, EXPECTED_AUTH_TOKEN);
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertNotNull(chain.getRequest(), "Replicated request with valid token should pass through");
        assertEquals(200, response.getStatus());
        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * A replicated request with a wrong auth token should be rejected with 403.
     */
    @Test
    public void testReplicatedRequestInvalidTokenReturnsForbidden() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.addHeader(WriteReplicationFilter.REPLICATION_HEADER, "true");
        request.addHeader(WriteReplicationFilter.INTERNAL_AUTH_HEADER, "wrong-token");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(HttpServletResponse.SC_FORBIDDEN, response.getStatus());
    }

    /**
     * A replicated request missing the auth header entirely should return 403.
     */
    @Test
    public void testReplicatedRequestMissingTokenReturnsForbidden() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.addHeader(WriteReplicationFilter.REPLICATION_HEADER, "true");
        // No INTERNAL_AUTH_HEADER set.
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(HttpServletResponse.SC_FORBIDDEN, response.getStatus());
    }

    /**
     * When this node is a follower it should return a 307 redirect to the leader
     * so the client can re-send its request with its own TLS/mTLS identity intact.
     */
    @Test
    public void testFollowerRedirectsToLeader() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(false);
        final NodeAddress leaderAddress = new NodeAddress("leader", "http://leader:18080");
        when(nodeRegistry.getLeaderAddress()).thenReturn(Optional.of(leaderAddress));

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(HttpServletResponse.SC_TEMPORARY_REDIRECT, response.getStatus());
        assertEquals("http://leader:18080/nifi-registry-api/buckets", response.getHeader("Location"));
        // Chain must NOT be called — follower does not process the write locally.
        assertTrue(chain.getRequest() == null, "Filter chain should NOT have been called for follower redirect");
        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * Query parameters must be preserved in the redirect Location.
     */
    @Test
    public void testFollowerRedirectPreservesQueryString() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(false);
        final NodeAddress leaderAddress = new NodeAddress("leader", "http://leader:18080");
        when(nodeRegistry.getLeaderAddress()).thenReturn(Optional.of(leaderAddress));

        final MockHttpServletRequest request = new MockHttpServletRequest("PUT", "/nifi-registry-api/buckets/123");
        request.setQueryString("version=2");
        final MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertEquals(HttpServletResponse.SC_TEMPORARY_REDIRECT, response.getStatus());
        assertEquals("http://leader:18080/nifi-registry-api/buckets/123?version=2",
                response.getHeader("Location"));
    }

    /**
     * When this node is a follower and no leader is known, return 503.
     */
    @Test
    public void testFollowerNoLeaderReturns503() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(false);
        when(nodeRegistry.getLeaderAddress()).thenReturn(Optional.empty());

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE, response.getStatus());
        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * When this node is the leader and the request succeeds (2xx), it should
     * fan out to followers via replicateToFollowers().
     */
    @Test
    public void testLeaderFansOutOn2xx() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(true);
        final NodeAddress follower = new NodeAddress("follower-1", "http://follower:18080");
        when(nodeRegistry.getOtherNodes()).thenReturn(List.of(follower));

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.setContent("{\"name\":\"test\"}".getBytes());

        // Use a FilterChain that sets a 201 response.
        final FilterChain chain201 = (req, res) ->
                ((HttpServletResponse) res).setStatus(201);

        final MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain201);

        verify(replicationClient).replicateToFollowers(anyString(), anyString(), any(), any(), anyList());
    }

    /**
     * When this node is the leader and the request fails (4xx), it must NOT
     * fan out to followers.
     */
    @Test
    public void testLeaderDoesNotFanOutOn4xx() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(true);

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.setContent("{}".getBytes());

        // FilterChain sets 400 Bad Request.
        final FilterChain chain400 = (req, res) ->
                ((HttpServletResponse) res).setStatus(400);

        final MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain400);

        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }

    /**
     * When this node is the leader and the request fails (5xx), it must NOT
     * fan out to followers.
     */
    @Test
    public void testLeaderDoesNotFanOutOn5xx() throws Exception {
        when(leaderElectionManager.isLeader()).thenReturn(true);

        final MockHttpServletRequest request = new MockHttpServletRequest("POST", "/nifi-registry-api/buckets");
        request.setContent("{}".getBytes());

        // FilterChain sets 500 Internal Server Error.
        final FilterChain chain500 = (req, res) ->
                ((HttpServletResponse) res).setStatus(500);

        final MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain500);

        verify(replicationClient, never()).replicateToFollowers(any(), any(), any(), any(), any());
    }
}
