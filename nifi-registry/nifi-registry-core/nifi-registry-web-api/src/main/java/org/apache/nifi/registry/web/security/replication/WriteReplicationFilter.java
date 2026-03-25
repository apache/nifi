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
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.registry.cluster.LeaderElectionManager;
import org.apache.nifi.registry.cluster.NodeAddress;
import org.apache.nifi.registry.cluster.NodeRegistry;
import org.apache.nifi.registry.cluster.ReplicationClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.GenericFilterBean;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Servlet filter that implements the NiFi-style write-replication protocol:
 *
 * <ol>
 *   <li><strong>Follower path</strong> — if this node is not the current leader
 *       it returns a {@code 307 Temporary Redirect} pointing at the leader's URL
 *       so that the client retries the write directly against the leader.  This
 *       preserves the client's TLS/mTLS identity end-to-end (unlike transparent
 *       proxying, which would substitute the follower node's certificate on the
 *       outbound connection).</li>
 *   <li><strong>Leader path</strong> — if this node is the leader it lets the
 *       request proceed through the normal filter/resource chain (writing to
 *       the local database), then fans out the same request to all followers
 *       asynchronously so each follower can apply the write to its own local
 *       database.</li>
 *   <li><strong>Replicated path</strong> — a request arriving at a follower
 *       that already carries the {@code X-Registry-Replication: true} header
 *       is a fan-out from the leader. The filter validates the shared-secret
 *       {@code X-Registry-Internal-Auth} header and passes the request through
 *       directly without further forwarding.</li>
 * </ol>
 *
 * <p>This filter is registered before {@link
 * org.apache.nifi.registry.web.security.authorization.ResourceAuthorizationFilter}
 * so that leader→follower fan-out requests (carrying a validated internal auth token)
 * bypass per-resource authorization checks on the receiving follower.
 *
 * <p>When the {@link NodeRegistry} or {@link ReplicationClient} is {@code null}
 * (standalone or database-coordination mode) every request passes through
 * unchanged.
 */
public class WriteReplicationFilter extends GenericFilterBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteReplicationFilter.class);

    private static final Set<String> WRITE_METHODS = Set.of("POST", "PUT", "PATCH", "DELETE");
    private static final String ACTUATOR_PATH_PREFIX = "/actuator/";

    /** Header the leader adds to fan-out requests; prevents forwarding loops. */
    public static final String REPLICATION_HEADER = "X-Registry-Replication";
    /** Shared-secret header verifying that a replicated request originates from the leader. */
    public static final String INTERNAL_AUTH_HEADER = "X-Registry-Internal-Auth";

    private final LeaderElectionManager leaderElectionManager;
    private final NodeRegistry nodeRegistry;          // null in non-ZK modes
    private final ReplicationClient replicationClient; // null in non-ZK modes
    private final String expectedAuthToken;

    public WriteReplicationFilter(final LeaderElectionManager leaderElectionManager,
            final NodeRegistry nodeRegistry,
            final ReplicationClient replicationClient,
            final String expectedAuthToken) {
        this.leaderElectionManager = leaderElectionManager;
        this.nodeRegistry = nodeRegistry;
        this.replicationClient = replicationClient;
        this.expectedAuthToken = expectedAuthToken;
    }

    @Override
    public void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
            final FilterChain chain) throws IOException, ServletException {

        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        // 1. Let read-only requests pass through immediately.
        if (!WRITE_METHODS.contains(request.getMethod())) {
            chain.doFilter(request, response);
            return;
        }

        // 2. Actuator writes (health/metrics management) bypass replication.
        if (request.getServletPath().startsWith(ACTUATOR_PATH_PREFIX)) {
            chain.doFilter(request, response);
            return;
        }

        // 3. If cluster replication infrastructure is not active (standalone or
        //    database-coordination mode), pass through unchanged.
        if (nodeRegistry == null || replicationClient == null) {
            chain.doFilter(request, response);
            return;
        }

        // 4. Incoming fan-out from the leader: validate token and apply locally.
        if (request.getHeader(REPLICATION_HEADER) != null) {
            if (!isValidInternalAuth(request)) {
                LOGGER.warn("Received replicated request with invalid or missing '{}' header from {}. Rejecting.",
                        INTERNAL_AUTH_HEADER, request.getRemoteAddr());
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setContentType("text/plain");
                response.getWriter().write("Invalid internal replication auth token.");
                return;
            }
            // Apply the replicated write to this node's local database.
            chain.doFilter(request, response);
            return;
        }

        // 5. This node is a follower: redirect the client to the leader.
        //    Transparent HTTP proxying (forwardToLeader) cannot preserve the client's
        //    TLS/mTLS identity because the outbound JDK HttpClient connection carries the
        //    node's own certificate, not the original client certificate.  A 307 Temporary
        //    Redirect instructs the client (or an intelligent load balancer) to retry the
        //    write directly against the leader, preserving client credentials end-to-end.
        if (!leaderElectionManager.isLeader()) {
            final Optional<NodeAddress> leaderOpt = nodeRegistry.getLeaderAddress();
            if (leaderOpt.isEmpty()) {
                LOGGER.warn("Cannot redirect write {} {}: no leader is currently known.",
                        request.getMethod(), request.getRequestURI());
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                response.setContentType("text/plain");
                response.getWriter().write("No cluster leader available. Please retry.");
                return;
            }
            final String path = buildPath(request);
            // Guard against open-redirect: the request path must begin with '/' to ensure it
            // is appended as a relative path to the leader's base URL, never as an absolute URL.
            if (path == null || !path.startsWith("/")) {
                LOGGER.warn("Rejecting redirect: unexpected request path '{}' does not start with '/'.", path);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            final String leaderUrl = leaderOpt.get().getBaseUrl() + path;
            LOGGER.debug("Redirecting {} {} to leader '{}' via 307.",
                    request.getMethod(), request.getRequestURI(), leaderOpt.get().getNodeId());
            response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
            response.setHeader("Location", leaderUrl);
            return;
        }

        // 6. This node is the leader: process the write locally, then fan out.
        final ContentCachingRequestWrapper cachedRequest = new ContentCachingRequestWrapper(request);
        final ContentCachingResponseWrapper cachedResponse = new ContentCachingResponseWrapper(response);

        chain.doFilter(cachedRequest, cachedResponse);

        // Always flush the buffered response body back to the client first.
        final int status = cachedResponse.getStatus();
        cachedResponse.copyBodyToResponse();

        // Fan out only on success; a failed write should not be replicated.
        if (status >= 200 && status < 300) {
            final List<NodeAddress> followers = nodeRegistry.getOtherNodes();
            if (!followers.isEmpty()) {
                final byte[] body = cachedRequest.getContentAsByteArray();
                final String path = buildPath(request);
                final Map<String, String> headers = collectHeaders(request);
                LOGGER.debug("Leader fanning out {} {} to {} follower(s).",
                        request.getMethod(), path, followers.size());
                replicationClient.replicateToFollowers(path, request.getMethod(), headers, body, followers);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean isValidInternalAuth(final HttpServletRequest request) {
        if (expectedAuthToken == null || expectedAuthToken.isBlank()) {
            // Auth token not configured — cannot validate; reject for safety.
            return false;
        }
        final String received = request.getHeader(INTERNAL_AUTH_HEADER);
        if (received == null) {
            return false;
        }
        // Constant-time comparison to prevent timing side-channel attacks.
        return MessageDigest.isEqual(
                expectedAuthToken.getBytes(StandardCharsets.UTF_8),
                received.getBytes(StandardCharsets.UTF_8));
    }

    private static String buildPath(final HttpServletRequest request) {
        final String uri = request.getRequestURI();
        final String query = request.getQueryString();
        return (query != null && !query.isEmpty()) ? uri + "?" + query : uri;
    }

    private static Map<String, String> collectHeaders(final HttpServletRequest request) {
        final Map<String, String> headers = new LinkedHashMap<>();
        Collections.list(request.getHeaderNames())
                .forEach(name -> headers.put(name, request.getHeader(name)));
        return headers;
    }
}
