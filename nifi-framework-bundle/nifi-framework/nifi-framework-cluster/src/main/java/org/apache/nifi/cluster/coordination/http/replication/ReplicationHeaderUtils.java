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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.http.SecurityCookieName;
import org.apache.nifi.web.security.http.SecurityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared header-preparation logic for cluster request replication. Both {@link ThreadPoolRequestReplicator}
 * and {@link StandardUploadRequestReplicator} use these utilities so that credential stripping, proxied-entity
 * injection, and replication-header sanitisation follow the same policy.
 */
public final class ReplicationHeaderUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationHeaderUtils.class);

    private static final String COOKIE_HEADER = "Cookie";
    private static final String HOST_HEADER = "Host";

    /**
     * Headers that must not be forwarded because they are hop-by-hop, describe the original
     * transport framing, or negotiate content/transfer encodings that the upload replication
     * path cannot handle transparently.
     *
     * <p>This is intentionally a superset of
     * {@link org.apache.nifi.cluster.coordination.http.replication.client.StandardHttpReplicationClient#DISALLOWED_HEADERS
     * StandardHttpReplicationClient.DISALLOWED_HEADERS}, which omits encoding-negotiation
     * headers because {@code StandardHttpReplicationClient} manages its own gzip pipeline.
     * {@code StandardUploadRequestReplicator} reads responses raw, so encoding headers must
     * also be stripped here.
     */
    static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            "accept-encoding", "connection", "content-encoding", "content-length",
            "expect", "host", "te", "transfer-encoding", "upgrade"
    );

    private ReplicationHeaderUtils() {
    }

    /**
     * Prepares headers for a replicated request. When a non-null user is provided, the
     * {@code X-ProxiedEntitiesChain} and {@code X-ProxiedEntityGroups} headers are set so the
     * receiving node knows the request is on behalf of that user. When user is {@code null},
     * these headers are omitted, indicating the request is made directly by the cluster node
     * itself (e.g. for background connector state polling).
     *
     * <p>In all cases the {@code Authorization} header, auth-related cookies, and the
     * {@code Host} header are removed because the replicated call uses mTLS for transport
     * authentication and the proxied-entity chain for user identity.
     *
     * @param headers mutable map of HTTP headers to update
     * @param user    the user on whose behalf the request is being made, or {@code null} for direct node requests
     */
    public static void applyUserProxyAndStripCredentials(final Map<String, String> headers, final NiFiUser user) {
        if (user == null) {
            logger.debug("No user provided: omitting proxied entities header from request");
        } else {
            logger.debug("NiFi User provided: adding proxied entities header to request");
            headers.put(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN,
                    ProxiedEntitiesUtils.buildProxiedEntitiesChainString(user));

            headers.put(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS,
                    ProxiedEntitiesUtils.buildProxiedEntityGroupsString(user.getIdentityProviderGroups()));
        }

        removeHeader(headers, SecurityHeader.AUTHORIZATION.getHeader());
        removeCookie(headers, SecurityCookieName.AUTHORIZATION_BEARER.getName());
        removeCookie(headers, SecurityCookieName.REQUEST_TOKEN.getName());

        removeHeader(headers, HOST_HEADER);
    }

    /**
     * Removes all {@link RequestReplicationHeader} names from the map (case-insensitive) so
     * that inbound client requests cannot spoof replication protocol headers.
     */
    public static void stripRequestReplicationHeaders(final Map<String, String> headers) {
        for (final RequestReplicationHeader rh : RequestReplicationHeader.values()) {
            removeHeader(headers, rh.getHeader());
        }
    }

    /**
     * Removes hop-by-hop / transport-framing headers that should not be forwarded in a
     * replicated request (case-insensitive).
     */
    public static void stripHopByHopHeaders(final Map<String, String> headers) {
        for (final String name : HOP_BY_HOP_HEADERS) {
            removeHeader(headers, name);
        }
    }

    static void removeHeader(final Map<String, String> headers, final String headerNameSearch) {
        findHeaderName(headers, headerNameSearch).ifPresent(headers::remove);
    }

    static void removeCookie(final Map<String, String> headers, final String cookieName) {
        final Optional<String> cookieHeaderNameFound = findHeaderName(headers, COOKIE_HEADER);

        if (cookieHeaderNameFound.isPresent()) {
            final String cookieHeaderName = cookieHeaderNameFound.get();
            final String rawCookies = headers.get(cookieHeaderName);
            final String[] rawCookieParts = rawCookies.split(";");
            final Set<String> filteredCookieParts = Stream.of(rawCookieParts)
                    .map(String::trim)
                    .filter(cookie -> !cookie.startsWith(cookieName + "="))
                    .collect(Collectors.toSet());

            if (filteredCookieParts.isEmpty()) {
                headers.remove(cookieHeaderName);
            } else {
                headers.put(cookieHeaderName, StringUtils.join(filteredCookieParts, "; "));
            }
        }
    }

    /**
     * Find an HTTP header name in a map regardless of case, since HTTP/1.1 capitalises headers
     * but HTTP/2 returns lowercased headers.
     */
    static Optional<String> findHeaderName(final Map<String, String> headers, final String headerName) {
        if (headerName == null || headerName.isBlank()) {
            return Optional.empty();
        }
        return headers.keySet().stream()
                .filter(headerName::equalsIgnoreCase)
                .findFirst();
    }
}
