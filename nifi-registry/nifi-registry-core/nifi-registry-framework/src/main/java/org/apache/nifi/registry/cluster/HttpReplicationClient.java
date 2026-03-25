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
package org.apache.nifi.registry.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link ReplicationClient} backed by the JDK {@link HttpClient} (JDK 11+).
 *
 * <p>No additional Maven dependency is required. A single shared
 * {@link HttpClient} instance is used for all requests; it is thread-safe
 * and manages its own connection pool.
 *
 * <p>Fan-out to followers is performed on a cached-thread-pool executor so
 * it does not block the leader's response to the original client. Failures
 * are logged; the implementation does not retry (a future phase can add
 * retry/reconciliation logic).
 *
 * <p>Hop-by-hop headers (RFC 7230 §6.1) are stripped before forwarding to
 * avoid protocol errors on independent TCP connections.
 */
public class HttpReplicationClient implements ReplicationClient, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpReplicationClient.class);

    /** Header added to fan-out requests so followers skip re-forwarding. */
    public static final String REPLICATION_HEADER = "X-Registry-Replication";
    /** Shared-secret header verifying that a replicated request comes from the leader. */
    public static final String INTERNAL_AUTH_HEADER = "X-Registry-Internal-Auth";

    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "keep-alive", "transfer-encoding", "te",
            "trailers", "upgrade", "proxy-authorization", "proxy-authenticate",
            "content-length"   // HttpClient sets this from the body publisher
    );

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final String authToken;
    private final HttpClient httpClient;
    private final ExecutorService fanOutExecutor;

    public HttpReplicationClient(final String authToken) {
        this.authToken = authToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .executor(Executors.newCachedThreadPool(r -> {
                    final Thread t = new Thread(r, "registry-replication-http");
                    t.setDaemon(true);
                    return t;
                }))
                .build();
        this.fanOutExecutor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r, "registry-fanout");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void destroy() {
        fanOutExecutor.shutdown();
        try {
            if (!fanOutExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                fanOutExecutor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // ReplicationClient
    // -------------------------------------------------------------------------

    @Override
    public void replicateToFollowers(final String path, final String method,
            final Map<String, String> headers, final byte[] body,
            final List<NodeAddress> followers) {

        for (final NodeAddress follower : followers) {
            fanOutExecutor.submit(() -> replicateToOne(path, method, headers, body, follower));
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void replicateToOne(final String path, final String method,
            final Map<String, String> headers, final byte[] body, final NodeAddress follower) {
        final String url = buildUrl(follower.getBaseUrl(), path, null);
        LOGGER.debug("Replicating {} {} to follower '{}'.", method, path, follower.getNodeId());

        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .method(method, bodyPublisher(body))
                    .timeout(REQUEST_TIMEOUT)
                    .header(REPLICATION_HEADER, "true")
                    .header(INTERNAL_AUTH_HEADER, authToken);

            headers.forEach((name, value) -> {
                if (!HOP_BY_HOP.contains(name.toLowerCase())
                        && !REPLICATION_HEADER.equalsIgnoreCase(name)
                        && !INTERNAL_AUTH_HEADER.equalsIgnoreCase(name)) {
                    try {
                        builder.header(name, value);
                    } catch (final IllegalArgumentException e) {
                        // Skip restricted headers (e.g. host).
                    }
                }
            });

            final HttpResponse<Void> response =
                    httpClient.send(builder.build(), HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() >= 400) {
                LOGGER.warn("Replication to follower '{}' for {} {} returned HTTP {}.",
                        follower.getNodeId(), method, path, response.statusCode());
            } else {
                LOGGER.debug("Replicated {} {} to follower '{}': HTTP {}.",
                        method, path, follower.getNodeId(), response.statusCode());
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted replicating {} {} to follower '{}'.", method, path, follower.getNodeId());
        } catch (final Exception e) {
            LOGGER.error("Failed to replicate {} {} to follower '{}': {}",
                    method, path, follower.getNodeId(), e.getMessage());
        }
    }

    private static String buildUrl(final String baseUrl, final String path, final String queryString) {
        final StringBuilder sb = new StringBuilder(baseUrl);
        if (!path.startsWith("/")) {
            sb.append('/');
        }
        sb.append(path);
        if (queryString != null && !queryString.isEmpty()) {
            sb.append('?').append(queryString);
        }
        return sb.toString();
    }

    private static HttpRequest.BodyPublisher bodyPublisher(final byte[] body) {
        return (body != null && body.length > 0)
                ? HttpRequest.BodyPublishers.ofByteArray(body)
                : HttpRequest.BodyPublishers.noBody();
    }

}
