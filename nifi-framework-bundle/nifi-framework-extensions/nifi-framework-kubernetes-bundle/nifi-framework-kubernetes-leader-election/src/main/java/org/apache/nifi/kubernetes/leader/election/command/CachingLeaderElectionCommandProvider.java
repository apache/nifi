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
package org.apache.nifi.kubernetes.leader.election.command;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Leader Election Command Provider implementing caching of Leader status minimizing requests to Kubernetes
 */
public class CachingLeaderElectionCommandProvider implements LeaderElectionCommandProvider {
    /** Expiration for cached Leader Identifiers shorter than the Lease Duration */
    private static final Duration DEFAULT_CACHE_EXPIRATION = Duration.ofSeconds(5);

    private final LeaderElectionCommandProvider provider;

    private final Duration cacheExpiration;

    private final Map<String, CachedLeader> cachedLeaders = new ConcurrentHashMap<>();

    /** Requests to the delegated provider currently running, at most one for each Election Name */
    private final Map<String, CompletableFuture<Optional<String>>> loadsInFlight = new ConcurrentHashMap<>();

    public CachingLeaderElectionCommandProvider(final LeaderElectionCommandProvider provider) {
        this(provider, DEFAULT_CACHE_EXPIRATION);
    }

    CachingLeaderElectionCommandProvider(final LeaderElectionCommandProvider provider, final Duration cacheExpiration) {
        this.provider = Objects.requireNonNull(provider, "Provider required");
        this.cacheExpiration = Objects.requireNonNull(cacheExpiration, "Cache Expiration required");
    }

    @Override
    public Runnable getCommand(
            final String name,
            final String identity,
            final Runnable onStartLeading,
            final Runnable onStopLeading,
            final Consumer<String> onNewLeader
    ) {
        return provider.getCommand(name, identity, onStartLeading, onStopLeading, onNewLeader);
    }

    /**
     * Find Leader Identifier for specified Election Name returning a cached result when available and not expired.
     * When the cached result is missing or expired, concurrent callers share one request to the delegated provider
     * instead of each sending a request, so the request rate does not depend on the number of calling threads.
     *
     * @param name Election Name
     * @return Leader Identifier or empty when not found
     */
    @Override
    public Optional<String> findLeader(final String name) {
        final CachedLeader cachedLeader = cachedLeaders.get(name);
        if (cachedLeader == null || cachedLeader.isExpired()) {
            return loadLeader(name);
        }
        return Optional.ofNullable(cachedLeader.leader);
    }

    private Optional<String> loadLeader(final String name) {
        final CompletableFuture<Optional<String>> load = new CompletableFuture<>();
        final CompletableFuture<Optional<String>> loadInFlight = loadsInFlight.putIfAbsent(name, load);
        if (loadInFlight == null) {
            return runLoad(name, load);
        }
        return awaitLoad(loadInFlight);
    }

    private Optional<String> runLoad(final String name, final CompletableFuture<Optional<String>> load) {
        try {
            // A previous load can finish between the caller checking the cache and registering this load
            final CachedLeader cachedLeader = cachedLeaders.get(name);
            final Optional<String> leader;
            if (cachedLeader == null || cachedLeader.isExpired()) {
                leader = provider.findLeader(name);
                final Instant expiration = Instant.now().plus(cacheExpiration);
                cachedLeaders.put(name, new CachedLeader(leader.orElse(null), expiration));
            } else {
                leader = Optional.ofNullable(cachedLeader.leader);
            }
            load.complete(leader);
            return leader;
        } catch (final Throwable e) {
            // Failures are shared with waiting callers but never cached, the next call sends a new request
            load.completeExceptionally(e);
            throw e;
        } finally {
            loadsInFlight.remove(name, load);
        }
    }

    private Optional<String> awaitLoad(final CompletableFuture<Optional<String>> load) {
        try {
            return load.join();
        } catch (final CompletionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof final RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof final Error error) {
                throw error;
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        cachedLeaders.clear();
        provider.close();
    }

    private record CachedLeader(String leader, Instant expiration) {

        private boolean isExpired() {
            return Instant.now().isAfter(expiration);
        }
    }
}
