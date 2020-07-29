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
package org.apache.nifi.web.security.otp;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.web.security.util.CacheKey;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a specific wrapper implementation based on the Guava {@link Cache} but with
 * reverse-index capability because of the special use case (a user [the cache value] can only have
 * one active token [the cache key] at a time). This allows reverse lookup semantics.
 */
public class TokenCache extends AbstractCache<CacheKey, String> {
    private static final Logger logger = LoggerFactory.getLogger(TokenCache.class);

    private final String contentsDescription;
    private final Cache<CacheKey, String> internalCache;

    public TokenCache(String contentsDescription, final int duration, final TimeUnit units) {
        this.contentsDescription = contentsDescription;
        internalCache = CacheBuilder.newBuilder().expireAfterWrite(duration, units).build();
    }

    /**
     * Returns the value associated with {@code key} in this cache, or {@code null} if there is no
     * cached value for {@code key}.
     *
     * @param key the (wrapped) {@code token}
     * @since 11.0
     * @return the retrieved value ({@code user})
     */
    @Override
    public @Nullable String getIfPresent(Object key) {
        return internalCache.getIfPresent(key);
    }

    /**
     * Puts the provided value ({@code user}) in the cache at the provided key (wrapped {@code token}).
     *
     * @param key the cache key
     * @param value the value to insert
     * @since 11.0
     */
    @Override
    public void put(CacheKey key, String value) {
            internalCache.put(key, value);
    }

    /**
     * Returns {@code true} if the cache contains the provided value.
     *
     * @param value the value ({@code user}) to look for
     * @return true if the user exists in the cache
     */
    public boolean containsValue(String value) {
        return internalCache.asMap().containsValue(value);
    }

    /**
     * Returns the {@link CacheKey} representing the key ({@code token}) associated with the provided value ({@code user}).
     *
     * @param value the value ({@code user}) to look for
     * @return the CacheKey ({@code token}) associated with this user, or {@code null} if the user has no tokens in this cache
     */
    @Nullable
    public CacheKey getKeyForValue(String value) {
        if (containsValue(value)) {
            Map<CacheKey, String> cacheMap = internalCache.asMap();
            for (Map.Entry<CacheKey, String> e : cacheMap.entrySet()) {
                if (e.getValue().equals(value)) {
                    return e.getKey();
                }
            }
            throw new IllegalStateException("The value existed in the cache but expired during retrieval");
        } else {
            return null;
        }
    }

    // Override the unsupported abstract methods from the parent

    @Override
    public void invalidate(Object key) {
        internalCache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        internalCache.invalidateAll(internalCache.asMap().keySet());
    }

    @Override
    public long size() {
        return internalCache.size();
    }

    @Override
    public CacheStats stats() {
        return internalCache.stats();
    }

    @Override
    public ConcurrentMap<CacheKey, String> asMap() {
        return internalCache.asMap();
    }

    /**
     * Returns a string representation of the cache.
     *
     * @return a string representation of the cache
     */
    @Override
    public String toString() {
        return new StringBuilder("TokenCache for ")
                .append(contentsDescription)
                .append(" with ")
                .append(internalCache.size())
                .append(" elements")
                .toString();
    }
}
