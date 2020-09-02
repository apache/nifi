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
package org.apache.nifi.hazelcast.services.cache;

import com.hazelcast.map.IMap;
import com.hazelcast.map.ReachedMaxSizeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Implementation of {@link HazelcastCache} backed by Hazelcast's IMap data structure. It's purpose is to wrap Hazelcast implementation specific details in order to
 * make it possible to easily change version or data structure.
 */
public class IMapBasedHazelcastCache implements HazelcastCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(IMapBasedHazelcastCache.class);

    private final String name;
    private final long ttlInMillis;
    private final IMap<String, byte[]> repository;

    /**
     * @param name Name of the cache stored for identification.
     * @param ttlInMillis The guaranteed lifetime of a cache entry in milliseconds.
     * @param repository Reference to the actual storage. It should be the IMap with the same identifier as cache name.
     */
    public IMapBasedHazelcastCache(
            final String name,
            final long ttlInMillis,
            final IMap<String, byte[]> repository) {
        this.name = name;
        this.ttlInMillis = ttlInMillis;
        this.repository = repository;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte[] get(final String key) {
        return repository.get(key);
    }

    @Override
    public byte[] putIfAbsent(final String key, final byte[] value) {
        return repository.putIfAbsent(key, value, ttlInMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean put(final String key, final byte[] value) {
        try {
            repository.put(key, value, ttlInMillis, TimeUnit.MILLISECONDS);
            return true;
        } catch (final ReachedMaxSizeException e) {
            LOGGER.error("Cache {} reached the maximum allowed size!", name);
            return false;
        }
    }

    @Override
    public boolean contains(final String key) {
        return repository.containsKey(key);
    }

    @Override
    public boolean remove(final String key) {
        return repository.remove(key) != null;
    }

    @Override
    public int removeAll(final Predicate<String> keyMatcher) {
        // Note: the Hazelcast IMap provides support for predicate based <code>removeAll</code> method, but it neither atomic or provides information about the number of deleted items.
        final Set<String> keys = repository.keySet();
        int result = 0;

        for (final String key : keys) {
            if (keyMatcher.test(key)) {
                repository.remove(key);
                result++;
            }
        }

        return result;
    }

    @Override
    public HazelcastCacheEntryLock acquireLock(final String key) {
        if (key == null) {
            throw new IllegalArgumentException("The key of acquired lock cannot be null!");
        }

        final IMapAdapterEntryLock lock = new IMapAdapterEntryLock(key);
        lock.lock();
        return lock;
    }

    private final class IMapAdapterEntryLock implements HazelcastCacheEntryLock {
        private final String key;

        private IMapAdapterEntryLock(final String key) {
            this.key = key;
        }

        void lock() {
            repository.lock(key);
        }

        @Override
        public void close() {
            repository.unlock(key);
        }
    }
}
