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
package org.apache.nifi.distributed.cache.server.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.distributed.cache.server.EvictionPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMapCache implements MapCache {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMapCache.class);

    private final Map<ByteBuffer, MapCacheRecord> cache = new HashMap<>();
    private final SortedMap<MapCacheRecord, ByteBuffer> inverseCacheMap;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final String serviceIdentifier;

    private final int maxSize;

    public SimpleMapCache(final String serviceIdentifier, final int maxSize, final EvictionPolicy evictionPolicy) {
        // need to change to ConcurrentMap as this is modified when only the readLock is held
        inverseCacheMap = new ConcurrentSkipListMap<>(evictionPolicy.getComparator());
        this.serviceIdentifier = serviceIdentifier;
        this.maxSize = maxSize;
    }

    @Override
    public String toString() {
        return "SimpleMapCache[service id=" + serviceIdentifier + "]";
    }

    // don't need synchronized because this method is only called when the writeLock is held, and all
    // public methods obtain either the read or write lock
    private MapCacheRecord evict() {
        if (cache.size() < maxSize) {
            return null;
        }

        final MapCacheRecord recordToEvict = inverseCacheMap.firstKey();
        final ByteBuffer valueToEvict = inverseCacheMap.remove(recordToEvict);
        cache.remove(valueToEvict);

        if (logger.isDebugEnabled()) {
            logger.debug("Evicting value {} from cache", new String(valueToEvict.array(), StandardCharsets.UTF_8));
        }

        return recordToEvict;
    }

    @Override
    public MapPutResult putIfAbsent(final ByteBuffer key, final ByteBuffer value) {
        writeLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                // Record is null. We will add.
                return put(key, value, record);
            }

            // Record is not null. Increment hit count and return result indicating that record was not added.
            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return new MapPutResult(false, record, record, null);
        } finally {
            writeLock.unlock();
        }
    }

    private MapPutResult put(final ByteBuffer key, final ByteBuffer value, final MapCacheRecord existing) {
        // evict if we need to in order to make room for a new entry.
        final MapCacheRecord evicted = evict();

        final long revision;
        if (existing == null) {
            revision = 0;
        } else {
            revision = existing.getRevision() + 1;
            inverseCacheMap.remove(existing);
        }

        final MapCacheRecord record = new MapCacheRecord(key, value, revision);
        cache.put(key, record);
        inverseCacheMap.put(record, key);

        return new MapPutResult(true, record, existing, evicted);
    }

    @Override
    public MapPutResult put(final ByteBuffer key, final ByteBuffer value) throws IOException {
        writeLock.lock();
        try {
            final MapCacheRecord existing = cache.get(key);
            return put(key, value, existing);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean containsKey(final ByteBuffer key) {
        readLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                return false;
            }

            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ByteBuffer get(final ByteBuffer key) {
        readLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                return null;
            }

            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return record.getValue();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> subMap(List<ByteBuffer> keys) throws IOException {
        if (keys == null) {
            return null;
        }
        Map<ByteBuffer, ByteBuffer> results = new HashMap<>(keys.size());
        readLock.lock();
        try {
            keys.forEach((key) -> {
                final MapCacheRecord record = cache.get(key);
                if (record == null) {
                    results.put(key, null);
                } else {
                    inverseCacheMap.remove(record);
                    record.hit();
                    inverseCacheMap.put(record, key);
                    results.put(key, record.getValue());
                }
            });
        } finally {
            readLock.unlock();
        }
        return results;
    }

    @Override
    public ByteBuffer remove(ByteBuffer key) throws IOException {
        writeLock.lock();
        try {
            final MapCacheRecord record = cache.remove(key);
            if (record == null) {
                return null;
            }
            inverseCacheMap.remove(record);
            return record.getValue();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> removeByPattern(String regex) throws IOException {
        writeLock.lock();
        try {
            final Map<ByteBuffer, ByteBuffer> removedMap = new HashMap<>();
            final List<MapCacheRecord> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (ByteBuffer key : cache.keySet()) {
                // Key must be backed by something that array() returns a byte[] that can be converted into a String via the default charset
                Matcher m = p.matcher(new String(key.array()));
                if (m.matches()) {
                    removedRecords.add(cache.get(key));
                }
            }
            removedRecords.forEach((record) -> {
                cache.remove(record.getKey());
                inverseCacheMap.remove(record);
                removedMap.put(record.getKey(), record.getValue());
            });

            return removedMap;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public MapCacheRecord fetch(ByteBuffer key) throws IOException {
        readLock.lock();
        try {
            final MapCacheRecord record = cache.get(key);
            if (record == null) {
                return null;
            }

            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, key);

            return record;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public MapPutResult replace(MapCacheRecord inputRecord) throws IOException {
        writeLock.lock();
        try {
            final ByteBuffer key = inputRecord.getKey();
            final ByteBuffer value = inputRecord.getValue();
            final MapCacheRecord existing = fetch(key);
            if (existing != null && inputRecord.getRevision() != existing.getRevision()) {
                // The key has been updated by other operation.
                return new MapPutResult(false, inputRecord, existing, null);
            }

            return put(key, value, existing);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void shutdown() throws IOException {
    }
}
