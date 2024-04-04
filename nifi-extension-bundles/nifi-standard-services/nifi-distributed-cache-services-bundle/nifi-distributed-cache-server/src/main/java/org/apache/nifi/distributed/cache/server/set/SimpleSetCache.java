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
package org.apache.nifi.distributed.cache.server.set;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.nifi.distributed.cache.server.EvictionPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSetCache implements SetCache {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSetCache.class);

    private final Map<ByteBuffer, SetCacheRecord> cache = new HashMap<>();
    private final SortedMap<SetCacheRecord, ByteBuffer> inverseCacheMap;

    private final String serviceIdentifier;

    private final int maxSize;

    public SimpleSetCache(final String serviceIdentifier, final int maxSize, final EvictionPolicy evictionPolicy) {
        inverseCacheMap = new TreeMap<>(evictionPolicy.getComparator());
        this.serviceIdentifier = serviceIdentifier;
        this.maxSize = maxSize;
    }

    private synchronized SetCacheRecord evict() {
        if (cache.size() < maxSize) {
            return null;
        }

        final SetCacheRecord recordToEvict = inverseCacheMap.firstKey();
        final ByteBuffer valueToEvict = inverseCacheMap.remove(recordToEvict);
        cache.remove(valueToEvict);

        if (logger.isDebugEnabled()) {
            logger.debug("Evicting value {} from cache", new String(valueToEvict.array(), StandardCharsets.UTF_8));
        }

        return recordToEvict;
    }

    @Override
    public synchronized SetCacheResult addIfAbsent(final ByteBuffer value) {
        final SetCacheRecord record = cache.get(value);
        if (record == null) {
            final SetCacheRecord evicted = evict();
            final SetCacheRecord newRecord = new SetCacheRecord(value);
            cache.put(value, newRecord);
            inverseCacheMap.put(newRecord, value);
            return new SetCacheResult(true, newRecord, evicted);
        } else {
            // We have to remove the record and add it again in order to cause the Map to stay sorted
            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, value);

            return new SetCacheResult(false, record, null);
        }
    }

    @Override
    public synchronized SetCacheResult contains(final ByteBuffer value) {
        final SetCacheRecord record = cache.get(value);
        if (record == null) {
            return new SetCacheResult(false, null, null);
        } else {
            // We have to remove the record and add it again in order to cause the Map to stay sorted
            inverseCacheMap.remove(record);
            record.hit();
            inverseCacheMap.put(record, value);

            return new SetCacheResult(true, record, null);
        }
    }

    @Override
    public synchronized SetCacheResult remove(final ByteBuffer value) {
        final SetCacheRecord record = cache.remove(value);
        if (record == null) {
            return new SetCacheResult(false, null, null);
        } else {
            inverseCacheMap.remove(record);
            return new SetCacheResult(true, record, null);
        }
    }

    @Override
    public String toString() {
        return "SimpleSetCache[service id=" + serviceIdentifier + "]";
    }

    @Override
    public void shutdown() throws IOException {
    }
}
