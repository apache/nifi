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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

final public class HashMapHazelcastCache implements HazelcastCache {
    private final String name;
    private final Map<String, byte[]> values = new HashMap<>();
    private final Set<String> lockedEntries = new HashSet<>();

    public HashMapHazelcastCache(final String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte[] get(final String key) {
        return values.get(key);
    }

    @Override
    public byte[] putIfAbsent(final String key, final byte[] value) {
        return values.putIfAbsent(key, value);
    }

    @Override
    public boolean put(final String key, final byte[] value) {
        values.put(key, value);
        return true;
    }

    @Override
    public boolean contains(final String key) {
        return values.containsKey(key);
    }

    @Override
    public boolean remove(final String key) {
        return values.remove(key) != null;
    }

    @Override
    public int removeAll(final Predicate<String> keyMatcher) {
        final Set<String> marked = new HashSet<>();

        for (final String key : values.keySet()) {
            if (keyMatcher.test(key)) {
                marked.add(key);
            }
        }

        for (final String key : marked) {
            values.remove(key);
        }

        return marked.size();
    }

    @Override
    public HazelcastCacheEntryLock acquireLock(final String key) {
        lockedEntries.add(key);
        return () -> lockedEntries.remove(key);
    }

    public Set<String> getLockedEntries() {
        return lockedEntries;
    }
}
