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
package org.apache.nifi.distributed.cache.client;

import java.util.Optional;

public class AtomicCacheEntry<K, V, R> {

    private final K key;
    private V value;
    private final R revision;

    /**
     * Create new cache entry.
     * @param key cache key
     * @param value cache value
     * @param revision cache revision, can be null with a brand new entry
     */
    public AtomicCacheEntry(final K key, final V value, final R revision) {
        this.key = key;
        this.value = value;
        this.revision = revision;
    }

    /**
     * @return the latest revision stored in a cache server
     */
    public Optional<R> getRevision() {
        return Optional.ofNullable(revision);
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

}
