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
package org.apache.nifi.distributed.cache.server.protocol;

import org.apache.nifi.distributed.cache.operations.CacheOperation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Map Cache Request with operation and other optional properties
 */
public class MapCacheRequest {
    private final CacheOperation cacheOperation;

    private byte[] key;

    private byte[] value;

    private String pattern;

    private long revision;

    private List<byte[]> keys = Collections.emptyList();

    public MapCacheRequest(
            final CacheOperation cacheOperation
    ) {
        this.cacheOperation = Objects.requireNonNull(cacheOperation, "Cache Operation required");
    }

    public MapCacheRequest(
            final CacheOperation cacheOperation,
            final byte[] key
    ) {
        this(cacheOperation);
        this.key = Objects.requireNonNull(key, "Key required");
    }

    public MapCacheRequest(
            final CacheOperation cacheOperation,
            final byte[] key,
            final byte[] value
    ) {
        this(cacheOperation, key);
        this.value = Objects.requireNonNull(value, "Value required");
    }

    public MapCacheRequest(
            final CacheOperation cacheOperation,
            final byte[] key,
            final long revision,
            final byte[] value
    ) {
        this(cacheOperation, key, value);
        this.revision = revision;
    }

    public MapCacheRequest(
            final CacheOperation cacheOperation,
            final String pattern
    ) {
        this(cacheOperation);
        this.pattern = Objects.requireNonNull(pattern, "Pattern required");
    }

    public MapCacheRequest(
            final CacheOperation cacheOperation,
            final List<byte[]> keys
    ) {
        this(cacheOperation);
        this.keys = Objects.requireNonNull(keys, "Keys required");
    }

    public CacheOperation getCacheOperation() {
        return cacheOperation;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public String getPattern() {
        return pattern;
    }

    public long getRevision() {
        return revision;
    }

    public List<byte[]> getKeys() {
        return keys;
    }
}
