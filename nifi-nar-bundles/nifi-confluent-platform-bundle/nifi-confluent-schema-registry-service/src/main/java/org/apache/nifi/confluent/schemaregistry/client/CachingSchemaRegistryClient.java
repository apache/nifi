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

package org.apache.nifi.confluent.schemaregistry.client;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;


public class CachingSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryClient client;
    private final long expirationNanos;

    private final Map<String, CachedRecordSchema> nameCache;
    private final Map<Integer, CachedRecordSchema> idCache;


    public CachingSchemaRegistryClient(final SchemaRegistryClient toWrap, final int cacheSize, final long expirationNanos) {
        this.client = toWrap;
        this.expirationNanos = expirationNanos;

        nameCache = new Cache<>(cacheSize);
        idCache = new Cache<>(cacheSize);
    }

    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        RecordSchema schema = getFromCache(nameCache, schemaName);
        if (schema != null) {
            return schema;
        }

        schema = client.getSchema(schemaName);

        synchronized (nameCache) {
            nameCache.put(schemaName, new CachedRecordSchema(schema));
        }

        return schema;
    }

    @Override
    public RecordSchema getSchema(final int schemaId) throws IOException, SchemaNotFoundException {
        RecordSchema schema = getFromCache(idCache, schemaId);
        if (schema != null) {
            return schema;
        }

        schema = client.getSchema(schemaId);

        synchronized (idCache) {
            idCache.put(schemaId, new CachedRecordSchema(schema));
        }

        return schema;
    }

    private RecordSchema getFromCache(final Map<?, CachedRecordSchema> cache, final Object key) {
        final CachedRecordSchema cachedSchema;
        synchronized (cache) {
            cachedSchema = cache.get(key);
        }

        if (cachedSchema == null) {
            return null;
        }

        if (cachedSchema.isOlderThan(System.nanoTime() - expirationNanos)) {
            return null;
        }

        return cachedSchema.getSchema();
    }


    private static class Cache<K, V> extends LinkedHashMap<K, V> {
        private final int cacheSize;

        public Cache(final int cacheSize) {
            this.cacheSize = cacheSize;
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
            return size() >= cacheSize;
        }
    }


    private static class CachedRecordSchema {
        private final RecordSchema schema;
        private final long cachedTimestamp;

        public CachedRecordSchema(final RecordSchema schema) {
            this(schema, System.nanoTime());
        }

        public CachedRecordSchema(final RecordSchema schema, final long timestamp) {
            this.schema = schema;
            this.cachedTimestamp = timestamp;
        }

        public RecordSchema getSchema() {
            return schema;
        }

        public boolean isOlderThan(final long timestamp) {
            return cachedTimestamp < timestamp;
        }
    }
}
