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

package org.apache.nifi.apicurio.schemaregistry.client;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.serialization.record.RecordSchema;

import java.time.Duration;

public class CachingSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryClient client;
    private final LoadingCache<Pair<String, Integer>, RecordSchema> schemaCache;

    public CachingSchemaRegistryClient(final SchemaRegistryClient toWrap, final int cacheSize, final long expirationNanos) {
        this.client = toWrap;

        schemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(Duration.ofNanos(expirationNanos))
                .build(key -> {
                    if (key.getRight() == -1) {
                        // If the version in the key is -1, fetch the schema by name only.
                        return client.getSchema(key.getLeft());
                    } else {
                        // If a specific version is provided in the key, fetch the schema with that version.
                        return client.getSchema(key.getLeft(), key.getRight());
                    }
                });
    }

    @Override
    public RecordSchema getSchema(final String schemaName) {
        return schemaCache.get(Pair.of(schemaName, -1));
    }

    @Override
    public RecordSchema getSchema(final String schemaName, final int version) {
        return schemaCache.get(Pair.of(schemaName, version));
    }
}
