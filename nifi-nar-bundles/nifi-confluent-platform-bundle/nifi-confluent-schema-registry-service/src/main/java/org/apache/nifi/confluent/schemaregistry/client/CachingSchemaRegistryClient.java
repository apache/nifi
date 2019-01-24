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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.time.Duration;


public class CachingSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryClient client;

    private final LoadingCache<String, RecordSchema> nameCache;
    private final LoadingCache<Integer, RecordSchema> idCache;


    public CachingSchemaRegistryClient(final SchemaRegistryClient toWrap, final int cacheSize, final long expirationNanos) {
        this.client = toWrap;

        nameCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(Duration.ofNanos(expirationNanos))
                .build(client::getSchema);
        idCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(Duration.ofNanos(expirationNanos))
                .build(client::getSchema);
    }

    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        return nameCache.get(schemaName);
    }

    @Override
    public RecordSchema getSchema(final int schemaId) throws IOException, SchemaNotFoundException {
        return idCache.get(schemaId);
    }

}
