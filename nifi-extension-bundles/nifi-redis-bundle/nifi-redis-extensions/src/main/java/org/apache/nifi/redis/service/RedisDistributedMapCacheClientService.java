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
package org.apache.nifi.redis.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.nifi.redis.util.RedisUtils.REDIS_CONNECTION_POOL;

@Tags({ "redis", "distributed", "cache", "map" })
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Redis as the backing cache. This service relies on " +
        "the WATCH, MULTI, and EXEC commands in Redis, which are not fully supported when Redis is clustered. As a result, this service " +
        "can only be used with a Redis Connection Pool that is configured for standalone or sentinel mode. Sentinel mode can be used to " +
        "provide high-availability configurations.")
public class RedisDistributedMapCacheClientService extends SimpleRedisDistributedMapCacheClientService implements AtomicDistributedMapCacheClient<byte[]> {

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final RedisConnectionPool redisConnectionPool = validationContext.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
        if (redisConnectionPool != null) {
            final RedisType redisType = redisConnectionPool.getRedisType();
            if (redisType != null && redisType == RedisType.CLUSTER) {
                results.add(new ValidationResult.Builder()
                        .subject(REDIS_CONNECTION_POOL.getDisplayName())
                        .valid(false)
                        .explanation(REDIS_CONNECTION_POOL.getDisplayName()
                                + " is configured in clustered mode, and this service requires a non-clustered Redis")
                        .build());
            }
        }

        return results;
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, byte[]> fetch(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);

            final byte[] v = redisConnection.stringCommands().get(k);
            if (v == null) {
                return null;
            }

            // for Redis we are going to use the raw bytes of the value as the revision
            return new AtomicCacheEntry<>(key, valueDeserializer.deserialize(v), v);
        });
    }

    @Override
    public <K, V> boolean replace(final AtomicCacheEntry<K, V, byte[]> entry, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return withConnection(redisConnection -> {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();

            keySerializer.serialize(entry.getKey(), out);
            final byte[] k = out.toByteArray();
            out.reset();

            valueSerializer.serialize(entry.getValue(), out);
            final byte[] newVal = out.toByteArray();

            // the revision of the cache entry holds the value of the key from a previous fetch
            final byte[] prevVal = entry.getRevision().orElse(null);

            boolean replaced = false;

            // start a watch on the key and retrieve the current value
            redisConnection.watch(k);
            final byte[] currValue = redisConnection.stringCommands().get(k);

            // start a transaction
            redisConnection.multi();

            // compare-and-set
            if (Arrays.equals(prevVal, currValue)) {
                // if we use set(k, newVal) then the results list will always have size == 0 b/c when convertPipelineAndTxResults is set to true,
                // status responses like "OK" are skipped over, so by using getSet we can rely on the results list to know if the transaction succeeded
                redisConnection.stringCommands().getSet(k, newVal);

                // set the TTL if specified
                final long ttl = getTtl();
                if (ttl != -1L) {
                    redisConnection.keyCommands().expire(k, ttl);
                }
            }

            // execute the transaction
            final List<Object> results = redisConnection.exec();

            // if we have a result then the replace succeeded
            if (results != null && results.size() > 0) {
                replaced = true;
            }

            return replaced;
        });
    }

}
