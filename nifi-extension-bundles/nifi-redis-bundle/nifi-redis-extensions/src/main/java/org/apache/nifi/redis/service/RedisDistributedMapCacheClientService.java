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
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.util.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SetCondition;
import org.springframework.data.redis.core.types.Expiration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Tags({ "redis", "distributed", "cache", "map" })
@CapabilityDescription("An implementation of AtomicDistributedMapCacheClient that uses Redis as the backing cache, including " +
        "Redis Enterprise Software and Redis Cloud. Atomic operations use single-key Lua scripts and require permission to execute EVAL. " +
        "Use standalone mode for a Redis Enterprise or Redis Cloud database endpoint, or cluster mode for an endpoint with the Redis Cluster API enabled. " +
        "Atomicity applies to the Redis endpoint and does not provide coordination across Active-Active regions.")
public class RedisDistributedMapCacheClientService extends SimpleRedisDistributedMapCacheClientService implements AtomicDistributedMapCacheClient<byte[]> {

    private static final byte[] GET_AND_PUT_IF_ABSENT_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if current then
                return current
            end
            if tonumber(ARGV[2]) > 0 then
                redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
            else
                redis.call('SET', KEYS[1], ARGV[1])
            end
            return false
            """.getBytes(StandardCharsets.UTF_8);

    private static final byte[] REPLACE_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            if ARGV[1] == '0' then
                if current then
                    return 0
                end
            elseif current ~= ARGV[2] then
                return 0
            end
            if tonumber(ARGV[4]) > 0 then
                redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[4])
            else
                redis.call('SET', KEYS[1], ARGV[3])
            end
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            return Boolean.TRUE.equals(redisConnection.stringCommands().set(kv.getKey(), kv.getValue(),
                    SetCondition.ifAbsent(), Expiration.seconds(getTtl())));
        });
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer,
                                     final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            final byte[] previous = redisConnection.scriptingCommands().eval(GET_AND_PUT_IF_ABSENT_SCRIPT, ReturnType.VALUE, 1,
                    kv.getKey(), kv.getValue(), getTtl().toString().getBytes(StandardCharsets.UTF_8));
            return previous == null ? null : valueDeserializer.deserialize(previous);
        });
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
            final Tuple<byte[], byte[]> kv = serialize(entry.getKey(), entry.getValue(), keySerializer, valueSerializer);
            // Keep the existing raw-value revision format, including the distinction between an absent key and an empty value.
            final byte[] previous = entry.getRevision().orElse(null);
            final Long result = redisConnection.scriptingCommands().eval(REPLACE_SCRIPT, ReturnType.INTEGER, 1,
                    kv.getKey(), (previous == null ? "0" : "1").getBytes(StandardCharsets.UTF_8),
                    previous == null ? new byte[0] : previous, kv.getValue(), getTtl().toString().getBytes(StandardCharsets.UTF_8));
            return Long.valueOf(1).equals(result);
        });
    }
}
