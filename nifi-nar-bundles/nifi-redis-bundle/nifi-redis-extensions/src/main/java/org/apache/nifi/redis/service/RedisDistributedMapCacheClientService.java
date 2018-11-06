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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.redis.util.RedisAction;
import org.apache.nifi.util.Tuple;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({ "redis", "distributed", "cache", "map" })
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Redis as the backing cache. This service relies on " +
        "the WATCH, MULTI, and EXEC commands in Redis, which are not fully supported when Redis is clustered. As a result, this service " +
        "can only be used with a Redis Connection Pool that is configured for standalone or sentinel mode. Sentinel mode can be used to " +
        "provide high-availability configurations.")
public class RedisDistributedMapCacheClientService extends AbstractControllerService implements AtomicDistributedMapCacheClient<byte[]> {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("redis-cache-ttl")
            .displayName("TTL")
            .description("Indicates how long the data should exist in Redis. Setting '0 secs' would mean the data would exist forever")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("0 secs")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_CONNECTION_POOL);
        props.add(TTL);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(props);
    }

    private volatile RedisConnectionPool redisConnectionPool;
    private Long ttl;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

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

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
        this.ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS);

        if (ttl == 0) {
            this.ttl = -1L;
        }
    }

    @OnDisabled
    public void onDisabled() {
        this.redisConnectionPool = null;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            boolean set = redisConnection.setNX(kv.getKey(), kv.getValue());

            if (ttl != -1L && set) {
                redisConnection.expire(kv.getKey(), ttl);
            }

            return set;
        });
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            do {
                // start a watch on the key and retrieve the current value
                redisConnection.watch(kv.getKey());
                final byte[] existingValue = redisConnection.get(kv.getKey());

                // start a transaction and perform the put-if-absent
                redisConnection.multi();
                redisConnection.setNX(kv.getKey(), kv.getValue());

                // Set the TTL only if the key doesn't exist already
                if (ttl != -1L && existingValue == null) {
                    redisConnection.expire(kv.getKey(), ttl);
                }

                // execute the transaction
                final List<Object> results = redisConnection.exec();

                // if the results list was empty, then the transaction failed (i.e. key was modified after we started watching), so keep looping to retry
                // if the results list has results, then the transaction succeeded and it should have the result of the setNX operation
                if (results.size() > 0) {
                    final Object firstResult = results.get(0);
                    if (firstResult instanceof Boolean) {
                        final Boolean absent = (Boolean) firstResult;
                        return absent ? null : valueDeserializer.deserialize(existingValue);
                    } else {
                        // this shouldn't really happen, but just in case there is a non-boolean result then bounce out of the loop
                        throw new IOException("Unexpected result from Redis transaction: Expected Boolean result, but got "
                                + firstResult.getClass().getName() + " with value " + firstResult.toString());
                    }
                }
            } while (isEnabled());

            return null;
        });
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            return redisConnection.exists(k);
        });
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.set(kv.getKey(), kv.getValue(), Expiration.seconds(ttl), SetOption.upsert());
            return null;
        });
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final byte[] v = redisConnection.get(k);
            return valueDeserializer.deserialize(v);
        });
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final long numRemoved = redisConnection.del(k);
            return numRemoved > 0;
        });
    }

    @Override
    public long removeByPattern(final String regex) throws IOException {
        return withConnection(redisConnection -> {
            long deletedCount = 0;
            final List<byte[]> batchKeys = new ArrayList<>();

            // delete keys in batches of 1000 using the cursor
            final Cursor<byte[]> cursor = redisConnection.scan(ScanOptions.scanOptions().count(100).match(regex).build());
            while (cursor.hasNext()) {
                batchKeys.add(cursor.next());

                if (batchKeys.size() == 1000) {
                    deletedCount += redisConnection.del(getKeys(batchKeys));
                    batchKeys.clear();
                }
            }

            // delete any left-over keys if some were added to the batch but never reached 1000
            if (batchKeys.size() > 0) {
                deletedCount += redisConnection.del(getKeys(batchKeys));
                batchKeys.clear();
            }

            return deletedCount;
        });
    }

    /**
     *  Convert the list of all keys to an array.
     */
    private byte[][] getKeys(final List<byte[]> keys) {
        final byte[][] allKeysArray = new byte[keys.size()][];
        for (int i=0; i < keys.size(); i++) {
            allKeysArray[i] = keys.get(i);
        }
        return  allKeysArray;
    }

    // ----------------- Methods from AtomicDistributedMapCacheClient ------------------------

    @Override
    public <K, V> AtomicCacheEntry<K, V, byte[]> fetch(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);

            final byte[] v = redisConnection.get(k);
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
            final byte[] currValue = redisConnection.get(k);

            // start a transaction
            redisConnection.multi();

            // compare-and-set
            if (Arrays.equals(prevVal, currValue)) {
                // if we use set(k, newVal) then the results list will always have size == 0 b/c when convertPipelineAndTxResults is set to true,
                // status responses like "OK" are skipped over, so by using getSet we can rely on the results list to know if the transaction succeeded
                redisConnection.getSet(k, newVal);
            }

            // execute the transaction
            final List<Object> results = redisConnection.exec();

            // if we have a result then the replace succeeded
            if (results.size() > 0) {
                replaced = true;
            }

            return replaced;
        });
    }

    // ----------------- END Methods from AtomicDistributedMapCacheClient ------------------------

    private <K, V> Tuple<byte[],byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();

        out.reset();

        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();

        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        return out.toByteArray();
    }

    private <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
           if (redisConnection != null) {
               try {
                   redisConnection.close();
               } catch (Exception e) {
                   getLogger().warn("Error closing connection: " + e.getMessage(), e);
               }
           }
        }
    }

}
