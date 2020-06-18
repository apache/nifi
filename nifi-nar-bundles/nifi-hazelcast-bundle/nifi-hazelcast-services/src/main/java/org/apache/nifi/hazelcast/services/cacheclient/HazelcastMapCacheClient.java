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
package org.apache.nifi.hazelcast.services.cacheclient;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hazelcast.services.cache.HazelcastCache;
import org.apache.nifi.hazelcast.services.cachemanager.HazelcastCacheManager;
import org.apache.nifi.hazelcast.services.util.LongUtil;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * An implementation of DistributedMapCacheClient that uses Hazelcast as the backing cache.
 *
 * Note: By design, the client should not directly depend on Hazelcast specific classes due to ease version and implementation changes.
 */
@Tags({ "hazelcast", "cache", "map"})
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Hazelcast as the backing cache. This service relies on " +
        "an abstracted repository manages the actual Hazelcast calls, provided by HazelcastConnectionService.")
public class HazelcastMapCacheClient extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    public static final PropertyDescriptor HAZELCAST_CACHE_MANAGER = new PropertyDescriptor.Builder()
            .name("hazelcast-cache-manager")
            .displayName("Hazelcast Cache Manager")
            .description("A Hazelcast Cache Manager which manages connections to Hazelcast and providing cache instances")
            .identifiesControllerService(HazelcastCacheManager.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HAZELCAST_CACHE_NAME = new PropertyDescriptor.Builder()
            .name("hazelcast-cache-name")
            .displayName("Hazelcast Repository Name")
            .description("The name of a given repository. Within a Hazelcast cluster, multiple unrelated repositories might be used")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor HAZELCAST_ENTRY_TTL = new PropertyDescriptor.Builder()
            .name("hazelcast-entry-ttl")
            .displayName("Hazelcast entry TTL")
            .description("Indicates how long the data should exist in Hazelcast. Setting '0 secs' would mean the data would exist forever")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 secs") // Note: in case of Hazelcast IMap, negative value would mean "map default" which might be overridden from a different client.
            .build();

    private static final long STARTING_VERSION = 1;
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HAZELCAST_CACHE_MANAGER);
        properties.add(HAZELCAST_CACHE_NAME);
        properties.add(HAZELCAST_ENTRY_TTL);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(properties);
    }

    private HazelcastCache cache = null;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final HazelcastCacheManager hazelcastCacheManager = context.getProperty(HAZELCAST_CACHE_MANAGER).asControllerService(HazelcastCacheManager.class);
        cache = hazelcastCacheManager.getCache(
                context.getProperty(HAZELCAST_CACHE_NAME).getValue(),
                context.getProperty(HAZELCAST_ENTRY_TTL).asTimePeriod(TimeUnit.MILLISECONDS));
        getLogger().debug("Enable Hazelcast repository " + cache.name());
    }

    @OnDisabled
    public void onDisabled() {
        // The cache state will be preserved until the Service is not stopped!
        getLogger().debug("Disable Hazelcast repository " + cache.name());
        cache = null;
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] result = cache.get(getCacheEntryKey(key, keySerializer));
        return (result == null) ? null : new AtomicCacheEntry<>(key, parsePayload(valueDeserializer, result), parseVersion(result));
    }

    @Override
    public <K, V> boolean replace(final AtomicCacheEntry<K, V, Long> entry, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        if (entry.getKey() == null) {
            return false;
        }

        final String key = getCacheEntryKey(entry.getKey(), keySerializer);

        try(final HazelcastCache.HazelcastCacheEntryLock lock = cache.acquireLock(key)) {
            final byte[] oldValue = cache.get(key);

            if (oldValue == null && (!entry.getRevision().isPresent() || entry.getRevision().get() < STARTING_VERSION)) {
                cache.put(key, serialize(entry.getValue(), valueSerializer, STARTING_VERSION));
                getLogger().debug("Entry with key " + key + " was added during replace");
                return true;
            } else if (oldValue != null && entry.getRevision().get() == parseVersion(oldValue)) {
                cache.put(key, serialize(entry.getValue(), valueSerializer, entry.getRevision().get() + 1));
                getLogger().debug("Entry with key " + key + " was updated during replace, with revision " + entry.getRevision().get() + 1);
                return true;
            }
        }

        return false;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return cache.putIfAbsent(getCacheEntryKey(key, keySerializer), serialize(value, valueSerializer, STARTING_VERSION)) == null;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(
            final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer
    ) throws IOException {
        final byte[] result = cache.putIfAbsent(getCacheEntryKey(key, keySerializer), serialize(value, valueSerializer, STARTING_VERSION));
        return (result == null) ? null : parsePayload(valueDeserializer, result);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return cache.contains(getCacheEntryKey(key, keySerializer));
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        cache.put(getCacheEntryKey(key, keySerializer), serialize(value, valueSerializer, STARTING_VERSION));
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] result = cache.get(getCacheEntryKey(key, keySerializer));
        return result == null ? null : parsePayload(valueDeserializer, result);
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        return cache.remove(getCacheEntryKey(key, keySerializer));
    }

    @Override
    public long removeByPattern(final String regex) throws IOException {
        return cache.removeAll(new RegexPredicate(regex));
    }

    private static class RegexPredicate implements Predicate<String>, Serializable {
        private final Pattern pattern;

        private RegexPredicate(final String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean test(final String string) {
            return pattern.matcher(string).matches();
        }
    }

    @Override
    public void close() throws IOException {
        getLogger().debug("Closing HazelcastMapCacheClient");
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private static long parseVersion(final byte[] value) {
        return LongUtil.fromBytes(Arrays.copyOfRange(value, 0, Long.BYTES));
    }

    private static <V> V parsePayload(final Deserializer<V> deserializer, final byte[] value) throws IOException {
        return deserializer.deserialize(Arrays.copyOfRange(value, Long.BYTES, value.length));
    }

    private <S> String getCacheEntryKey(final S value, final Serializer<S> serializer) throws IOException {
        final String result;

        if (value instanceof String) {
            result = (String) value;
        } else {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            serializer.serialize(value, stream);
            result = stream.toString("UTF-8");
        }

        if (result.isEmpty()) {
            throw new IOException("Cache record key cannot be empty!");
        }

        return result;
    }

    /**
     * Serializes incoming value using the given serializer. The end result is a byte array might be parsed by this
     * implementation. As a convention, the first eight bytes of the array contains a long value, serves as version
     * identifier. From the ninth byte, the actual value starts. During parsing this, the version must be read every
     * time as convention.
     *
     * @param value The value to serialize.
     * @param serializer The serializer to use in order to serialize the incoming value.
     * @param version The version of the entry
     * @param <S> The type of the value to be serialized
     *
     * @return Byte array containing both version and value of the cache entry.
     *
     * @throws IOException In case of any issue during working with intermediate byte stream.
     */
    private <S> byte[] serialize(final S value, final Serializer<S> serializer, final long version) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(LongUtil.toBytes(version));
        serializer.serialize(value, stream);
        return stream.toByteArray();
    }
}
