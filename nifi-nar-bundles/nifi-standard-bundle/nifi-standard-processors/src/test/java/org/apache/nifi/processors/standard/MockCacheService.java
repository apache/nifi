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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class MockCacheService extends AbstractControllerService implements DistributedMapCacheClient {
    private final Map<Object, Object> storage;

    public MockCacheService() {
        storage = new HashMap<>();
    }

    /**
     * Puts the value to the cache with the specified key, if it doesn't already exist
     * @param key the key for into the map
     * @param value the value to add to the map if and only if the key is absent
     * @param keySerializer key serializer
     * @param valueSerializer value serializer
     * @return true if the value was added to the cache, false if it already exists
     */
    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return storage.putIfAbsent(key, value) == null;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        if (storage.containsKey(key)) {
            return (V) storage.get(key);
        } else {
            storage.put(key, value);
            return null;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return storage.containsKey(key);
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        storage.put(key, value);
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        return (V) storage.get(key);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
        if (storage.containsKey(key)) {
            storage.remove(key);
            return true;
        } else {
            return false;
        }
    }
}
