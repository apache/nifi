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
package org.apache.nifi.processors.asana.mocks;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

public class MockDistributedMapCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
    private final Map<Object, Object> stored = new HashMap<>();

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        throw new NotImplementedException();
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        throw new NotImplementedException();
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) {
        return stored.containsKey(key);
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        put(key, value);
    }

    public <K, V> void put(K key, V value) {
        stored.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) {
        return (V) stored.get(key);
    }

    @SuppressWarnings("unchecked")
    public <K, V> V get(K key) {
        return (V) stored.get(key);
    }

    @Override
    public void close() {
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) {
        throw new NotImplementedException();
    }
}
