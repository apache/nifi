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

package org.apache.nifi.processor.util.list;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class DistributedCacheFake extends AbstractControllerService implements DistributedMapCacheClient {
    final Map<String, String> stored = new HashMap<>();
    int fetchCount = 0;

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return stored.putIfAbsent(serialize(key, keySerializer), serialize(value, valueSerializer)) == null;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        String serializedKey = serialize(key, keySerializer);

        fetchCount += 1;
        if (stored.containsKey(serializedKey)) {
            return deserialize(stored.get(serializedKey), valueDeserializer);
        } else {
            stored.put(serializedKey, serialize(value, valueSerializer));
            return null;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return stored.containsKey(serialize(key, keySerializer));
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        stored.put(serialize(key, keySerializer), serialize(value, valueSerializer));
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        String serializedKey = serialize(key, keySerializer);
        fetchCount += 1;

        if (stored.containsKey(serializedKey)) {
            return deserialize(stored.get(serializedKey), valueDeserializer);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
        final Object serializedKey = serialize(key, serializer);

        if (stored.containsKey(serializedKey)) {
            stored.remove(serializedKey);
            return true;
        } else {
            return false;
        }
    }

    private <T> String serialize(T rawValue, Serializer<T> serializer) {
        try(final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            serializer.serialize(rawValue, outputStream);
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T deserialize(String serializedValue, Deserializer<T> deserializer) {
        try {
            return deserializer.deserialize(serializedValue.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
