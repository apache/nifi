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

package org.apache.nifi.processors.standard

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient
import org.apache.nifi.distributed.cache.client.Serializer

class MockCacheService extends AbstractControllerService implements DistributedMapCacheClient {
    def map = [:]

    @Override
    def <K, V> boolean putIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
        def retVal = map.containsKey(k)
        if (retVal) {
            false
        } else {
            map[k] = v
            true
        }
    }

    @Override
    def <K, V> V getAndPutIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1, Deserializer<V> deserializer) throws IOException {
        return null
    }

    @Override
    def <K> boolean containsKey(K k, Serializer<K> serializer) throws IOException {
        return map.containsKey(k)
    }

    @Override
    def <K, V> void put(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {

    }

    @Override
    def <K, V> V get(K k, Serializer<K> serializer, Deserializer<V> deserializer) throws IOException {
        return null
    }

    @Override
    void close() throws IOException {

    }

    @Override
    def <K> boolean remove(K k, Serializer<K> serializer) throws IOException {
        return false
    }

    @Override
    long removeByPattern(String s) throws IOException {
        return 0
    }

    void assertContains(String key, String value) {
        assert map.containsKey(key) && map[key] == value
    }
}