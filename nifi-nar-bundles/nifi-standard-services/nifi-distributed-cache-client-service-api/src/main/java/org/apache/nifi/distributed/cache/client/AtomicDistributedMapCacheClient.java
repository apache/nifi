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
package org.apache.nifi.distributed.cache.client;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import java.io.IOException;

/**
 * <p>This interface defines an API that can be used for interacting with a
 * Distributed Cache that functions similarly to a {@link java.util.Map Map}.
 *
 * <p>In addition to the API defined in {@link DistributedMapCacheClient} super class,
 * this class provides methods for concurrent atomic updates those are added since Map Cache protocol version 2.
 *
 * <p>If a remote cache server doesn't support Map Cache protocol version 2, these methods throw UnsupportedOperationException.
 */
@Tags({"distributed", "client", "cluster", "map", "cache"})
@CapabilityDescription("Provides the ability to communicate with a DistributedMapCacheServer. This allows "
        + "multiple nodes to coordinate state with a single remote entity.")
public interface AtomicDistributedMapCacheClient extends DistributedMapCacheClient {

    interface CacheEntry<K, V> {

        long getRevision();

        K getKey();

        V getValue();

    }

    /**
     * Fetch a CacheEntry with a key.
     * @param <K> the key type
     * @param <V> the value type
     * @param key the key to lookup in the map
     * @param keySerializer key serializer
     * @param valueDeserializer value deserializer
     * @return A CacheEntry instance if one exists, otherwise <cod>null</cod>.
     * @throws IOException if unable to communicate with the remote instance
     */
    <K, V> CacheEntry<K, V> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException;

    /**
     * Replace an existing key with new value.
     * @param <K> the key type
     * @param <V> the value type
     * @param key the key to replace
     * @param value the new value for the key
     * @param keySerializer key serializer
     * @param valueSerializer value serializer
     * @param revision a revision that was retrieved by a preceding fetch operation, if the key is already updated by other client,
     *                 this doesn't match with the one on server, therefore the replace operation will not be performed.
     *                 If there's no existing entry for the key, any revision can replace the key.
     * @return true only if the key is replaced.
     * @throws IOException if unable to communicate with the remote instance
     */
    <K, V> boolean replace(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, long revision) throws IOException;

}