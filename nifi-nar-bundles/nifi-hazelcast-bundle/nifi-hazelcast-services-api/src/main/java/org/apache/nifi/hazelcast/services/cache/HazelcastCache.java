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
package org.apache.nifi.hazelcast.services.cache;

import java.util.function.Predicate;

/**
 * Represents a cache storage. The API gives no restriction for the data structure used or to the Hazelcast setup. There
 * can be multiple separate cache instances, sharing the same underlying Hazelcast. The cache instances with the same name
 * are pointing to the same storage. It is recommended to use unique names for the different purposes.
 */
public interface HazelcastCache {
    /**
     * Serves as identifier for the cache. Defines the underlying storage.
     *
     * @return The name of the cache.
     */
    String name();

    /**
     * Returns the value of the cache entry defined by the the key.
     *
     * @param key Key of the entry, must not be null.
     *
     * @return The serialized value of the cache entry if it exits. The serialization and deserialization is handled by the client. In case the entry does not exist, the result is null.
     */
    byte[] get(String key);

    /**
     * Adds a new entry to the cache under the given key. If the entry already exists, it will be overwritten. In case the entry is locked by an other client, the method will wait or return
     * with false depending on the implementation.
     *
     * @param key Key of the entry, must not be null.
     * @param value The serialized value of the entry. In case the entry already exists, the new value. The value must not be null. The serialization and deserialization is handled by the client.
     *
     * @return True if the writing was successful.
     */
    boolean put(String key, byte[] value);

    /**
     * Adds a new entry to the cache under the given key. If the entry already exists, no changes will be applied. In case the entry is locked by an other client, the method will wait or return
     * with false depending on the implementation.
     *
     * @param key Key of the entry, must not be null.
     * @param value The serialized value of the entry. The value must not be null. The serialization and deserialization is handled by the client.
     *
     * @return The serialized value of the cache entry if exists already. Null otherwise. The serialization and deserialization is handled by the client.
     */
    byte[] putIfAbsent(String key, byte[] value);

    /**
     * Returns true if an entry with the given key exists in the cache. Returns false otherwise.
     *
     * @param key Key of the entry, must not be null.
     *
     * @return True if an entry with the given key exists.
     */
    boolean contains(String key);

    /**
     * Removes the entry from the cache with the given key.
     *
     * @param key Key of the entry, must not be null.
     *
     * @return True if the entry existed in the cache before the removal, false otherwise.
     */
    boolean remove(String key);

    /**
     * Removes all matching entries from the cache. An entry is considered matching if its key matches the provided predicate.
     *
     * @param keyMatcher The predicate determines if an entry is matching.
     *
     * @return The number of deleted entries.
     *
     * Note: the implementation of this method is not necessarily atomic. Because of this, in some cases the number of deleted entries might
     * not be equal to the number of matching entries at the moment of calling. There is no guarantee for that, during the execution of the method a new matching entry is not added
     * or an already existing is being not deleted.
     */
    int removeAll(Predicate<String> keyMatcher);

    /**
     * Locks an entry with the given key to prevent its modification by other clients. Closing the connection automatically releases the lock.
     * Non-existing keys might be locked in this way as well. This operation is not transactional and other clients might read the value while the entry is locked. For further
     * information please check Hazelcast documentation.
     *
     * Note: the current implementation of Hazelcast (4.X) also prevents modification by the same client on a different thread.
     *
     * @param key Key of the entry, must not be null.
     *
     * @return The entry lock instance.
     */
    HazelcastCacheEntryLock acquireLock(String key);

    /**
     * Represents a lock on a given entry based on key. The lock is bound to a cache and does not allow other caches to modify the entry in any manner until it is released. Calling close
     * on the HazelcastCacheEntryLock instance will release the lock if not already released.
     */
    interface HazelcastCacheEntryLock extends AutoCloseable {
        /**
         * Note: Entry lock will not throw generic exception.
         */
        @Override
        void close();
    }
}
