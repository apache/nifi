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
package org.apache.nifi.pulsar.cache;

import java.util.LinkedHashMap;

import org.apache.nifi.pulsar.pool.PoolableResource;
import org.apache.nifi.pulsar.pool.ResourcePool;

public class LRUCache<K, V extends PoolableResource> {

    private LinkedHashMap<K, V> lruCacheMap;
    private final int capacity;
    private final boolean SORT_BY_ACCESS = true;
    private final float LOAD_FACTOR = 0.75F;
    private final ResourcePool<V> resourcePool;

    public LRUCache(int capacity, ResourcePool<V> resourcePool){
        this.capacity = capacity;
        this.lruCacheMap = new LinkedHashMap<>(capacity, LOAD_FACTOR, SORT_BY_ACCESS);
        this.resourcePool = resourcePool;
    }

    public V get(K k){
        return lruCacheMap.get(k);
    }

    public void put(K k, V v){
        if(lruCacheMap.containsKey(k)){
            lruCacheMap.remove(k);
        } else if(lruCacheMap.size() >= capacity){
            K victimKey = lruCacheMap.keySet().iterator().next();
            V victim = lruCacheMap.get(victimKey);
            lruCacheMap.remove(victimKey);
            resourcePool.evict(victim);
        }
        lruCacheMap.put(k, v);
    }

    public int getSize() {
       return lruCacheMap.size();
    }

    public void clear() {
        for (V victim :lruCacheMap.values()) {
           resourcePool.evict(victim);
        }
        lruCacheMap.clear();
    }

    public void printSequence() {
       System.out.println(lruCacheMap.keySet());
    }
}
