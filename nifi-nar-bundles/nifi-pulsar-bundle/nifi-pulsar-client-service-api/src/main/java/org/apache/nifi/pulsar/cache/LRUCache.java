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

import java.io.Closeable;
import java.util.Collection;
import java.util.LinkedHashMap;

public class LRUCache<K, V extends Closeable> {

    private LinkedHashMap<K, V> lruCacheMap;
    private final int capacity;
    private final boolean SORT_BY_ACCESS = true;
    private final float LOAD_FACTOR = 0.75F;

    public LRUCache(int capacity){
        this.capacity = capacity;
        this.lruCacheMap = new LinkedHashMap<>(capacity, LOAD_FACTOR, SORT_BY_ACCESS);
    }

    public V get(K k){
        return lruCacheMap.get(k);
    }

    public void put(K k, V v){

        if (k == null || v == null) {
          return;
        }

        if (lruCacheMap.containsKey(k)) {
           evict(k);
        } else if(lruCacheMap.size() >= capacity){
           evict(lruCacheMap.keySet().iterator().next());
        }
        lruCacheMap.put(k, v);
    }

    public int getSize() {
       return lruCacheMap.size();
    }

    public void clear() {
        lruCacheMap.values().stream().forEach(c -> {
            try {
               c.close();
            } catch (Exception e) {
               // Ignore these
            }
        });
        lruCacheMap.clear();
    }

    public String getSequence() {
       return lruCacheMap.keySet().toString();
    }

    public Collection<V> getValues() {
       return lruCacheMap.values();
    }

    private void evict(K victimKey) {
        try {
          lruCacheMap.get(victimKey).close();
        } catch (Exception e) {
          // Ignore these
        }
        lruCacheMap.remove(victimKey);
    }
}
