/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.pulsar.cache;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.collections4.map.LRUMap;

public class PulsarClientLRUCache<K, V extends Closeable> extends LRUMap<K, V> {

    private static final long serialVersionUID = 730163138087670453L;
    private final static float LOAD_FACTOR = 0.75F;
    private final static boolean SCAN_UNTIL_REMOVABLE = false;

    public PulsarClientLRUCache(int maxSize) {
       this(maxSize, LOAD_FACTOR, SCAN_UNTIL_REMOVABLE);
    }

    public PulsarClientLRUCache(int maxSize, float loadFactor, boolean scanUntilRemovable) {
       super(maxSize, loadFactor, scanUntilRemovable);
    }

    @Override
    public void clear() {
        this.values().parallelStream().forEach(closable -> {
           releaseResources(closable);
        });
        super.clear();
    }

    @Override
    protected boolean removeLRU(LinkEntry<K, V> entry) {
       releaseResources(entry.getValue());  // release resources held by entry
       return true;  // actually delete entry
    }

    private void releaseResources(V value) {
       try {
          value.close();
       } catch (IOException e) {
         e.printStackTrace();
       }
    }
}