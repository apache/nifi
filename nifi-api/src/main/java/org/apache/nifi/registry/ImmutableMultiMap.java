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
package org.apache.nifi.registry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImmutableMultiMap<V> implements Map<String,V> {

    private final List<Map<String,V>> maps;

    ImmutableMultiMap() {
        this.maps = new ArrayList<>();
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        for (final Map<String,V> map : maps) {
            if (!map.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(final Object key) {
        if (key == null) {
            return false;
        }

        for (final Map<String,V> map : maps) {
            if (map.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        for (final Map<String,V> map : maps) {
            if (map.containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public V get(final Object key) {
        if (key == null) {
            throw new IllegalArgumentException("Null Keys are not allowed");
        }

        for (final Map<String,V> map : maps) {
            final V val = map.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }

    @Override
    public V put(String key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Set<String> keySet() {
        final Set<String> keySet = new HashSet<>();
        for (final Map map : maps) {
            keySet.addAll(map.keySet());
        }
        return keySet;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Collection<V> values() {
        final Set<V> values = new HashSet<>();
        for (final Map map : maps) {
            values.addAll(map.values());
        }
        return values;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Set<java.util.Map.Entry<String, V>> entrySet() {
        final Set<java.util.Map.Entry<String, V>> entrySet = new HashSet<>();
        for (final Map map : maps) {
            entrySet.addAll(map.entrySet());
        }
        return entrySet;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void addMap(Map<String,V> map){
        this.maps.add(map);
    }


}
