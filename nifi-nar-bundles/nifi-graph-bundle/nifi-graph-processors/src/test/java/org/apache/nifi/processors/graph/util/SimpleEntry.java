package org.apache.nifi.processors.graph.util;

import java.util.Map;

final class SimpleEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    public SimpleEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }

    @Override
    public String toString() {
        return String.format("%s:%s", this.key, this.value);
    }
}

