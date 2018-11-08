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
package org.apache.nifi.attribute.expression.language;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SingleValueLookup implements ValueLookup {
    private final VariableRegistry registry;
    private final Map<String, String> flowFileValues;

    SingleValueLookup(final VariableRegistry registry, final FlowFile flowFile) {
        this.registry = registry;
        if (flowFile == null) {
            flowFileValues = Collections.emptyMap();
        } else {
            flowFileValues = new HashMap<>(flowFile.getAttributes());

            flowFileValues.put("flowFileId", String.valueOf(flowFile.getId()));
            flowFileValues.put("fileSize", String.valueOf(flowFile.getSize()));
            flowFileValues.put("entryDate", String.valueOf(flowFile.getEntryDate()));
            flowFileValues.put("lineageStartDate", String.valueOf(flowFile.getLineageStartDate()));
            flowFileValues.put("lastQueueDate", String.valueOf(flowFile.getLastQueueDate()));
            flowFileValues.put("queueDateIndex", String.valueOf(flowFile.getQueueDateIndex()));
        }
    }

    SingleValueLookup(final VariableRegistry registry, final Map<String, String> values) {
        this.registry = registry;
        if (values == null) {
            flowFileValues = Collections.emptyMap();
        } else {
            flowFileValues = values;
        }
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        if (!flowFileValues.isEmpty()) {
            return false;
        }
        return registry.getVariableMap().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            return false;
        }

        return flowFileValues.containsKey(key) || registry.getVariableKey(key.toString()) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            return false;
        }

        //check entrySet then iterate through values (otherwise might find a value that was hidden/overridden
        final Collection<String> values = values();
        return values.contains(value.toString());
    }

    @Override
    public String get(Object key) {
        if (key == null) {
            return null;
        }

        final String val = flowFileValues.get(key);
        if (val != null) {
            return val;
        }

        return registry.getVariableValue(key.toString());
    }

    @Override
    public String put(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        final Set<String> keySet = new HashSet<>();
        entrySet().forEach((entry) -> {
            keySet.add(entry.getKey());
        });
        return keySet;
    }

    @Override
    public Collection<String> values() {
        final Set<String> values = new HashSet<>();
        entrySet().forEach((entry) -> {
            values.add(entry.getValue());
        });
        return values;
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        final Map<String, String> newMap = new HashMap<>();

        //put variable registry entries first
        for (final Map.Entry<VariableDescriptor, String> entry : registry.getVariableMap().entrySet()) {
            newMap.put(entry.getKey().getName(), entry.getValue());
        }

        for (final Map.Entry<String, String> entry : flowFileValues.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }

        return newMap.entrySet();
    }
}
