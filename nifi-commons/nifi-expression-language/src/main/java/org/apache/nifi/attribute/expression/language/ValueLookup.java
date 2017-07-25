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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;

/**
 * A convenience class to encapsulate the logic of variable substitution
 * based first on any additional variable maps, then flow file properties,
 * then flow file attributes, and finally the provided variable registry.
 */
final class ValueLookup implements Map<String, String> {

    final List<Map<String, String>> maps = new ArrayList<>();
    final VariableRegistry registry;

    /**
     * Constructs a ValueLookup where values are looked up first based any
     * provided additional maps, then flowfile properties, then flowfile
     * attributes, then based on the provided variable registry. The lookup is
     * immutable and operations which attempt to alter state will throw
     * UnsupportedOperationException
     *
     * @param registry the variable registry to lookup from; may be null
     * @param flowFile the flowFile to pull attributes from; may be null
     * @param additionalMaps the maps to pull values from; may be null or empty
     */
    @SuppressWarnings("unchecked")
    ValueLookup(final VariableRegistry registry, final FlowFile flowFile, final Map<String, String>... additionalMaps) {
        for (final Map<String, String> map : additionalMaps) {
            if (map != null && !map.isEmpty()) {
                maps.add(map);
            }
        }
        if (flowFile != null) {
            maps.add(ValueLookup.extractFlowFileProperties(flowFile));
            maps.add(flowFile.getAttributes());
        }

        this.registry = registry == null ? VariableRegistry.EMPTY_REGISTRY : registry;
    }

    static final Map<String, String> extractFlowFileProperties(final FlowFile flowFile) {
        final Map<String, String> flowFileProps = new HashMap<>();
        flowFileProps.put("flowFileId", String.valueOf(flowFile.getId()));
        flowFileProps.put("fileSize", String.valueOf(flowFile.getSize()));
        flowFileProps.put("entryDate", String.valueOf(flowFile.getEntryDate()));
        flowFileProps.put("lineageStartDate", String.valueOf(flowFile.getLineageStartDate()));
        flowFileProps.put("lastQueueDate", String.valueOf(flowFile.getLastQueueDate()));
        flowFileProps.put("queueDateIndex", String.valueOf(flowFile.getQueueDateIndex()));
        return flowFileProps;
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        for (final Map<String, String> map : maps) {
            if (!map.isEmpty()) {
                return false;
            }
        }
        return registry.getVariableMap().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (key == null) {
            return false;
        }
        if (maps.stream().anyMatch((map) -> (map.containsKey(key)))) {
            return true;
        }
        return registry.getVariableKey(key.toString()) != null;
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

        for (final Map<String, String> map : maps) {
            final String val = map.get(key.toString());
            if (val != null) {
                return val;
            }
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
        entrySet().stream().forEach((entry) -> {
            keySet.add(entry.getKey());
        });
        return keySet;
    }

    @Override
    public Collection<String> values() {
        final Set<String> values = new HashSet<>();
        entrySet().stream().forEach((entry) -> {
            values.add(entry.getValue());
        });
        return values;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        final Map<String, String> newMap = new HashMap<>();
        //put variable registry entries first
        for (final Map.Entry<VariableDescriptor, String> entry : registry.getVariableMap().entrySet()) {
            newMap.put(entry.getKey().getName(), entry.getValue());
        }
        //put attribute maps in reverse order
        final List<Map<String,String>> listOfMaps = new ArrayList<>(maps);
        Collections.reverse(listOfMaps);
        for(final Map<String,String> map : listOfMaps){
            for(final Map.Entry<String, String> entry : map.entrySet()){
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        return newMap.entrySet();
    }

}
