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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/*
 *This class is passed to Evaluators so that certain evaluators that specifically work with state will have access to the state values explicitly.
 *It implements Map so that other evaluators don't have to be changed.
 */
public class AttributesAndState implements Map<String, String> {

    private final Map<String, String> stateMap;
    private final Map<String, String> attributes;

    public AttributesAndState(Map<String, String> attributes, Map<String, String> state) {
        super();
        stateMap = state;
        this.attributes = attributes;
    }

    public Map<String, String> getStateMap() {
        return stateMap;
    }

    @Override
    public int size() {
        return attributes.size();
    }

    @Override
    public boolean isEmpty() {
        return attributes.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return attributes.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return attributes.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return attributes.get(key);
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
        return attributes.keySet();
    }

    @Override
    public Collection<String> values() {
        return attributes.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return attributes.entrySet();
    }
}
