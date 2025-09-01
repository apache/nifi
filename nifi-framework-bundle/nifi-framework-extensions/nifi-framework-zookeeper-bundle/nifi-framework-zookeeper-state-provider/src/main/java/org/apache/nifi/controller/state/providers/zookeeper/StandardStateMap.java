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
package org.apache.nifi.controller.state.providers.zookeeper;

import org.apache.nifi.components.state.StateMap;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Standard implementation of StateMap
 */
class StandardStateMap implements StateMap {
    private final Map<String, String> data;

    private final Optional<String> version;

    StandardStateMap(final Map<String, String> data, final Optional<String> version) {
        this.data = Collections.unmodifiableMap(data);
        this.version = version;
    }


    /**
     * Get State Version
     *
     * @return State Version or empty when not known
     */
    @Override
    public Optional<String> getStateVersion() {
        return version;
    }

    /**
     * Get Value from State Map
     *
     * @param key the key whose value should be retrieved
     * @return Value or null when not found
     */
    @Override
    public String get(final String key) {
        return data.get(key);
    }

    /**
     * Get State Map
     *
     * @return State Map
     */
    @Override
    public Map<String, String> toMap() {
        return data;
    }
}
