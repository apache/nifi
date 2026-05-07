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

package org.apache.nifi.controller.state;

import org.apache.nifi.components.state.StateMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StandardStateMap implements StateMap {

    private final Map<String, String> stateValues;

    private final Optional<String> stateVersion;

    public StandardStateMap(final Map<String, String> stateValues, final Optional<String> stateVersion) {
        // Defensively copy the caller's Map so that this StateMap's contents are immutable and
        // isolated from any subsequent mutations to the caller's reference. Extensions that retain
        // and continue to mutate the Map they pass to setState() across invocations would otherwise
        // race with the framework's checkpoint thread iterating the StateMap during snapshot
        // serialization.
        this.stateValues = (stateValues == null || stateValues.isEmpty()) ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(stateValues));
        this.stateVersion = stateVersion;
    }

    @Override
    public Optional<String> getStateVersion() {
        return stateVersion;
    }

    @Override
    public String get(final String key) {
        return stateValues.get(key);
    }

    @Override
    public Map<String, String> toMap() {
        return stateValues;
    }

    @Override
    public String toString() {
        return "StandardStateMap[version=" + stateVersion + ", values=" + stateValues + "]";
    }
}
