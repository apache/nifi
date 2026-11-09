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
package org.apache.nifi.controller.repository;

import org.apache.nifi.components.state.StateMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A state update that a Process Session holds until it is committed. The update always provides the full state as the session sees it.
 * When the update was made only by patching state, it also provides the patches, so that only the patches need to be sent to the
 * State Manager on commit.
 */
final class SessionStateUpdate {
    private final StateMap stateMap;
    private final Map<String, String> patches;

    private SessionStateUpdate(final StateMap stateMap, final Map<String, String> patches) {
        this.stateMap = stateMap;
        this.patches = patches;
    }

    /**
     * Creates an update that replaces the whole state with the given state
     *
     * @param stateMap the new state
     * @return an update that replaces the whole state
     */
    static SessionStateUpdate createWholeStateUpdate(final StateMap stateMap) {
        return new SessionStateUpdate(stateMap, null);
    }

    /**
     * Creates an update that changes only the keys in the given patches
     *
     * @param stateMap the full state that results from applying the patches
     * @param patches the keys to change, where a {@code null} value means that the key is removed
     * @return an update that changes only the keys in the given patches
     */
    static SessionStateUpdate createPatchUpdate(final StateMap stateMap, final Map<String, String> patches) {
        // Map.copyOf does not allow null values, which are needed to indicate removed keys
        return new SessionStateUpdate(stateMap, Collections.unmodifiableMap(new HashMap<>(patches)));
    }

    /**
     * @return the full state as the session sees it once this update is applied
     */
    StateMap getStateMap() {
        return stateMap;
    }

    /**
     * @return an unmodifiable map of the keys to change, where a {@code null} value means that the key is removed, or {@code null} if this update
     *         replaces the whole state
     */
    Map<String, String> getPatches() {
        return patches;
    }
}
