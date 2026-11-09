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
import org.wali.UpdateType;

import java.util.Map;

public class StateMapUpdate {
    private final StateMap stateMap;
    private final String componentId;
    private final UpdateType updateType;
    private final Map<String, String> patches;

    public StateMapUpdate(final StateMap stateMap, final String componentId, final UpdateType updateType) {
        this(stateMap, componentId, updateType, null);
    }

    /**
     * @param stateMap the full state of the component once the update is applied
     * @param componentId the identifier of the component
     * @param updateType the type of update
     * @param patches the keys that were changed, where a {@code null} value means that the key was removed, or {@code null} if the update replaces the whole state
     */
    public StateMapUpdate(final StateMap stateMap, final String componentId, final UpdateType updateType, final Map<String, String> patches) {
        this.stateMap = stateMap;
        this.componentId = componentId;
        this.updateType = updateType;
        this.patches = patches;
    }

    public StateMap getStateMap() {
        return stateMap;
    }

    public String getComponentId() {
        return componentId;
    }

    public UpdateType getUpdateType() {
        return updateType;
    }

    /**
     * @return the keys that were changed, where a {@code null} value means that the key was removed, or {@code null} if the update replaces the whole state
     */
    public Map<String, String> getPatches() {
        return patches;
    }
}
