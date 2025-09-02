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

package org.apache.nifi.controller;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.state.StandardStateMap;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;

public class MockStateManagerProvider implements StateManagerProvider {
    @Override
    public StateManager getStateManager(final String componentId) {
        final StateManager stateManager = Mockito.mock(StateManager.class);
        final StateMap emptyStateMap = new StandardStateMap(Collections.emptyMap(), Optional.empty());
        try {
            Mockito.when(stateManager.getState(any(Scope.class))).thenReturn(emptyStateMap);
        } catch (IOException e) {
            throw new AssertionError();
        }

        return stateManager;
    }

    public StateManager getStateManager(final String componentId, final boolean dropStateKeySupported) {
        return getStateManager(componentId);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void enableClusterProvider() {
    }

    @Override
    public void disableClusterProvider() {
    }

    public boolean isClusterProviderEnabled() {
        return false;
    }

    @Override
    public void onComponentRemoved(final String componentId) {
    }
}
