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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockStateManagerProvider implements StateManagerProvider {
    private final ConcurrentMap<String, StateManager> stateManagers = new ConcurrentHashMap<>();
    private volatile boolean clusterProviderEnabled = false;

    @Override
    public StateManager getStateManager(final String componentId, final boolean dropStateKeySupported) {
        return stateManagers.computeIfAbsent(componentId, id -> new MockStateManager(dropStateKeySupported));
    }

    @Override
    public void onComponentRemoved(final String componentId) {
        stateManagers.remove(componentId);
    }

    @Override
    public void shutdown() {
        stateManagers.clear();
    }

    @Override
    public void enableClusterProvider() {
        clusterProviderEnabled = true;
    }

    @Override
    public void disableClusterProvider() {
        clusterProviderEnabled = false;
    }

    @Override
    public boolean isClusterProviderEnabled() {
        return clusterProviderEnabled;
    }
}
