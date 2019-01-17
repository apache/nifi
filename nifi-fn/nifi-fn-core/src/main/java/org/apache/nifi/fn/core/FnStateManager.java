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
package org.apache.nifi.fn.core;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FnStateManager implements StateManager {
    private final AtomicInteger versionIndex = new AtomicInteger(0);

    private Map<Scope, FnStateMap> maps; //Local, Cluster

    public FnStateManager() {
        this.maps = new HashMap<>();
        for (Scope s : Scope.values()) {
            this.maps.put(s, new FnStateMap(null, -1L));
        }
    }

    public synchronized void setState(final Map<String, String> state, final Scope scope) {
        maps.put(scope, new FnStateMap(state, versionIndex.incrementAndGet()));
    }

    public synchronized StateMap getState(final Scope scope) {
        return maps.get(scope);
    }

    public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        if (oldValue == maps.get(scope)) {
            maps.put(scope, new FnStateMap(newValue, versionIndex.incrementAndGet()));
            return true;
        } else {
            return false;
        }
    }

    public synchronized void clear(final Scope scope) {
        setState(Collections.<String, String>emptyMap(), scope);
    }
}
