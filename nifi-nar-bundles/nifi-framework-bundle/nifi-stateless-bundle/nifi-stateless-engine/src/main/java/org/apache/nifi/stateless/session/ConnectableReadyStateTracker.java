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

package org.apache.nifi.stateless.session;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Simple component used to track which Connectables are ready to be triggered
 */
public class ConnectableReadyStateTracker {
    private final Set<Connectable> ready = new LinkedHashSet<>();
    private final Set<Connectable> readOnly = Collections.unmodifiableSet(ready);

    public void addConnectable(final Connectable connectable) {
        ready.add(connectable);
    }

    public void removeConnectable(final Connectable connectable) {
        ready.remove(connectable);
    }

    public Set<Connectable> getReady() {
        return readOnly;
    }

    public boolean isAnyReady() {
        return !ready.isEmpty();
    }

    public boolean isReady(final Connectable connectable) {
        if (!ready.contains(connectable)) {
            return false;
        }

        for (final Connection incoming : connectable.getIncomingConnections()) {
            if (!incoming.getFlowFileQueue().isEmpty()) {
                return true;
            }
        }

        ready.remove(connectable);
        return false;
    }
}
