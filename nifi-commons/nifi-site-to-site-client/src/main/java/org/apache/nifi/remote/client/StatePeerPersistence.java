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
package org.apache.nifi.remote.client;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.remote.util.PeerStatusCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class StatePeerPersistence extends AbstractPeerPersistence {

    static final String STATE_KEY_PEERS = "peers";
    static final String STATE_KEY_TRANSPORT_PROTOCOL = "protocol";
    static final String STATE_KEY_PEERS_TIMESTAMP = "peers.ts";

    private final StateManager stateManager;

    public StatePeerPersistence(StateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public void save(final PeerStatusCache peerStatusCache) throws IOException {
        final StateMap state = stateManager.getState(Scope.LOCAL);
        final Map<String, String> stateMap = state.toMap();
        final Map<String, String> updatedStateMap = new HashMap<>(stateMap);
        final StringBuilder peers = new StringBuilder();
        write(peerStatusCache, peers::append);
        updatedStateMap.put(STATE_KEY_PEERS, peers.toString());
        updatedStateMap.put(STATE_KEY_PEERS_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
        stateManager.setState(updatedStateMap, Scope.LOCAL);
    }

    @Override
    public PeerStatusCache restore() throws IOException {
        final StateMap state = stateManager.getState(Scope.LOCAL);
        final String storedPeers = state.get(STATE_KEY_PEERS);
        if (storedPeers != null && !storedPeers.isEmpty()) {
            try (final BufferedReader reader = new BufferedReader(new StringReader(storedPeers))) {
                return restorePeerStatuses(reader, Long.parseLong(state.get(STATE_KEY_PEERS_TIMESTAMP)));
            }
        }
        return null;
    }
}
