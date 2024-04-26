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
package org.apache.nifi.web.security.jwt.revocation;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Standard JSON Web Token Revocation Service using State Manager
 */
public class StandardJwtRevocationService implements JwtRevocationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardJwtRevocationService.class);

    private static final Scope SCOPE = Scope.LOCAL;

    private final StateManager stateManager;

    public StandardJwtRevocationService(final StateManager stateManager) {
        this.stateManager = stateManager;
    }

    /**
     * Delete Expired Revocations is synchronized is avoid losing updates from setRevoked()
     */
    @Override
    public synchronized void deleteExpired() {
        final Map<String, String> state = getStateMap().toMap();

        final Instant now = Instant.now();
        final Map<String, String> updatedState = state
                .entrySet()
                .stream()
                .filter(entry -> Instant.parse(entry.getValue()).isAfter(now))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (updatedState.equals(state)) {
            LOGGER.debug("Expired Revocations not found");
        } else {
            try {
                stateManager.setState(updatedState, SCOPE);
            } catch (final IOException e) {
                throw new UncheckedIOException("Delete Expired Revocations Failed", e);
            }
            LOGGER.debug("Delete Expired Revocations: Before [{}] After [{}]", state.size(), updatedState.size());
        }
    }

    /**
     * Is JSON Web Token Identifier Revoked based on State Map Status
     *
     * @param id JSON Web Token Identifier
     * @return Revoked Status
     */
    @Override
    public boolean isRevoked(final String id) {
        final StateMap stateMap = getStateMap();
        return stateMap.toMap().containsKey(id);
    }

    /**
     * Set Revoked Status is synchronized to avoid competing changes to the State Map
     *
     * @param id JSON Web Token Identifier
     * @param expiration Expiration of Revocation Status after which the status record can be removed
     */
    @Override
    public synchronized void setRevoked(final String id, final Instant expiration) {
        final StateMap stateMap = getStateMap();
        final Map<String, String> state = new HashMap<>(stateMap.toMap());
        state.put(id, expiration.toString());
        try {
            stateManager.setState(state, SCOPE);
            LOGGER.debug("JWT Identifier [{}] Revocation Completed", id);
        } catch (final IOException e) {
            LOGGER.error("JWT Identifier [{}] Revocation Failed", id, e);
        }
    }

    private StateMap getStateMap() {
        try {
            return stateManager.getState(SCOPE);
        } catch (final IOException e) {
            throw new UncheckedIOException("Get State Failed", e);
        }
    }
}
