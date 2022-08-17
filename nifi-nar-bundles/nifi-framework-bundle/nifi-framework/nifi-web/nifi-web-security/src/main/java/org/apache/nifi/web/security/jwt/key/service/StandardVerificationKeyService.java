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
package org.apache.nifi.web.security.jwt.key.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Standard Verification Key Service implemented using State Manager
 */
public class StandardVerificationKeyService implements VerificationKeyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardVerificationKeyService.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final Scope SCOPE = Scope.LOCAL;

    private final StateManager stateManager;

    public StandardVerificationKeyService(final StateManager stateManager) {
        this.stateManager = stateManager;
    }

    /**
     * Find Key using specified Key Identifier
     *
     * @param id Key Identifier
     * @return Optional Key
     */
    @Override
    public Optional<Key> findById(final String id) {
        final Optional<String> serializedKey = findSerializedKey(id);
        return serializedKey.map(this::getVerificationKey).map(this::getKey);
    }

    /**
     * Delete Expired Verification Keys is synchronized to avoid losing updates from other methods
     */
    @Override
    public synchronized void deleteExpired() {
        final Map<String, String> state = getStateMap().toMap();

        final Instant now = Instant.now();
        final Map<String, String> updatedState = state
                .values()
                .stream()
                .map(this::getVerificationKey)
                .filter(verificationKey -> verificationKey.getExpiration().isAfter(now))
                .collect(Collectors.toMap(VerificationKey::getId, this::serializeVerificationKey));

        if (updatedState.equals(state)) {
            LOGGER.debug("Expired Verification Keys not found");
        } else {
            try {
                stateManager.setState(updatedState, SCOPE);
            } catch (final IOException e) {
                throw new UncheckedIOException("Delete Expired Verification Keys Failed", e);
            }
            LOGGER.debug("Delete Expired Verification Keys Completed: Keys Before [{}] Keys After [{}]", state.size(), updatedState.size());
        }
    }

    /**
     * Save Verification Key
     *
     * @param id Key Identifier
     * @param key Key
     * @param expiration Expiration
     */
    @Override
    public void save(final String id, final Key key, final Instant expiration) {
        final VerificationKey verificationKey = new VerificationKey();
        verificationKey.setId(id);
        verificationKey.setEncoded(key.getEncoded());
        verificationKey.setAlgorithm(key.getAlgorithm());
        verificationKey.setExpiration(expiration);
        setVerificationKey(verificationKey);
    }

    /**
     * Set Expiration of Verification Key when found
     *
     * @param id Key Identifier
     * @param expiration Expiration
     */
    @Override
    public void setExpiration(final String id, final Instant expiration) {
        final Optional<String> serializedKey = findSerializedKey(id);
        if (serializedKey.isPresent()) {
            final VerificationKey verificationKey = getVerificationKey(serializedKey.get());
            verificationKey.setExpiration(expiration);
            setVerificationKey(verificationKey);
        }
    }

    /**
     * Set Verification Key is synchronized to avoid competing updates to the State Map
     *
     * @param verificationKey Verification Key to be stored
     */
    private synchronized void setVerificationKey(final VerificationKey verificationKey) {
        try {
            final String serialized = serializeVerificationKey(verificationKey);
            final Map<String, String> state = new HashMap<>(getStateMap().toMap());
            state.put(verificationKey.getId(), serialized);
            stateManager.setState(state, SCOPE);
        } catch (final IOException e) {
            throw new UncheckedIOException("Set Verification Key State Failed", e);
        }
        LOGGER.debug("Stored Verification Key [{}] Expiration [{}]", verificationKey.getId(), verificationKey.getExpiration());
    }

    private Optional<String> findSerializedKey(final String id) {
        final StateMap stateMap = getStateMap();
        return Optional.ofNullable(stateMap.get(id));
    }

    private String serializeVerificationKey(final VerificationKey verificationKey) {
        try {
            return OBJECT_MAPPER.writeValueAsString(verificationKey);
        } catch (final JsonProcessingException e) {
            throw new UncheckedIOException("Serialize Verification Key Failed", e);
        }
    }

    private VerificationKey getVerificationKey(final String serialized) {
        try {
            return OBJECT_MAPPER.readValue(serialized, VerificationKey.class);
        } catch (final JsonProcessingException e) {
            throw new UncheckedIOException("Read Verification Key Failed", e);
        }
    }

    private Key getKey(final VerificationKey verificationKey) {
        final KeySpec keySpec = new X509EncodedKeySpec(verificationKey.getEncoded());
        final String algorithm = verificationKey.getAlgorithm();
        try {
            final KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
            return keyFactory.generatePublic(keySpec);
        } catch (final InvalidKeySpecException | NoSuchAlgorithmException e) {
            final String message = String.format("Parsing Encoded Key [%s] Algorithm [%s] Failed", verificationKey.getId(), algorithm);
            throw new IllegalStateException(message, e);
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
