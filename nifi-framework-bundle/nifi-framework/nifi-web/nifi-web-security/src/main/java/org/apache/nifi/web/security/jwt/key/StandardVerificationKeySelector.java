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
package org.apache.nifi.web.security.jwt.key;

import org.apache.nifi.web.security.jwt.jws.SigningKeyListener;
import org.apache.nifi.web.security.jwt.key.service.VerificationKeyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard Verification Key Selector implements listener interfaces for updating Key status
 */
public class StandardVerificationKeySelector implements SigningKeyListener, VerificationKeyListener, VerificationKeySelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardVerificationKeySelector.class);

    private final VerificationKeyService verificationKeyService;

    private final Duration keyRotationPeriod;

    public StandardVerificationKeySelector(final VerificationKeyService verificationKeyService, final Duration keyRotationPeriod) {
        this.verificationKeyService = Objects.requireNonNull(verificationKeyService, "Verification Key Service required");
        this.keyRotationPeriod = Objects.requireNonNull(keyRotationPeriod, "Key Rotation Period required");
    }

    /**
     * On Verification Key Generated persist encoded Key with expiration
     *
     * @param keyIdentifier Key Identifier
     * @param key Key
     */
    @Override
    public void onVerificationKeyGenerated(final String keyIdentifier, final Key key) {
        final Instant expiration = Instant.now().plus(keyRotationPeriod);
        verificationKeyService.save(keyIdentifier, key, expiration);
        LOGGER.debug("Verification Key Saved [{}] Expiration [{}]", keyIdentifier, expiration);
    }

    /**
     * Get Verification Keys
     *
     * @param keyIdentifier Key Identifier
     * @return List of Keys
     */
    @Override
    public List<? extends Key> getVerificationKeys(final String keyIdentifier) {
        final Optional<Key> key = verificationKeyService.findById(keyIdentifier);
        final List<? extends Key> keys = key.map(Collections::singletonList).orElse(Collections.emptyList());
        LOGGER.debug("Key Identifier [{}] Verification Keys Found [{}]", keyIdentifier, keys.size());
        return keys;
    }

    /**
     * On Signing Key Used set new expiration
     *
     * @param keyIdentifier Key Identifier
     * @param expiration JSON Web Token Expiration
     */
    @Override
    public void onSigningKeyUsed(final String keyIdentifier, final Instant expiration) {
        LOGGER.debug("Signing Key Used [{}] Expiration [{}]", keyIdentifier, expiration);
        verificationKeyService.setExpiration(keyIdentifier, expiration);
    }
}
