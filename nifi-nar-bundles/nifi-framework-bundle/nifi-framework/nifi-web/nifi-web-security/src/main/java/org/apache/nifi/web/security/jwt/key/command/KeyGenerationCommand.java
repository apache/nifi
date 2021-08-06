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
package org.apache.nifi.web.security.jwt.key.command;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import org.apache.nifi.web.security.jwt.jws.JwsSignerContainer;
import org.apache.nifi.web.security.jwt.jws.SignerListener;
import org.apache.nifi.web.security.jwt.key.VerificationKeyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.UUID;

/**
 * Key Generation Command produces new RSA Key Pairs and configures a JWS Signer
 */
public class KeyGenerationCommand implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyGenerationCommand.class);

    private static final String KEY_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 4096;

    private static final JWSAlgorithm JWS_ALGORITHM = JWSAlgorithm.PS512;

    private final KeyPairGenerator keyPairGenerator;

    private final SignerListener signerListener;

    private final VerificationKeyListener verificationKeyListener;

    public KeyGenerationCommand(final SignerListener signerListener, final VerificationKeyListener verificationKeyListener) {
        this.signerListener = Objects.requireNonNull(signerListener, "Signer Listener required");
        this.verificationKeyListener = Objects.requireNonNull(verificationKeyListener, "Verification Key Listener required");
        try {
            keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
            keyPairGenerator.initialize(KEY_SIZE, new SecureRandom());
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Run generates a new Key Pair and notifies configured listeners
     */
    @Override
    public void run() {
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final String keyIdentifier = UUID.randomUUID().toString();
        LOGGER.debug("Generated Key Pair [{}] Key Identifier [{}]", KEY_ALGORITHM, keyIdentifier);

        verificationKeyListener.onVerificationKeyGenerated(keyIdentifier, keyPair.getPublic());

        final JWSSigner jwsSigner = new RSASSASigner(keyPair.getPrivate());
        signerListener.onSignerUpdated(new JwsSignerContainer(keyIdentifier, JWS_ALGORITHM, jwsSigner));
    }
}
