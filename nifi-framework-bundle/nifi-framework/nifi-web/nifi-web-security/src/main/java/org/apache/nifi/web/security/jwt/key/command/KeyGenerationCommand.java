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
import org.apache.nifi.web.security.jwt.key.Ed25519Signer;
import org.apache.nifi.web.security.jwt.key.VerificationKeyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.UUID;

/**
 * Key Generation Command produces new RSA Key Pairs and configures a JWS Signer
 */
public class KeyGenerationCommand implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyGenerationCommand.class);

    private static final String RSA_KEY_ALGORITHM = "RSA";

    private static final JWSAlgorithm RSA_JWS_ALGORITHM = JWSAlgorithm.PS512;

    private static final JWSAlgorithm DEFAULT_JWS_ALGORITHM = JWSAlgorithm.EdDSA;

    private final KeyPairGenerator keyPairGenerator;

    private final JWSAlgorithm jwsAlgorithm;

    private final SignerListener signerListener;

    private final VerificationKeyListener verificationKeyListener;

    public KeyGenerationCommand(final SignerListener signerListener, final VerificationKeyListener verificationKeyListener, final KeyPairGenerator keyPairGenerator) {
        this.signerListener = Objects.requireNonNull(signerListener, "Signer Listener required");
        this.verificationKeyListener = Objects.requireNonNull(verificationKeyListener, "Verification Key Listener required");
        this.keyPairGenerator = Objects.requireNonNull(keyPairGenerator, "Key Pair Generator required");

        // Configure JWS Algorithm based on Key Pair Generator algorithm with fallback to RSA when Ed25519 not supported
        final String keyPairAlgorithm = keyPairGenerator.getAlgorithm();
        if (RSA_KEY_ALGORITHM.equals(keyPairAlgorithm)) {
            this.jwsAlgorithm = RSA_JWS_ALGORITHM;
        } else {
            this.jwsAlgorithm = DEFAULT_JWS_ALGORITHM;
        }
    }

    /**
     * Run generates a new Key Pair and notifies configured listeners
     */
    @Override
    public void run() {
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final String keyIdentifier = UUID.randomUUID().toString();
        LOGGER.debug("Generated Key Pair [{}] Key Identifier [{}]", keyPairGenerator.getAlgorithm(), keyIdentifier);

        verificationKeyListener.onVerificationKeyGenerated(keyIdentifier, keyPair.getPublic());

        final PrivateKey privateKey = keyPair.getPrivate();
        final JWSSigner jwsSigner = getJwsSigner(privateKey);
        signerListener.onSignerUpdated(new JwsSignerContainer(keyIdentifier, jwsAlgorithm, jwsSigner));
    }

    private JWSSigner getJwsSigner(final PrivateKey privateKey) {
        final JWSSigner jwsSigner;
        if (RSA_JWS_ALGORITHM.equals(jwsAlgorithm)) {
            jwsSigner = new RSASSASigner(privateKey);
        } else {
            jwsSigner = new Ed25519Signer(privateKey);
        }
        return jwsSigner;
    }
}
