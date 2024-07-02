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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.impl.EdDSAProvider;
import com.nimbusds.jose.util.Base64URL;

import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Objects;

/**
 * Ed25519 implementation of JSON Web Security Verifier using Java cryptography
 */
public class Ed25519Verifier extends EdDSAProvider implements JWSVerifier {
    private static final String SIGNING_ALGORITHM = "Ed25519";

    private final PublicKey publicKey;

    public Ed25519Verifier(final PublicKey publicKey) {
        this.publicKey = Objects.requireNonNull(publicKey, "Public Key required");
    }

    /**
     * Verify input bytes for EdDSA using configured Ed25519 Public Key
     *
     * @param jwsHeader JSON Web Security Header
     * @param bytes Byte array for calculating signature to be verified
     * @param jwsSignature Provided signature to be verified
     * @return Signature verification status
     * @throws JOSEException Thrown on verification failures
     */
    @Override
    public boolean verify(final JWSHeader jwsHeader, final byte[] bytes, final Base64URL jwsSignature) throws JOSEException {
        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();
        if (JWSAlgorithm.EdDSA.equals(algorithm)) {
            final byte[] signatureDecoded = jwsSignature.decode();

            try {
                final Signature signature = getInitializedSignature();
                signature.update(bytes);
                return signature.verify(signatureDecoded);
            } catch (final GeneralSecurityException e) {
                return false;
            }
        } else {
            throw new JOSEException("JWS Algorithm EdDSA not found [%s]".formatted(algorithm));
        }
    }

    private Signature getInitializedSignature() throws JOSEException {
        try {
            final Signature signature = Signature.getInstance(SIGNING_ALGORITHM);
            signature.initVerify(publicKey);
            return signature;
        } catch (final GeneralSecurityException e) {
            throw new JOSEException("Ed25519 signature initialization failed", e);
        }
    }
}
