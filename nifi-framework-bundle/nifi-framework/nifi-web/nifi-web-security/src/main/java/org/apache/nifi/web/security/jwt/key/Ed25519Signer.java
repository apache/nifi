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
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.impl.EdDSAProvider;
import com.nimbusds.jose.util.Base64URL;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Objects;

/**
 * Ed25519 implementation of JSON Web Security Signer using Java cryptography
 */
public class Ed25519Signer extends EdDSAProvider implements JWSSigner {
    private static final String SIGNING_ALGORITHM = "Ed25519";

    private final PrivateKey privateKey;

    public Ed25519Signer(final PrivateKey privateKey) {
        this.privateKey = Objects.requireNonNull(privateKey, "Private Key required");
    }

    /**
     * Sign bytes for EdDSA algorithm using configured Ed25519 Private Key
     *
     * @param jwsHeader JSON Web Security Header
     * @param bytes Byte array to be signed
     * @return Base64 encoded signature
     * @throws JOSEException Thrown on failure produce signature
     */
    @Override
    public Base64URL sign(final JWSHeader jwsHeader, final byte[] bytes) throws JOSEException {
        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();
        if (JWSAlgorithm.EdDSA.equals(algorithm)) {
            try {
                final Signature signature = Signature.getInstance(SIGNING_ALGORITHM);
                signature.initSign(privateKey);
                signature.update(bytes);

                final byte[] jwsSignature = signature.sign();
                return Base64URL.encode(jwsSignature);
            } catch (final GeneralSecurityException e) {
                throw new JOSEException("Ed25519 signing failed", e);
            }
        } else {
            throw new JOSEException("JWS Algorithm EdDSA not found [%s]".formatted(algorithm));
        }
    }
}
