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
import com.nimbusds.jose.KeyTypeException;
import com.nimbusds.jose.jca.JCAContext;
import com.nimbusds.jose.proc.JWSVerifierFactory;

import java.security.Key;
import java.security.PublicKey;
import java.util.Set;

/**
 * Ed25519 implementation of Verifier Factory
 */
public class Ed25519VerifierFactory implements JWSVerifierFactory {
    private static final Set<JWSAlgorithm> SUPPORTED_ALGORITHMS = Set.of(JWSAlgorithm.EdDSA);

    private final JCAContext jcaContext = new JCAContext();

    /**
     * Create JSON Web Security Verifier for EdDSA using Ed25519 Public Key
     *
     * @param jwsHeader JSON Web Security Header
     * @param key Ed25519 Public Key required
     * @return JSON Web Security Verifier
     * @throws JOSEException Thrown on failure to create verifier
     */
    @Override
    public JWSVerifier createJWSVerifier(final JWSHeader jwsHeader, final Key key) throws JOSEException {
        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();

        if (SUPPORTED_ALGORITHMS.contains(algorithm)) {
           if (key instanceof PublicKey publicKey) {
                final Ed25519Verifier verifier = new Ed25519Verifier(publicKey);
                verifier.getJCAContext().setProvider(jcaContext.getProvider());
                return verifier;
           } else {
               throw new KeyTypeException(PublicKey.class);
           }
        } else {
            throw new JOSEException("JWS Algorithm [%s] not supported".formatted(algorithm));
        }
    }

    @Override
    public Set<JWSAlgorithm> supportedJWSAlgorithms() {
        return SUPPORTED_ALGORITHMS;
    }

    @Override
    public JCAContext getJCAContext() {
        return jcaContext;
    }
}
