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
package org.apache.nifi.web.security.jwt.jws;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;

import java.util.Objects;

/**
 * JSON Web Signature Signer Container
 */
public class JwsSignerContainer {
    private final String keyIdentifier;

    private final JWSAlgorithm jwsAlgorithm;

    private final JWSSigner jwsSigner;

    public JwsSignerContainer(final String keyIdentifier, final JWSAlgorithm jwsAlgorithm, final JWSSigner jwsSigner) {
        this.keyIdentifier = Objects.requireNonNull(keyIdentifier, "Key Identifier required");
        this.jwsAlgorithm = Objects.requireNonNull(jwsAlgorithm, "JWS Algorithm required");
        this.jwsSigner = Objects.requireNonNull(jwsSigner, "JWS Signer required");
    }

    public String getKeyIdentifier() {
        return keyIdentifier;
    }

    public JWSAlgorithm getJwsAlgorithm() {
        return jwsAlgorithm;
    }

    public JWSSigner getJwsSigner() {
        return jwsSigner;
    }
}
