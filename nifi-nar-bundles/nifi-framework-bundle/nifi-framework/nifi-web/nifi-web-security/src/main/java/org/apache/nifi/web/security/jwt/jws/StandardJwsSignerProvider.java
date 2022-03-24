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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard JSON Web Signature Signer Provider
 */
public class StandardJwsSignerProvider implements JwsSignerProvider, SignerListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardJwsSignerProvider.class);

    private final AtomicReference<JwsSignerContainer> currentSigner = new AtomicReference<>();

    private final SigningKeyListener signingKeyListener;

    public StandardJwsSignerProvider(final SigningKeyListener signingKeyListener) {
        this.signingKeyListener = signingKeyListener;
    }

    /**
     * Get Current JWS Signer Container and update expiration
     *
     * @param expiration New JSON Web Token Expiration to be set for the returned Signing Key
     * @return JWS Signer Container
     */
    @Override
    public JwsSignerContainer getJwsSignerContainer(final Instant expiration) {
        final JwsSignerContainer jwsSignerContainer = currentSigner.get();
        if (jwsSignerContainer == null) {
            throw new IllegalStateException("JSON Web Signature Signer not configured");
        }
        final String keyIdentifier = jwsSignerContainer.getKeyIdentifier();
        LOGGER.debug("Signer Used with Key Identifier [{}]", keyIdentifier);
        signingKeyListener.onSigningKeyUsed(keyIdentifier, expiration);
        return jwsSignerContainer;
    }

    /**
     * On Signer Updated changes the current JWS Signer
     *
     * @param jwsSignerContainer JWS Signer Container
     */
    @Override
    public void onSignerUpdated(final JwsSignerContainer jwsSignerContainer) {
        LOGGER.debug("Signer Updated with Key Identifier [{}]", jwsSignerContainer.getKeyIdentifier());
        currentSigner.set(jwsSignerContainer);
    }
}
