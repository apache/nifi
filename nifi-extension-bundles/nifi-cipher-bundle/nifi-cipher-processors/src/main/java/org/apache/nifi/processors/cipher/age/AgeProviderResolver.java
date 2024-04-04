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
package org.apache.nifi.processors.cipher.age;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Provider;
import java.security.Security;
import java.util.Optional;

/**
 * Resolver abstraction for age-encryption Security Algorithm Providers
 */
public final class AgeProviderResolver {
    private static final String KEY_ALGORITHM_FILTER = "KeyFactory.X25519";

    private static final String CIPHER_ALGORITHM_FILTER = "Cipher.ChaCha20-Poly1305";

    private AgeProviderResolver() {

    }

    /**
     * Get Java Security Provider supporting ChaCha20-Poly1305
     *
     * @return Provider supporting ChaCha20-Poly1305 Cipher that defaults to Bouncy Castle when no other Providers found
     */
    public static Provider getCipherProvider() {
        final Provider cipherProvider;

        final Provider[] providers = Security.getProviders(CIPHER_ALGORITHM_FILTER);
        if (providers == null) {
            cipherProvider = new BouncyCastleProvider();
        } else {
            cipherProvider = providers[0];
        }

        return cipherProvider;
    }

    /**
     * Get available Java Security Provider supporting X25519 and ChaCha20-Poly1305 when registered Provider not found
     *
     * @return Empty Provider indicating platform support for X25519 Key Factory or Bouncy Castle when no other Providers found
     */
    public static Optional<Provider> getKeyProvider() {
        final Provider keyProvider;

        final Provider[] providers = Security.getProviders(KEY_ALGORITHM_FILTER);
        if (providers == null) {
            keyProvider = new BouncyCastleProvider();
        } else {
            keyProvider = null;
        }

        return Optional.ofNullable(keyProvider);
    }
}
