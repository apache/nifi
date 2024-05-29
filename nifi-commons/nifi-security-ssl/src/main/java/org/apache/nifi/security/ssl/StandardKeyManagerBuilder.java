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
package org.apache.nifi.security.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of X.509 Key Manager Builder
 */
public class StandardKeyManagerBuilder implements KeyManagerBuilder {
    private KeyStore keyStore;

    private char[] keyPassword;

    /**
     * Build X.509 Extended Key Manager using configured properties
     *
     * @return X.509 Extended Key Manager
     */
    @Override
    public X509ExtendedKeyManager build() {
        final KeyManager[] keyManagers = getKeyManagers();
        if (keyManagers == null) {
            throw new BuilderConfigurationException("Key Managers not found: Key Store required");
        }

        final Optional<X509ExtendedKeyManager> configuredKeyManager = Arrays.stream(keyManagers)
                .filter(keyManager -> keyManager instanceof X509ExtendedKeyManager)
                .map(keyManager -> (X509ExtendedKeyManager) keyManager)
                .findFirst();

        return configuredKeyManager.orElseThrow(() -> new BuilderConfigurationException("X.509 Extended Key Manager not found"));
    }

    /**
     * Set Key Store with Key and Certificate Entries
     *
     * @param keyStore Key Store
     * @return Builder
     */
    public StandardKeyManagerBuilder keyStore(final KeyStore keyStore) {
        this.keyStore = Objects.requireNonNull(keyStore, "Key Store required");
        return this;
    }

    /**
     * Set Key Password for reading Private Key entries from Key Store
     *
     * @param keyPassword Key Password
     * @return Builder
     */
    public StandardKeyManagerBuilder keyPassword(final char[] keyPassword) {
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key Password required");
        return this;
    }

    private KeyManager[] getKeyManagers() {
        final KeyManager[] keyManagers;
        if (keyStore == null) {
            keyManagers = null;
        } else {
            final KeyManagerFactory keyManagerFactory = new StandardKeyManagerFactoryBuilder().keyStore(keyStore).keyPassword(keyPassword).build();
            keyManagers = keyManagerFactory.getKeyManagers();
        }
        return keyManagers;
    }
}
