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

import javax.net.ssl.KeyManagerFactory;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Standard implementation of Key Manager Factory Builder
 */
public class StandardKeyManagerFactoryBuilder implements KeyManagerFactoryBuilder {
    private KeyStore keyStore;

    private char[] keyPassword;

    /**
     * Build Key Manager Factory using configured properties
     *
     * @return Key Manager Factory
     */
    @Override
    public KeyManagerFactory build() {
        final KeyManagerFactory keyManagerFactory;
        if (keyStore == null) {
            keyManagerFactory = null;
        } else {
            keyManagerFactory = getKeyManagerFactory();
            try {
                keyManagerFactory.init(keyStore, keyPassword);
            } catch (final GeneralSecurityException e) {
                throw new BuilderConfigurationException("Key Manager Factory initialization failed", e);
            }
        }
        return keyManagerFactory;
    }

    /**
     * Set Key Store with Key and Certificate Entries
     *
     * @param keyStore Key Store
     * @return Builder
     */
    public StandardKeyManagerFactoryBuilder keyStore(final KeyStore keyStore) {
        this.keyStore = Objects.requireNonNull(keyStore, "Key Store required");
        return this;
    }

    /**
     * Set Key Password for reading Private Key entries from Key Store
     *
     * @param keyPassword Key Password
     * @return Builder
     */
    public StandardKeyManagerFactoryBuilder keyPassword(final char[] keyPassword) {
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key Password required");
        return this;
    }

    private KeyManagerFactory getKeyManagerFactory() {
        final String algorithm = KeyManagerFactory.getDefaultAlgorithm();
        try {
            return KeyManagerFactory.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException e) {
            final String message = String.format("KeyManagerFactory creation failed with algorithm [%s]", algorithm);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
