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

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.util.Objects;

/**
 * Standard implementation of Key Store Builder
 */
public class StandardKeyStoreBuilder implements InputStreamKeyStoreBuilder {
    private Provider provider;

    private String type = KeyStore.getDefaultType();

    private InputStream inputStream;

    private char[] password;

    /**
     * Build Key Store using configured properties
     *
     * @return Key Store
     */
    @Override
    public KeyStore build() {
        final KeyStore keyStore = getKeyStore();

        if (inputStream == null) {
            throw new BuilderConfigurationException("Key Store InputStream not configured");
        }

        try {
            keyStore.load(inputStream, password);
        } catch (final IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new BuilderConfigurationException("Key Store loading failed", e);
        }

        return keyStore;
    }

    /**
     * Set Key Store Provider for Key Store implementation
     *
     * @param provider Key Store Provider
     * @return Builder
     */
    public StandardKeyStoreBuilder provider(final Provider provider) {
        this.provider = Objects.requireNonNull(provider, "Key Store Provider required");
        return this;
    }

    /**
     * Set Key Store Type defaults to platform configuration derived from KeyStore.getDefaultType()
     *
     * @param type Key Store Type
     * @return Builder
     */
    public StandardKeyStoreBuilder type(final String type) {
        this.type = Objects.requireNonNull(type, "Key Store Type required");
        return this;
    }

    /**
     * Set Key Store Password
     *
     * @param password Key Store Password
     * @return Builder
     */
    public StandardKeyStoreBuilder password(final char[] password) {
        this.password = Objects.requireNonNull(password, "Key Store Password required");
        return this;
    }

    /**
     * Set Key Store InputStream to be loaded
     *
     * @param inputStream Key Store InputStream
     * @return Builder
     */
    @Override
    public StandardKeyStoreBuilder inputStream(final InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream, "Key Store InputStream required");
        return this;
    }

    private KeyStore getKeyStore() {
        try {
            return provider == null ? KeyStore.getInstance(type) : KeyStore.getInstance(type, provider);
        } catch (final KeyStoreException e) {
            final String message = String.format("Key Store Type [%s] creation failed", type);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
