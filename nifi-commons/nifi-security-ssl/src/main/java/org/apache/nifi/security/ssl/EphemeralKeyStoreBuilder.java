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

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

/**
 * Ephemeral implementation of KeyStore Builder that creates a KeyStore in memory without persistent entries
 */
public class EphemeralKeyStoreBuilder implements KeyStoreBuilder {

    private static final String CERTIFICATE_ALIAS = "certificate-%d";

    private static final String PRIVATE_KEY_ALIAS = "private-key-%d";

    private final List<X509Certificate> certificates = new ArrayList<>();

    private final List<KeyStore.PrivateKeyEntry> privateKeyEntries = new ArrayList<>();

    private char[] keyPassword = null;

    /**
     * Add X.509 Certificate to list of trusted certificates
     *
     * @param certificate Certificate to be added as a trusted entry
     * @return Builder
     */
    public EphemeralKeyStoreBuilder addCertificate(final X509Certificate certificate) {
        Objects.requireNonNull(certificate, "Certificate required");
        certificates.add(certificate);
        return this;
    }

    /**
     * Add Private Key Entry containing one or more associated X.509 Certificates
     *
     * @param privateKeyEntry Private Key Entry to be added
     * @return Builder
     */
    public EphemeralKeyStoreBuilder addPrivateKeyEntry(final KeyStore.PrivateKeyEntry privateKeyEntry) {
        Objects.requireNonNull(privateKeyEntry, "Private Key Entry required");
        privateKeyEntries.add(privateKeyEntry);
        return this;
    }

    /**
     * Set Key Password for Private Key Entries
     *
     * @param keyPassword Key password array of characters
     * @return Builder
     */
    public EphemeralKeyStoreBuilder keyPassword(final char[] keyPassword) {
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key Password required").clone();
        return this;
    }

    /**
     * Build Key Store with provided Certificates and Private Key Entries
     *
     * @return Key Store
     */
    @Override
    public KeyStore build() {
        final KeyStore keyStore = getInitializedKeyStore();

        final ListIterator<X509Certificate> certificateEntries = certificates.listIterator();
        while (certificateEntries.hasNext()) {
            final String alias = CERTIFICATE_ALIAS.formatted(certificateEntries.nextIndex());
            final X509Certificate certificate = certificateEntries.next();

            try {
                keyStore.setCertificateEntry(alias, certificate);
            } catch (final KeyStoreException e) {
                final String message = String.format("Set certificate entry [%s] failed", alias);
                throw new BuilderConfigurationException(message, e);
            }
        }

        final ListIterator<KeyStore.PrivateKeyEntry> privateKeys = privateKeyEntries.listIterator();
        while (privateKeys.hasNext()) {
            final String alias = PRIVATE_KEY_ALIAS.formatted(privateKeys.nextIndex());
            final KeyStore.PrivateKeyEntry privateKeyEntry = privateKeys.next();

            final PrivateKey privateKey = privateKeyEntry.getPrivateKey();
            final Certificate[] certificateChain = privateKeyEntry.getCertificateChain();

            try {
                keyStore.setKeyEntry(alias, privateKey, keyPassword, certificateChain);
            } catch (final KeyStoreException e) {
                final String message = String.format("Set key entry [%s] failed", alias);
                throw new BuilderConfigurationException(message, e);
            }
        }

        return keyStore;
    }

    private KeyStore getInitializedKeyStore() {
        final String keyStoreType = KeyStore.getDefaultType();
        try {
            final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null);
            return keyStore;
        } catch (final Exception e) {
            final String message = String.format("Key Store Type [%s] initialization failed", keyStoreType);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
