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

import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Objects;

/**
 * Key Store Builder capable of reading a Private Key and one or more X.509 Certificates formatted with PEM headers and footers
 */
public class PemPrivateKeyCertificateKeyStoreBuilder implements KeyStoreBuilder {
    private static final String ALIAS = "private-key-1";

    private static final char[] EMPTY_PROTECTION_PARAMETER = new char[]{};

    private InputStream privateKeyInputStream;

    private InputStream certificateInputStream;

    /**
     * Build Key Store using configured properties
     *
     * @return Key Store
     */
    @Override
    public KeyStore build() {
        final KeyStore keyStore = getInitializedKeyStore();

        if (certificateInputStream == null) {
            throw new BuilderConfigurationException("Certificate InputStream not configured");
        }
        if (privateKeyInputStream == null) {
            throw new BuilderConfigurationException("Private Key InputStream not configured");
        }

        loadKeyStore(keyStore);

        return keyStore;
    }

    /**
     * Set Certificate InputStream to be loaded
     *
     * @param certificateInputStream Certificate InputStream
     * @return Builder
     */
    public PemPrivateKeyCertificateKeyStoreBuilder certificateInputStream(final InputStream certificateInputStream) {
        this.certificateInputStream = Objects.requireNonNull(certificateInputStream, "Certificate InputStream required");
        return this;
    }

    /**
     * Set Private Key InputStream to be loaded
     *
     * @param privateKeyInputStream Private Key InputStream
     * @return Builder
     */
    public PemPrivateKeyCertificateKeyStoreBuilder privateKeyInputStream(final InputStream privateKeyInputStream) {
        this.privateKeyInputStream = Objects.requireNonNull(privateKeyInputStream, "Private Key InputStream required");
        return this;
    }

    private void loadKeyStore(final KeyStore keyStore) {
        final PemCertificateReader pemCertificateReader = new StandardPemCertificateReader();
        final List<Certificate> certificates = pemCertificateReader.readCertificates(certificateInputStream);
        final Certificate[] certificateChain = certificates.toArray(new Certificate[]{});

        final PemPrivateKeyReader pemPrivateKeyReader = new StandardPemPrivateKeyReader();
        final PrivateKey privateKey = pemPrivateKeyReader.readPrivateKey(privateKeyInputStream);

        try {
            keyStore.setKeyEntry(ALIAS, privateKey, EMPTY_PROTECTION_PARAMETER, certificateChain);
        } catch (final KeyStoreException e) {
            throw new BuilderConfigurationException("Set key entry failed", e);
        }
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
