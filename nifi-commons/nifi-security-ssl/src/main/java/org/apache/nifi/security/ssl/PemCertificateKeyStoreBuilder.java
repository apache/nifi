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
import java.security.cert.Certificate;
import java.util.List;
import java.util.Objects;

/**
 * Key Store Builder capable of reading one or more X.509 Certificates formatted with PEM headers and footers
 */
public class PemCertificateKeyStoreBuilder implements InputStreamKeyStoreBuilder {
    private static final String CERTIFICATE_ALIAS = "certificate-%d";

    private InputStream inputStream;

    /**
     * Build Key Store using configured properties
     *
     * @return Key Store
     */
    @Override
    public KeyStore build() {
        final KeyStore keyStore = getInitializedKeyStore();

        if (inputStream == null) {
            throw new BuilderConfigurationException("Key Store InputStream not configured");
        }

        loadKeyStore(keyStore);

        return keyStore;
    }

    /**
     * Set Key Store InputStream to be loaded
     *
     * @param inputStream Key Store InputStream
     * @return Builder
     */
    @Override
    public PemCertificateKeyStoreBuilder inputStream(final InputStream inputStream) {
        this.inputStream = Objects.requireNonNull(inputStream, "Key Store InputStream required");
        return this;
    }

    private void loadKeyStore(final KeyStore keyStore) {
        final PemCertificateReader pemCertificateReader = new StandardPemCertificateReader();
        final List<Certificate> certificates = pemCertificateReader.readCertificates(inputStream);

        int certificateIndex = 0;
        for (final Certificate certificate : certificates) {
            final String alias = CERTIFICATE_ALIAS.formatted(certificateIndex++);

            try {
                keyStore.setCertificateEntry(alias, certificate);
            } catch (final KeyStoreException e) {
                final String message = String.format("Set certificate entry [%s] failed", alias);
                throw new BuilderConfigurationException(message, e);
            }
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
