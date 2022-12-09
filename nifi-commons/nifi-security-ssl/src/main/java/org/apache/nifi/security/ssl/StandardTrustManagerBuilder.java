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

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of X.509 Trust Manager Builder
 */
public class StandardTrustManagerBuilder implements TrustManagerBuilder {
    private KeyStore trustStore;

    /**
     * Build X.509 Trust Manager using configured properties
     *
     * @return X.509 Trust Manager
     */
    @Override
    public X509TrustManager build() {
        final TrustManager[] trustManagers = getTrustManagers();
        if (trustManagers == null) {
            throw new BuilderConfigurationException("Trust Managers not found: Trust Store required");
        }

        final Optional<X509TrustManager> configuredTrustManager = Arrays.stream(trustManagers)
                .filter(trustManager -> trustManager instanceof X509TrustManager)
                .map(trustManager -> (X509TrustManager) trustManager)
                .findFirst();

        return configuredTrustManager.orElseThrow(() -> new BuilderConfigurationException("X.509 Trust Manager not found"));
    }

    /**
     * Set Trust Store with Certificate Entries
     *
     * @param trustStore Trust Store
     * @return Builder
     */
    public StandardTrustManagerBuilder trustStore(final KeyStore trustStore) {
        this.trustStore = Objects.requireNonNull(trustStore, "Trust Store required");
        return this;
    }

    private TrustManager[] getTrustManagers() {
        final TrustManager[] trustManagers;
        if (trustStore == null) {
            trustManagers = null;
        } else {
            final TrustManagerFactory trustManagerFactory = getTrustManagerFactory();
            try {
                trustManagerFactory.init(trustStore);
            } catch (final KeyStoreException e) {
                throw new BuilderConfigurationException("Trust Manager initialization failed", e);
            }
            trustManagers = trustManagerFactory.getTrustManagers();
        }
        return trustManagers;
    }

    private TrustManagerFactory getTrustManagerFactory() {
        final String algorithm = TrustManagerFactory.getDefaultAlgorithm();
        try {
            return TrustManagerFactory.getInstance(algorithm);
        } catch (final NoSuchAlgorithmException e) {
            final String message = String.format("TrustManagerFactory creation failed with algorithm [%s]", algorithm);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
