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

import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Standard implementation of Trust Manager Factory Builder
 */
public class StandardTrustManagerFactoryBuilder implements TrustManagerFactoryBuilder {
    private KeyStore trustStore;

    /**
     * Build Trust Manager Factory using configured properties
     *
     * @return Trust Manager Factory
     */
    @Override
    public TrustManagerFactory build() {
        final TrustManagerFactory trustManagerFactory;
        if (trustStore == null) {
            trustManagerFactory = null;
        } else {
            trustManagerFactory = getTrustManagerFactory();
            try {
                trustManagerFactory.init(trustStore);
            } catch (final KeyStoreException e) {
                throw new BuilderConfigurationException("Trust Manager Factory initialization failed", e);
            }
        }
        return trustManagerFactory;
    }

    /**
     * Set Trust Store with Certificate Entries
     *
     * @param trustStore Trust Store
     * @return Builder
     */
    public StandardTrustManagerFactoryBuilder trustStore(final KeyStore trustStore) {
        this.trustStore = Objects.requireNonNull(trustStore, "Trust Store required");
        return this;
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
