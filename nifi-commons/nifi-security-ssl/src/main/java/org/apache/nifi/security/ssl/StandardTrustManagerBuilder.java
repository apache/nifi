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
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of X.509 Trust Manager Builder
 */
public class StandardTrustManagerBuilder implements TrustManagerBuilder {
    private KeyStore trustStore;

    /**
     * Build X.509 Extended Trust Manager using configured properties
     *
     * @return X.509 Extended Trust Manager
     */
    @Override
    public X509ExtendedTrustManager build() {
        final TrustManager[] trustManagers = getTrustManagers();
        if (trustManagers == null) {
            throw new BuilderConfigurationException("Trust Managers not found: Trust Store required");
        }

        final Optional<X509ExtendedTrustManager> configuredTrustManager = Arrays.stream(trustManagers)
                .filter(trustManager -> trustManager instanceof X509ExtendedTrustManager)
                .map(trustManager -> (X509ExtendedTrustManager) trustManager)
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
            final TrustManagerFactory trustManagerFactory = new StandardTrustManagerFactoryBuilder().trustStore(trustStore).build();
            trustManagers = trustManagerFactory.getTrustManagers();
        }
        return trustManagers;
    }
}
