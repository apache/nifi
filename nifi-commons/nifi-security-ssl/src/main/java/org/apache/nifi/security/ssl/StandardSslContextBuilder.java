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
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * Standard implementation of SSL Context Builder
 */
public class StandardSslContextBuilder implements SslContextBuilder {
    private static final String DEFAULT_PROTOCOL = "TLS";

    private String protocol = DEFAULT_PROTOCOL;

    private KeyManager keyManager;

    private KeyStore keyStore;

    private char[] keyPassword;

    private TrustManager trustManager;

    private KeyStore trustStore;

    /**
     * Build and initialize an SSL Context using configured Key Manager and Trust Manager sources
     *
     * @return SSL Context
     */
    @Override
    public SSLContext build() {
        final SSLContext sslContext = getSslContext();
        final SecureRandom secureRandom = new SecureRandom();
        final KeyManager[] keyManagers = getKeyManagers();
        final TrustManager[] trustManagers = getTrustManagers();

        try {
            sslContext.init(keyManagers, trustManagers, secureRandom);
        } catch (final KeyManagementException e) {
            throw new BuilderConfigurationException("SSLContext initialization failed", e);
        }

        return sslContext;
    }

    /**
     * Set TLS Protocol defaults to TLS without a specific version number
     *
     * @param protocol TLS Protocol
     * @return Builder
     */
    public StandardSslContextBuilder protocol(final String protocol) {
        this.protocol = Objects.requireNonNull(protocol, "Protocol required");
        return this;
    }

    /**
     * Set Kay Manager takes precedence over Key Store
     *
     * @param keyManager Key Manager
     * @return Builder
     */
    public StandardSslContextBuilder keyManager(final KeyManager keyManager) {
        this.keyManager = Objects.requireNonNull(keyManager, "Key Manager required");
        return this;
    }

    /**
     * Set Key Store with Private Key and Certificate Entry
     *
     * @param keyStore Key Store
     * @return Builder
     */
    public StandardSslContextBuilder keyStore(final KeyStore keyStore) {
        this.keyStore = Objects.requireNonNull(keyStore, "Key Store required");
        return this;
    }

    /**
     * Set Key Password for reading Private Key entries from Key Store
     *
     * @param keyPassword Key Password
     * @return Builder
     */
    public StandardSslContextBuilder keyPassword(final char[] keyPassword) {
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key Password required");
        return this;
    }

    /**
     * Set Trust Manager takes precedence over Trust Store
     *
     * @param trustManager Trust Manager
     * @return Builder
     */
    public StandardSslContextBuilder trustManager(final TrustManager trustManager) {
        this.trustManager = Objects.requireNonNull(trustManager, "Trust Manager required");
        return this;
    }

    /**
     * Set Trust Store with Certificate Entries
     *
     * @param trustStore Trust Store
     * @return Builder
     */
    public StandardSslContextBuilder trustStore(final KeyStore trustStore) {
        this.trustStore = Objects.requireNonNull(trustStore, "Trust Store required");
        return this;
    }

    private KeyManager[] getKeyManagers() {
        final KeyManager[] keyManagers;
        if (keyStore == null) {
            if (keyManager == null) {
                keyManagers = null;
            } else {
                keyManagers = new KeyManager[]{keyManager};
            }
        } else {
            final X509ExtendedKeyManager configuredKeyManager = new StandardKeyManagerBuilder().keyStore(keyStore).keyPassword(keyPassword).build();
            keyManagers = new KeyManager[]{configuredKeyManager};
        }
        return keyManagers;
    }

    private TrustManager[] getTrustManagers() {
        final TrustManager[] trustManagers;
        if (trustStore == null) {
            if (trustManager == null) {
                trustManagers = null;
            } else {
                trustManagers = new TrustManager[]{trustManager};
            }
        } else {
            final X509TrustManager configuredTrustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();
            trustManagers = new TrustManager[]{configuredTrustManager};
        }
        return trustManagers;
    }

    private SSLContext getSslContext() {
        try {
            return SSLContext.getInstance(protocol);
        } catch (final NoSuchAlgorithmException e) {
            final String message = String.format("SSLContext creation failed with protocol [%s]", protocol);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
