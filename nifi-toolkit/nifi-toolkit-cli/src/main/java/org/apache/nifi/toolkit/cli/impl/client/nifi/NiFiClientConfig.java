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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import org.apache.nifi.registry.security.util.KeyStoreUtils;
import org.apache.nifi.registry.security.util.KeystoreType;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

/**
 * Configuration for a NiFiClient.
 */
public class NiFiClientConfig {

    public static final String DEFAULT_PROTOCOL = "TLSv1.2";

    private final String baseUrl;
    private final SSLContext sslContext;
    private final String keystoreFilename;
    private final String keystorePass;
    private final String keyPass;
    private final KeystoreType keystoreType;
    private final String truststoreFilename;
    private final String truststorePass;
    private final KeystoreType truststoreType;
    private final String protocol;
    private final HostnameVerifier hostnameVerifier;
    private final Integer readTimeout;
    private final Integer connectTimeout;


    private NiFiClientConfig(final NiFiClientConfig.Builder builder) {
        this.baseUrl = builder.baseUrl;
        this.sslContext = builder.sslContext;
        this.keystoreFilename = builder.keystoreFilename;
        this.keystorePass = builder.keystorePass;
        this.keyPass = builder.keyPass;
        this.keystoreType = builder.keystoreType;
        this.truststoreFilename = builder.truststoreFilename;
        this.truststorePass = builder.truststorePass;
        this.truststoreType = builder.truststoreType;
        this.protocol = builder.protocol == null ? DEFAULT_PROTOCOL : builder.protocol;
        this.hostnameVerifier = builder.hostnameVerifier;
        this.readTimeout = builder.readTimeout;
        this.connectTimeout = builder.connectTimeout;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public SSLContext getSslContext() {
        if (sslContext != null) {
            return sslContext;
        }

        final KeyManagerFactory keyManagerFactory;
        if (keystoreFilename != null && keystorePass != null && keystoreType != null) {
            try {
                // prepare the keystore
                final KeyStore keyStore = KeyStoreUtils.getKeyStore(keystoreType.name());
                try (final InputStream keyStoreStream = new FileInputStream(new File(keystoreFilename))) {
                    keyStore.load(keyStoreStream, keystorePass.toCharArray());
                }
                keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

                if (keyPass == null) {
                    keyManagerFactory.init(keyStore, keystorePass.toCharArray());
                } else {
                    keyManagerFactory.init(keyStore, keyPass.toCharArray());
                }
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to load Keystore", e);
            }
        } else {
            keyManagerFactory = null;
        }

        final TrustManagerFactory trustManagerFactory;
        if (truststoreFilename != null && truststorePass != null && truststoreType != null) {
            try {
                // prepare the truststore
                final KeyStore trustStore = KeyStoreUtils.getTrustStore(truststoreType.name());
                try (final InputStream trustStoreStream = new FileInputStream(new File(truststoreFilename))) {
                    trustStore.load(trustStoreStream, truststorePass.toCharArray());
                }
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to load Truststore", e);
            }
        } else {
            trustManagerFactory = null;
        }

        if (keyManagerFactory != null || trustManagerFactory != null) {
            try {
                // initialize the ssl context
                KeyManager[] keyManagers = keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null;
                TrustManager[] trustManagers = trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null;
                final SSLContext sslContext = SSLContext.getInstance(getProtocol());
                sslContext.init(keyManagers, trustManagers, new SecureRandom());
                sslContext.getDefaultSSLParameters().setNeedClientAuth(true);

                return sslContext;
            } catch (final Exception e) {
                throw new IllegalStateException("Created keystore and truststore but failed to initialize SSLContext", e);
            }
        } else {
            return null;
        }
    }

    public String getKeystoreFilename() {
        return keystoreFilename;
    }

    public String getKeystorePass() {
        return keystorePass;
    }

    public String getKeyPass() {
        return keyPass;
    }

    public KeystoreType getKeystoreType() {
        return keystoreType;
    }

    public String getTruststoreFilename() {
        return truststoreFilename;
    }

    public String getTruststorePass() {
        return truststorePass;
    }

    public KeystoreType getTruststoreType() {
        return truststoreType;
    }

    public String getProtocol() {
        return protocol;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Builder for client configuration.
     */
    public static class Builder {

        private String baseUrl;
        private SSLContext sslContext;
        private String keystoreFilename;
        private String keystorePass;
        private String keyPass;
        private KeystoreType keystoreType;
        private String truststoreFilename;
        private String truststorePass;
        private KeystoreType truststoreType;
        private String protocol;
        private HostnameVerifier hostnameVerifier;
        private Integer readTimeout;
        private Integer connectTimeout;

        public NiFiClientConfig.Builder baseUrl(final String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public NiFiClientConfig.Builder sslContext(final SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public NiFiClientConfig.Builder keystoreFilename(final String keystoreFilename) {
            this.keystoreFilename = keystoreFilename;
            return this;
        }

        public NiFiClientConfig.Builder keystorePassword(final String keystorePass) {
            this.keystorePass = keystorePass;
            return this;
        }

        public NiFiClientConfig.Builder keyPassword(final String keyPass) {
            this.keyPass = keyPass;
            return this;
        }

        public NiFiClientConfig.Builder keystoreType(final KeystoreType keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }

        public NiFiClientConfig.Builder truststoreFilename(final String truststoreFilename) {
            this.truststoreFilename = truststoreFilename;
            return this;
        }

        public NiFiClientConfig.Builder truststorePassword(final String truststorePass) {
            this.truststorePass = truststorePass;
            return this;
        }

        public NiFiClientConfig.Builder truststoreType(final KeystoreType truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        public NiFiClientConfig.Builder protocol(final String protocol) {
            this.protocol = protocol;
            return this;
        }

        public NiFiClientConfig.Builder hostnameVerifier(final HostnameVerifier hostnameVerifier) {
            this.hostnameVerifier = hostnameVerifier;
            return this;
        }

        public NiFiClientConfig.Builder readTimeout(final Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public NiFiClientConfig.Builder connectTimeout(final Integer connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public NiFiClientConfig build() {
            return new NiFiClientConfig(this);
        }

    }
}
