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
package org.apache.nifi.security.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.nifi.security.ssl.StandardKeyManagerFactoryBuilder;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for creating SSL contexts using the application's security properties. By requiring callers to bundle
 * the properties in a {@link TlsConfiguration} container object, much better validation and property matching can
 * occur. The {@code public} methods are designed for easy use, while the {@code protected} methods provide more
 * granular (but less common) access to intermediate objects if required.
 */
public final class SslContextFactory {
    private static final Logger logger = LoggerFactory.getLogger(SslContextFactory.class);

    /**
     * Create and initialize a {@link SSLContext} from the provided TLS configuration.
     *
     * @param tlsConfiguration the TLS configuration container object
     * @return {@link SSLContext} initialized from TLS Configuration or null when TLS Configuration is empty
     * @throws TlsException if there is a problem configuring the SSLContext
     */
    public static SSLContext createSslContext(final TlsConfiguration tlsConfiguration) throws TlsException {
        if (TlsConfiguration.isEmpty(tlsConfiguration)) {
            logger.debug("Cannot create SSLContext from empty TLS configuration; returning null");
            return null;
        }

        // If the keystore properties are present, truststore properties are required to be present as well
        if (tlsConfiguration.isKeystorePopulated() && !tlsConfiguration.isTruststorePopulated()) {
            logger.error("The TLS config keystore properties were populated but the truststore properties were not");
            if (logger.isDebugEnabled()) {
                logger.debug("Provided TLS config: {}", tlsConfiguration);
            }
            throw new TlsException("Truststore properties are required if keystore properties are present");
        }
        final TrustManager[] trustManagers = getTrustManagers(tlsConfiguration);

        return createSslContext(tlsConfiguration, trustManagers);
    }

    /**
     * Create and initialize a {@link SSLContext} from the provided TLS configuration and Trust Managers.
     *
     * @param tlsConfiguration the TLS configuration container object
     * @param trustManagers    Trust Managers can be null to use platform default Trust Managers
     * @return {@link SSLContext} initialized from TLS Configuration or null when TLS Configuration is empty
     * @throws TlsException if there is a problem configuring the SSLContext
     */
    public static SSLContext createSslContext(final TlsConfiguration tlsConfiguration, final TrustManager[] trustManagers) throws TlsException {
        if (TlsConfiguration.isEmpty(tlsConfiguration)) {
            logger.debug("Cannot create SSLContext from empty TLS configuration; returning null");
            return null;
        }

        final KeyManager[] keyManagers = getKeyManagers(tlsConfiguration);
        return initializeSSLContext(tlsConfiguration, keyManagers, trustManagers);
    }

    /**
     * Returns a configured {@link X509TrustManager} for the provided configuration. Useful for
     * constructing HTTP clients which require their own trust management rather than an
     * {@link SSLContext}. Filters and removes any trust managers that are not
     * {@link javax.net.ssl.X509TrustManager} implementations, and returns the <em>first</em>
     * X.509 trust manager.
     *
     * @param tlsConfiguration the TLS configuration container object
     * @return an X.509 TrustManager (can be {@code null})
     * @throws TlsException if there is a problem reading the truststore to create the trust managers
     */
    public static X509TrustManager getX509TrustManager(TlsConfiguration tlsConfiguration) throws TlsException {
        TrustManager[] trustManagers = getTrustManagers(tlsConfiguration);
        if (trustManagers == null) {
            return null;
        }
        Optional<X509TrustManager> x509TrustManager = Arrays.stream(trustManagers)
                .filter(tm -> tm instanceof X509TrustManager)
                .map(tm -> (X509TrustManager) tm)
                .findFirst();
        return x509TrustManager.orElse(null);
    }

    /**
     * Convenience method to return the {@link SSLSocketFactory} from the created {@link SSLContext}
     *
     * @param tlsConfiguration the TLS configuration container object
     * @return the configured SSLSocketFactory (can be {@code null})
     * @throws TlsException if there is a problem creating the SSLContext or SSLSocketFactory
     */
    public static SSLSocketFactory createSSLSocketFactory(final TlsConfiguration tlsConfiguration) throws TlsException {
        SSLContext sslContext = createSslContext(tlsConfiguration);
        if (sslContext == null) {
            // Only display an error in the log if the provided config wasn't empty
            if (!TlsConfiguration.isEmpty(tlsConfiguration)) {
                logger.error("The SSLContext could not be formed from the provided TLS configuration. Check the provided keystore and truststore properties");
            }
            return null;
        }
        return sslContext.getSocketFactory();
    }

    /**
     * Returns an array of {@link KeyManager}s for the provided configuration. Useful for constructing
     * HTTP clients which require their own key management rather than an {@link SSLContext}. The result can be
     * {@code null} or empty. If an empty configuration is provided, {@code null} is returned. However, if a partially-populated
     * but invalid configuration is provided, a {@link TlsException} is thrown.
     *
     * @param tlsConfiguration the TLS configuration container object with keystore properties
     * @return an array of KeyManagers (can be {@code null})
     * @throws TlsException if there is a problem reading the keystore to create the key managers
     */
    protected static KeyManager[] getKeyManagers(final TlsConfiguration tlsConfiguration) throws TlsException {
        KeyManager[] keyManagers = null;

        final String keystorePath = tlsConfiguration.getKeystorePath();
        final KeystoreType keystoreType = tlsConfiguration.getKeystoreType();

        if (tlsConfiguration.isKeystorePopulated()) {
            try (InputStream inputStream = new FileInputStream(keystorePath)) {
                final KeyStore keyStore = new StandardKeyStoreBuilder()
                        .inputStream(inputStream)
                        .type(keystoreType.getType())
                        .password(tlsConfiguration.getKeystorePassword().toCharArray())
                        .build();

                keyManagers = new StandardKeyManagerFactoryBuilder()
                        .keyStore(keyStore)
                        .keyPassword(tlsConfiguration.getFunctionalKeyPassword().toCharArray())
                        .build()
                        .getKeyManagers();
            } catch (final IOException e) {
                final String message = "Key Store [%s] Type [%s] loading failed".formatted(keystorePath, keystoreType);
                throw new TlsException(message, e);
            }
        } else if (tlsConfiguration.isAnyKeystorePopulated()) {
            throw new TlsException("Key Store properties missing: Set Path [%s] Type [%s]".formatted(keystorePath, keystoreType));
        }

        return keyManagers;
    }

    /**
     * Returns an array of {@link TrustManager} implementations based on the provided truststore configurations. The result can be
     * {@code null} or empty. If an empty configuration is provided, {@code null} is returned. However, if a partially-populated
     * but invalid configuration is provided, a {@link TlsException} is thrown.
     * <p>
     *
     * @param tlsConfiguration the TLS configuration container object with truststore properties
     * @return the loaded trust managers
     * @throws TlsException if there is a problem reading from the truststore
     */
    public static TrustManager[] getTrustManagers(final TlsConfiguration tlsConfiguration) throws TlsException {
        Objects.requireNonNull(tlsConfiguration, "TLS Configuration required");

        final String truststorePath = tlsConfiguration.getTruststorePath();
        final KeystoreType truststoreType = tlsConfiguration.getTruststoreType();

        TrustManager[] trustManagers = null;
        if (tlsConfiguration.isTruststorePopulated()) {
            try (InputStream inputStream = new FileInputStream(truststorePath)) {
                final StandardKeyStoreBuilder keyStoreBuilder = new StandardKeyStoreBuilder()
                        .inputStream(inputStream)
                        .type(truststoreType.getType());

                final String truststorePassword = tlsConfiguration.getTruststorePassword();
                if (truststorePassword != null) {
                    keyStoreBuilder.password(truststorePassword.toCharArray());
                }
                final KeyStore trustStore = keyStoreBuilder.build();

                trustManagers = new StandardTrustManagerFactoryBuilder()
                        .trustStore(trustStore)
                        .build()
                        .getTrustManagers();
            } catch (final IOException e) {
                final String message = "Trust Store [%s] Type [%s] loading failed".formatted(truststorePath, truststoreType);
                throw new TlsException(message, e);
            }
        } else if (tlsConfiguration.isAnyTruststorePopulated()) {
            throw new TlsException("Trust Store properties missing: Set Path [%s] Type [%s]".formatted(truststorePath, truststoreType));
        }

        return trustManagers;
    }

    private static SSLContext initializeSSLContext(final TlsConfiguration tlsConfiguration, final KeyManager[] keyManagers, final TrustManager[] trustManagers) throws TlsException {
        try {
            final SSLContext sslContext = SSLContext.getInstance(tlsConfiguration.getProtocol());
            sslContext.init(keyManagers, trustManagers, new SecureRandom());
            return sslContext;
        } catch (final NoSuchAlgorithmException | KeyManagementException e) {
            logger.error("Encountered an error creating SSLContext from TLS configuration ({}): {}", tlsConfiguration, e.getLocalizedMessage());
            throw new TlsException("Error creating SSL context", e);
        }
    }
}
