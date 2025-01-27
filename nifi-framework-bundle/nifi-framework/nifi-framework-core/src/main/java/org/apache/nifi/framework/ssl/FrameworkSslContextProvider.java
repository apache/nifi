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
package org.apache.nifi.framework.ssl;

import org.apache.nifi.security.ssl.KeyManagerBuilder;
import org.apache.nifi.security.ssl.PemCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.PemPrivateKeyCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.TrustManagerBuilder;
import org.apache.nifi.util.NiFiProperties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_CERTIFICATE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_PRIVATE_KEY;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_TYPE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEY_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_CERTIFICATE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_TYPE;

/**
 * Framework SSL Context Provider handles loading of Security Key Store and Trust Store along with component registration
 */
public class FrameworkSslContextProvider {
    private static final String EMPTY = "";

    private static final String PEM_STORE_TYPE = "PEM";

    private final NiFiProperties properties;

    public FrameworkSslContextProvider(final NiFiProperties properties) {
        this.properties = Objects.requireNonNull(properties, "Application Properties required");
    }

    /**
     * Load SSL Context from Application Properties and register components with framework
     *
     * @return Loaded SSL Context or null when not configured
     */
    public Optional<SSLContext> loadSslContext() {
        final KeyManagerBuilder keyManagerBuilder = getKeyManagerBuilder();
        final TrustManagerBuilder trustManagerBuilder = getTrustManagerBuilder();

        final Optional<SSLContext> sslContextHolder;
        if (keyManagerBuilder == null || trustManagerBuilder == null) {
            sslContextHolder = Optional.empty();
        } else {
            final X509ExtendedKeyManager keyManager = keyManagerBuilder.build();
            final X509ExtendedTrustManager trustManager = trustManagerBuilder.build();

            final SSLContext sslContext = new StandardSslContextBuilder().keyManager(keyManager).trustManager(trustManager).build();
            sslContextHolder = Optional.of(sslContext);

            FrameworkSslContextHolder.setSslContext(sslContext);
            FrameworkSslContextHolder.setKeyManager(keyManager);
            FrameworkSslContextHolder.setKeyManagerBuilder(keyManagerBuilder);
            FrameworkSslContextHolder.setTrustManager(trustManager);
            FrameworkSslContextHolder.setTrustManagerBuilder(trustManagerBuilder);
        }

        return sslContextHolder;
    }

    private KeyManagerBuilder getKeyManagerBuilder() {
        final KeyManagerBuilder keyManagerBuilder;

        if (isPropertyConfigured(SECURITY_KEYSTORE) && isPropertyConfigured(SECURITY_KEYSTORE_PASSWD)) {
            final Path keyStorePath = getPropertyPath(SECURITY_KEYSTORE);
            final String keyStorePassword = properties.getProperty(SECURITY_KEYSTORE_PASSWD, EMPTY);
            final char[] keyPassword = properties.getProperty(SECURITY_KEY_PASSWD, keyStorePassword).toCharArray();
            final String keyStoreType = properties.getProperty(SECURITY_KEYSTORE_TYPE);
            final StandardKeyStoreBuilder keyStoreBuilder = new StandardKeyStoreBuilder()
                    .password(keyStorePassword.toCharArray())
                    .type(keyStoreType);

            keyManagerBuilder = new FrameworkKeyManagerBuilder(keyStorePath, keyStoreBuilder, keyPassword);
        } else if (isPemStoreType(SECURITY_KEYSTORE_TYPE)) {
            final Path privateKeyPath = getPropertyPath(SECURITY_KEYSTORE_PRIVATE_KEY);
            final Path certificatePath = getPropertyPath(SECURITY_KEYSTORE_CERTIFICATE);
            final PemPrivateKeyCertificateKeyStoreBuilder pemKeyStoreBuilder = new PemPrivateKeyCertificateKeyStoreBuilder();

            keyManagerBuilder = new FrameworkKeyManagerBuilder(privateKeyPath, certificatePath, pemKeyStoreBuilder);
        } else {
            keyManagerBuilder = null;
        }

        return keyManagerBuilder;
    }

    private TrustManagerBuilder getTrustManagerBuilder() {
        final TrustManagerBuilder trustManagerBuilder;

        if (isPropertyConfigured(SECURITY_TRUSTSTORE) && isPropertyConfigured(SECURITY_TRUSTSTORE_PASSWD)) {
            final Path trustStorePath = getPropertyPath(SECURITY_TRUSTSTORE);
            final String trustStorePassword = properties.getProperty(SECURITY_TRUSTSTORE_PASSWD, EMPTY);
            final String trustStoreType = properties.getProperty(SECURITY_TRUSTSTORE_TYPE);
            final StandardKeyStoreBuilder trustStoreBuilder = new StandardKeyStoreBuilder()
                    .password(trustStorePassword.toCharArray())
                    .type(trustStoreType);

            trustManagerBuilder = new FrameworkTrustManagerBuilder(trustStorePath, trustStoreBuilder);
        } else if (isPemStoreType(SECURITY_TRUSTSTORE_TYPE)) {
            final Path trustStoreCertificatePath = getPropertyPath(SECURITY_TRUSTSTORE_CERTIFICATE);
            final PemCertificateKeyStoreBuilder trustStoreBuilder = new PemCertificateKeyStoreBuilder();
            trustManagerBuilder = new FrameworkTrustManagerBuilder(trustStoreCertificatePath, trustStoreBuilder);
        } else {
            trustManagerBuilder = null;
        }

        return trustManagerBuilder;
    }

    private Path getPropertyPath(final String propertyName) {
        final String propertyPath = properties.getProperty(propertyName);
        if (propertyPath == null || propertyPath.isBlank()) {
            throw new IllegalStateException("Security Property [%s] not configured".formatted(propertyName));
        }

        return Paths.get(propertyPath);
    }

    private boolean isPropertyConfigured(final String propertyName) {
        final String value = properties.getProperty(propertyName);
        return value != null && !value.isBlank();
    }

    private boolean isPemStoreType(final String storeTypePropertyName) {
        final String storeType = properties.getProperty(storeTypePropertyName, EMPTY);
        return PEM_STORE_TYPE.contentEquals(storeType);
    }
}
