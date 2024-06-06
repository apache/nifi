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
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_TYPE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEY_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_TYPE;

/**
 * Framework SSL Context Provider handles loading of Security Key Store and Trust Store along with component registration
 */
public class FrameworkSslContextProvider {
    private static final String EMPTY = "";

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

        if (properties.isHTTPSConfigured()) {
            final Path keyStorePath = getKeyStorePath();
            final String keyStorePassword = properties.getProperty(SECURITY_KEYSTORE_PASSWD, EMPTY);
            final char[] keyPassword = properties.getProperty(SECURITY_KEY_PASSWD, keyStorePassword).toCharArray();
            final String keyStoreType = properties.getProperty(SECURITY_KEYSTORE_TYPE);
            final StandardKeyStoreBuilder keyStoreBuilder = new StandardKeyStoreBuilder()
                    .password(keyStorePassword.toCharArray())
                    .type(keyStoreType);

            keyManagerBuilder = new FrameworkKeyManagerBuilder(keyStorePath, keyStoreBuilder, keyPassword);
        } else {
            keyManagerBuilder = null;
        }

        return keyManagerBuilder;
    }

    private TrustManagerBuilder getTrustManagerBuilder() {
        final TrustManagerBuilder trustManagerBuilder;

        if (properties.isHTTPSConfigured()) {
            final Path trustStorePath = getTrustStorePath();
            final String trustStorePassword = properties.getProperty(SECURITY_TRUSTSTORE_PASSWD, EMPTY);
            final String trustStoreType = properties.getProperty(SECURITY_TRUSTSTORE_TYPE);
            final StandardKeyStoreBuilder trustStoreBuilder = new StandardKeyStoreBuilder()
                    .password(trustStorePassword.toCharArray())
                    .type(trustStoreType);

            trustManagerBuilder = new FrameworkTrustManagerBuilder(trustStorePath, trustStoreBuilder);
        } else {
            trustManagerBuilder = null;
        }

        return trustManagerBuilder;
    }

    private Path getKeyStorePath() {
        final String keyStoreProperty = properties.getProperty(SECURITY_KEYSTORE);
        if (keyStoreProperty == null || keyStoreProperty.isBlank()) {
            throw new IllegalStateException("Security Property [%s] not configured".formatted(SECURITY_KEYSTORE));
        }
        return Paths.get(keyStoreProperty);
    }

    private Path getTrustStorePath() {
        final String trustStoreProperty = properties.getProperty(SECURITY_TRUSTSTORE);
        if (trustStoreProperty == null || trustStoreProperty.isBlank()) {
            throw new IllegalStateException("Security Property [%s] not configured".formatted(SECURITY_TRUSTSTORE));
        }

        return Paths.get(trustStoreProperty);
    }
}
