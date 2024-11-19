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
package org.apache.nifi.ldap.ssl;

import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.ldap.ProviderProperty;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of LDAP SSLContext Provider supporting common properties
 */
public class StandardLdapSslContextProvider implements LdapSslContextProvider {
    private static final Logger logger = LoggerFactory.getLogger(StandardLdapSslContextProvider.class);

    private static final String DEFAULT_PROTOCOL = "TLS";

    /**
     * Create SSLContext using configured properties defaulting to system trust store when trust store properties not configured
     *
     * @param properties Provider properties
     * @return SSLContext initialized using configured properties
     */
    @Override
    public SSLContext createContext(final Map<String, String> properties) {
        Objects.requireNonNull(properties, "Properties required");

        final String rawProtocol = properties.get(ProviderProperty.TLS_PROTOCOL.getProperty());
        final String protocol;
        if (rawProtocol == null || rawProtocol.isBlank()) {
            protocol = DEFAULT_PROTOCOL;
        } else {
            protocol = rawProtocol;
        }

        try {
            final SSLContext sslContext;
            final StandardSslContextBuilder sslContextBuilder = new StandardSslContextBuilder();
            sslContextBuilder.protocol(protocol);

            final KeyStore trustStore = getTrustStore(properties);
            if (trustStore == null) {
                logger.debug("LDAP TLS Truststore not configured");
            } else {
                sslContextBuilder.trustStore(trustStore);
            }

            final KeyStore keyStore = getKeyStore(properties);
            if (keyStore == null) {
                logger.debug("LDAP TLS Keystore not configured");
            } else {
                final String keyStorePassword = properties.get(ProviderProperty.KEYSTORE_PASSWORD.getProperty());
                final char[] keyPassword = keyStorePassword.toCharArray();

                sslContextBuilder.keyStore(keyStore);
                sslContextBuilder.keyPassword(keyPassword);
            }

            sslContext = sslContextBuilder.build();
            return sslContext;
        } catch (final Exception e) {
            throw new ProviderCreationException("Error configuring TLS for LDAP Provider", e);
        }
    }

    private KeyStore getKeyStore(final Map<String, String> properties) throws IOException {
        final String rawKeystore = properties.get(ProviderProperty.KEYSTORE.getProperty());
        final String rawKeystorePassword = properties.get(ProviderProperty.KEYSTORE_PASSWORD.getProperty());
        final String rawKeystoreType = properties.get(ProviderProperty.KEYSTORE_TYPE.getProperty());

        final KeyStore keyStore;

        if (rawKeystore == null || rawKeystore.isBlank()) {
            keyStore = null;
        } else if (rawKeystorePassword == null) {
            throw new ProviderCreationException("Keystore Password not configured");
        } else {
            final StandardKeyStoreBuilder builder = new StandardKeyStoreBuilder();
            builder.type(rawKeystoreType);

            final char[] keyStorePassword = rawKeystorePassword.toCharArray();
            builder.password(keyStorePassword);

            final Path trustStorePath = Paths.get(rawKeystore);
            try (InputStream trustStoreStream = Files.newInputStream(trustStorePath)) {
                builder.inputStream(trustStoreStream);
                keyStore = builder.build();
            }
        }

        return keyStore;
    }

    private KeyStore getTrustStore(final Map<String, String> properties) throws IOException  {
        final String rawTruststore = properties.get(ProviderProperty.TRUSTSTORE.getProperty());
        final String rawTruststorePassword = properties.get(ProviderProperty.TRUSTSTORE_PASSWORD.getProperty());
        final String rawTruststoreType = properties.get(ProviderProperty.TRUSTSTORE_TYPE.getProperty());

        final KeyStore trustStore;

        if (rawTruststore == null || rawTruststore.isBlank()) {
            trustStore = null;
        } else if (rawTruststorePassword == null) {
            throw new ProviderCreationException("Truststore Password not configured");
        } else {
            final StandardKeyStoreBuilder builder = new StandardKeyStoreBuilder();
            builder.type(rawTruststoreType);

            final char[] trustStorePassword = rawTruststorePassword.toCharArray();
            builder.password(trustStorePassword);

            final Path trustStorePath = Paths.get(rawTruststore);
            try (InputStream trustStoreStream = Files.newInputStream(trustStorePath)) {
                builder.inputStream(trustStoreStream);
                trustStore = builder.build();
            }
        }

        return trustStore;
    }
}
