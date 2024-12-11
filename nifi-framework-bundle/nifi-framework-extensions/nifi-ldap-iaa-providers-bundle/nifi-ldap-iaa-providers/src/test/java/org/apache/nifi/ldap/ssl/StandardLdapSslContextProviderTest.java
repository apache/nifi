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

import org.apache.nifi.ldap.ProviderProperty;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardLdapSslContextProviderTest {

    private static final String TLS_PROTOCOL = "TLS";

    private static final String ALIAS = "entry-0";

    private static final String KEY_STORE_EXTENSION = ".p12";

    private static final String KEY_STORE_PASS = UUID.randomUUID().toString();

    private static final String TRUST_STORE_PASS = UUID.randomUUID().toString();

    @TempDir
    private static Path keyStoreDirectory;

    private static String keyStoreType;

    private static Path keyStorePath;

    private static String trustStoreType;

    private static Path trustStorePath;

    private StandardLdapSslContextProvider provider;

    @BeforeAll
    public static void setConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder().build();
        keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), KEY_STORE_PASS.toCharArray(), new Certificate[]{certificate});

        keyStorePath = Files.createTempFile(keyStoreDirectory, "keyStore", KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEY_STORE_PASS.toCharArray());
        }

        keyStoreType = keyStore.getType().toUpperCase();

        final KeyStore trustStore = new EphemeralKeyStoreBuilder().addCertificate(certificate).build();
        trustStorePath = Files.createTempFile(keyStoreDirectory, "trustStore", KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(trustStorePath)) {
            trustStore.store(outputStream, TRUST_STORE_PASS.toCharArray());
        }

        trustStoreType = trustStore.getType().toUpperCase();
    }

    @BeforeEach
    void setProvider() {
        provider = new StandardLdapSslContextProvider();
    }

    @Test
    void testCreateContextEmptyProperties() {
        final SSLContext sslContext = provider.createContext(Map.of());

        assertNotNull(sslContext);
    }

    @Test
    void testCreateContextProtocol() {
        final Map<String, String> properties = Map.of(
                ProviderProperty.TLS_PROTOCOL.getProperty(), TLS_PROTOCOL
        );

        final SSLContext sslContext = provider.createContext(properties);

        assertNotNull(sslContext);
    }

    @Test
    void testCreateContextTrustStore() {
        final Map<String, String> properties = Map.of(
                ProviderProperty.TLS_PROTOCOL.getProperty(), TLS_PROTOCOL,
                ProviderProperty.TRUSTSTORE.getProperty(), trustStorePath.toString(),
                ProviderProperty.TRUSTSTORE_TYPE.getProperty(), trustStoreType,
                ProviderProperty.TRUSTSTORE_PASSWORD.getProperty(), TRUST_STORE_PASS
        );

        final SSLContext sslContext = provider.createContext(properties);

        assertNotNull(sslContext);
    }

    @Test
    void testCreateContextKeyStore() {
        final Map<String, String> properties = Map.of(
                ProviderProperty.TLS_PROTOCOL.getProperty(), TLS_PROTOCOL,
                ProviderProperty.KEYSTORE.getProperty(), keyStorePath.toString(),
                ProviderProperty.KEYSTORE_TYPE.getProperty(), keyStoreType,
                ProviderProperty.KEYSTORE_PASSWORD.getProperty(), KEY_STORE_PASS
        );

        final SSLContext sslContext = provider.createContext(properties);

        assertNotNull(sslContext);
    }

    @Test
    void testCreateContext() {
        final Map<String, String> properties = Map.of(
                ProviderProperty.TLS_PROTOCOL.getProperty(), TLS_PROTOCOL,
                ProviderProperty.TRUSTSTORE.getProperty(), trustStorePath.toString(),
                ProviderProperty.TRUSTSTORE_TYPE.getProperty(), trustStoreType,
                ProviderProperty.TRUSTSTORE_PASSWORD.getProperty(), TRUST_STORE_PASS,
                ProviderProperty.KEYSTORE.getProperty(), keyStorePath.toString(),
                ProviderProperty.KEYSTORE_TYPE.getProperty(), keyStoreType,
                ProviderProperty.KEYSTORE_PASSWORD.getProperty(), KEY_STORE_PASS
        );

        final SSLContext sslContext = provider.createContext(properties);

        assertNotNull(sslContext);
    }
}
