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
package org.apache.nifi.web.client.provider.service;

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.ssl.SSLContextService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.X509KeyManager;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardKeyManagerProviderTest {
    @TempDir
    static Path keyStoreDirectory;

    static Path keyStorePath;

    static String keyStoreType;

    static String keyStorePass;

    private static final String KEY_STORE_EXTENSION = ".p12";

    @Mock
    SSLContextService sslContextService;

    StandardKeyManagerProvider provider;

    @BeforeAll
    static void setKeyStore() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};

        keyStorePath = Files.createTempFile(keyStoreDirectory, StandardKeyManagerProviderTest.class.getSimpleName(), KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, protectionParameter);
        }

        keyStoreType = keyStore.getType();
        keyStorePass = new String(protectionParameter);
    }

    @BeforeEach
    void setProvider() {
        provider = new StandardKeyManagerProvider();
    }

    @Test
    void testGetKeyManagerNotConfigured() {
        when(sslContextService.isKeyStoreConfigured()).thenReturn(false);

        final Optional<X509KeyManager> keyManager = provider.getKeyManager(sslContextService);

        assertFalse(keyManager.isPresent());
    }

    @Test
    void testGetKeyManager() {
        when(sslContextService.isKeyStoreConfigured()).thenReturn(true);
        when(sslContextService.getKeyStoreType()).thenReturn(keyStoreType);
        when(sslContextService.getKeyStoreFile()).thenReturn(keyStorePath.toString());
        when(sslContextService.getKeyStorePassword()).thenReturn(keyStorePass);

        final Optional<X509KeyManager> keyManager = provider.getKeyManager(sslContextService);

        assertTrue(keyManager.isPresent());
    }
}
