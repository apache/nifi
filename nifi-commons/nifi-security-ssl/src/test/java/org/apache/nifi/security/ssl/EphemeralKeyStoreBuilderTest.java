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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EphemeralKeyStoreBuilderTest {

    private static final String KEY_ALGORITHM = "RSA";

    private static final char[] KEY_PASSWORD = EphemeralKeyStoreBuilderTest.class.getSimpleName().toCharArray();

    private static KeyPair keyPair;

    @Mock
    private X509Certificate certificate;

    @BeforeAll
    static void setKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPair = keyPairGenerator.generateKeyPair();
    }

    @Test
    void testBuild() {
        final EphemeralKeyStoreBuilder builder = new EphemeralKeyStoreBuilder();

        final KeyStore keyStore = builder.build();

        assertNotNull(keyStore);

        final String expectedKeyStoreType = KeyStore.getDefaultType();
        assertEquals(expectedKeyStoreType, keyStore.getType());
    }

    @Test
    void testAddCertificateBuild() throws KeyStoreException {
        final EphemeralKeyStoreBuilder builder = new EphemeralKeyStoreBuilder();

        final KeyStore keyStore = builder.addCertificate(certificate).build();

        assertNotNull(keyStore);

        final String expectedKeyStoreType = KeyStore.getDefaultType();
        assertEquals(expectedKeyStoreType, keyStore.getType());

        final Enumeration<String> aliases = keyStore.aliases();
        assertTrue(aliases.hasMoreElements());
        final String alias = aliases.nextElement();
        assertNotNull(alias);
        assertFalse(aliases.hasMoreElements());
    }

    @Test
    void testAddPrivateKeyEntryBuild() throws Exception {
        final EphemeralKeyStoreBuilder builder = new EphemeralKeyStoreBuilder();

        final PrivateKey privateKey = keyPair.getPrivate();
        final PublicKey publicKey = keyPair.getPublic();
        when(certificate.getPublicKey()).thenReturn(publicKey);

        final KeyStore.PrivateKeyEntry privateKeyEntry = new KeyStore.PrivateKeyEntry(privateKey, new Certificate[]{certificate});

        final KeyStore keyStore = builder.addPrivateKeyEntry(privateKeyEntry).keyPassword(KEY_PASSWORD).build();

        assertNotNull(keyStore);

        final String expectedKeyStoreType = KeyStore.getDefaultType();
        assertEquals(expectedKeyStoreType, keyStore.getType());

        final Enumeration<String> aliases = keyStore.aliases();
        assertTrue(aliases.hasMoreElements());
        final String alias = aliases.nextElement();
        assertNotNull(alias);

        final Key key = keyStore.getKey(alias, KEY_PASSWORD);
        assertEquals(privateKey, key);

        assertFalse(aliases.hasMoreElements());
    }
}
