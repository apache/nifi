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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyStoreUtilsTest {
    private static final String SIGNING_ALGORITHM = "SHA256withRSA";
    private static final int DURATION_DAYS = 365;
    private static final char[] KEY_PASSWORD = UUID.randomUUID().toString().toCharArray();
    private static final char[] STORE_PASSWORD = UUID.randomUUID().toString().toCharArray();
    private static final String ALIAS = "alias";
    private static final String KEY_ALGORITHM = "RSA";
    private static final String SUBJECT_DN = "CN=localhost";
    private static final String SECRET_KEY_ALGORITHM = "AES";
    private static final String KEY_PROTECTION_ALGORITHM = "PBEWithHmacSHA256AndAES_256";
    private static final String HYPHEN_SEPARATOR = "-";

    private static KeyPair keyPair;
    private static X509Certificate certificate;
    private static SecretKey secretKey;

    @BeforeClass
    public static void generateKeysAndCertificates() throws NoSuchAlgorithmException, CertificateException {
        keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, DURATION_DAYS);
        final byte[] encodedKey = StringUtils.remove(UUID.randomUUID().toString(), HYPHEN_SEPARATOR).getBytes(StandardCharsets.UTF_8);
        secretKey = new SecretKeySpec(encodedKey, SECRET_KEY_ALGORITHM);
    }

    @Test
    public void testCreateTlsConfigAndNewKeystoreTruststore() throws GeneralSecurityException, IOException {
        final TlsConfiguration configuration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();
        final File keystoreFile = new File(configuration.getKeystorePath());
        assertTrue("Keystore File not found", keystoreFile.exists());
        keystoreFile.deleteOnExit();

        final File truststoreFile = new File(configuration.getTruststorePath());
        assertTrue("Truststore File not found", truststoreFile.exists());
        truststoreFile.deleteOnExit();

        assertEquals("Keystore Type not matched", KeystoreType.PKCS12, configuration.getKeystoreType());
        assertEquals("Truststore Type not matched", KeystoreType.PKCS12, configuration.getTruststoreType());

        assertTrue("Keystore not valid", KeyStoreUtils.isStoreValid(keystoreFile.toURI().toURL(), configuration.getKeystoreType(), configuration.getKeystorePassword().toCharArray()));
        assertTrue("Truststore not valid", KeyStoreUtils.isStoreValid(truststoreFile.toURI().toURL(), configuration.getTruststoreType(), configuration.getTruststorePassword().toCharArray()));
    }

    @Test
    public void testKeystoreTypesPrivateKeyEntry() throws GeneralSecurityException, IOException {
        for (final KeystoreType keystoreType : KeystoreType.values()) {
            final KeyStore sourceKeyStore = KeyStoreUtils.getKeyStore(keystoreType.getType());
            final KeyStore destinationKeyStore = KeyStoreUtils.getKeyStore(keystoreType.getType());
            assertKeyEntryStoredLoaded(sourceKeyStore, destinationKeyStore);
        }
    }

    @Test
    public void testKeystoreTypesCertificateEntry() throws GeneralSecurityException, IOException {
        for (final KeystoreType keystoreType : KeystoreType.values()) {
            final KeyStore sourceKeyStore = KeyStoreUtils.getKeyStore(keystoreType.getType());
            final KeyStore destinationKeyStore = KeyStoreUtils.getKeyStore(keystoreType.getType());
            assertCertificateEntryStoredLoaded(sourceKeyStore, destinationKeyStore);
        }
    }

    @Test
    public void testKeystoreTypesSecretKeyEntry() throws GeneralSecurityException, IOException {
        for (final KeystoreType keystoreType : KeystoreType.values()) {
            if (KeyStoreUtils.isSecretKeyEntrySupported(keystoreType)) {
                final KeyStore sourceKeyStore = KeyStoreUtils.getSecretKeyStore(keystoreType.getType());
                final KeyStore destinationKeyStore = KeyStoreUtils.getSecretKeyStore(keystoreType.getType());
                try {
                    assertSecretKeyStoredLoaded(sourceKeyStore, destinationKeyStore);
                } catch (final GeneralSecurityException e) {
                    throw new GeneralSecurityException(String.format("Keystore Type [%s] Failed", keystoreType), e);
                }
            }
        }
    }

    private void assertCertificateEntryStoredLoaded(final KeyStore sourceKeyStore, final KeyStore destinationKeyStore) throws GeneralSecurityException, IOException {
        sourceKeyStore.load(null, null);
        sourceKeyStore.setCertificateEntry(ALIAS, certificate);

        final KeyStore copiedKeyStore = copyKeyStore(sourceKeyStore, destinationKeyStore);
        assertEquals(String.format("[%s] Certificate not matched", sourceKeyStore.getType()), certificate, copiedKeyStore.getCertificate(ALIAS));
    }

    private void assertKeyEntryStoredLoaded(final KeyStore sourceKeyStore, final KeyStore destinationKeyStore) throws GeneralSecurityException, IOException {
        sourceKeyStore.load(null, null);
        final Certificate[] certificateChain = new Certificate[]{certificate};
        sourceKeyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), KEY_PASSWORD, certificateChain);

        final KeyStore copiedKeyStore = copyKeyStore(sourceKeyStore, destinationKeyStore);
        final KeyStore.Entry entry = copiedKeyStore.getEntry(ALIAS, new KeyStore.PasswordProtection(KEY_PASSWORD));
        assertTrue(String.format("[%s] Private Key entry not found", sourceKeyStore.getType()), entry instanceof KeyStore.PrivateKeyEntry);
        final KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;

        final Certificate[] entryCertificateChain = privateKeyEntry.getCertificateChain();
        assertArrayEquals(String.format("[%s] Certificate Chain not matched", sourceKeyStore.getType()), certificateChain, entryCertificateChain);
        assertEquals(String.format("[%s] Private Key not matched", sourceKeyStore.getType()), keyPair.getPrivate(), privateKeyEntry.getPrivateKey());
        assertEquals(String.format("[%s] Public Key not matched", sourceKeyStore.getType()), keyPair.getPublic(), entryCertificateChain[0].getPublicKey());
    }

    private void assertSecretKeyStoredLoaded(final KeyStore sourceKeyStore, final KeyStore destinationKeyStore) throws GeneralSecurityException, IOException {
        sourceKeyStore.load(null, null);
        final KeyStore.ProtectionParameter protection = getProtectionParameter(sourceKeyStore.getType());
        sourceKeyStore.setEntry(ALIAS, new KeyStore.SecretKeyEntry(secretKey), protection);

        final KeyStore copiedKeyStore = copyKeyStore(sourceKeyStore, destinationKeyStore);
        final KeyStore.Entry entry = copiedKeyStore.getEntry(ALIAS, protection);
        assertTrue(String.format("[%s] Secret Key entry not found", sourceKeyStore.getType()), entry instanceof KeyStore.SecretKeyEntry);
    }

    private KeyStore copyKeyStore(final KeyStore sourceKeyStore, final KeyStore destinationKeyStore) throws GeneralSecurityException, IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        sourceKeyStore.store(byteArrayOutputStream, STORE_PASSWORD);

        destinationKeyStore.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), STORE_PASSWORD);
        return destinationKeyStore;
    }

    private KeyStore.ProtectionParameter getProtectionParameter(final String keyStoreType) {
        if (KeystoreType.PKCS12.getType().equals(keyStoreType)) {
            // Select Key Protection Algorithm for PKCS12 to avoid unsupported algorithm on Java 1.8.0.292
            return new KeyStore.PasswordProtection(KEY_PASSWORD, KEY_PROTECTION_ALGORITHM, null);
        } else {
            return new KeyStore.PasswordProtection(KEY_PASSWORD);
        }
    }
}
