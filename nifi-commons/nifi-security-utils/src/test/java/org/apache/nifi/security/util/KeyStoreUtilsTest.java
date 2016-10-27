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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyStoreUtilsTest {
    public static final String SIGNING_ALGORITHM = "SHA256withRSA";
    public static final int DURATION_DAYS = 365;
    public static final char[] BAD_TEST_PASSWORD_DONT_USE_THIS = "changek".toCharArray();
    public static final char[] BAD_KEY_STORE_TEST_PASSWORD_DONT_USE_THIS = "changes".toCharArray();
    public static final String ALIAS = "alias";

    private static KeyPair caCertKeyPair;
    private static X509Certificate caCertificate;

    private static KeyPair issuedCertificateKeyPair;
    private static X509Certificate issuedCertificate;

    @BeforeClass
    public static void generateKeysAndCertificates() throws NoSuchAlgorithmException, CertificateException {
        caCertKeyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        issuedCertificateKeyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();

        caCertificate = CertificateUtils.generateSelfSignedX509Certificate(caCertKeyPair, "CN=testca,O=Apache,OU=NiFi", SIGNING_ALGORITHM, DURATION_DAYS);
        issuedCertificate = CertificateUtils.generateIssuedCertificate("CN=testcert,O=Apache,OU=NiFi", issuedCertificateKeyPair.getPublic(), caCertificate, caCertKeyPair, SIGNING_ALGORITHM,
                DURATION_DAYS);
    }

    @Test
    public void testJksKeyStoreRoundTrip() throws GeneralSecurityException, IOException {
        testKeyStoreRoundTrip(() -> KeyStoreUtils.getKeyStore(KeystoreType.JKS.toString().toLowerCase()));
    }

    @Test
    public void testPkcs12KeyStoreBcRoundTrip() throws GeneralSecurityException, IOException {
        testKeyStoreRoundTrip(() -> KeyStoreUtils.getKeyStore(KeystoreType.PKCS12.toString().toLowerCase()));
    }

    @Test
    public void testPkcs12KeyStoreRoundTripBcReload() throws GeneralSecurityException, IOException {
        // Pkcs12 Bouncy Castle needs same key and keystore password to interoperate with Java provider
        testKeyStoreRoundTrip(() -> KeyStore.getInstance(KeystoreType.PKCS12.toString().toLowerCase()),
                () -> KeyStoreUtils.getKeyStore(KeystoreType.PKCS12.toString().toLowerCase()), BAD_KEY_STORE_TEST_PASSWORD_DONT_USE_THIS);
    }

    @Test
    public void testJksTrustStoreRoundTrip() throws GeneralSecurityException, IOException {
        testTrustStoreRoundTrip(() -> KeyStoreUtils.getTrustStore(KeystoreType.JKS.toString().toLowerCase()));
    }

    @Test
    public void testPkcs12TrustStoreBcRoundTrip() throws GeneralSecurityException, IOException {
        testTrustStoreRoundTrip(() -> KeyStoreUtils.getTrustStore(KeystoreType.PKCS12.toString().toLowerCase()));
    }

    @Test
    public void testPkcs12TrustStoreRoundTripBcReload() throws GeneralSecurityException, IOException {
        testTrustStoreRoundTrip(() -> KeyStore.getInstance(KeystoreType.PKCS12.toString().toLowerCase()), () -> KeyStoreUtils.getTrustStore(KeystoreType.PKCS12.toString().toLowerCase()));
    }

    private void testTrustStoreRoundTrip(KeyStoreSupplier keyStoreSupplier) throws GeneralSecurityException, IOException {
        testTrustStoreRoundTrip(keyStoreSupplier, keyStoreSupplier);
    }

    private void testTrustStoreRoundTrip(KeyStoreSupplier initialKeyStoreSupplier, KeyStoreSupplier reloadKeyStoreSupplier) throws GeneralSecurityException, IOException {
        KeyStore trustStore = initialKeyStoreSupplier.get();
        trustStore.load(null, null);
        trustStore.setCertificateEntry(ALIAS, caCertificate);

        KeyStore roundTrip = roundTrip(trustStore, reloadKeyStoreSupplier);
        assertEquals(caCertificate, roundTrip.getCertificate(ALIAS));
    }

    private void testKeyStoreRoundTrip(KeyStoreSupplier keyStoreSupplier) throws GeneralSecurityException, IOException {
        testKeyStoreRoundTrip(keyStoreSupplier, keyStoreSupplier, BAD_TEST_PASSWORD_DONT_USE_THIS);
    }

    private void testKeyStoreRoundTrip(KeyStoreSupplier initialKeyStoreSupplier, KeyStoreSupplier reloadKeyStoreSupplier, char[] keyPassword) throws GeneralSecurityException, IOException {
        KeyStore keyStore = initialKeyStoreSupplier.get();
        keyStore.load(null, null);
        keyStore.setKeyEntry(ALIAS, issuedCertificateKeyPair.getPrivate(), keyPassword, new Certificate[]{issuedCertificate, caCertificate});

        KeyStore roundTrip = roundTrip(keyStore, reloadKeyStoreSupplier);
        KeyStore.Entry entry = roundTrip.getEntry(ALIAS, new KeyStore.PasswordProtection(keyPassword));
        assertTrue(entry instanceof KeyStore.PrivateKeyEntry);
        KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;

        Certificate[] certificateChain = privateKeyEntry.getCertificateChain();
        assertArrayEquals(new Certificate[]{issuedCertificate, caCertificate}, certificateChain);
        assertEquals(issuedCertificateKeyPair.getPrivate(), privateKeyEntry.getPrivateKey());
        assertEquals(issuedCertificateKeyPair.getPublic(), certificateChain[0].getPublicKey());
    }

    private KeyStore roundTrip(KeyStore keyStore, KeyStoreSupplier keyStoreSupplier) throws GeneralSecurityException, IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        keyStore.store(byteArrayOutputStream, BAD_KEY_STORE_TEST_PASSWORD_DONT_USE_THIS);

        KeyStore result = keyStoreSupplier.get();
        result.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), BAD_KEY_STORE_TEST_PASSWORD_DONT_USE_THIS);
        return result;
    }

    private interface KeyStoreSupplier {
        KeyStore get() throws GeneralSecurityException;
    }
}
