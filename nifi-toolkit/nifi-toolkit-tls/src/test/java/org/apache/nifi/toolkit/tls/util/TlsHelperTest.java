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

package org.apache.nifi.toolkit.tls.util;

import org.apache.nifi.security.util.CertificateUtils;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreSpi;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TlsHelperTest {
    private static final boolean originalUnlimitedCrypto = TlsHelper.isUnlimitedStrengthCryptographyEnabled();

    private int days;

    private int keySize;

    private String keyPairAlgorithm;

    private String signingAlgorithm;

    private KeyPairGenerator keyPairGenerator;

    private KeyStore keyStore;

    @Mock
    KeyStoreSpi keyStoreSpi;

    @Mock
    Provider keyStoreProvider;

    @Mock
    OutputStreamFactory outputStreamFactory;

    private ByteArrayOutputStream tmpFileOutputStream;

    private File file;

    private static void setUnlimitedCrypto(boolean value) {
        try {
            Field isUnlimitedStrengthCryptographyEnabled = TlsHelper.class.getDeclaredField("isUnlimitedStrengthCryptographyEnabled");
            isUnlimitedStrengthCryptographyEnabled.setAccessible(true);
            isUnlimitedStrengthCryptographyEnabled.set(null, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static KeyPair loadKeyPair(Reader reader) throws IOException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(PEMKeyPair.class, object.getClass());
            return new JcaPEMKeyConverter().getKeyPair((PEMKeyPair) object);
        }
    }

    public static KeyPair loadKeyPair(File file) throws IOException {
        return loadKeyPair(new FileReader(file));
    }

    public static X509Certificate loadCertificate(Reader reader) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(X509CertificateHolder.class, object.getClass());
            return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate((X509CertificateHolder) object);
        }
    }

    public static X509Certificate loadCertificate(File file) throws IOException, CertificateException {
        return loadCertificate(new FileReader(file));
    }

    @BeforeClass
    public static void beforeClass() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setup() throws Exception {
        days = 360;
        keySize = 2048;
        keyPairAlgorithm = "RSA";
        signingAlgorithm = "SHA1WITHRSA";
        keyPairGenerator = KeyPairGenerator.getInstance(keyPairAlgorithm);
        keyPairGenerator.initialize(keySize);
        Constructor<KeyStore> keyStoreConstructor = KeyStore.class.getDeclaredConstructor(KeyStoreSpi.class, Provider.class, String.class);
        keyStoreConstructor.setAccessible(true);
        keyStore = keyStoreConstructor.newInstance(keyStoreSpi, keyStoreProvider, "faketype");
        keyStore.load(null, null);
        file = File.createTempFile("keystore", "file");
        when(outputStreamFactory.create(file)).thenReturn(tmpFileOutputStream);
    }

    @After
    public void tearDown() {
        setUnlimitedCrypto(originalUnlimitedCrypto);
        file.delete();
    }

    private Date inFuture(int days) {
        return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days));
    }

    @Test
    public void testGenerateSelfSignedCert() throws GeneralSecurityException, IOException, OperatorCreationException {
        String dn = "CN=testDN,O=testOrg";

        X509Certificate x509Certificate = CertificateUtils.generateSelfSignedX509Certificate(TlsHelper.generateKeyPair(keyPairAlgorithm, keySize), dn, signingAlgorithm, days);

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(dn, x509Certificate.getIssuerX500Principal().getName());
        assertEquals(signingAlgorithm, x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.checkValidity();
    }

    @Test
    public void testIssueCert() throws IOException, CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException, InvalidKeyException, SignatureException {
        X509Certificate issuer = loadCertificate(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.crt")));
        KeyPair issuerKeyPair = loadKeyPair(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.key")));

        String dn = "CN=testIssued, O=testOrg";

        KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
        X509Certificate x509Certificate = CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKeyPair, signingAlgorithm, days);
        assertEquals(dn, x509Certificate.getSubjectX500Principal().toString());
        assertEquals(issuer.getSubjectX500Principal().toString(), x509Certificate.getIssuerX500Principal().toString());
        assertEquals(keyPair.getPublic(), x509Certificate.getPublicKey());

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(signingAlgorithm, x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.verify(issuerKeyPair.getPublic());
    }

    @Test
    public void testWriteKeyStoreSuccess() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        assertEquals(testPassword, TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, false));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
    }

    @Test
    public void testWriteKeyStoreFailure() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        IOException ioException = new IOException("Fail");
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException);
        } catch (IOException e) {
            assertEquals(ioException, e);
        }
    }

    @Test
    public void testWriteKeyStoreTruncate() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        String truncatedPassword = testPassword.substring(0, 7);
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        assertEquals(truncatedPassword, TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(truncatedPassword.toCharArray()));
    }

    @Test
    public void testWriteKeyStoreUnlimitedWontTruncate() throws GeneralSecurityException, IOException {
        setUnlimitedCrypto(true);
        String testPassword = "testPassword";
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException);
        } catch (IOException e) {
            assertEquals(ioException, e);
        }
    }

    @Test
    public void testWriteKeyStoreNoTruncate() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, false);
            fail("Expected " + GeneralSecurityException.class);
        } catch (GeneralSecurityException e) {
            assertTrue("Expected exception to contain " + TlsHelper.JCE_URL, e.getMessage().contains(TlsHelper.JCE_URL));
        }
    }

    @Test
    public void testWriteKeyStoreTruncateFailure() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        String truncatedPassword = testPassword.substring(0, 7);
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        IOException ioException2 = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        doThrow(ioException2).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(truncatedPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException2);
        } catch (IOException e) {
            assertEquals(ioException2, e);
        }
    }
}
