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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TlsHelperTest {
    private int days;

    private int keySize;

    private String keyPairAlgorithm;

    private String signingAlgorithm;

    private String keyStoreType;

    private SecureRandom secureRandom;

    private KeyPairGenerator keyPairGenerator;

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
    public void setup() throws NoSuchAlgorithmException {
        days = 360;
        keySize = 2048;
        keyPairAlgorithm = "RSA";
        signingAlgorithm = "SHA1WITHRSA";
        keyStoreType = KeyStore.getDefaultType();
        secureRandom = mock(SecureRandom.class);
        keyPairGenerator = KeyPairGenerator.getInstance(keyPairAlgorithm);
        keyPairGenerator.initialize(keySize);
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

        assertEquals(dn, x509Certificate.getIssuerDN().getName());
        assertEquals(signingAlgorithm, x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.checkValidity();
    }

    @Test
    public void testIssueCert() throws IOException, CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException, InvalidKeyException, SignatureException {
        X509Certificate issuer = loadCertificate(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.crt")));
        KeyPair issuerKeyPair = loadKeyPair(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.key")));

        String dn = "CN=testIssued,O=testOrg";

        KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
        X509Certificate x509Certificate = CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKeyPair, signingAlgorithm, days);
        assertEquals(dn, x509Certificate.getSubjectDN().toString());
        assertEquals(issuer.getSubjectDN().toString(), x509Certificate.getIssuerDN().toString());
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
}
