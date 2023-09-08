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

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.util.file.FileUtils;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.util.IPAddress;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.x500.X500Principal;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TlsHelperTest {
    private static final String PASSWORD = "changeit";
    private static X509Certificate rootCert;
    private int days;
    private int keySize;
    private String keyPairAlgorithm;

    public static KeyPair loadKeyPair(final Reader reader) throws IOException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(PEMKeyPair.class, object.getClass());
            return new JcaPEMKeyConverter().getKeyPair((PEMKeyPair) object);
        }
    }

    public static KeyPair loadKeyPair(File file) throws IOException {
        try (final FileReader fileReader = new FileReader(file)) {
            return loadKeyPair(fileReader);
        }
    }

    public static X509Certificate loadCertificate(final Reader reader) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(X509CertificateHolder.class, object.getClass());
            return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate((X509CertificateHolder) object);
        }
    }

    public static X509Certificate loadCertificate(final File file) throws IOException, CertificateException {
        try (FileReader fileReader = new FileReader(file)) {
            return loadCertificate(fileReader);
        }
    }

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        rootCert = TlsHelper.parseCertificate(new FileReader("src/test/resources/rootCert.crt"));
    }

    @BeforeEach
    public void setup() throws Exception {
        days = 360;
        keySize = 2048;
        keyPairAlgorithm = "RSA";
    }

    private Date inFuture(int days) {
        return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days));
    }

    @Test
    public void testTokenLengthInCalculateHmac() throws GeneralSecurityException {
        List<String> badTokens = Arrays.asList(null, "", "123");
        List<String> goodTokens = Arrays.asList("0123456789abcdefghijklm", "0123456789abcdef");

        String dn = "CN=testDN,O=testOrg";
        X509Certificate x509Certificate = new StandardCertificateBuilder(TlsHelper.generateKeyPair(keyPairAlgorithm, keySize), new X500Principal(dn), Duration.ofDays(days)).build();
        PublicKey pubKey = x509Certificate.getPublicKey();

        for (String token : badTokens) {
            try {
                TlsHelper.calculateHMac(token, pubKey);
                fail("HMAC was calculated with a token that was too short.");
            } catch (GeneralSecurityException e) {
                assertEquals("Token does not meet minimum size of 16 bytes.", e.getMessage());
            } catch (IllegalArgumentException e) {
                assertEquals("Token cannot be null", e.getMessage());
            }
        }

        for (String token : goodTokens) {
            byte[] hmac = TlsHelper.calculateHMac(token, pubKey);
            assertTrue(hmac.length > 0, "HMAC length OK");
        }
    }

    @Test
    public void testGenerateSelfSignedCert() throws GeneralSecurityException {
        String dn = "CN=testDN,O=testOrg";

        X509Certificate x509Certificate = new StandardCertificateBuilder(TlsHelper.generateKeyPair(keyPairAlgorithm, keySize), new X500Principal(dn), Duration.ofDays(days)).build();

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(dn, x509Certificate.getIssuerX500Principal().getName());
        assertEquals("SHA256withRSA", x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.checkValidity();
    }

    @Test
    public void testWriteKeyStoreSuccess(@Mock OutputStreamFactory outputStreamFactory, @TempDir File file) throws IOException, GeneralSecurityException {
        when(outputStreamFactory.create(file)).thenReturn(new ByteArrayOutputStream());
        String testPassword = "testPassword";
        final KeyStore keyStore = setupKeystore();
        assertEquals(testPassword, TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, false));
    }

    @Test
    public void testShouldIncludeSANFromCSR() throws Exception {
        // Arrange
        final List<String> sanEntries = Arrays.asList("127.0.0.1", "nifi.nifi.apache.org");
        final int sanCount = sanEntries.size();
        final String dn = "CN=localhost";
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(keyPairAlgorithm);
        keyPairGenerator.initialize(keySize);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        // Act
        JcaPKCS10CertificationRequest csrWithSan = TlsHelper.generateCertificationRequest(dn, sanEntries, keyPair, TlsConfig.DEFAULT_SIGNING_ALGORITHM);

        // Assert
        String subjectName = csrWithSan.getSubject().toString();
        assertEquals(dn, subjectName);

        List<String> extractedSans = extractSanFromCsr(csrWithSan);
        assertEquals(sanCount + 1, extractedSans.size());
        List<String> formattedSans = sanEntries.stream()
                .map(s -> (IPAddress.isValid(s) ? "IP Address: " + new GeneralName(GeneralName.iPAddress, s).getName() : "DNS: " + s))
                .collect(Collectors.toList());
        assertTrue(extractedSans.containsAll(formattedSans));

        // We check that the SANs also contain the CN
        assertTrue(extractedSans.contains("DNS: localhost"));
    }

    private List<String> extractSanFromCsr(JcaPKCS10CertificationRequest csr) {
        List<String> sans = new ArrayList<>();
        Attribute[] certAttributes = csr.getAttributes();
        for (Attribute attribute : certAttributes) {
            if (attribute.getAttrType().equals(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest)) {
                Extensions extensions = Extensions.getInstance(attribute.getAttrValues().getObjectAt(0));
                GeneralNames gns = GeneralNames.fromExtensions(extensions, Extension.subjectAlternativeName);
                GeneralName[] names = gns.getNames();
                for (GeneralName name : names) {
                    String title = "";
                    if (name.getTagNo() == GeneralName.dNSName) {
                        title = "DNS";
                    } else if (name.getTagNo() == GeneralName.iPAddress) {
                        title = "IP Address";
                        // name.toASN1Primitive();
                    } else if (name.getTagNo() == GeneralName.otherName) {
                        title = "Other Name";
                    }
                    sans.add(title + ": " + name.getName());
                }
            }
        }

        return sans;
    }

    @Test
    public void testEscapeAliasFilenameWithForwardSlashes() {
        String result = TlsHelper.escapeFilename("my/silly/filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithBackSlashes() {
        String result = TlsHelper.escapeFilename("my\\silly\\filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithDollarSign() {
        String result = TlsHelper.escapeFilename("my$illyfilename.pem");
        assertEquals("my_illyfilename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameTwoSymbolsInARow() {
        String result = TlsHelper.escapeFilename("my!?sillyfilename.pem");
        assertEquals("my_sillyfilename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameKeepHyphens() {
        String result = TlsHelper.escapeFilename("my-silly-filename.pem");
        assertEquals("my-silly-filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameDoubleSpaces() {
        String result = TlsHelper.escapeFilename("my  silly  filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameSymbols() {
        String result = TlsHelper.escapeFilename("./\\!@#$%^&*()_-+=.pem");
        assertEquals(".__-_=.pem", result);
    }

    @Test
    public void testClientDnFilenameSlashes() {
        String clientDn = "CN=testuser,OU=NiFi/Organisation";
        String escapedClientDn = TlsHelper.escapeFilename(clientDn);

        assertEquals("CN=testuser_OU=NiFi_Organisation", escapedClientDn);
    }

    @Test
    public void testClientDnFilenameSpecialChars() {
        String clientDn = "CN=testuser,OU=NiFi#!Organisation";
        String escapedClientDn = TlsHelper.escapeFilename(clientDn);

        assertEquals("CN=testuser_OU=NiFi_Organisation", escapedClientDn);
    }

    private KeyStore setupKeystore() throws CertificateException, NoSuchAlgorithmException, IOException, KeyStoreException {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (InputStream readStream = getClass().getClassLoader().getResourceAsStream("keystore.jks")) {
            ks.load(readStream, PASSWORD.toCharArray());
        }
        return ks;
    }

    @Test
    public void testOutputToFileTwoCertsAsPem(@TempDir final File folder) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        KeyStore keyStore = setupKeystore();
        HashMap<String, Certificate> certs = TlsHelper.extractCerts(keyStore);
        TlsHelper.outputCertsAsPem(certs, folder,".crt");

        assertEquals(2, folder.listFiles().length);

        for (File file : folder.listFiles()) {
            X509Certificate certFromFile = loadCertificate(file);
            assertTrue(certs.containsValue(certFromFile));
            X509Certificate originalCert = (X509Certificate) certs.get(file.getName().split("\\.")[0]);
            assertEquals(originalCert, certFromFile);
            assertArrayEquals(originalCert.getSignature(), certFromFile.getSignature());
            FileUtils.deleteFile(file, false);
        }
    }

    // Keystore contains two certificates, but one key. This is to test the edge case where a certificate does not have a key.
    @Test
    public void testOutputToFileOneKeyAsPem(@TempDir final File folder) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
        KeyStore keyStore = setupKeystore();
        HashMap<String, Key> keys = TlsHelper.extractKeys(keyStore, PASSWORD.toCharArray());
        TlsHelper.outputKeysAsPem(keys, folder, ".key");

        for (File file : folder.listFiles()) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                PEMParser pemParser = new PEMParser(br);
                PEMKeyPair key = (PEMKeyPair) pemParser.readObject();
                assertArrayEquals(keys.get(file.getName().split("\\.")[0]).getEncoded(), key.getPrivateKeyInfo().getEncoded());
            } finally {
                FileUtils.deleteFile(file, false);
            }
        }
    }

    @Test
    public void testExtractCerts() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        KeyStore keyStore = setupKeystore();
        HashMap<String, Certificate> certs = TlsHelper.extractCerts(keyStore);
        assertEquals(2, certs.size());
        certs.forEach((String p, Certificate q) -> assertEquals("X.509", q.getType()));
    }

    @Test
    public void testExtractKeys() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
        KeyStore keyStore = setupKeystore();
        HashMap<String, Key> keys = TlsHelper.extractKeys(keyStore, PASSWORD.toCharArray());
        assertEquals(1, keys.size());
        keys.forEach((String alias, Key key) -> assertEquals("PKCS#8", key.getFormat()));
    }

    @Test
    public void testShouldVerifyCertificateSignatureWhenSelfSigned() {
        assertTrue(TlsHelper.verifyCertificateSignature(rootCert, Collections.singletonList(rootCert)));
    }

    @Test
    public void testShouldVerifyCertificateSignatureWithMultipleSigningCerts(@Mock X509Certificate mockCertificate) {
        when(mockCertificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=Mock Certificate"));
        assertTrue(TlsHelper.verifyCertificateSignature(rootCert, Arrays.asList(mockCertificate, rootCert)));
    }

    @Test
    public void testShouldNotVerifyCertificateSignatureWithNoSigningCerts() {
        assertFalse(TlsHelper.verifyCertificateSignature(rootCert, new ArrayList<>()));
    }

    @Test
    public void testShouldNotVerifyCertificateSignatureWithWrongSigningCert(@Mock X509Certificate mockCertificate) {
        when(mockCertificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=Mock Certificate"));
        assertFalse(TlsHelper.verifyCertificateSignature(rootCert, Collections.singletonList(mockCertificate)));
    }

    @Test
    public void testParseKeyPairFromReaderShouldHandlePKCS8PrivateKey() throws Exception {
        final KeyPair expectedKeyPair = TlsHelper.parseKeyPairFromReader(new FileReader("src/test/resources/rootCert.key"));
        final PrivateKey expectedPrivateKey = expectedKeyPair.getPrivate();
        final KeyPair keyPair = TlsHelper.parseKeyPairFromReader(new FileReader("src/test/resources/rootCert-pkcs8.key"));

        assertEquals(expectedPrivateKey, keyPair.getPrivate());
    }
}
