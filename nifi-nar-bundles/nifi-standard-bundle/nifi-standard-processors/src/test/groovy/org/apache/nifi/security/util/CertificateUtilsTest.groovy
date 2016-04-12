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
package org.apache.nifi.security.util

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.asn1.x509.X509Extension
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.NoSuchAlgorithmException
import java.security.NoSuchProviderException
import java.security.PrivateKey
import java.security.PublicKey
import java.security.Security
import java.security.SignatureException
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class CertificateUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CertificateUtilsTest.class);

    private static final int KEY_SIZE = 2048;

    private static final long YESTERDAY = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
    private static final long ONE_YEAR_FROM_NOW = System.currentTimeMillis() + 365 * 24 * 60 * 60 * 1000;
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String PROVIDER = "BC";

    private static final String SUBJECT_DN = "CN=NiFi Test Server,OU=Security,O=Apache,ST=CA,C=US";
    private static final String ISSUER_DN = "CN=NiFi Test CA,OU=Security,O=Apache,ST=CA,C=US";

    @BeforeClass
    static void setUpOnce() {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        super.setUp()

    }

    @After
    void tearDown() {

    }

    /**
     * Generates a public/private RSA keypair using the default key size.
     *
     * @return the keypair
     * @throws java.security.NoSuchAlgorithmException if the RSA algorithm is not available
     */
    private static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(KEY_SIZE);
        return keyPairGenerator.generateKeyPair();
    }

    /**
     * Generates a signed certificate using an on-demand keypair.
     *
     * @param dn the DN
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws java.security.cert.CertificateException
     * @throws java.security.NoSuchProviderException
     * @throws java.security.SignatureException
     * @throws java.security.InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateCertificate(String dn) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair();
        return generateCertificate(dn, keyPair);
    }

    /**
     * Generates a signed certificate with a specific keypair.
     *
     * @param dn the DN
     * @param keyPair the public key will be included in the certificate and the the private key is used to sign the certificate
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateCertificate(String dn, KeyPair keyPair) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        PrivateKey privateKey = keyPair.getPrivate();
        ContentSigner sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(PROVIDER).build(privateKey);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
        Date startDate = new Date(YESTERDAY);
        Date endDate = new Date(ONE_YEAR_FROM_NOW);

        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                new X500Name(dn),
                BigInteger.valueOf(System.currentTimeMillis()),
                startDate, endDate,
                new X500Name(dn),
                subPubKeyInfo);

        // Set certificate extensions
        // (1) digitalSignature extension
        certBuilder.addExtension(X509Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment | KeyUsage.dataEncipherment | KeyUsage.keyAgreement));

        // (2) extendedKeyUsage extension
        Vector<KeyPurposeId> ekUsages = new Vector<>();
        ekUsages.add(KeyPurposeId.id_kp_clientAuth);
        ekUsages.add(KeyPurposeId.id_kp_serverAuth);
        certBuilder.addExtension(X509Extension.extendedKeyUsage, false, new ExtendedKeyUsage(ekUsages));

        // Sign the certificate
        X509CertificateHolder certificateHolder = certBuilder.build(sigGen);
        return new JcaX509CertificateConverter().setProvider(PROVIDER)
                .getCertificate(certificateHolder);
    }

    /**
     * Generates a certificate signed by the issuer key.
     *
     * @param dn the subject DN
     * @param issuerDn the issuer DN
     * @param issuerKey the issuer private key
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateIssuedCertificate(String dn, String issuerDn, PrivateKey issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair();
        return generateIssuedCertificate(dn, keyPair.getPublic(), issuerDn, issuerKey);
    }

    /**
     * Generates a certificate with a specific public key signed by the issuer key.
     *
     * @param dn the subject DN
     * @param publicKey the subject public key
     * @param issuerDn the issuer DN
     * @param issuerKey the issuer private key
     * @return the certificate
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws OperatorCreationException
     */
    private
    static X509Certificate generateIssuedCertificate(String dn, PublicKey publicKey, String issuerDn, PrivateKey issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        ContentSigner sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(PROVIDER).build(issuerKey);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        Date startDate = new Date(YESTERDAY);
        Date endDate = new Date(ONE_YEAR_FROM_NOW);

        X509v3CertificateBuilder v3CertGen = new X509v3CertificateBuilder(
                new X500Name(issuerDn),
                BigInteger.valueOf(System.currentTimeMillis()),
                startDate, endDate,
                new X500Name(dn),
                subPubKeyInfo);

        X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
        return new JcaX509CertificateConverter().setProvider(PROVIDER)
                .getCertificate(certificateHolder);
    }

    private static X509Certificate[] generateCertificateChain(String dn = SUBJECT_DN, String issuerDn = ISSUER_DN) {
        final KeyPair issuerKeyPair = generateKeyPair();
        final PrivateKey issuerPrivateKey = issuerKeyPair.getPrivate();

        final X509Certificate issuerCertificate = generateCertificate(issuerDn, issuerKeyPair);
        final X509Certificate certificate = generateIssuedCertificate(dn, issuerDn, issuerPrivateKey);
        [certificate, issuerCertificate] as X509Certificate[]
    }

    private static javax.security.cert.X509Certificate generateLegacyCertificate(X509Certificate x509Certificate) {
        return javax.security.cert.X509Certificate.getInstance(x509Certificate.getEncoded())
    }

    private static Certificate generateAbstractCertificate(X509Certificate x509Certificate) {
        return x509Certificate as Certificate
    }

    @Test
    void testShouldConvertLegacyX509Certificate() {
        // Arrange
        final X509Certificate EXPECTED_NEW_CERTIFICATE = generateCertificate(SUBJECT_DN)
        logger.info("Expected certificate: ${EXPECTED_NEW_CERTIFICATE.class.canonicalName} ${EXPECTED_NEW_CERTIFICATE.subjectDN.toString()} (${EXPECTED_NEW_CERTIFICATE.getSerialNumber()})")

        // Form the legacy certificate
        final javax.security.cert.X509Certificate LEGACY_CERTIFICATE = generateLegacyCertificate(EXPECTED_NEW_CERTIFICATE)
        logger.info("Legacy certificate: ${LEGACY_CERTIFICATE.class.canonicalName} ${LEGACY_CERTIFICATE.subjectDN.toString()} (${LEGACY_CERTIFICATE.getSerialNumber()})")

        // Act
        X509Certificate convertedCertificate = CertificateUtils.convertLegacyX509Certificate(LEGACY_CERTIFICATE)
        logger.info("Converted certificate: ${convertedCertificate.class.canonicalName} ${convertedCertificate.subjectDN.toString()} (${convertedCertificate.getSerialNumber()})")

        // Assert
        assert convertedCertificate instanceof X509Certificate
        assert convertedCertificate == EXPECTED_NEW_CERTIFICATE
    }

    @Test
    void testShouldConvertAbstractX509Certificate() {
        // Arrange
        final X509Certificate EXPECTED_NEW_CERTIFICATE = generateCertificate(SUBJECT_DN)
        logger.info("Expected certificate: ${EXPECTED_NEW_CERTIFICATE.class.canonicalName} ${EXPECTED_NEW_CERTIFICATE.subjectDN.toString()} (${EXPECTED_NEW_CERTIFICATE.getSerialNumber()})")

        // Form the abstract certificate
        final Certificate ABSTRACT_CERTIFICATE = generateAbstractCertificate(EXPECTED_NEW_CERTIFICATE)
        logger.info("Abstract certificate: ${ABSTRACT_CERTIFICATE.class.canonicalName} (?)")

        // Act
        X509Certificate convertedCertificate = CertificateUtils.convertAbstractX509Certificate(ABSTRACT_CERTIFICATE)
        logger.info("Converted certificate: ${convertedCertificate.class.canonicalName} ${convertedCertificate.subjectDN.toString()} (${convertedCertificate.getSerialNumber()})")

        // Assert
        assert convertedCertificate instanceof X509Certificate
        assert convertedCertificate == EXPECTED_NEW_CERTIFICATE
    }
}
