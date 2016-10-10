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
package org.apache.nifi.web.security.x509.ocsp
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.sun.jersey.api.client.Client
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.*
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.junit.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.*
import java.security.cert.CertificateException
import java.security.cert.X509Certificate

import static groovy.test.GroovyAssert.shouldFail
import static org.junit.Assert.fail

public class OcspCertificateValidatorGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(OcspCertificateValidatorGroovyTest.class);

    private static final int KEY_SIZE = 2048;

    private static final long YESTERDAY = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
    private static final long ONE_YEAR_FROM_NOW = System.currentTimeMillis() + 365 * 24 * 60 * 60 * 1000;
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String PROVIDER = "BC";

    private static final String SUBJECT_DN = "CN=NiFi Test Server,OU=Security,O=Apache,ST=CA,C=US";
    private static final String ISSUER_DN = "CN=NiFi Test CA,OU=Security,O=Apache,ST=CA,C=US";

    private NiFiProperties mockProperties

    // System under test
    OcspCertificateValidator certificateValidator

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setUp() throws Exception {
        mockProperties = new NiFiProperties() {
            @Override
            String getProperty(String key) {
                return 'value_for_' + key
            }

            @Override
            Set<String> getPropertyKeys() {
                return ["A", "set"]
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        certificateValidator?.metaClass = null
    }

    /**
     * Generates a public/private RSA keypair using the default key size.
     *
     * @return the keypair
     * @throws NoSuchAlgorithmException if the RSA algorithm is not available
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
     * @throws CertificateException
     * @throws NoSuchProviderException
     * @throws SignatureException
     * @throws InvalidKeyException
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

    @Test
    public void testShouldGenerateCertificate() throws Exception {
        // Arrange
        final String testDn = "CN=This is a test";

        // Act
        X509Certificate certificate = generateCertificate(testDn);
        logger.info("Generated certificate: \n{}", certificate);

        // Assert
        assert certificate.getSubjectDN().getName().equals(testDn);
        assert certificate.getIssuerDN().getName().equals(testDn);
        certificate.verify(certificate.getPublicKey());
    }

    @Test
    public void testShouldGenerateCertificateFromKeyPair() throws Exception {
        // Arrange
        final String testDn = "CN=This is a test";
        final KeyPair keyPair = generateKeyPair();

        // Act
        X509Certificate certificate = generateCertificate(testDn, keyPair);
        logger.info("Generated certificate: \n{}", certificate);

        // Assert
        assert certificate.getPublicKey().equals(keyPair.getPublic());
        assert certificate.getSubjectDN().getName().equals(testDn);
        assert certificate.getIssuerDN().getName().equals(testDn);
        certificate.verify(certificate.getPublicKey());
    }

    @Test
    public void testShouldGenerateIssuedCertificate() throws Exception {
        // Arrange
        final String testDn = "CN=This is a signed test";
        final String issuerDn = "CN=Issuer CA";
        final KeyPair issuerKeyPair = generateKeyPair();
        final PrivateKey issuerPrivateKey = issuerKeyPair.getPrivate();

        final X509Certificate issuerCertificate = generateCertificate(issuerDn, issuerKeyPair);
        logger.info("Generated issuer certificate: \n{}", issuerCertificate);

        // Act
        X509Certificate certificate = generateIssuedCertificate(testDn, issuerDn, issuerPrivateKey);
        logger.info("Generated signed certificate: \n{}", certificate);

        // Assert
        assert issuerCertificate.getPublicKey().equals(issuerKeyPair.getPublic());
        assert certificate.getSubjectX500Principal().getName().equals(testDn);
        assert certificate.getIssuerX500Principal().getName().equals(issuerDn);
        certificate.verify(issuerCertificate.getPublicKey());

        try {
            certificate.verify(certificate.getPublicKey());
            fail("Should have thrown exception");
        } catch (Exception e) {
            assert e instanceof SignatureException;
            assert e.getMessage().contains("certificate does not verify with supplied key");
        }
    }

    @Test
    public void testShouldValidateCertificate() throws Exception {
        // Arrange
        X509Certificate[] certificateChain = generateCertificateChain();

        certificateValidator = new OcspCertificateValidator(mockProperties)

        // Must populate the client even though it is not used in this check
        certificateValidator.client = new Client()

        // Form a map of the request to a good status and load it into the cache
        OcspRequest revokedRequest = new OcspRequest(certificateChain.first(), certificateChain.last())
        OcspStatus revokedStatus = new OcspStatus()
        revokedStatus.responseStatus = OcspStatus.ResponseStatus.Successful
        revokedStatus.validationStatus = OcspStatus.ValidationStatus.Good
        revokedStatus.verificationStatus = OcspStatus.VerificationStatus.Verified
        LoadingCache<OcspRequest, OcspStatus> cacheWithRevokedCertificate = buildCacheWithContents([(revokedRequest): revokedStatus])
        certificateValidator.ocspCache = cacheWithRevokedCertificate

        // Act
        certificateValidator.validate(certificateChain)

        // Assert
        assert true
    }

    // TODO - NIFI-1364
    @Ignore("To be implemented with Groovy test")
    @Test
    public void testShouldNotValidateEmptyCertificate() throws Exception {

    }

    @Test
    public void testShouldNotValidateRevokedCertificate() throws Exception {
        // Arrange
        X509Certificate[] certificateChain = generateCertificateChain();

        certificateValidator = new OcspCertificateValidator(mockProperties)

        // Must populate the client even though it is not used in this check
        certificateValidator.client = new Client()

        // Form a map of the request to a revoked status and load it into the cache
        OcspRequest revokedRequest = new OcspRequest(certificateChain.first(), certificateChain.last())
        OcspStatus revokedStatus = new OcspStatus()
        revokedStatus.responseStatus = OcspStatus.ResponseStatus.Successful
        revokedStatus.validationStatus = OcspStatus.ValidationStatus.Revoked
        revokedStatus.verificationStatus = OcspStatus.VerificationStatus.Verified
        LoadingCache<OcspRequest, OcspStatus> cacheWithRevokedCertificate = buildCacheWithContents([(revokedRequest): revokedStatus])
        certificateValidator.ocspCache = cacheWithRevokedCertificate

        // Act
        def msg = shouldFail(CertificateStatusException) {
            certificateValidator.validate(certificateChain)
        }

        // Assert
        assert msg =~ "is revoked according to the certificate authority"
    }

    LoadingCache<OcspRequest, OcspStatus> buildCacheWithContents(Map map) {
        CacheBuilder.newBuilder().build(new CacheLoader<OcspRequest, OcspStatus>() {
            @Override
            public OcspStatus load(OcspRequest ocspRequest) throws Exception {
                logger.info("Mock cache implementation load(${ocspRequest}) returns ${map.get(ocspRequest)}")
                return map.get(ocspRequest) as OcspStatus
            }
        });
    }

    // TODO - NIFI-1364
    @Ignore("To be implemented with Groovy test")
    @Test
    public void testValidateShouldHandleUnsignedResponse() throws Exception {

    }

    // TODO - NIFI-1364
    @Ignore("To be implemented with Groovy test")
    @Test
    public void testValidateShouldHandleResponseWithIncorrectNonce() throws Exception {

    }
}