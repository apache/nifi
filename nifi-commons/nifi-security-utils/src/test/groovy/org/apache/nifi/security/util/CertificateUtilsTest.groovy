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

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.OperatorCreationException
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.NoSuchAlgorithmException
import java.security.NoSuchProviderException
import java.security.Security
import java.security.SignatureException
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertEquals

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
        return CertificateUtils.generateSelfSignedX509Certificate(keyPair, dn, SIGNATURE_ALGORITHM, 365);
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
    static X509Certificate generateIssuedCertificate(String dn, X509Certificate issuer, KeyPair issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair();
        return CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKey, SIGNATURE_ALGORITHM, 365);
    }

    private static X509Certificate[] generateCertificateChain(String dn = SUBJECT_DN, String issuerDn = ISSUER_DN) {
        final KeyPair issuerKeyPair = generateKeyPair();

        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, issuerDn, SIGNATURE_ALGORITHM, 365);
        final X509Certificate certificate = generateIssuedCertificate(dn, issuerCertificate, issuerKeyPair);
        [certificate, issuerCertificate] as X509Certificate[]
    }

    private static javax.security.cert.X509Certificate generateLegacyCertificate(X509Certificate x509Certificate) {
        return javax.security.cert.X509Certificate.getInstance(x509Certificate.getEncoded())
    }

    private static Certificate generateAbstractCertificate(X509Certificate x509Certificate) {
        return x509Certificate as Certificate
    }

    private static Date inFuture(int days) {
        return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days));
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

    @Test
    void testShouldDetermineClientAuthStatusFromSocket() {
        // Arrange
        SSLSocket needSocket = [getNeedClientAuth: { -> true }] as SSLSocket
        SSLSocket wantSocket = [getNeedClientAuth: { -> false }, getWantClientAuth: { -> true }] as SSLSocket
        SSLSocket noneSocket = [getNeedClientAuth: { -> false }, getWantClientAuth: { -> false }] as SSLSocket

        // Act
        CertificateUtils.ClientAuth needClientAuthStatus = CertificateUtils.getClientAuthStatus(needSocket)
        logger.info("Client auth (needSocket): ${needClientAuthStatus}")
        CertificateUtils.ClientAuth wantClientAuthStatus = CertificateUtils.getClientAuthStatus(wantSocket)
        logger.info("Client auth (wantSocket): ${wantClientAuthStatus}")
        CertificateUtils.ClientAuth noneClientAuthStatus = CertificateUtils.getClientAuthStatus(noneSocket)
        logger.info("Client auth (noneSocket): ${noneClientAuthStatus}")

        // Assert
        assert needClientAuthStatus == CertificateUtils.ClientAuth.NEED
        assert wantClientAuthStatus == CertificateUtils.ClientAuth.WANT
        assert noneClientAuthStatus == CertificateUtils.ClientAuth.NONE
    }


    @Test
    void testShouldExtractClientCertificatesFromSSLServerSocketWithAnyClientAuth() {
        final String EXPECTED_DN = "CN=ncm.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN)
        logger.info("Expected DN: ${EXPECTED_DN}")
        logger.info("Expected certificate chain: ${certificateChain.collect { (it as X509Certificate).getSubjectDN().name }.join(" issued by ")}")

        SSLSession mockSession = [getPeerCertificates: { -> certificateChain }] as SSLSession

        // This socket is in client mode, so the peer ("target") is a server

        // Create mock sockets for each possible value of ClientAuth
        SSLSocket mockNoneSocket = [
                getUseClientMode : { -> true },
                getNeedClientAuth: { -> false },
                getWantClientAuth: { -> false },
                getSession       : { -> mockSession }
        ] as SSLSocket

        SSLSocket mockNeedSocket = [
                getUseClientMode : { -> true },
                getNeedClientAuth: { -> true },
                getWantClientAuth: { -> false },
                getSession       : { -> mockSession }
        ] as SSLSocket

        SSLSocket mockWantSocket = [
                getUseClientMode : { -> true },
                getNeedClientAuth: { -> false },
                getWantClientAuth: { -> true },
                getSession       : { -> mockSession }
        ] as SSLSocket

        // Act
        def resolvedServerDNs = [mockNeedSocket, mockWantSocket, mockNoneSocket].collect { SSLSocket mockSocket ->
            logger.info("Running test with socket ClientAuth setting: ${CertificateUtils.getClientAuthStatus(mockSocket)}")
            String serverDN = CertificateUtils.extractPeerDNFromSSLSocket(mockNoneSocket)
            logger.info("Extracted server DN: ${serverDN}")
            serverDN
        }

        // Assert
        assert resolvedServerDNs.every { String serverDN ->
            CertificateUtils.compareDNs(serverDN, EXPECTED_DN)
        }
    }

    @Test
    void testShouldNotExtractClientCertificatesFromSSLClientSocketWithClientAuthNone() {
        // Arrange

        // This socket is in server mode, so the peer ("target") is a client
        SSLSocket mockSocket = [
                getUseClientMode : { -> false },
                getNeedClientAuth: { -> false },
                getWantClientAuth: { -> false }
        ] as SSLSocket

        // Act
        String clientDN = CertificateUtils.extractPeerDNFromSSLSocket(mockSocket)
        logger.info("Extracted client DN: ${clientDN}")

        // Assert
        assert !clientDN
    }

    @Test
    void testShouldExtractClientCertificatesFromSSLClientSocketWithClientAuthWant() {
        // Arrange
        final String EXPECTED_DN = "CN=client.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN)
        logger.info("Expected DN: ${EXPECTED_DN}")
        logger.info("Expected certificate chain: ${certificateChain.collect { (it as X509Certificate).getSubjectDN().name }.join(" issued by ")}")

        SSLSession mockSession = [getPeerCertificates: { -> certificateChain }] as SSLSession

        // This socket is in server mode, so the peer ("target") is a client
        SSLSocket mockSocket = [
                getUseClientMode : { -> false },
                getNeedClientAuth: { -> false },
                getWantClientAuth: { -> true },
                getSession       : { -> mockSession }
        ] as SSLSocket

        // Act
        String clientDN = CertificateUtils.extractPeerDNFromSSLSocket(mockSocket)
        logger.info("Extracted client DN: ${clientDN}")

        // Assert
        assert CertificateUtils.compareDNs(clientDN, EXPECTED_DN)
    }

    @Test
    void testShouldHandleFailureToExtractClientCertificatesFromSSLClientSocketWithClientAuthWant() {
        // Arrange
        SSLSession mockSession = [getPeerCertificates: { ->
            throw new SSLPeerUnverifiedException("peer not authenticated")
        }] as SSLSession

        // This socket is in server mode, so the peer ("target") is a client
        SSLSocket mockSocket = [
                getUseClientMode : { -> false },
                getNeedClientAuth: { -> false },
                getWantClientAuth: { -> true },
                getSession       : { -> mockSession }
        ] as SSLSocket

        // Act
        String clientDN = CertificateUtils.extractPeerDNFromSSLSocket(mockSocket)
        logger.info("Extracted client DN: ${clientDN}")

        // Assert
        assert CertificateUtils.compareDNs(clientDN, null)
    }


    @Test
    void testShouldExtractClientCertificatesFromSSLClientSocketWithClientAuthNeed() {
        // Arrange
        final String EXPECTED_DN = "CN=client.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN)
        logger.info("Expected DN: ${EXPECTED_DN}")
        logger.info("Expected certificate chain: ${certificateChain.collect { (it as X509Certificate).getSubjectDN().name }.join(" issued by ")}")

        SSLSession mockSession = [getPeerCertificates: { -> certificateChain }] as SSLSession

        // This socket is in server mode, so the peer ("target") is a client
        SSLSocket mockSocket = [
                getUseClientMode : { -> false },
                getNeedClientAuth: { -> true },
                getWantClientAuth: { -> false },
                getSession       : { -> mockSession }
        ] as SSLSocket

        // Act
        String clientDN = CertificateUtils.extractPeerDNFromSSLSocket(mockSocket)
        logger.info("Extracted client DN: ${clientDN}")

        // Assert
        assert CertificateUtils.compareDNs(clientDN, EXPECTED_DN)
    }

    @Test
    void testShouldHandleFailureToExtractClientCertificatesFromSSLClientSocketWithClientAuthNeed() {
        // Arrange
        SSLSession mockSession = [getPeerCertificates: { ->
            throw new SSLPeerUnverifiedException("peer not authenticated")
        }] as SSLSession

        // This socket is in server mode, so the peer ("target") is a client
        SSLSocket mockSocket = [
                getUseClientMode : { -> false },
                getNeedClientAuth: { -> true },
                getWantClientAuth: { -> false },
                getSession       : { -> mockSession }
        ] as SSLSocket

        // Act
        def msg = shouldFail(CertificateException) {
            String clientDN = CertificateUtils.extractPeerDNFromSSLSocket(mockSocket)
            logger.info("Extracted client DN: ${clientDN}")
        }

        // Assert
        assert msg =~ "peer not authenticated"
    }

    @Test
    void testShouldCompareDNs() {
        // Arrange
        final String DN_1_ORDERED = "CN=test1.nifi.apache.org, OU=Apache NiFi, O=Apache, ST=California, C=US"
        logger.info("DN 1 Ordered : ${DN_1_ORDERED}")
        final String DN_1_REVERSED = DN_1_ORDERED.split(", ").reverse().join(", ")
        logger.info("DN 1 Reversed: ${DN_1_REVERSED}")

        final String DN_2_ORDERED = "CN=test2.nifi.apache.org, OU=Apache NiFi, O=Apache, ST=California, C=US"
        logger.info("DN 2 Ordered : ${DN_2_ORDERED}")
        final String DN_2_REVERSED = DN_2_ORDERED.split(", ").reverse().join(", ")
        logger.info("DN 2 Reversed: ${DN_2_REVERSED}")

        // Act

        // True
        boolean dn1MatchesSelf = CertificateUtils.compareDNs(DN_1_ORDERED, DN_1_ORDERED)
        logger.matches("DN 1, DN 1: ${dn1MatchesSelf}")

        boolean dn1MatchesReversed = CertificateUtils.compareDNs(DN_1_ORDERED, DN_1_REVERSED)
        logger.matches("DN 1, DN 1 (R): ${dn1MatchesReversed}")

        boolean emptyMatchesEmpty = CertificateUtils.compareDNs("", "")
        logger.matches("empty, empty: ${emptyMatchesEmpty}")

        boolean nullMatchesNull = CertificateUtils.compareDNs(null, null)
        logger.matches("null, null: ${nullMatchesNull}")

        // False
        boolean dn1MatchesDn2 = CertificateUtils.compareDNs(DN_1_ORDERED, DN_2_ORDERED)
        logger.matches("DN 1, DN 2: ${dn1MatchesDn2}")

        boolean dn1MatchesDn2Reversed = CertificateUtils.compareDNs(DN_1_ORDERED, DN_2_REVERSED)
        logger.matches("DN 1, DN 2 (R): ${dn1MatchesDn2Reversed}")

        boolean dn1MatchesEmpty = CertificateUtils.compareDNs(DN_1_ORDERED, "")
        logger.matches("DN 1, empty: ${dn1MatchesEmpty}")

        // Assert
        assert dn1MatchesSelf
        assert dn1MatchesReversed
        assert emptyMatchesEmpty
        assert nullMatchesNull

        assert !dn1MatchesDn2
        assert !dn1MatchesDn2Reversed
        assert !dn1MatchesEmpty
    }



    @Test
    public void testShouldGenerateSelfSignedCert() throws Exception {
        String dn = "CN=testDN,O=testOrg";

        int days = 365;
        X509Certificate x509Certificate = CertificateUtils.generateSelfSignedX509Certificate(generateKeyPair(), dn, SIGNATURE_ALGORITHM, days);

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(dn, x509Certificate.getIssuerX500Principal().getName());
        assertEquals(SIGNATURE_ALGORITHM.toUpperCase(), x509Certificate.getSigAlgName().toUpperCase());
        assertEquals("RSA", x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.checkValidity();
    }



    @Test
    public void testIssueCert() throws Exception {
        int days = 365;
        KeyPair issuerKeyPair = generateKeyPair();
        X509Certificate issuer = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, "CN=testCa,O=testOrg", SIGNATURE_ALGORITHM, days);

        String dn = "CN=testIssued, O=testOrg";

        KeyPair keyPair = generateKeyPair();
        X509Certificate x509Certificate = CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKeyPair, SIGNATURE_ALGORITHM, days);
        assertEquals(dn, x509Certificate.getSubjectX500Principal().toString());
        assertEquals(issuer.getSubjectX500Principal().toString(), x509Certificate.getIssuerX500Principal().toString());
        assertEquals(keyPair.getPublic(), x509Certificate.getPublicKey());

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(SIGNATURE_ALGORITHM.toUpperCase(), x509Certificate.getSigAlgName().toUpperCase());
        assertEquals("RSA", x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.verify(issuerKeyPair.getPublic());
    }

    @Test
    public void reorderShouldPutElementsInCorrectOrder() {
        String cn = "CN=testcn";
        String l = "L=testl";
        String st = "ST=testst";
        String o = "O=testo";
        String ou = "OU=testou";
        String c = "C=testc";
        String street = "STREET=teststreet";
        String dc = "DC=testdc";
        String uid = "UID=testuid";
        String surname = "SURNAME=testsurname";
        String initials = "INITIALS=testinitials";
        String givenName = "GIVENNAME=testgivenname";
        assertEquals("$cn,$l,$st,$o,$ou,$c,$street,$dc,$uid,$surname,$givenName,$initials".toString(),
                CertificateUtils.reorderDn("$surname,$st,$o,$initials,$givenName,$uid,$street,$c,$cn,$ou,$l,$dc"));
    }
}
