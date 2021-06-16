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

import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.style.BCStyle
import org.bouncycastle.asn1.x500.style.IETFUtils
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.Extensions
import org.bouncycastle.asn1.x509.ExtensionsGenerator
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder
import org.bouncycastle.util.IPAddress
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLException
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.NoSuchAlgorithmException
import java.security.NoSuchProviderException
import java.security.SignatureException
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

@RunWith(JUnit4.class)
class CertificateUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CertificateUtilsTest.class)

    private static final int KEY_SIZE = 2048

    private static final int DAYS_IN_YEAR = 365
    private static final long YESTERDAY = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    private static final long ONE_YEAR_FROM_NOW = System.currentTimeMillis() + 365 * 24 * 60 * 60 * 1000
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA"
    private static final String PROVIDER = "BC"

    private static final String SUBJECT_DN = "CN=NiFi Test Server,OU=Security,O=Apache,ST=CA,C=US"
    private static final String SUBJECT_DN_LEGACY_EMAIL_ATTR_RFC2985 = "CN=NiFi Test Server/emailAddress=test@apache.org,OU=Security,O=Apache,ST=CA,C=US"
    private static final String ISSUER_DN = "CN=NiFi Test CA,OU=Security,O=Apache,ST=CA,C=US"
    private static final List<String> SUBJECT_ALT_NAMES = ["127.0.0.1", "nifi.nifi.apache.org"]

    @BeforeClass
    static void setUpOnce() {
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
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA")
        keyPairGenerator.initialize(KEY_SIZE)
        return keyPairGenerator.generateKeyPair()
    }

    /**
     * Generates a signed certificate using an on-demand keypair.
     *
     * @param dn the DN
     * @return the certificate
     * @throws IOException* @throws NoSuchAlgorithmException* @throws java.security.cert.CertificateException* @throws java.security.NoSuchProviderException* @throws java.security.SignatureException* @throws java.security.InvalidKeyException* @throws OperatorCreationException
     */
    private
    static X509Certificate generateCertificate(String dn) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair()
        return CertificateUtils.generateSelfSignedX509Certificate(keyPair, dn, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
    }

    /**
     * Generates a certificate signed by the issuer key.
     *
     * @param dn the subject DN
     * @param issuerDn the issuer DN
     * @param issuerKey the issuer private key
     * @return the certificate
     * @throws IOException* @throws NoSuchAlgorithmException* @throws CertificateException* @throws NoSuchProviderException* @throws SignatureException* @throws InvalidKeyException* @throws OperatorCreationException
     */
    private
    static X509Certificate generateIssuedCertificate(String dn, X509Certificate issuer, KeyPair issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair()
        return CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKey, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
    }

    private static X509Certificate[] generateCertificateChain(String dn = SUBJECT_DN, String issuerDn = ISSUER_DN) {
        final KeyPair issuerKeyPair = generateKeyPair()

        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, issuerDn, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
        final X509Certificate certificate = generateIssuedCertificate(dn, issuerCertificate, issuerKeyPair)
        [certificate, issuerCertificate] as X509Certificate[]
    }

    @SuppressWarnings("deprecation")
    private static javax.security.cert.X509Certificate generateLegacyCertificate(X509Certificate x509Certificate) {
        return javax.security.cert.X509Certificate.getInstance(x509Certificate.getEncoded())
    }

    private static Certificate generateAbstractCertificate(X509Certificate x509Certificate) {
        return x509Certificate as Certificate
    }

    private static Date inFuture(int days) {
        return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days))
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
        ClientAuth needClientAuthStatus = CertificateUtils.getClientAuthStatus(needSocket)
        logger.info("Client auth (needSocket): ${needClientAuthStatus}")
        ClientAuth wantClientAuthStatus = CertificateUtils.getClientAuthStatus(wantSocket)
        logger.info("Client auth (wantSocket): ${wantClientAuthStatus}")
        ClientAuth noneClientAuthStatus = CertificateUtils.getClientAuthStatus(noneSocket)
        logger.info("Client auth (noneSocket): ${noneClientAuthStatus}")

        // Assert
        assert needClientAuthStatus == ClientAuth.REQUIRED
        assert wantClientAuthStatus == ClientAuth.WANT
        assert noneClientAuthStatus == ClientAuth.NONE
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
    void testGetCommonName(){
        String dn1 = "CN=testDN,O=testOrg"
        String dn2 = "O=testDN,O=testOrg"

        assertEquals("testDN", CertificateUtils.getCommonName(dn1))
        assertNull(CertificateUtils.getCommonName(dn2))
    }

    @Test
    void testShouldGenerateSelfSignedCert() throws Exception {
        String dn = "CN=testDN,O=testOrg"

        int days = 365
        X509Certificate x509Certificate = CertificateUtils.generateSelfSignedX509Certificate(generateKeyPair(), dn, SIGNATURE_ALGORITHM, days)

        Date notAfter = x509Certificate.getNotAfter()
        assertTrue(notAfter.after(inFuture(days - 1)))
        assertTrue(notAfter.before(inFuture(days + 1)))

        Date notBefore = x509Certificate.getNotBefore()
        assertTrue(notBefore.after(inFuture(-1)))
        assertTrue(notBefore.before(inFuture(1)))

        assertEquals(dn, x509Certificate.getIssuerX500Principal().getName())
        assertEquals(SIGNATURE_ALGORITHM.toUpperCase(), x509Certificate.getSigAlgName().toUpperCase())
        assertEquals("RSA", x509Certificate.getPublicKey().getAlgorithm())

        assertEquals(1, x509Certificate.getSubjectAlternativeNames().size())

        GeneralName gn = x509Certificate.getSubjectAlternativeNames().iterator().next()
        assertEquals(GeneralName.dNSName, gn.getTagNo())
        assertEquals("testDN", gn.getName().toString())

        x509Certificate.checkValidity()
    }

    @Test
    void testIssueCert() throws Exception {
        int days = 365
        KeyPair issuerKeyPair = generateKeyPair()
        X509Certificate issuer = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, "CN=testCa,O=testOrg", SIGNATURE_ALGORITHM, days)

        String dn = "CN=testIssued, O=testOrg"

        KeyPair keyPair = generateKeyPair()
        X509Certificate x509Certificate = CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKeyPair, SIGNATURE_ALGORITHM, days)
        assertEquals(dn, x509Certificate.getSubjectX500Principal().toString())
        assertEquals(issuer.getSubjectX500Principal().toString(), x509Certificate.getIssuerX500Principal().toString())
        assertEquals(keyPair.getPublic(), x509Certificate.getPublicKey())

        Date notAfter = x509Certificate.getNotAfter()
        assertTrue(notAfter.after(inFuture(days - 1)))
        assertTrue(notAfter.before(inFuture(days + 1)))

        Date notBefore = x509Certificate.getNotBefore()
        assertTrue(notBefore.after(inFuture(-1)))
        assertTrue(notBefore.before(inFuture(1)))

        assertEquals(SIGNATURE_ALGORITHM.toUpperCase(), x509Certificate.getSigAlgName().toUpperCase())
        assertEquals("RSA", x509Certificate.getPublicKey().getAlgorithm())

        x509Certificate.verify(issuerKeyPair.getPublic())
    }

    @Test
    void reorderShouldPutElementsInCorrectOrder() {
        String cn = "CN=testcn"
        String l = "L=testl"
        String st = "ST=testst"
        String o = "O=testo"
        String ou = "OU=testou"
        String c = "C=testc"
        String street = "STREET=teststreet"
        String dc = "DC=testdc"
        String uid = "UID=testuid"
        String surname = "SURNAME=testsurname"
        String initials = "INITIALS=testinitials"
        String givenName = "GIVENNAME=testgivenname"
        assertEquals("$cn,$l,$st,$o,$ou,$c,$street,$dc,$uid,$surname,$givenName,$initials".toString(),
                CertificateUtils.reorderDn("$surname,$st,$o,$initials,$givenName,$uid,$street,$c,$cn,$ou,$l,$dc"))
    }

    @Test
    void testUniqueSerialNumbers() {
        def running = new AtomicBoolean(true)
        def executorService = Executors.newCachedThreadPool()
        def serialNumbers = Collections.newSetFromMap(new ConcurrentHashMap())
        try {
            def futures = new ArrayList<Future>()
            for (int i = 0; i < 8; i++) {
                futures.add(executorService.submit(new Callable<Integer>() {
                    @Override
                    Integer call() throws Exception {
                        int count = 0
                        while (running.get()) {
                            def before = System.currentTimeMillis()
                            def serialNumber = CertificateUtils.getUniqueSerialNumber()
                            def after = System.currentTimeMillis()
                            def serialNumberMillis = serialNumber.shiftRight(32)
                            assertTrue(serialNumberMillis >= before)
                            assertTrue(serialNumberMillis <= after)
                            assertTrue(serialNumbers.add(serialNumber))
                            count++
                        }
                        return count
                    }
                }))
            }

            Thread.sleep(1000)

            running.set(false)

            def totalRuns = 0
            for (int i = 0; i < futures.size(); i++) {
                try {
                    def numTimes = futures.get(i).get()
                    logger.info("future $i executed $numTimes times")
                    totalRuns += numTimes
                } catch (ExecutionException e) {
                    throw e.getCause()
                }
            }
            logger.info("Generated ${serialNumbers.size()} unique serial numbers")
            assertEquals(totalRuns, serialNumbers.size())
        } finally {
            executorService.shutdown()
        }
    }

    @Test
    void testShouldGenerateIssuedCertificateWithSans() {
        // Arrange
        final String SUBJECT_DN = "CN=localhost"
        final List<String> SANS = ["127.0.0.1", "nifi.nifi.apache.org"]
        logger.info("Creating a certificate with subject: ${SUBJECT_DN} and SAN: ${SANS}")

        final KeyPair subjectKeyPair = generateKeyPair()
        final KeyPair issuerKeyPair = generateKeyPair()

        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, ISSUER_DN, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)

        // Form the SANS into GeneralName instances and populate the container with the array
        def gns = SANS.collect { String san ->
            new GeneralName(GeneralName.dNSName, san)
        }
        def generalNames = new GeneralNames(gns as GeneralName[])
        logger.info("Created GeneralNames object: ${generalNames.names*.toString()}")

        // Form the Extensions object
        ExtensionsGenerator extensionsGenerator = new ExtensionsGenerator()
        extensionsGenerator.addExtension(Extension.subjectAlternativeName, false, generalNames)
        Extensions extensions = extensionsGenerator.generate()
        logger.info("Generated extensions object: ${extensions.oids()*.toString()}")

        // Act
        X509Certificate certificate = CertificateUtils.generateIssuedCertificate(SUBJECT_DN, subjectKeyPair.public, extensions, issuerCertificate, issuerKeyPair, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
        logger.info("Issued certificate with subject: ${certificate.getSubjectDN().name} and SAN: ${certificate.getSubjectAlternativeNames().join(",")}")

        // Assert
        assert certificate instanceof X509Certificate
        assert certificate.getSubjectDN().name == SUBJECT_DN
        assert certificate.getSubjectAlternativeNames().size() == SANS.size()
        assert certificate.getSubjectAlternativeNames()*.last().containsAll(SANS)
    }

    @Test
    void testShouldDetectTlsErrors() {
        // Arrange
        final String msg = "Test exception"

        // SSLPeerUnverifiedException isn't specifically defined in the method, but is a subclass of SSLException so it should be caught
        List<Throwable> directErrors = [new TlsException(msg), new SSLPeerUnverifiedException(msg), new CertificateException(msg), new SSLException(msg)]
        List<Throwable> causedErrors = directErrors.collect { Throwable cause -> new Exception(msg, cause) } + [
                new Exception(msg,
                        new Exception("Nested $msg",
                                new Exception("Double nested $msg",
                                        new TlsException("Triple nested $msg"))))]
        List<Throwable> unrelatedErrors = [new Exception(msg), new IllegalArgumentException(msg), new NullPointerException(msg)]

        // Act
        def directResults = directErrors.collect { Throwable e -> CertificateUtils.isTlsError(e) }
        def causedResults = causedErrors.collect { Throwable e -> CertificateUtils.isTlsError(e) }
        def unrelatedResults = unrelatedErrors.collect { Throwable e -> CertificateUtils.isTlsError(e) }

        logger.info("Direct results: ${directResults}")
        logger.info("Caused results: ${causedResults}")
        logger.info("Unrelated results: ${unrelatedResults}")

        // Assert
        assert directResults.every()
        assert causedResults.every()
        assert !unrelatedResults.any()
    }

    @Test
    void testGetExtensionsFromCSR() {
        // Arrange
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA")
        KeyPair keyPair = generator.generateKeyPair()
        Extensions sanExtensions = createDomainAlternativeNamesExtensions(SUBJECT_ALT_NAMES, SUBJECT_DN)

        JcaPKCS10CertificationRequestBuilder jcaPKCS10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(new X500Name(SUBJECT_DN), keyPair.getPublic())
        jcaPKCS10CertificationRequestBuilder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, sanExtensions)
        JcaContentSignerBuilder jcaContentSignerBuilder = new JcaContentSignerBuilder("SHA256WITHRSA")
        JcaPKCS10CertificationRequest jcaPKCS10CertificationRequest = new JcaPKCS10CertificationRequest(jcaPKCS10CertificationRequestBuilder.build(jcaContentSignerBuilder.build(keyPair.getPrivate())))

        // Act
        Extensions extensions = CertificateUtils.getExtensionsFromCSR(jcaPKCS10CertificationRequest)

        // Assert
        assert(extensions.equivalent(sanExtensions))
    }

    @Test
    void testExtractUserNameFromDN() {
        String expected = "NiFi Test Server"
        assertEquals(CertificateUtils.extractUsername(SUBJECT_DN), expected)
        assertEquals(CertificateUtils.extractUsername(SUBJECT_DN_LEGACY_EMAIL_ATTR_RFC2985), expected)
    }

    // Using this directly from tls-toolkit results in a dependency loop, so it's added here for testing purposes.
    private static Extensions createDomainAlternativeNamesExtensions(List<String> domainAlternativeNames, String requestedDn) throws IOException {
        List<GeneralName> namesList = new ArrayList<>()

        try {
            final String cn = IETFUtils.valueToString(new X500Name(requestedDn).getRDNs(BCStyle.CN)[0].getFirst().getValue())
            namesList.add(new GeneralName(GeneralName.dNSName, cn))
        } catch (Exception e) {
            throw new IOException("Failed to extract CN from request DN: " + requestedDn, e)
        }

        if (domainAlternativeNames != null) {
            for (String alternativeName : domainAlternativeNames) {
                namesList.add(new GeneralName(IPAddress.isValid(alternativeName) ? GeneralName.iPAddress : GeneralName.dNSName, alternativeName))
            }
        }

        GeneralNames subjectAltNames = new GeneralNames(namesList.toArray([] as GeneralName[]))
        ExtensionsGenerator extGen = new ExtensionsGenerator()
        extGen.addExtension(Extension.subjectAlternativeName, false, subjectAltNames)
        return extGen.generate()
    }
}
