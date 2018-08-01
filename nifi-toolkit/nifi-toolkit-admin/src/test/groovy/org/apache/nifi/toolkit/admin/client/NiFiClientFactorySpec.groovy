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

package org.apache.nifi.toolkit.admin.client

import org.apache.commons.lang3.SystemUtils
import org.apache.http.conn.ssl.DefaultHostnameVerifier
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.security.util.CertificateUtils
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.X500NameBuilder
import org.bouncycastle.asn1.x500.style.BCStyle
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.Extensions
import org.bouncycastle.asn1.x509.ExtensionsGenerator
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import spock.lang.Specification

import javax.net.ssl.SSLSession
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.InvalidKeyException
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.NoSuchAlgorithmException
import java.security.NoSuchProviderException
import java.security.SignatureException
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit

class NiFiClientFactorySpec extends Specification {

    private static final int KEY_SIZE = 2048
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA"
    private static final int DAYS_IN_YEAR = 365
    private static final String ISSUER_DN = "CN=NiFi Test CA,OU=Security,O=Apache,ST=CA,C=US"

    def "get client for unsecure nifi"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client

    }

    def "get client for secured nifi"(){

        given:
        def File tmpDir = setupTmpDir()
        def File testDir = new File("target/tmp/keys")
        def toolkitCommandLine = ["-O", "-o",testDir.absolutePath,"-n","localhost","-C", "CN=user1","-S", "badKeyPass", "-K", "badKeyPass", "-P", "badTrustPass"]

        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine()
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine as String[])
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig())

        def bootstrapConfFile = "src/test/resources/notify/conf/bootstrap.conf"
        def nifiPropertiesFile = "src/test/resources/notify/conf/nifi-secured.properties"
        def key = NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfFile)
        def NiFiProperties niFiProperties = NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFile)
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client

        cleanup:
        tmpDir.deleteDir()

    }

    def "should verify CN in certificate based on subjectDN"(){

        given:
        final String EXPECTED_DN = "CN=client.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN,ISSUER_DN)
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def verified = verifier.verify("client.nifi.apache.org",mockSession)

        then:
        verified

    }

    def "should verify wildcard in CN in certificate based on subjectDN"(){

        given:
        final String EXPECTED_DN = "CN=*.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN,ISSUER_DN)
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def validSubdomainIsVerified = verifier.verify("client.nifi.apache.org",mockSession)
        def validSubdomainIsVerified2 = verifier.verify("server.nifi.apache.org",mockSession)
        def invalidSubdomainIsNotVerified = !verifier.verify("client.hive.apache.org",mockSession)

        then:
        validSubdomainIsVerified
        validSubdomainIsVerified2
        invalidSubdomainIsNotVerified
    }

    def "should verify appropriately CN in certificate based on TLD wildcard in SAN"(){

        given:
        final String EXPECTED_DN = "CN=client.nifi.apache.*,OU=Security,O=Apache,ST=CA,C=US"
        final String wildcardHostname = "client.nifi.apache.com"
        byte[] subjectAltName = new GeneralNames(new GeneralName(GeneralName.dNSName, wildcardHostname)).getEncoded()
        Extensions extensions = new Extensions(new Extension(Extension.subjectAlternativeName, false, subjectAltName))
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN, ISSUER_DN, extensions)
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def validTLDIsVerified = verifier.verify("client.nifi.apache.org",mockSession)
        def validTLDIsVerified2 = verifier.verify("client.nifi.apache.com",mockSession)
        def validTLDIsNotVerified = !verifier.verify("client.hive.apache.org",mockSession)

        then:
        //validTLDIsVerified
        validTLDIsVerified2
        validTLDIsNotVerified
    }

    def "should verify appropriately CN in certificate based on subdomain wildcard in SAN"(){

        given:
        final String EXPECTED_DN = "CN=client.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final String wildcardHostname = "*.nifi.apache.org"
        byte[] subjectAltName = new GeneralNames(new GeneralName(GeneralName.dNSName, wildcardHostname)).getEncoded()
        Extensions extensions = new Extensions(new Extension(Extension.subjectAlternativeName, false, subjectAltName))
        Certificate[] certificateChain = generateCertificateChain(EXPECTED_DN, ISSUER_DN, extensions)
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def validSubdomainIsVerified = verifier.verify("client.nifi.apache.org", mockSession)
        def validSubdomainIsVerified1 = verifier.verify("egg.nifi.apache.org", mockSession)
        def invalidSubdomainIsNotVerified = !verifier.verify("client.hive.apache.org", mockSession)
        def invalidDomainIsNotVerified = !verifier.verify("egg.com", mockSession)

        then:
        validSubdomainIsVerified
        validSubdomainIsVerified1
        invalidSubdomainIsNotVerified
        invalidDomainIsNotVerified
    }

    def "should not verify based on no certificate chain"(){

        given:
        final String EXPECTED_DN = "CN=client.nifi.apache.org, OU=Security, O=Apache, ST=CA, C=US"
        Certificate[] certificateChain = [] as Certificate[]
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def notVerified = !verifier.verify("client.nifi.apache.org",mockSession)

        then:
        final ArrayIndexOutOfBoundsException exception = thrown()
    }

    def "should not verify based on multiple CN values"(){

        given:
        final KeyPair issuerKeyPair = generateKeyPair()
        KeyPair keyPair = generateKeyPair()
        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair,ISSUER_DN, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)

        ContentSigner sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(issuerKeyPair.getPrivate());
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.public.getEncoded());
        Date startDate = new Date();
        Date endDate = new Date(startDate.getTime() + TimeUnit.DAYS.toMillis(DAYS_IN_YEAR));

        def X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE)
        nameBuilder.addRDN(BCStyle.CN,"client.nifi.apache.org,nifi.apache.org")
        def name = nameBuilder.build()

        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(new X500Name(issuerCertificate.getSubjectX500Principal().getName()),
                BigInteger.valueOf(System.currentTimeMillis()), startDate, endDate, name,
                subPubKeyInfo);

        X509CertificateHolder certificateHolder = certBuilder.build(sigGen);
        Certificate certificate = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certificateHolder);

        Certificate[] certificateChain = [certificate,issuerCertificate] as Certificate[]
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain


        when:
        def notVerified = !verifier.verify("client.nifi.apache.org",mockSession)

        then:
        notVerified

    }

    def "should verify appropriately CN in certificate based on SAN"() {

        given:

        final List<String> SANS = ["127.0.0.1", "nifi.apache.org"]
        def gns = SANS.collect { String san ->
            new GeneralName(GeneralName.dNSName, san)
        }
        def generalNames = new GeneralNames(gns as GeneralName[])
        ExtensionsGenerator extensionsGenerator = new ExtensionsGenerator()
        extensionsGenerator.addExtension(Extension.subjectAlternativeName, false, generalNames)
        Extensions extensions = extensionsGenerator.generate()

        final String EXPECTED_DN = "CN=client.nifi.apache.org,OU=Security,O=Apache,ST=CA,C=US"
        final KeyPair issuerKeyPair = generateKeyPair()
        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair,ISSUER_DN, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
        final X509Certificate certificate = generateIssuedCertificate(EXPECTED_DN, issuerCertificate,extensions, issuerKeyPair)
        Certificate[] certificateChain = [certificate, issuerCertificate] as X509Certificate[]
        def mockSession = Mock(SSLSession)
        DefaultHostnameVerifier verifier = new DefaultHostnameVerifier()
        mockSession.getPeerCertificates() >> certificateChain

        when:
        def verified = verifier.verify("nifi.apache.org",mockSession)
        def notVerified = !verifier.verify("fake.apache.org",mockSession)


        then:
        verified
        notVerified

    }

    def KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA")
        keyPairGenerator.initialize(KEY_SIZE)
        return keyPairGenerator.generateKeyPair()
    }

    def X509Certificate generateIssuedCertificate(String dn, X509Certificate issuer,Extensions extensions, KeyPair issuerKey) throws IOException, NoSuchAlgorithmException, CertificateException, NoSuchProviderException, SignatureException, InvalidKeyException, OperatorCreationException {
        KeyPair keyPair = generateKeyPair()
        return CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(),extensions, issuer, issuerKey, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
    }

    def X509Certificate[] generateCertificateChain(String dn,String issuerDn) {
        generateCertificateChain(dn, issuerDn, null)
    }

    def X509Certificate[] generateCertificateChain(String dn,String issuerDn, Extensions extensions) {
        final KeyPair issuerKeyPair = generateKeyPair()
        final X509Certificate issuerCertificate = CertificateUtils.generateSelfSignedX509Certificate(issuerKeyPair, issuerDn, SIGNATURE_ALGORITHM, DAYS_IN_YEAR)
        final X509Certificate certificate = generateIssuedCertificate(dn, issuerCertificate, extensions, issuerKeyPair)
        [certificate, issuerCertificate] as X509Certificate[]
    }

    def setFilePermissions(File file, List<PosixFilePermission> permissions = []) {
        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }
    }
    def setupTmpDir(String tmpDirPath = "target/tmp/") {
        File tmpDir = new File(tmpDirPath)
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
                                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])
        tmpDir
    }

}
