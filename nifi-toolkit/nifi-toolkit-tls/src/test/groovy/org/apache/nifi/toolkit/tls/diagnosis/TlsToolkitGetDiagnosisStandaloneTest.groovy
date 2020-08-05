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

package org.apache.nifi.toolkit.tls.diagnosis

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.security.util.CertificateUtils
import org.apache.nifi.security.util.KeyStoreUtils
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.toolkit.tls.util.TlsHelper
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.Extensions
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.security.auth.x500.X500Principal
import java.security.KeyPair
import java.security.KeyStore
import java.security.Security
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit


@RunWith(JUnit4.class)
class TlsToolkitGetDiagnosisStandaloneTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitGetDiagnosisCommandLineTest.class)
    public static final String DEFAULT_SIGNING_ALGORITHM = "SHA256WITHRSA"

    private static final KeyPair keyPair = TlsHelper.generateKeyPair("RSA", 2048)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
        //setupTmpDir() ???
    }

    static X509Certificate signAndBuildCert(String dn, String signingAlgorithm, KeyPair keyPair) {
        ContentSigner sigGen = new JcaContentSignerBuilder(signingAlgorithm).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(keyPair.getPrivate())
        X509v3CertificateBuilder certBuilder = certBuilder(new Date(), dn, keyPair, 365 * 24)
        X509Certificate cert = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))
        return cert
    }

    static X509v3CertificateBuilder certBuilder(Date startDate, String dn, KeyPair keyPair, int hours) {
        Date endDate = new Date(startDate.getTime() + TimeUnit.HOURS.toMillis(hours));

        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded())
        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                CertificateUtils.reverseX500Name(new X500Name(dn)),
                CertificateUtils.getUniqueSerialNumber(),
                startDate, endDate,
                CertificateUtils.reverseX500Name(new X500Name(dn)),
                subPubKeyInfo)
        return certBuilder
    }

    void setUp() {
        super.setUp()
    }

    void tearDown() {
    }

    @Ignore("No assertions to make here")
    @Test
    void testPrintUsage() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()

        //Act
        standalone.printUsage("This is an error message");

        //Assert
    }

    @Test
    void testShouldParseCommandLine() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String args = "-n src/test/resources/diagnosis/nifi.properties"

        //Act
        standalone.parseCommandLine(args.split(" ") as String[])

        //Assert
        assert standalone.niFiPropertiesPath == "src/test/resources/diagnosis/nifi.properties"
    }

    @Test
    void testParseCommandLineShouldFail() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String args = "-w wrongservice -p"

        //Act
        def msg = shouldFail(CommandLineParseException) {
            standalone.parseCommandLine(args.split(" ") as String[])
        }

        assert msg == "Error parsing command line. (Unrecognized option: -w)"
    }

    @Test
    void testParseCommandLineShouldDetectHelpArg() {
        //Arrange
        exit.expectSystemExitWithStatus(0)

        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String args = "-h -n wrongservice"

        //Act
        standalone.parseCommandLine(args.split(" ") as String[])
    }

    @Test
    void testShouldLoadNiFiProperties() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        String[] args = ["-n", niFiPropertiesPath] as String[]
        standalone.parseCommandLine(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()

        //Assert
        assert properties
        assert properties.size() > 0
    }

    @Test
    void testShouldLoadNiFiPropertiesFromEncryptedNifiFile() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/encrypted_nifi.properties"
        String bootstrapPath = "src/test/resources/diagnosis/bootstrap_with_key.conf"
        String[] args = ["-n", niFiPropertiesPath, "-b", bootstrapPath] as String[]
        standalone.parseCommandLine(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")
        logger.info("Parsed boostrap.conf location: ${standalone.bootstrapPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()

        //Assert
        assert properties
        assert properties.size() > 0
    }

    @Test
    void testShouldCheckDoesFileExist() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        String[] args = ["-n", niFiPropertiesPath] as String[]
        standalone.parseCommandLine(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()
        def keystorePath = properties.getProperty("nifi.security.keystore")
        def doesFileExist = standalone.doesFileExist(keystorePath, standalone.niFiPropertiesPath, ".jks")

        //Assert
        assert doesFileExist
    }

    @Test
    void testShouldFailDoesFileExist() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesWrongPath = "src/test/resources/diagnosis/nifi_wrong_keystore_path.properties"
        String[] args = ["-n", niFiPropertiesWrongPath] as String[]
        standalone.parseCommandLine(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()
        def keystorePath = properties.getProperty("nifi.security.keystore")
        def doesFileExist = standalone.doesFileExist(keystorePath, standalone.niFiPropertiesPath, ".jks")

        //Assert
        assert !doesFileExist
    }

    @Test
    void testShouldCheckPasswordForKeystore() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        standalone.niFiPropertiesPath = niFiPropertiesPath
        standalone.bootstrapPath = new File(niFiPropertiesPath).getParent() + "/bootstrap.conf";
        standalone.niFiProperties = standalone.loadNiFiProperties()
        def keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore")
        def keystoreType = standalone.niFiProperties.getProperty("nifi.security.keystoreType")
        char[] keystorePassword = standalone.niFiProperties.getProperty("nifi.security.keystorePasswd")

        //Act
        def keystore = TlsToolkitGetDiagnosisStandalone.checkPasswordForKeystoreAndLoadKeystore(keystorePassword, keystorePath, keystoreType)

        //Assert
        assert keystore != null
    }

    @Test
    void testCheckPasswordForKeystoreShouldFail() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        standalone.niFiPropertiesPath = niFiPropertiesPath
        standalone.bootstrapPath = new File(niFiPropertiesPath).getParent() + "/bootstrap.conf";
        standalone.niFiProperties = standalone.loadNiFiProperties()
        def keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore")
        def keystoreType = standalone.niFiProperties.getProperty("nifi.security.keystoreType")
        char[] keystorePassword = ['c' * 16] as char[]

        //Act
        def keystore = TlsToolkitGetDiagnosisStandalone.checkPasswordForKeystoreAndLoadKeystore(keystorePassword, keystorePath, keystoreType)

        //Assert
        assert !keystore
    }

    @Test
    void testShouldExtractPrimaryPrivateKeyEntry() {

        //Arrange
        KeyStore ks = KeyStore.getInstance("JKS")
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()

        def password = "password" as char[]
        ks.load(null, password);
        KeyPair keyPair1 = keyPair
        KeyPair keyPair2 = keyPair

        def chain1 = [[
                              getSubjectX500Principal: { -> new X500Principal("CN=ForChain1") },
                              getPublicKey           : { -> keyPair1.getPublic() }
                      ],
                      [
                              getSubjectX500Principal: { -> new X500Principal("CN=ForChain1Root") },
                              getPublicKey           : { -> keyPair1.getPublic() }
                      ]
        ] as X509Certificate[]

        def chain2 = [[
                              getSubjectX500Principal: { -> new X500Principal("CN=ForChain2") },
                              getPublicKey           : { -> keyPair2.getPublic() }
                      ],
                      [
                              getSubjectX500Principal: { -> new X500Principal("CN=ForChain2Root") },
                              getPublicKey           : { -> keyPair2.getPublic() }
                      ]

        ] as X509Certificate[]

        ks.setKeyEntry("test1", keyPair1.getPrivate(), password, chain1)
        ks.setKeyEntry("test2", keyPair2.getPrivate(), password, chain2)
        standalone.keystore = ks

        //Act
        def primaryPrivateKeyEntry = standalone.extractPrimaryPrivateKeyEntry(ks, password)

        //Assert
        assert primaryPrivateKeyEntry.getCertificate() instanceof X509Certificate
        X509Certificate test = (X509Certificate) primaryPrivateKeyEntry.getCertificate()
        assert CertificateUtils.extractUsername(test.getSubjectX500Principal().getName().toString()) == "ForChain2"

    }


    @Test
    void testShouldExtractPrimaryPrivateKeyEntryForSingleEntry() {
        //Arrange
        KeyStore ks = KeyStore.getInstance("JKS")
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()

        def password = "password" as char[]
        ks.load(null, password);
        KeyPair keyPair1 = keyPair
        X509Certificate[] chain1 = new X509Certificate[2];
        chain1[0] = [
                getSubjectX500Principal: { -> new X500Principal("CN=ForChain1") },
                getPublicKey           : { -> keyPair1.getPublic() }
        ] as X509Certificate

        chain1[1] = [
                getSubjectX500Principal: { -> new X500Principal("CN=ForChain1Root") },
                getPublicKey           : { -> keyPair1.getPublic() }
        ] as X509Certificate

        ks.setKeyEntry("test1", keyPair1.getPrivate(), password, chain1)
        standalone.keystore = ks

        //Act
        def primaryPrivateKeyEntry = standalone.extractPrimaryPrivateKeyEntry(ks, password)

        //Assert
        assert primaryPrivateKeyEntry.getCertificate() instanceof X509Certificate
        X509Certificate test = (X509Certificate) primaryPrivateKeyEntry.getCertificate()
        assert CertificateUtils.extractUsername(test.getSubjectX500Principal().getName().toString()) == "ForChain1"
    }

    @Test
    void testExtractPrimaryPrivateKeyEntryForEmptyKeystore() {
        //Arrange
        KeyStore ks = KeyStore.getInstance("JKS")
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()

        def password = "password" as char[]
        ks.load(null, password);

        //Act
        def output = standalone.extractPrimaryPrivateKeyEntry(ks, password)

        //
        assert output == null
    }

    @Test
    void testShouldCheckCNAllScenarios() {
        //Tests for CN compared with hostname in nifi.properties for: Exact Match, Wildcard, and Wrong Match
        //Arrange
        KeyPair keyPair = keyPair
        def certificateCorrect = CertificateUtils.generateSelfSignedX509Certificate(keyPair, "CN=fakeCN", "SHA256WITHRSA", 365)
        def certificateWildcard = CertificateUtils.generateSelfSignedX509Certificate(keyPair, "CN=*.fakeCN", "SHA256WITHRSA", 365)
        def certificateWrong = CertificateUtils.generateSelfSignedX509Certificate(keyPair, "CN=fakeCN", "SHA256WITHRSA", 365)
        //Act
        def outputCorrect = TlsToolkitGetDiagnosisStandalone.checkCN(certificateCorrect, "fakeCN")
        def outputWrong = TlsToolkitGetDiagnosisStandalone.checkCN(certificateWrong, "WrongCN")
        def outputWildcard = TlsToolkitGetDiagnosisStandalone.checkCN(certificateWildcard, "*.fakeCN")

        //Assert
        assert outputCorrect.getValue().toString() == "CORRECT"
        assert outputWrong.getValue().toString() == "WRONG"
        assert outputWildcard.getValue().toString() == "NEEDS_ATTENTION"
    }

    @Test
    void testShouldCheckSANAllScenarios() {
        //Arrange
        KeyPair keyPair = keyPair
        String dn = "CN=fakeCN"
        ContentSigner sigGen = new JcaContentSignerBuilder(DEFAULT_SIGNING_ALGORITHM).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(keyPair.getPrivate())

        Extensions extensions = TlsHelper.createDomainAlternativeNamesExtensions(["120.60.23.24", "127.0.0.1"] as List<String>, dn)
        X509v3CertificateBuilder certBuilder = certBuilder(new Date(), dn, keyPair, 365 * 24)
        certBuilder.addExtension(Extension.subjectAlternativeName, false, extensions.getExtensionParsedValue(Extension.subjectAlternativeName))
        X509Certificate cert = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))

        def correctHostnames = [
                "fakeCN",          //specifiedHostname == CN == SAN(DNS entry)
                "120.60.23.24",    //specifiedHostname == SAN(IP entry)
                "localhost",      //specifiedHostname will resolve to entry in SAN(IP entry)
        ]

        def wrongHostnames = [
                "nifi.apache.org", //specifiedHostname(DNS) not present SAN(DNS or IP entry)
                "121.60.23.24",    //specifiedHostname(IP) not present SAN(IP entry)
        ]

        def needsAttentionHostnames = [
                "nifi.fake"        //specifiedHostname cannot be resolved to IP
        ]

        //Act
        def correctOutputs = correctHostnames.collect() {
            def output = TlsToolkitGetDiagnosisStandalone.checkSAN(cert, it)
            output
        }

        def wrongOutputs = wrongHostnames.collect() {
            def output = TlsToolkitGetDiagnosisStandalone.checkSAN(cert, it)
            output
        }

        def needsAttentionOutputs = needsAttentionHostnames.collect() {
            def output = TlsToolkitGetDiagnosisStandalone.checkSAN(cert, it)
            output
        }

        //Assert
        assert correctOutputs.every { it.getValue().toString() == "CORRECT" }
        assert wrongOutputs.every { it.getValue().toString() == "WRONG" }
        assert needsAttentionOutputs.every { it.getValue().toString() == "NEEDS_ATTENTION" }
    }

    @Test
    void testShouldCheckSANForNoIPEntries() {
        //Arrange
        KeyPair keyPair = keyPair
        String dn = "CN=fakeCN"
        ContentSigner sigGen = new JcaContentSignerBuilder(DEFAULT_SIGNING_ALGORITHM).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(keyPair.getPrivate())

        Extensions extensions = TlsHelper.createDomainAlternativeNamesExtensions(["anotherCN", "nifi.apache.org"] as List<String>, dn)
        X509v3CertificateBuilder certBuilder = certBuilder(new Date(), dn, keyPair, 365 * 24)
        certBuilder.addExtension(Extension.subjectAlternativeName, false, extensions.getExtensionParsedValue(Extension.subjectAlternativeName))
        X509Certificate cert = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))

        //Act
        def outputDNS = TlsToolkitGetDiagnosisStandalone.checkSAN(cert, "newCN")
        def outputIP = TlsToolkitGetDiagnosisStandalone.checkSAN(cert, "120.34.34.20")
        //Assert
        outputDNS.getValue().toString() == "WRONG"
        outputIP.getValue().toString() == "WRONG"

    }

    @Test
    void testShouldCheckEKUAllScenarios() {
        //Arrange
        ContentSigner sigGen = new JcaContentSignerBuilder(DEFAULT_SIGNING_ALGORITHM).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(keyPair.getPrivate())

        //Both clientAuth and serverAuth
        def correctEKUs = [
                new ExtendedKeyUsage([KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth] as KeyPurposeId[])
        ]

        def wrongEKUs = [
                //Either severAuth or clientAuth
                new ExtendedKeyUsage([KeyPurposeId.id_kp_serverAuth] as KeyPurposeId[]),
                new ExtendedKeyUsage([KeyPurposeId.id_kp_clientAuth] as KeyPurposeId[]),
                //Other auth
                new ExtendedKeyUsage([KeyPurposeId.id_kp_codeSigning, KeyPurposeId.id_kp_emailProtection] as KeyPurposeId[])
        ]

        //No EKU
        X509v3CertificateBuilder certBuilderNoEKU = certBuilder(new Date(), "CN=fakeCN", keyPair, 365 * 24)
        X509Certificate certNoEKU = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilderNoEKU.build(sigGen))


        //Act
        def correctOutputs = correctEKUs.collect() {
            X509v3CertificateBuilder certBuilder = certBuilder(new Date(), "CN=fakeCN", keyPair, 365 * 24)
            certBuilder.addExtension(Extension.extendedKeyUsage, false, it)
            X509Certificate certificate = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))
            TlsToolkitGetDiagnosisStandalone.checkEKU(certificate)
        }


        def wrongOutputs = wrongEKUs.collect() {
            X509v3CertificateBuilder certBuilder = certBuilder(new Date(), "CN=fakeCN", keyPair, 365 * 24)
            certBuilder.addExtension(Extension.extendedKeyUsage, false, it)
            X509Certificate certificate = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))
            TlsToolkitGetDiagnosisStandalone.checkEKU(certificate)
        }


        def outputNoEKU = TlsToolkitGetDiagnosisStandalone.checkEKU(certNoEKU)

        //Assert
        assert correctOutputs.every { it.getValue().toString() == "CORRECT" }
        assert wrongOutputs.every { it.getValue().toString() == "WRONG" }
        assert outputNoEKU.getValue().toString() == "NEEDS_ATTENTION"
    }

    @Test
    void testShouldCheckValidity() {
        //Arrange
        String dn = "CN=fakeCN"
        ContentSigner sigGen = new JcaContentSignerBuilder(DEFAULT_SIGNING_ALGORITHM).setProvider(BouncyCastleProvider.PROVIDER_NAME).build(keyPair.getPrivate())
        def correctStartDates = [
                //Current
                new Date(),
        ]
        def wrongDates = [
                // Tomorrow (not yet valid)
                new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000),
                // Yesterday (expired)
                new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000)
        ]

        //Act
        def correctOutputs = correctStartDates.collect { date ->
            X509v3CertificateBuilder certBuilder = certBuilder(date, dn, keyPair, 1)
            X509Certificate cert = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))
            TlsToolkitGetDiagnosisStandalone.checkValidity(cert)
        }
        def wrongOutputs = wrongDates.collect { date ->
            X509v3CertificateBuilder certBuilder = certBuilder(date, dn, keyPair, 1)
            X509Certificate cert = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(sigGen))
            TlsToolkitGetDiagnosisStandalone.checkValidity(cert)
        }

        //Assert
        assert correctOutputs.every { it.getValue().toString() == "CORRECT" }
        assert wrongOutputs.every { it.getValue().toString() == "WRONG" }
    }

    @Test
    void testShouldCheckKeySize() {
        //Arrange
        String dn = "CN=fakeCN"
        KeyPair DSAKey = TlsHelper.generateKeyPair("DSA", 2048)

        def correctCerts = [
                signAndBuildCert(dn, "SHA512WITHDSA", TlsHelper.generateKeyPair("DSA", 2048)),
                signAndBuildCert(dn, "SHA256WITHRSA", keyPair)
        ]
        def needsAttentionCerts = [
                signAndBuildCert(dn, "SHA256WITHECDSA", TlsHelper.generateKeyPair("EC", 571))
        ]
        def wrongCerts = [
                signAndBuildCert(dn, "SHA512WITHDSA", TlsHelper.generateKeyPair("DSA", 1024)),
                signAndBuildCert(dn, "SHA256WITHRSA", TlsHelper.generateKeyPair("RSA", 1024))
        ]

        //Act
        def outputsCorrect = correctCerts.collect() {
            TlsToolkitGetDiagnosisStandalone.checkKeySize(it)
        }
        def outputsNeedsAttention = needsAttentionCerts.collect() {
            TlsToolkitGetDiagnosisStandalone.checkKeySize(it)
        }
        def outputsWrong = wrongCerts.collect() {
            TlsToolkitGetDiagnosisStandalone.checkKeySize(it)
        }

        //Assert
        assert outputsCorrect.every { it.getValue().toString() == "CORRECT" }
        assert outputsNeedsAttention.every { it.getValue().toString() == "NEEDS_ATTENTION" }
        assert outputsWrong.every { it.getValue().toString() == "WRONG" }
    }

    @Test
    void testShouldCheckSignature() {
        //Arrange
        String dn = "CN=fakeCN"
        String dnIssuedCert = "CN=fakefakeCN"
        KeyPair newKeyPair = TlsHelper.generateKeyPair("RSA", 1024)
        X509Certificate selfSignedCert = signAndBuildCert(dn, "SHA256WITHRSA", keyPair)
        X509Certificate issuedBySelfSignedCert = CertificateUtils.generateIssuedCertificate(dnIssuedCert, newKeyPair.getPublic(), selfSignedCert, keyPair, "SHA256WITHRSA", 325)

        def certList = [
                selfSignedCert,
                issuedBySelfSignedCert
        ] as List<X509Certificate>

        //Act
        def output = TlsToolkitGetDiagnosisStandalone.checkSignature(certList, issuedBySelfSignedCert)

        //Assert
        assert output.getValue().toString() == "CORRECT"
    }

    @Test
    void testCheckSignatureShouldFail() {
        //Arrange
        String dn = "CN=fakeCN"
        String dnIssuedCert = "CN=fakefakeCN"
        KeyPair newKeyPair = TlsHelper.generateKeyPair("RSA", 1024)
        X509Certificate selfSignedCert = signAndBuildCert(dn, "SHA256WITHRSA", keyPair)
        X509Certificate issuedBySelfSignedCert = CertificateUtils.generateIssuedCertificate(dnIssuedCert, keyPair.getPublic(), selfSignedCert, newKeyPair, "SHA256WITHRSA", 325)

        def certList = [
                selfSignedCert,
                issuedBySelfSignedCert
        ] as List<X509Certificate>

        //Act
        def output = TlsToolkitGetDiagnosisStandalone.checkSignature(certList, issuedBySelfSignedCert)

        //Assert
        assert output.getValue().toString() == "WRONG"
    }

    @Test
    void testShouldCheckTruststore() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        standalone.niFiPropertiesPath = niFiPropertiesPath
        standalone.bootstrapPath = new File(niFiPropertiesPath).getParent() + "/bootstrap.conf";
        standalone.niFiProperties = standalone.loadNiFiProperties()
        def keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore")
        def keystoreType = standalone.niFiProperties.getProperty("nifi.security.keystoreType")
        char[] keystorePassword = standalone.niFiProperties.getProperty("nifi.security.keystorePasswd")
        standalone.keystore = KeyStoreUtils.loadKeyStore(keystorePath, keystorePassword, keystoreType)
        KeyStore.PrivateKeyEntry privateKeyEntry = standalone.extractPrimaryPrivateKeyEntry(standalone.keystore, keystorePassword)
        X509Certificate certList = privateKeyEntry.getCertificate()
        def truststorePath = standalone.niFiProperties.getProperty("nifi.security.truststore")
        def truststoreType = standalone.niFiProperties.getProperty("nifi.security.truststoreType")
        char[] truststorePassword = standalone.niFiProperties.getProperty("nifi.security.truststorePasswd")
        standalone.truststore = KeyStoreUtils.loadKeyStore(truststorePath, truststorePassword, truststoreType)

        //Act
        def output = standalone.checkTruststore(privateKeyEntry)

        //Assert
        assert output.getValue().toString() == "CORRECT"
    }

    @Test
    void testCheckTruststoreShouldFail() {
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        standalone.niFiPropertiesPath = niFiPropertiesPath
        standalone.bootstrapPath = new File(niFiPropertiesPath).getParent() + "/bootstrap.conf";
        standalone.niFiProperties = standalone.loadNiFiProperties()
        def keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore")
        def keystoreType = standalone.niFiProperties.getProperty("nifi.security.keystoreType")
        char[] keystorePassword = standalone.niFiProperties.getProperty("nifi.security.keystorePasswd")
        standalone.keystore = KeyStoreUtils.loadKeyStore(keystorePath, keystorePassword, keystoreType)
        KeyStore.PrivateKeyEntry privateKeyEntry = standalone.extractPrimaryPrivateKeyEntry(standalone.keystore, keystorePassword)
        X509Certificate certList = privateKeyEntry.getCertificate()
        def truststorePath = "src/test/resources/diagnosis/other_truststore.jks"
        def truststoreType = standalone.niFiProperties.getProperty("nifi.security.truststoreType")
        char[] truststorePassword = "Qbe9wKqb29QTLpForsDPGn9vOD2Pc4FlnBvqSPZSKpA" as char[]
        standalone.truststore = KeyStoreUtils.loadKeyStore(truststorePath, truststorePassword, truststoreType)

        //Act
        def output = standalone.checkTruststore(privateKeyEntry)

        //Assert
        assert output.getValue().toString() == "WRONG"
    }

}
