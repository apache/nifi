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

package org.apache.nifi.toolkit.tls.standalone

import org.apache.nifi.security.util.CertificateUtils
import org.apache.nifi.toolkit.tls.configuration.StandaloneConfig
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator
import org.bouncycastle.util.io.pem.PemWriter
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Security
import java.security.SignatureException
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class TlsToolkitStandaloneGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitStandaloneGroovyTest.class)

    private final String TEST_SRC_DIR = "src/test/resources/"
    private final String DEFAULT_KEY_PAIR_ALGORITHM = "RSA"
    private final String DEFAULT_SIGNING_ALGORITHM = "SHA256WITHRSA"

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Test
    void testShouldVerifyCertificateSignatureWhenSelfSigned() {
        // Arrange

        // Create a temp directory for this test and populate it with the nifi-cert.pem and nifi-key.key files
        File baseDir = createBaseDirAndPopulateWithCAFiles()

        // Make a standalone config which doesn't trigger any keystore generation and just has a self-signed cert and key
        StandaloneConfig standaloneConfig = new StandaloneConfig()
        standaloneConfig.setBaseDir(baseDir)
        standaloneConfig.setInstanceDefinitions([])
        standaloneConfig.setClientDns([])
        standaloneConfig.initDefaults()

        TlsToolkitStandalone standalone = new TlsToolkitStandalone()

        // Act
        standalone.createNifiKeystoresAndTrustStores(standaloneConfig)

        // Assert

        // The test will fail with an exception if the certificate is not signed by a known certificate
    }

    /**
     * The certificate under examination is self-signed, but there is another signing cert which will be iterated over first, fail, and then the self-signed signature will be validated.
     */
    @Test
    void testShouldVerifyCertificateSignatureWithMultipleSigningCerts() {
        // Arrange

        // Create a temp directory for this test and populate it with the nifi-cert.pem and nifi-key.key files
        File baseDir = createBaseDirAndPopulateWithCAFiles()

        // Create a different cert and persist it to the base dir
        X509Certificate otherCert = generateX509Certificate()
        File otherCertFile = writeCertificateToPEMFile(otherCert, "${baseDir.path}/other.pem")
        logger.info("Wrote other CA cert to ${otherCertFile.path}")

        // Make a standalone config which doesn't trigger any keystore generation and just has a self-signed cert and key
        StandaloneConfig standaloneConfig = new StandaloneConfig()
        standaloneConfig.setBaseDir(baseDir)
        standaloneConfig.setInstanceDefinitions([])
        standaloneConfig.setClientDns([])
        standaloneConfig.initDefaults()

        // Inject the additional CA cert path
        standaloneConfig.setAdditionalCACertificate(otherCertFile.path)

        TlsToolkitStandalone standalone = new TlsToolkitStandalone()

        // Act
        standalone.createNifiKeystoresAndTrustStores(standaloneConfig)

        // Assert

        // The test will fail with an exception if the certificate is not signed by a known certificate
    }

    /**
     * The certificate under examination is signed with the external signing cert.
     */
    @Test
    void testShouldVerifyCertificateSignatureWithAdditionalSigningCert() {
        // Arrange

        // Create a temp directory for this test
        File baseDir = createBaseDir()

        // Create a root CA, create an intermediate CA, use the root to sign the intermediate and then provide the root
        KeyPair rootKeyPair = generateKeyPair()
        X509Certificate rootCert = generateX509Certificate("CN=Root CA", rootKeyPair)

        File rootCertFile = writeCertificateToPEMFile(rootCert, "${baseDir.path}/root.pem")
        logger.info("Wrote root CA cert to ${rootCertFile.path}")

        KeyPair intermediateKeyPair = generateKeyPair()
        X509Certificate intermediateCert = CertificateUtils.generateIssuedCertificate("CN=Intermediate CA", intermediateKeyPair.getPublic(), rootCert, rootKeyPair, DEFAULT_SIGNING_ALGORITHM, 1)

        File intermediateCertFile = writeCertificateToPEMFile(intermediateCert, "${baseDir.path}/nifi-cert.pem")
        logger.info("Wrote intermediate CA cert to ${intermediateCertFile.path}")

        // Write the private key of the intermediate cert to nifi-key.key
        File intermediateKeyFile = writePrivateKeyToFile(intermediateKeyPair, "${baseDir}/nifi-key.key")
        logger.info("Wrote intermediate private key to ${intermediateKeyFile.path}")

        // Make a standalone config which doesn't trigger any keystore generation and just has a signed cert and key
        StandaloneConfig standaloneConfig = new StandaloneConfig()
        standaloneConfig.setBaseDir(baseDir)
        standaloneConfig.setInstanceDefinitions([])
        standaloneConfig.setClientDns([])
        standaloneConfig.initDefaults()

        // Inject the additional CA cert path
        standaloneConfig.setAdditionalCACertificate(rootCertFile.path)

        TlsToolkitStandalone standalone = new TlsToolkitStandalone()

        // Act
        standalone.createNifiKeystoresAndTrustStores(standaloneConfig)

        // Assert

        // The test will fail with an exception if the certificate is not signed by a known certificate
    }

    @Test
    void testShouldNotVerifyCertificateSignatureWithWrongSigningCert() {
        // Arrange

        // Create a temp directory for this test
        File baseDir = createBaseDir()

        // Create a root CA, create an intermediate CA, use the root to sign the intermediate and then do not provide the root
        KeyPair rootKeyPair = generateKeyPair()
        X509Certificate rootCert = generateX509Certificate("CN=Root CA", rootKeyPair)

        KeyPair intermediateKeyPair = generateKeyPair()
        X509Certificate intermediateCert = CertificateUtils.generateIssuedCertificate("CN=Intermediate CA", intermediateKeyPair.getPublic(), rootCert, rootKeyPair, DEFAULT_SIGNING_ALGORITHM, 1)

        File intermediateCertFile = writeCertificateToPEMFile(intermediateCert, "${baseDir.path}/nifi-cert.pem")
        logger.info("Wrote intermediate CA cert to ${intermediateCertFile.path}")

        // Write the private key of the intermediate cert to nifi-key.key
        File intermediateKeyFile = writePrivateKeyToFile(intermediateKeyPair, "${baseDir.path}/nifi-key.key")
        logger.info("Wrote intermediate private key to ${intermediateKeyFile.path}")

        // Make a standalone config which doesn't trigger any keystore generation and just has a signed cert and key
        StandaloneConfig standaloneConfig = new StandaloneConfig()
        standaloneConfig.setBaseDir(baseDir)
        standaloneConfig.setInstanceDefinitions([])
        standaloneConfig.setClientDns([])
        standaloneConfig.initDefaults()

        TlsToolkitStandalone standalone = new TlsToolkitStandalone()

        // Act
        def msg = shouldFail(SignatureException) {
            standalone.createNifiKeystoresAndTrustStores(standaloneConfig)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ 'The signing certificate was not signed by any known certificates'
    }

    private static File writePrivateKeyToFile(KeyPair intermediateKeyPair, String destination) {
        File intermediateKeyFile = new File(destination)
        PemWriter pemWriter = new PemWriter(new FileWriter(intermediateKeyFile))
        pemWriter.writeObject(new JcaMiscPEMGenerator(intermediateKeyPair))
        pemWriter.close()
        intermediateKeyFile
    }

    private File createBaseDirAndPopulateWithCAFiles() {
        File baseDir = createBaseDir()

        populateBaseDirWithCAFiles(baseDir)
    }

    private File createBaseDir() {
        File baseDir = tmpDir.newFolder()
        logger.info("Created base dir at ${baseDir.path}")
        baseDir
    }

    private File populateBaseDirWithCAFiles(File baseDir) {
        File certificateFile = new File(TEST_SRC_DIR, "rootCert.crt")
        File keyFile = new File(TEST_SRC_DIR, "rootCert.key")
        File destinationCertFile = new File(baseDir.path, "nifi-cert.pem")
        Files.copy(certificateFile.toPath(), destinationCertFile.toPath())
        logger.info("Wrote certificate to ${destinationCertFile.path}")
        File destinationKeyFile = new File(baseDir.path, "nifi-key.key")
        Files.copy(keyFile.toPath(), destinationKeyFile.toPath())
        logger.info("Wrote private key to ${destinationKeyFile.path}")

        baseDir
    }

    /**
     * Returns an {@link X509Certificate} with the provided DN and default algorithms. The validity period is only 1 day.
     *
     * @param dn the DN (defaults to {@code CN=Test Certificate})
     * @return the X509Certificate
     */
    private X509Certificate generateX509Certificate(String dn = "CN=Test Certificate", KeyPair keyPair = generateKeyPair()) {
        CertificateUtils.generateSelfSignedX509Certificate(keyPair, CertificateUtils.reorderDn(dn), DEFAULT_SIGNING_ALGORITHM, 1)
    }

    private KeyPair generateKeyPair() {
        KeyPairGenerator instance = KeyPairGenerator.getInstance(DEFAULT_KEY_PAIR_ALGORITHM)
        instance.initialize(2048)
        instance.generateKeyPair()
    }

    /**
     * Writes the provided {@link X509Certificate} to the specified file in PEM format.
     *
     * @param certificate the certificate
     * @param destination the path to write the certificate in PEM format
     * @return the file
     */
    private static File writeCertificateToPEMFile(X509Certificate certificate, String destination) {
        File certificateFile = new File(destination)
        PemWriter pemWriter = new PemWriter(new FileWriter(certificateFile))
        pemWriter.writeObject(new JcaMiscPEMGenerator(certificate))
        pemWriter.close()

        certificateFile
    }
}
