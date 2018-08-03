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

package org.apache.nifi.toolkit.tls.util

import org.bouncycastle.crypto.params.RSAKeyParameters
import org.bouncycastle.jcajce.provider.asymmetric.rsa.BCRSAPublicKey
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.security.auth.x500.X500Principal
import java.security.KeyPair
import java.security.PrivateKey
import java.security.Security
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class TlsHelperGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TlsHelperGroovyTest.class)
    private
    final BCRSAPublicKey BAD_PUBLIC_KEY = new BCRSAPublicKey(new RSAKeyParameters(false, new BigInteger("1", 10), new BigInteger("1", 10)))

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
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)
        logger.info("Read certificate ${certificate.getSubjectX500Principal().name} from ${certificateFile.path}")

        // Act
        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [certificate])
        logger.info("Certificate signature valid: ${isCertificateSigned}")

        // Assert
        assert isCertificateSigned
    }

    @Test
    void testShouldVerifyCertificateSignatureWithMultipleSigningCerts() {
        // Arrange
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)
        logger.info("Read certificate ${certificate.getSubjectX500Principal().name} from ${certificateFile.path}")

        X509Certificate mockCertificate = [
                getSubjectX500Principal: { -> new X500Principal("CN=Mock Certificate") },
                getPublicKey           : { -> BAD_PUBLIC_KEY }
        ] as X509Certificate

        // Act
        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [mockCertificate, certificate])
        logger.info("Certificate signature valid: ${isCertificateSigned}")

        // Assert
        assert isCertificateSigned
    }

    @Test
    void testShouldNotVerifyCertificateSignatureWithNoSigningCerts() {
        // Arrange
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)
        logger.info("Read certificate ${certificate.getSubjectX500Principal().name} from ${certificateFile.path}")

        // Act
        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [])
        logger.info("Certificate signature valid: ${isCertificateSigned}")

        // Assert
        assert !isCertificateSigned
    }

    @Test
    void testShouldNotVerifyCertificateSignatureWithWrongSigningCert() {
        // Arrange
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)
        logger.info("Read certificate ${certificate.getSubjectX500Principal().name} from ${certificateFile.path}")

        X509Certificate mockCertificate = [
                getSubjectX500Principal: { -> new X500Principal("CN=Mock Certificate") },
                getPublicKey           : { -> BAD_PUBLIC_KEY }
        ] as X509Certificate

        // Act
        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [mockCertificate])
        logger.info("Certificate signature valid: ${isCertificateSigned}")

        // Assert
        assert !isCertificateSigned
    }

    @Test
    void testParseKeyPairFromReaderShouldHandlePKCS8PrivateKey() {
        // Arrange
        File keyFile = new File("src/test/resources/rootCert-pkcs8.key")
        FileReader keyReader = new FileReader(keyFile)

        final PrivateKey EXPECTED_PRIVATE_KEY = TlsHelper.parseKeyPairFromReader(new FileReader(new File ("src/test/resources/rootCert.key"))).private

        // Act
        KeyPair keyPair = TlsHelper.parseKeyPairFromReader(keyReader)
        logger.info("Successfully read PKCS #8 unencrypted key from ${keyFile.path}")

        // Assert
        assert keyPair.private == EXPECTED_PRIVATE_KEY
    }
}
