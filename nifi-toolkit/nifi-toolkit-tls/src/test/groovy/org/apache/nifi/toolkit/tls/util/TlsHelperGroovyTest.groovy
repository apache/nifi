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
}
