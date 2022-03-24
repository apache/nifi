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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

import javax.security.auth.x500.X500Principal
import java.security.KeyPair
import java.security.PrivateKey
import java.security.Security
import java.security.cert.X509Certificate

class TlsHelperGroovyTest {
    private final BCRSAPublicKey BAD_PUBLIC_KEY = new BCRSAPublicKey(new RSAKeyParameters(false, new BigInteger("1", 10), new BigInteger("1", 10)))

    @BeforeAll
    static void setProvider() {
        Security.addProvider(new BouncyCastleProvider())
    }

    @Test
    void testShouldVerifyCertificateSignatureWhenSelfSigned() {
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)

        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [certificate])
        assert isCertificateSigned
    }

    @Test
    void testShouldVerifyCertificateSignatureWithMultipleSigningCerts() {
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)

        X509Certificate mockCertificate = [
                getSubjectX500Principal: { -> new X500Principal("CN=Mock Certificate") },
                getPublicKey           : { -> BAD_PUBLIC_KEY }
        ] as X509Certificate

        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [mockCertificate, certificate])
        assert isCertificateSigned
    }

    @Test
    void testShouldNotVerifyCertificateSignatureWithNoSigningCerts() {
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)

        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [])
        assert !isCertificateSigned
    }

    @Test
    void testShouldNotVerifyCertificateSignatureWithWrongSigningCert() {
        File certificateFile = new File("src/test/resources/rootCert.crt")
        FileReader certReader = new FileReader(certificateFile)
        X509Certificate certificate = TlsHelper.parseCertificate(certReader)

        X509Certificate mockCertificate = [
                getSubjectX500Principal: { -> new X500Principal("CN=Mock Certificate") },
                getPublicKey           : { -> BAD_PUBLIC_KEY }
        ] as X509Certificate

        boolean isCertificateSigned = TlsHelper.verifyCertificateSignature(certificate, [mockCertificate])
        assert !isCertificateSigned
    }

    @Test
    void testParseKeyPairFromReaderShouldHandlePKCS8PrivateKey() {
        File keyFile = new File("src/test/resources/rootCert-pkcs8.key")
        FileReader keyReader = new FileReader(keyFile)

        final KeyPair expectedKeyPair = TlsHelper.parseKeyPairFromReader(new FileReader(new File ("src/test/resources/rootCert.key")))
        final PrivateKey EXPECTED_PRIVATE_KEY = expectedKeyPair.getPrivate()

        KeyPair keyPair = TlsHelper.parseKeyPairFromReader(keyReader)
        assert keyPair.private == EXPECTED_PRIVATE_KEY
    }
}
