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

import org.apache.nifi.util.StringUtils
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.Certificate
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class KeyStoreUtilsGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtilsGroovyTest.class)

    private static final String TEST_KEYSTORE_PASSWORD = KeyStoreUtils.generatePassword()
    private static final String TEST_KEY_PASSWORD = KeyStoreUtils.generatePassword()
    private static final String TEST_TRUSTSTORE_PASSWORD = KeyStoreUtils.generatePassword()
    private static final KeystoreType DEFAULT_STORE_TYPE = KeystoreType.JKS
    private static final String KEY_ALIAS = "nifi-key"

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {

    }

    @After
    void tearDown() {

    }

    @Test
    void testShouldVerifyKeystoreIsValid() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, DEFAULT_STORE_TYPE, null, null, DEFAULT_STORE_TYPE)

        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        final URL ksUrl = getKeystorePathAsUrl(tlsConfig.getKeystorePath())

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.toCharArray())
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert keystoreIsValid
    }

    @Test
    void testShouldVerifyKeystoreIsNotValid() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, DEFAULT_STORE_TYPE, null, null, DEFAULT_STORE_TYPE)

        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        final URL ksUrl = getKeystorePathAsUrl(tlsConfig.getKeystorePath())

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.reverse().toCharArray())
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert !keystoreIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsValid() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, TEST_KEY_PASSWORD, DEFAULT_STORE_TYPE, null, null, DEFAULT_STORE_TYPE)

        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        final URL ksUrl = getKeystorePathAsUrl(tlsConfig.getKeystorePath())

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.toCharArray(), TEST_KEY_PASSWORD.toCharArray())
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert keyPasswordIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsNotValid() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, TEST_KEY_PASSWORD, DEFAULT_STORE_TYPE, null, null, DEFAULT_STORE_TYPE)

        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        final URL ksUrl = getKeystorePathAsUrl(tlsConfig.getKeystorePath())

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(ksUrl, tlsConfig.getKeystoreType(), TEST_KEYSTORE_PASSWORD.toCharArray(), TEST_KEY_PASSWORD.reverse().toCharArray())
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert !keyPasswordIsValid
    }

    @Test
    @Ignore("Used to create passwordless truststore file for testing NIFI-6770")
    void createPasswordlessTruststore() {
        // Retrieve the public certificate from https://nifi.apache.org
        String hostname = "nifi.apache.org"
        SSLSocketFactory factory = HttpsURLConnection.getDefaultSSLSocketFactory()
        SSLSocket socket = (SSLSocket) factory.createSocket(hostname, 443)
        socket.startHandshake()
        List<Certificate> certs = socket.session.peerCertificateChain as List<Certificate>
        Certificate nodeCert = CertificateUtils.formX509Certificate(certs.first().encoded)

        // Create a JKS truststore containing that cert as a trustedCertEntry and do not put a password on the truststore
        KeyStore truststore = KeyStore.getInstance("JKS")
        // Explicitly set the second parameter to empty to avoid a password
        truststore.load(null, "".chars)
        truststore.setCertificateEntry("nifi.apache.org", nodeCert)

        // Save the truststore to disk
        FileOutputStream fos = new FileOutputStream("target/nifi.apache.org.ts.jks")
        truststore.store(fos, "".chars)
    }

    @Test
    @Ignore("Used to create passwordless truststore file for testing NIFI-6770")
    void createLocalPasswordlessTruststore() {
        KeyStore truststoreWithPassword = KeyStore.getInstance("JKS")
        truststoreWithPassword.load(new FileInputStream("/Users/alopresto/Workspace/nifi/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/test/resources/truststore.jks"), "passwordpassword".chars)
        Certificate nodeCert = truststoreWithPassword.getCertificate("nifi-cert")

        // Create a JKS truststore containing that cert as a trustedCertEntry and do not put a password on the truststore
        KeyStore truststore = KeyStore.getInstance("JKS")
        // Explicitly set the second parameter to empty to avoid a password
        truststore.load(null, "".chars)
        truststore.setCertificateEntry("nifi.apache.org", nodeCert)

        // Save the truststore to disk
        FileOutputStream fos = new FileOutputStream("/Users/alopresto/Workspace/nifi/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/test/resources/truststore.no-password.jks")
        truststore.store(fos, "".chars)
    }

    @Test
    void testShouldValidateTempKeystorePath() {
        // Act
        Path testKeystorePath = KeyStoreUtils.generateTempKeystorePath(DEFAULT_STORE_TYPE)
        deletePath(testKeystorePath)

        // Assert
        logger.info("Keystore path: ${testKeystorePath.toString()}")
        assert testKeystorePath
    }

    @Test
    void testShouldValidateTempTruststorePath() {
        // Act
        Path truststorePath = KeyStoreUtils.generateTempTruststorePath(DEFAULT_STORE_TYPE)
        deletePath(truststorePath)

        // Assert
        logger.info("Truststore path: ${truststorePath.toString()}")
        assert truststorePath
    }

    @Test
    void testShouldValidateTlsConfigAndNewKeystoreTruststoreWithParams() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, TEST_KEY_PASSWORD, DEFAULT_STORE_TYPE, null, TEST_TRUSTSTORE_PASSWORD, DEFAULT_STORE_TYPE)

        // Act
        final TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert tlsConfig.getKeystorePath()
        assert tlsConfig.getTruststorePath()
        assert tlsConfig.getKeystoreType() == KeystoreType.JKS
        assert tlsConfig.getTruststoreType() == KeystoreType.JKS
        assert tlsConfig.getKeystorePassword() == TEST_KEYSTORE_PASSWORD
    }

    @Test
    void testShouldValidateTlsConfigAndNewKeystoreTruststoreWithoutParams() {
        // Act
        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore()
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert tlsConfig.getKeystorePath()
        assert tlsConfig.getKeyPassword() == tlsConfig.getKeystorePassword()
        assert tlsConfig.getTruststorePassword()
        assert tlsConfig.getKeystoreType() == KeystoreType.PKCS12
        assert tlsConfig.getTruststoreType() == KeystoreType.PKCS12
    }

    @Test
    void testShouldValidateTlsConfigWithoutKeyPasswordParam() {
        // Arrange
        TlsConfiguration tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, null, DEFAULT_STORE_TYPE, null, TEST_TRUSTSTORE_PASSWORD, DEFAULT_STORE_TYPE)

        // Act
        final TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
        deleteKeystoreTruststore(tlsConfig)

        // Assert
        assert tlsConfig.getKeyPassword() == tlsConfig.getKeystorePassword()
    }

    @Test
    void testShouldReturnX509CertWithNewKeystore() {
        // Arrange
        Path keystorePath = KeyStoreUtils.generateTempKeystorePath(DEFAULT_STORE_TYPE)

        // Act
        X509Certificate x509Cert = KeyStoreUtils.createKeyStoreAndGetX509Certificate(KEY_ALIAS, TEST_KEYSTORE_PASSWORD, TEST_KEYSTORE_PASSWORD, keystorePath.toString(), DEFAULT_STORE_TYPE)

        boolean isKeystoreValid = KeyStoreUtils.isStoreValid(keystorePath.toUri().toURL(), DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.toCharArray())
        deletePath(keystorePath)

        // Assert
        final String certDN = x509Cert.getIssuerDN().toString()
        assert certDN =~ "CN="
        assert isKeystoreValid
    }

    @Test
    void testShouldValidateGetKeystoreExtension() {
        // Arrange
        final String EXPECTED_JKS_EXTENSION = ".jks"
        final String EXPECTED_PKCS12_EXTENSION = ".p12"
        final String EXPECTED_BCFKS_EXTENSION = ".bcfks"

        // Act
        String correct_jks_ext = KeyStoreUtils.getKeystoreExtension(KeystoreType.JKS)
        String correct_pkcs12_ext = KeyStoreUtils.getKeystoreExtension(KeystoreType.PKCS12)
        String correct_bcfks_ext = KeyStoreUtils.getKeystoreExtension(KeystoreType.BCFKS)

        // Assert
        assert correct_jks_ext == EXPECTED_JKS_EXTENSION
        assert correct_pkcs12_ext == EXPECTED_PKCS12_EXTENSION
        assert correct_bcfks_ext == EXPECTED_BCFKS_EXTENSION
    }

    private static URL getKeystorePathAsUrl(String path) {
        return new File(path).toURI().toURL()
    }

    private static void deleteKeystoreTruststore(TlsConfiguration tlsConfig) {
        if (tlsConfig != null) {
            try {
                if (StringUtils.isNotBlank(tlsConfig.getKeystorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfig.getKeystorePath()))
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: ${e.getMessage()}, ${e}");
            }

            try {
                if (StringUtils.isNotBlank(tlsConfig.getTruststorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfig.getTruststorePath()))
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: ${e.getMessage()}, ${e}");
            }
        }
    }

    private static void deletePath(Path path) {
        try {
            if (path != null) {
                Files.deleteIfExists(path)
            }
        } catch (IOException e) {
            throw new IOException("There was an error deleting a file at ${path}: ${e.getMessage()}, ${e}");
        }
    }
}
