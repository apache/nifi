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
import org.junit.AfterClass
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
import java.nio.file.Paths
import java.security.KeyStore
import java.security.cert.Certificate

@RunWith(JUnit4.class)
class KeyStoreUtilsGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtilsGroovyTest.class)

    private static final String TEST_KEYSTORE_PASSWORD = "keystorepassword"
    private static final String TEST_KEY_PASSWORD = "keypassword"
    private static final String TEST_TRUSTSTORE_PASSWORD = "truststorepassword"
    private static final KeystoreType DEFAULT_STORE_TYPE = KeystoreType.JKS
    private static final KeystoreType PKCS12_STORE_TYPE = KeystoreType.PKCS12

    private static TlsConfiguration tlsConfigParam
    private static TlsConfiguration tlsConfiguration

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        tlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, TEST_KEY_PASSWORD, DEFAULT_STORE_TYPE, null, TEST_TRUSTSTORE_PASSWORD, null)

        tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(tlsConfigParam)
    }

    @AfterClass
    static void afterClass() throws Exception {
        deleteKeystoreTruststore(tlsConfiguration);
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
        final URL ksUrl = getKeystorePathAsUrl(tlsConfiguration.getKeystorePath())

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.toCharArray())

        // Assert
        assert keystoreIsValid
    }

    @Test
    void testShouldVerifyKeystoreIsNotValid() {
        // Arrange
        final URL ksUrl = getKeystorePathAsUrl(tlsConfiguration.getKeystorePath())

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.reverse().toCharArray())

        // Assert
        assert !keystoreIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsValid() {
        // Arrange
        final URL ksUrl = getKeystorePathAsUrl(tlsConfiguration.getKeystorePath())

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(ksUrl, DEFAULT_STORE_TYPE, TEST_KEYSTORE_PASSWORD.toCharArray(), TEST_KEY_PASSWORD.toCharArray())

        // Assert
        assert keyPasswordIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsNotValid() {
        // Arrange
        final URL ksUrl = getKeystorePathAsUrl(tlsConfiguration.getKeystorePath())

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(ksUrl, tlsConfiguration.getKeystoreType(), TEST_KEYSTORE_PASSWORD.toCharArray(), TEST_KEY_PASSWORD.reverse().toCharArray())

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
    void testShouldValidateTlsConfigAndNewKeystoreTruststoreWithParams() {
        // Assert
        assert tlsConfiguration.getKeystorePath()
        assert tlsConfiguration.getTruststorePath()
        assert tlsConfiguration.getKeystoreType() == DEFAULT_STORE_TYPE
        assert tlsConfiguration.getTruststoreType() == PKCS12_STORE_TYPE
        assert tlsConfiguration.getKeystorePassword() == TEST_KEYSTORE_PASSWORD
    }

    @Test
    void testShouldValidateTlsConfigAndNewKeystoreTruststoreWithoutParams() {
        // Act
        TlsConfiguration testTlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore()
        deleteKeystoreTruststore(testTlsConfig)

        // Assert
        assert testTlsConfig.getKeystorePath()
        assert testTlsConfig.getKeyPassword() == testTlsConfig.getKeystorePassword()
        assert testTlsConfig.getTruststorePassword()
        assert testTlsConfig.getKeystoreType() == PKCS12_STORE_TYPE
        assert testTlsConfig.getTruststoreType() == PKCS12_STORE_TYPE
    }

    @Test
    void testShouldValidateTlsConfigWithoutKeyPasswordParam() {
        // Arrange
        TlsConfiguration testTlsConfigParam = new StandardTlsConfiguration(null, TEST_KEYSTORE_PASSWORD, null, DEFAULT_STORE_TYPE, null, TEST_TRUSTSTORE_PASSWORD, DEFAULT_STORE_TYPE)

        // Act
        final TlsConfiguration testTlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(testTlsConfigParam)
        deleteKeystoreTruststore(testTlsConfig)

        // Assert
        assert testTlsConfig.getKeyPassword() == testTlsConfig.getKeystorePassword()
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
}
