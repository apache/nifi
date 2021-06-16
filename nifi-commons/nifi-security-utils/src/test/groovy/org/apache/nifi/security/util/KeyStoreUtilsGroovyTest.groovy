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
import java.security.KeyStore
import java.security.cert.Certificate

@RunWith(JUnit4.class)
class KeyStoreUtilsGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtilsGroovyTest.class)

    private static final File KEYSTORE_FILE = new File("src/test/resources/keystore.jks")
    private static final String KEYSTORE_PASSWORD = "passwordpassword"
    private static final String KEY_PASSWORD = "keypassword"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

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

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray())

        // Assert
        assert keystoreIsValid
    }

    @Test
    void testShouldVerifyKeystoreIsNotValid() {
        // Arrange

        // Act
        boolean keystoreIsValid = KeyStoreUtils.isStoreValid(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.reverse().toCharArray())

        // Assert
        assert !keystoreIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsValid() {
        // Arrange

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray(), KEYSTORE_PASSWORD.toCharArray())

        // Assert
        assert keyPasswordIsValid
    }

    @Test
    void testShouldVerifyKeyPasswordIsNotValid() {
        // Arrange

        // Act
        boolean keyPasswordIsValid = KeyStoreUtils.isKeyPasswordCorrect(KEYSTORE_FILE.toURI().toURL(), KEYSTORE_TYPE, KEYSTORE_PASSWORD.toCharArray(), KEYSTORE_PASSWORD.reverse().toCharArray())

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
}
