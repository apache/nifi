/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.web.util

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider
import org.apache.nifi.security.util.crypto.KeyedCipherProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLSession
import java.security.SecureRandom
import java.security.Security
import java.security.cert.X509Certificate

import static groovy.test.GroovyAssert.shouldFail

@Ignore("Not yet implemented")
@RunWith(JUnit4.class)
class NiFiHostnameVerifierTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(NiFiHostnameVerifierTest.class)

    private static final String HOSTNAME = "test.nifi.apache.org"

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldVerifyExactCN() throws Exception {
        // Arrange
       HostnameVerifier hostnameVerifier = new NiFiHostnameVerifier()

        final X509Certificate mockExactCNCertificate = [:] as X509Certificate
        final SSLSession mockSSLS = [:] as SSLSession

        // Act
        boolean hostnameIsValid = hostnameVerifier.verify(HOSTNAME, mockSSLS)
        logger.info("Hostname is valid: ${hostnameIsValid}")

        // Assert
        assert hostnameIsValid
    }

    @Test
    void testShouldFailOnIncorrectCN() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final String plaintext = "This is a plaintext message."

        // Act
        keyedEncryptionMethods.each { EncryptionMethod em ->
            logger.info("Using algorithm: ${em.getAlgorithm()}")
            byte[] iv = cipherProvider.generateIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, iv, true)

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, key, iv, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testShouldVerifyWildcardCN() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.", isUnlimitedStrengthCryptoAvailable())

        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()
        final List<Integer> LONG_KEY_LENGTHS = [192, 256]

        final String plaintext = "This is a plaintext message."

        SecureRandom secureRandom = new SecureRandom()

        // Act
        keyedEncryptionMethods.each { EncryptionMethod em ->
            // Re-use the same IV for the different length keys to ensure the encryption is different
            byte[] iv = cipherProvider.generateIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            LONG_KEY_LENGTHS.each { int keyLength ->
                logger.info("Using algorithm: ${em.getAlgorithm()} with key length ${keyLength}")

                // Generate a key
                byte[] keyBytes = new byte[keyLength / 8]
                secureRandom.nextBytes(keyBytes)
                SecretKey localKey = new SecretKeySpec(keyBytes, "AES")
                logger.info("Key: ${Hex.encodeHexString(keyBytes)} ${keyBytes.length}")

                // Initialize a cipher for encryption
                Cipher cipher = cipherProvider.getCipher(em, localKey, iv, true)

                byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
                logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

                cipher = cipherProvider.getCipher(em, localKey, iv, false)
                byte[] recoveredBytes = cipher.doFinal(cipherBytes)
                String recovered = new String(recoveredBytes, "UTF-8")
                logger.info("Recovered: ${recovered}")

                // Assert
                assert plaintext.equals(recovered)
            }
        }
    }

    @Test
    void testShouldVerifyExactSAN() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider.getCipher(encryptionMethod, null, true)
        }

        // Assert
        assert msg =~ "The key must be specified"
    }

    @Test
    void testShouldFailOnIncorrectSAN() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        SecretKey localKey = new SecretKeySpec(Hex.decodeHex("0123456789ABCDEF" as char[]), "AES")
        assert ![128, 192, 256].contains(localKey.encoded.length)

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider.getCipher(encryptionMethod, localKey, true)
        }

        // Assert
        assert msg =~ "The key must be of length \\[128, 192, 256\\]"
    }

    @Test
    void testShouldVerifyWildcardSAN() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider.getCipher(null, key, true)
        }

        // Assert
        assert msg =~ "The encryption method must be specified"
    }
}