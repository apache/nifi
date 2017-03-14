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
package org.apache.nifi.security.util.crypto

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.security.util.EncryptionMethod
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import java.security.SecureRandom
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class AESKeyedCipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(AESKeyedCipherProviderGroovyTest.class)

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"

    private static final List<EncryptionMethod> keyedEncryptionMethods = EncryptionMethod.values().findAll { it.keyedCipher }

    private static final SecretKey key = new SecretKeySpec(Hex.decodeHex(KEY_HEX as char[]), "AES")

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

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : keyedEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

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
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
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
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
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
    void testShouldRejectEmptyKey() throws Exception {
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
    void testShouldRejectIncorrectLengthKey() throws Exception {
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
    void testShouldRejectEmptyEncryptionMethod() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider.getCipher(null, key, true)
        }

        // Assert
        assert msg =~ "The encryption method must be specified"
    }

    @Test
    void testShouldRejectUnsupportedEncryptionMethod() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider.getCipher(encryptionMethod, key, true)
        }

        // Assert
        assert msg =~ " requires a PBECipherProvider"
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final String PLAINTEXT = "This is a plaintext message."

        // These values can be generated by running `$ ./openssl_aes.rb` in the terminal
        final byte[] IV = Hex.decodeHex("e0bc8cc7fbc0bdfdc184dc22ce2fcb5b" as char[])
        final byte[] LOCAL_KEY = Hex.decodeHex("c72943d27c3e5a276169c5998a779117" as char[])
        final String CIPHER_TEXT = "a2725ea55c7dd717664d044cab0f0b5f763653e322c27df21954f5be394efb1b"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        SecretKey localKey = new SecretKeySpec(LOCAL_KEY, "AES")

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, localKey, IV, false)
        byte[] recoveredBytes = cipher.doFinal(cipherBytes)
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
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

            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(em, key, false)
            }

            // Assert
            assert msg =~ "Cannot decrypt without a valid IV"
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidIVLengths() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final def INVALID_IVS = (0..15).collect { int length -> new byte[length] }

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        INVALID_IVS.each { byte[] badIV ->
            logger.info("IV: ${Hex.encodeHexString(badIV)} ${badIV.length}")

            // Encrypt should print a warning about the bad IV but overwrite it
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, true)

            // Decrypt should fail
            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, false)
            }
            logger.expected(msg)

            // Assert
            assert msg =~ "Cannot decrypt without a valid IV"
        }
    }

    @Test
    void testGetCipherShouldRejectEmptyIV() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        byte[] badIV = [0x00 as byte] * 16 as byte[]

        // Act
        logger.info("IV: ${Hex.encodeHexString(badIV)} ${badIV.length}")

        // Encrypt should print a warning about the bad IV but overwrite it
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, true)
        logger.info("IV after encrypt: ${Hex.encodeHexString(cipher.getIV())}")

        // Decrypt should fail
        def msg = shouldFail(IllegalArgumentException) {
            cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, false)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Cannot decrypt without a valid IV"
    }
}