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
package org.apache.nifi.encrypt

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider
import org.apache.nifi.security.util.crypto.KeyedCipherProvider
import org.apache.nifi.util.NiFiProperties
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
import java.security.SecureRandom
import java.security.Security

@RunWith(JUnit4.class)
class StringEncryptorTest {
    private static final Logger logger = LoggerFactory.getLogger(StringEncryptorTest.class)

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"

    private static final List<EncryptionMethod> keyedEncryptionMethods = EncryptionMethod.values().findAll {
        it.keyedCipher
    }
    private static final List<EncryptionMethod> pbeEncryptionMethods = EncryptionMethod.values().findAll {
        it.algorithm =~ "PBE"
    }

    private static final SecretKey key = new SecretKeySpec(Hex.decodeHex(KEY_HEX as char[]), "AES")

    private static final String KEY = "nifi.sensitive.props.key"
    private static final String ALGORITHM = "nifi.sensitive.props.algorithm"
    private static final String PROVIDER = "nifi.sensitive.props.provider"

    private static final String DEFAULT_ALGORITHM = "PBEWITHMD5AND128BITAES-CBC-OPENSSL"
    private static final String DEFAULT_PROVIDER = "BC"
    private static final String DEFAULT_PASSWORD = "nififtw!"
    private static final String OTHER_PASSWORD = "thisIsABadPassword"
    private static
    final Map RAW_PROPERTIES = [(ALGORITHM): DEFAULT_ALGORITHM, (PROVIDER): DEFAULT_PROVIDER, (KEY): DEFAULT_PASSWORD]
    private static final NiFiProperties STANDARD_PROPERTIES = new StandardNiFiProperties(new Properties(RAW_PROPERTIES))

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
    void testEncryptionShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : pbeEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")
            NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties(RAW_PROPERTIES + [(ALGORITHM): em.algorithm]))
            StringEncryptor encryptor = StringEncryptor.createEncryptor(niFiProperties)

            String cipherText = encryptor.encrypt(plaintext)
            logger.info("Cipher text: ${cipherText}")

            String recovered = encryptor.decrypt(cipherText)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    @Ignore("Not yet implemented")
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

    @Ignore("Not yet implemented")
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
}
