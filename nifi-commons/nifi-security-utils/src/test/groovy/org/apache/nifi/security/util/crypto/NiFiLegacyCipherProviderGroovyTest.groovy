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
package org.apache.nifi.security.util.crypto

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.security.util.EncryptionMethod
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
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec
import java.security.Security

import static org.junit.Assert.fail

@RunWith(JUnit4.class)
class NiFiLegacyCipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(NiFiLegacyCipherProviderGroovyTest.class)

    private static List<EncryptionMethod> pbeEncryptionMethods = new ArrayList<>()
    private static List<EncryptionMethod> limitedStrengthPbeEncryptionMethods = new ArrayList<>()

    private static final String PROVIDER_NAME = "BC"
    private static final int ITERATION_COUNT = 1000

    private static final byte[] SALT_16_BYTES = Hex.decodeHex("aabbccddeeff00112233445566778899".toCharArray())

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        pbeEncryptionMethods = EncryptionMethod.values().findAll { it.algorithm.toUpperCase().startsWith("PBE") }
        limitedStrengthPbeEncryptionMethods = pbeEncryptionMethods.findAll { !it.isUnlimitedStrength() }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {

    }

    private static Cipher getLegacyCipher(String password, byte[] salt, String algorithm) {
        try {
            final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray())
            final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, PROVIDER_NAME)
            SecretKey tempKey = factory.generateSecret(pbeKeySpec)

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, ITERATION_COUNT)
            Cipher cipher = Cipher.getInstance(algorithm, PROVIDER_NAME)
            cipher.init(Cipher.ENCRYPT_MODE, tempKey, parameterSpec)
            return cipher
        } catch (Exception e) {
            logger.error("Error generating legacy cipher", e)
            fail(e.getMessage())
        }

        return null
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()

        final String PASSWORD = "shortPassword"
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod encryptionMethod : limitedStrengthPbeEncryptionMethods) {
            logger.info("Using algorithm: {}", encryptionMethod.getAlgorithm())

            if (!CipherUtility.passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(PASSWORD.length(), encryptionMethod)) {
                logger.warn("This test is skipped because the password length exceeds the undocumented limit BouncyCastle imposes on a JVM with limited strength crypto policies")
                continue
            }

            byte[] salt = cipherProvider.generateSalt(encryptionMethod)
            logger.info("Generated salt ${Hex.encodeHexString(salt)} (${salt.length})")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt, true)

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length)

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                CipherUtility.isUnlimitedStrengthCryptoSupported())

        NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()

        final String PASSWORD = "shortPassword"
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod encryptionMethod : pbeEncryptionMethods) {
            logger.info("Using algorithm: {}", encryptionMethod.getAlgorithm())

            byte[] salt = cipherProvider.generateSalt(encryptionMethod)
            logger.info("Generated salt ${Hex.encodeHexString(salt)} (${salt.length})")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt, true)

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length)

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldSupportLegacyCode() throws Exception {
        // Arrange
        NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()

        final String PASSWORD = "short"
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod encryptionMethod : limitedStrengthPbeEncryptionMethods) {
            logger.info("Using algorithm: {}", encryptionMethod.getAlgorithm())

            if (!CipherUtility.passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(PASSWORD.length(), encryptionMethod)) {
                logger.warn("This test is skipped because the password length exceeds the undocumented limit BouncyCastle imposes on a JVM with limited strength crypto policies")
                continue
            }

            byte[] salt = cipherProvider.generateSalt(encryptionMethod)
            logger.info("Generated salt ${Hex.encodeHexString(salt)} (${salt.length})")

            // Initialize a legacy cipher for encryption
            Cipher legacyCipher = getLegacyCipher(PASSWORD, salt, encryptionMethod.getAlgorithm())

            byte[] cipherBytes = legacyCipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length)

            Cipher providedCipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt, false)
            byte[] recoveredBytes = providedCipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherWithoutSaltShouldSupportLegacyCode() throws Exception {
        // Arrange
        NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()

        final String PASSWORD = "short"
        final byte[] SALT = new byte[0]

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            logger.info("Using algorithm: {}", em.getAlgorithm())

            if (!CipherUtility.passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(PASSWORD.length(), em)) {
                logger.warn("This test is skipped because the password length exceeds the undocumented limit BouncyCastle imposes on a JVM with limited strength crypto policies")
                continue
            }

            // Initialize a legacy cipher for encryption
            Cipher legacyCipher = getLegacyCipher(PASSWORD, SALT, em.getAlgorithm())

            byte[] cipherBytes = legacyCipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length)

            Cipher providedCipher = cipherProvider.getCipher(em, PASSWORD, false)
            byte[] recoveredBytes = providedCipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldIgnoreKeyLength() throws Exception {
        // Arrange
        NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()

        final String PASSWORD = "shortPassword"
        final byte[] SALT = SALT_16_BYTES

        final String plaintext = "This is a plaintext message."

        final def KEY_LENGTHS = [-1, 40, 64, 128, 192, 256]

        // Initialize a cipher for encryption
        EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES
        final Cipher cipher128 = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, true)
        byte[] cipherBytes = cipher128.doFinal(plaintext.getBytes("UTF-8"))
        logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length)

        // Act
        KEY_LENGTHS.each { int keyLength ->
            logger.info("Decrypting with 'requested' key length: ${keyLength}")

            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, keyLength, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    /**
     * This test determines for each PBE encryption algorithm if it actually requires the JCE unlimited strength jurisdiction policies to be installed.
     * Even some algorithms that use 128-bit keys (which should be allowed on all systems) throw exceptions because BouncyCastle derives the key
     * from the password using a long digest result at the time of key length checking.
     * @throws IOException
     */
    @Ignore("Only needed once to determine max supported password lengths")
    @Test
    void testShouldDetermineDependenceOnUnlimitedStrengthCrypto() throws IOException {
        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        boolean unlimitedCryptoSupported = PasswordBasedEncryptor.supportsUnlimitedStrength()
        logger.info("This JVM supports unlimited strength crypto: ${unlimitedCryptoSupported}")

        def longestSupportedPasswordByEM = [:]

        encryptionMethods.each { EncryptionMethod encryptionMethod ->
            logger.info("Attempting ${encryptionMethod.name()} (${encryptionMethod.algorithm}) which claims unlimited strength required: ${encryptionMethod.unlimitedStrength}")

            (1..20).find { int length ->
                String password = "x" * length

                try {
                    NiFiLegacyCipherProvider cipherProvider = new NiFiLegacyCipherProvider()
                    Cipher cipher = cipherProvider.getCipher(encryptionMethod, password, true)
                    return false
                } catch (Exception e) {
                    logger.error("Unable to create the cipher with ${encryptionMethod.algorithm} and password ${password} (${password.length()}) due to ${e.getMessage()}")
                    if (!longestSupportedPasswordByEM.containsKey(encryptionMethod)) {
                        longestSupportedPasswordByEM.put(encryptionMethod, password.length() - 1)
                    }
                    return true
                }
            }
            logger.info("\n")
        }

        logger.info("Longest supported password by encryption method:")
        longestSupportedPasswordByEM.each { EncryptionMethod encryptionMethod, int length ->
            logger.info("\t${encryptionMethod.algorithm}\t${length}")
        }
    }
}