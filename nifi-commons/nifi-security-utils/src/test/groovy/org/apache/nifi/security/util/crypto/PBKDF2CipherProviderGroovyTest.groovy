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
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail
import static org.junit.Assert.assertTrue

@RunWith(JUnit4.class)
class PBKDF2CipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProviderGroovyTest.class)

    private static List<EncryptionMethod> strongKDFEncryptionMethods

    public static final String MICROBENCHMARK = "microbenchmark"
    private static final int DEFAULT_KEY_LENGTH = 128
    private static final int TEST_ITERATION_COUNT = 1000
    private final String DEFAULT_PRF = "SHA-512"
    private final String SALT_HEX = "0123456789ABCDEFFEDCBA9876543210"
    private final String IV_HEX = "01" * 16
    private static ArrayList<Integer> AES_KEY_LENGTHS

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        strongKDFEncryptionMethods = EncryptionMethod.values().findAll { it.isCompatibleWithStrongKDFs() }

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        if (CipherUtility.isUnlimitedStrengthCryptoSupported()) {
            AES_KEY_LENGTHS = [128, 192, 256]
        } else {
            AES_KEY_LENGTHS = [128]
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final def INVALID_IVS = (0..15).collect { int length -> new byte[length] }

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        INVALID_IVS.each { byte[] badIV ->
            logger.info("IV: ${Hex.encodeHexString(badIV)} ${badIV.length}")

            // Encrypt should print a warning about the bad IV but overwrite it
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, badIV, DEFAULT_KEY_LENGTH, true)

            // Decrypt should fail
            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, badIV, DEFAULT_KEY_LENGTH, false)
            }

            // Assert
            assert msg =~ "Cannot decrypt without a valid IV"
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false)
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
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                CipherUtility.isUnlimitedStrengthCryptoSupported())

        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])

        final int LONG_KEY_LENGTH = 256

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, LONG_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, LONG_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testShouldRejectEmptyPRF() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String plaintext = "This is a plaintext message."
        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        String prf = ""

        // Act
        logger.info("Using PRF ${prf}")
        def msg = shouldFail(IllegalArgumentException) {
            cipherProvider = new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT)
        }

        // Assert
        assert msg =~ "Cannot resolve empty PRF"
    }

    @Test
    void testShouldResolveDefaultPRF() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String plaintext = "This is a plaintext message."
        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        final PBKDF2CipherProvider SHA512_PROVIDER = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        String prf = "sha768"
        logger.info("Using ${prf}")

        // Act
        cipherProvider = new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT)
        logger.info("Resolved PRF to ${cipherProvider.getPRFName()}")
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")

        // Initialize a cipher for encryption
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
        logger.info("IV: ${Hex.encodeHexString(IV)}")

        byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        cipher = SHA512_PROVIDER.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false)
        byte[] recoveredBytes = cipher.doFinal(cipherBytes)
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: ${recovered}")

        // Assert
        assert plaintext.equals(recovered)
    }

    @Test
    void testShouldResolveVariousPRFs() throws Exception {
        // Arrange
        final List<String> PRFS = ["SHA-1", "MD5", "SHA-256", "SHA-384", "SHA-512"]
        RandomIVPBECipherProvider cipherProvider

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String plaintext = "This is a plaintext message."
        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        PRFS.each { String prf ->
            logger.info("Using ${prf}")
            cipherProvider = new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT)
            logger.info("Resolved PRF to ${cipherProvider.getPRFName()}")

            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", TEST_ITERATION_COUNT)

        final String PLAINTEXT = "This is a plaintext message."
        final String PASSWORD = "thisIsABadPassword"

        // These values can be generated by running `$ ./openssl_pbkdf2.rb` in the terminal
        final byte[] SALT = Hex.decodeHex("ae2481bee3d8b5d5b732bf464ea2ff01" as char[])
        final byte[] IV = Hex.decodeHex("26db997dcd18472efd74dabe5ff36853" as char[])

        final String CIPHER_TEXT = "92edbabae06add6275a1d64815755a9ba52afc96e2c1a316d3abbe1826e96f6c"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false)
        byte[] recoveredBytes = cipher.doFinal(cipherBytes)
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, false)
            }

            // Assert
            assert msg =~ "Cannot decrypt without a valid IV"
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "thisIsABadPassword"

        final def INVALID_SALTS = ['pbkdf2', '$3a$11$', 'x', '$2a$10$', '', null]

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")

        // Act
        INVALID_SALTS.each { String salt ->
            logger.info("Checking salt ${salt}")

            def msg = shouldFail(IllegalArgumentException) {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt?.bytes, DEFAULT_KEY_LENGTH, true)
            }

            // Assert
            assert msg =~ "The salt must be at least 16 bytes\\. To generate a salt, use PBKDF2CipherProvider#generateSalt"
        }
    }

    @Test
    void testGetCipherShouldAcceptValidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        final String PLAINTEXT = "This is a plaintext message."

        // Currently only AES ciphers are compatible with PBKDF2, so redundant to test all algorithms
        final def VALID_KEY_LENGTHS = AES_KEY_LENGTHS
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        VALID_KEY_LENGTHS.each { int keyLength ->
            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()} with key length ${keyLength}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldNotAcceptInvalidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = Hex.decodeHex(SALT_HEX as char[])
        final byte[] IV = Hex.decodeHex(IV_HEX as char[])

        // Currently only AES ciphers are compatible with PBKDF2, so redundant to test all algorithms
        final def VALID_KEY_LENGTHS = [-1, 40, 64, 112, 512]
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        VALID_KEY_LENGTHS.each { int keyLength ->
            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()} with key length ${keyLength}")

            // Initialize a cipher for encryption
            def msg = shouldFail(IllegalArgumentException) {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, true)
            }

            // Assert
            assert msg =~ "${keyLength} is not a valid key length for AES"
        }
    }

    @Ignore("This test can be run on a specific machine to evaluate if the default iteration count is sufficient")
    @Test
    void testDefaultConstructorShouldProvideStrongIterationCount() {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider()

        // Values taken from http://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/ and http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt

        // Calculate the iteration count to reach 500 ms
        int minimumIterationCount = calculateMinimumIterationCount()
        logger.info("Determined minimum safe iteration count to be ${minimumIterationCount}")

        // Act
        int iterationCount = cipherProvider.getIterationCount()
        logger.info("Default iteration count ${iterationCount}")

        // Assert
        assertTrue("The default iteration count for PBKDF2CipherProvider is too weak. Please update the default value to a stronger level.", iterationCount >= minimumIterationCount)
    }

    /**
     * Returns the iteration count required for a derivation to exceed 500 ms on this machine using the default PRF.
     * Code adapted from http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
     *
     * @return the minimum iteration count
     */
    private static int calculateMinimumIterationCount() {
        // High start-up cost, so run multiple times for better benchmarking
        final int RUNS = 10

        // Benchmark using an iteration count of 10k
        int iterationCount = 10_000

        final byte[] SALT = [0x00 as byte] * 16
        final byte[] IV = [0x01 as byte] * 16

        String defaultPrf = new PBKDF2CipherProvider().getPRFName()
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(defaultPrf, iterationCount)

        // Run once to prime the system
        double duration = time {
            Cipher cipher = cipherProvider.getCipher(EncryptionMethod.AES_CBC, MICROBENCHMARK, SALT, IV, DEFAULT_KEY_LENGTH, false)
        }
        logger.info("First run of iteration count ${iterationCount} took ${duration} ms (ignored)")

        def durations = []

        RUNS.times { int i ->
            duration = time {
                // Use encrypt mode with provided salt and IV to minimize overhead during benchmark call
                Cipher cipher = cipherProvider.getCipher(EncryptionMethod.AES_CBC, "${MICROBENCHMARK}${i}", SALT, IV, DEFAULT_KEY_LENGTH, false)
            }
            logger.info("Iteration count ${iterationCount} took ${duration} ms")
            durations << duration
        }

        duration = durations.sum() / durations.size()
        logger.info("Iteration count ${iterationCount} averaged ${duration} ms")

        // Keep increasing iteration count until the estimated duration is over 500 ms
        while (duration < 500) {
            iterationCount *= 2
            duration *= 2
        }

        logger.info("Returning iteration count ${iterationCount} for ${duration} ms")

        return iterationCount
    }

    private static double time(Closure c) {
        long start = System.nanoTime()
        c.call()
        long end = System.nanoTime()
        return (end - start) / 1_000_000.0
    }

    @Test
    void testGenerateSaltShouldProvideValidSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT)

        // Act
        byte[] salt = cipherProvider.generateSalt()
        logger.info("Checking salt ${Hex.encodeHexString(salt)}")

        // Assert
        assert salt.length == 16
        assert salt != [(0x00 as byte) * 16]
    }
}