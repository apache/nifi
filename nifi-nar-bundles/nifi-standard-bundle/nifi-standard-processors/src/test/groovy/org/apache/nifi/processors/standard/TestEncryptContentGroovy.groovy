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
package org.apache.nifi.processors.standard

import org.apache.nifi.components.ValidationResult
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.apache.nifi.security.util.crypto.CipherUtility
import org.apache.nifi.security.util.crypto.PasswordBasedEncryptor
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.security.Security

@RunWith(JUnit4.class)
public class TestEncryptContentGroovy {
    private static final Logger logger = LoggerFactory.getLogger(TestEncryptContentGroovy.class)

    private static final String WEAK_CRYPTO_ALLOWED = EncryptContent.WEAK_CRYPTO_ALLOWED_NAME
    private static final String WEAK_CRYPTO_NOT_ALLOWED = EncryptContent.WEAK_CRYPTO_NOT_ALLOWED_NAME

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testShouldValidateMaxKeySizeForAlgorithmsOnUnlimitedStrengthJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                PasswordBasedEncryptor.supportsUnlimitedStrength());

        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Integer.MAX_VALUE or 128, so use 256 or 128
        final int MAX_KEY_LENGTH = [PasswordBasedEncryptor.getMaxAllowedKeyLength(encryptionMethod.algorithm), 256].min()
        final String TOO_LONG_KEY_HEX = "ab" * (MAX_KEY_LENGTH / 8 + 1)
        logger.info("Using key ${TOO_LONG_KEY_HEX} (${TOO_LONG_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())
        runner.setProperty(EncryptContent.RAW_KEY_HEX, TOO_LONG_KEY_HEX)

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        results = pc.validate()

        // Assert
        Assert.assertEquals(1, results.size())
        logger.expected(results)
        ValidationResult vr = results.first()

        String expectedResult = "'raw-key-hex' is invalid because Key must be valid length [128, 192, 256]"
        String message = "'" + vr.toString() + "' contains '" + expectedResult + "'"
        Assert.assertTrue(message, vr.toString().contains(expectedResult))
    }

    @Test
    public void testShouldValidateMaxKeySizeForAlgorithmsOnLimitedStrengthJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Test is being skipped because this JVM supports unlimited strength crypto.",
                !PasswordBasedEncryptor.supportsUnlimitedStrength());

        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        final int MAX_KEY_LENGTH = 128
        final String TOO_LONG_KEY_HEX = "ab" * (MAX_KEY_LENGTH / 8 + 1)
        logger.info("Using key ${TOO_LONG_KEY_HEX} (${TOO_LONG_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())
        runner.setProperty(EncryptContent.RAW_KEY_HEX, TOO_LONG_KEY_HEX)

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        results = pc.validate()

        // Assert

        // Two validation problems -- max key size and key length is invalid
        Assert.assertEquals(2, results.size())
        logger.expected(results)
        ValidationResult maxKeyLengthVR = results.first()

        String expectedResult = "'raw-key-hex' is invalid because Key length greater than ${MAX_KEY_LENGTH} bits is not supported"
        String message = "'" + maxKeyLengthVR.toString() + "' contains '" + expectedResult + "'"
        Assert.assertTrue(message, maxKeyLengthVR.toString().contains(expectedResult))

        expectedResult = "'raw-key-hex' is invalid because Key must be valid length [128, 192, 256]"
        ValidationResult keyLengthInvalidVR = results.last()
        message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'"
        Assert.assertTrue(message, keyLengthInvalidVR.toString().contains(expectedResult))
    }

    @Test
    public void testShouldValidateKeyFormatAndSizeForAlgorithms() throws IOException {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        final int INVALID_KEY_LENGTH = 120
        final String INVALID_KEY_HEX = "ab" * (INVALID_KEY_LENGTH / 8)
        logger.info("Using key ${INVALID_KEY_HEX} (${INVALID_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())
        runner.setProperty(EncryptContent.RAW_KEY_HEX, INVALID_KEY_HEX)

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        results = pc.validate()

        // Assert
        Assert.assertEquals(1, results.size())
        logger.expected(results)
        ValidationResult keyLengthInvalidVR = results.first()

        String expectedResult = "'raw-key-hex' is invalid because Key must be valid length [128, 192, 256]"
        String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'"
        Assert.assertTrue(message, keyLengthInvalidVR.toString().contains(expectedResult))
    }

    @Test
    public void testShouldValidateKDFWhenKeyedCipherSelected() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.isKeyedCipher() }

        final int VALID_KEY_LENGTH = 128
        final String VALID_KEY_HEX = "ab" * (VALID_KEY_LENGTH / 8)
        logger.info("Using key ${VALID_KEY_HEX} (${VALID_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        encryptionMethods.each { EncryptionMethod encryptionMethod ->
            logger.info("Trying encryption method ${encryptionMethod.name()}")
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())

            final def INVALID_KDFS = [KeyDerivationFunction.NIFI_LEGACY, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY]
            INVALID_KDFS.each { KeyDerivationFunction invalidKDF ->
                logger.info("Trying KDF ${invalidKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, invalidKDF.name())
                runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX)

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                Assert.assertEquals(1, results.size())
                logger.expected(results)
                ValidationResult keyLengthInvalidVR = results.first()

                String expectedResult = "'key-derivation-function' is invalid because Key Derivation Function is required to be NONE, BCRYPT, SCRYPT, PBKDF2 when using " +
                        "algorithm ${encryptionMethod.algorithm}"
                String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'"
                Assert.assertTrue(message, keyLengthInvalidVR.toString().contains(expectedResult))
            }

            final def VALID_KDFS = [KeyDerivationFunction.NONE, KeyDerivationFunction.BCRYPT, KeyDerivationFunction.SCRYPT, KeyDerivationFunction.PBKDF2]
            VALID_KDFS.each { KeyDerivationFunction validKDF ->
                logger.info("Trying KDF ${validKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, validKDF.name())

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                Assert.assertEquals(0, results.size())
            }
        }
    }

    @Test
    public void testShouldValidateKDFWhenPBECipherSelected() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc
        final String PASSWORD = "short"

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }
        if (!PasswordBasedEncryptor.supportsUnlimitedStrength()) {
            // Remove all unlimited strength algorithms
            encryptionMethods.removeAll { it.unlimitedStrength }
        }

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.PASSWORD, PASSWORD)
        runner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_ALLOWED)

        encryptionMethods.each { EncryptionMethod encryptionMethod ->
            logger.info("Trying encryption method ${encryptionMethod.name()}")
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())

            final def INVALID_KDFS = [KeyDerivationFunction.NONE, KeyDerivationFunction.BCRYPT, KeyDerivationFunction.SCRYPT, KeyDerivationFunction.PBKDF2]
            INVALID_KDFS.each { KeyDerivationFunction invalidKDF ->
                logger.info("Trying KDF ${invalidKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, invalidKDF.name())

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                logger.expected(results)
                Assert.assertEquals(1, results.size())
                ValidationResult keyLengthInvalidVR = results.first()

                String expectedResult = "'Key Derivation Function' is invalid because Key Derivation Function is required to be NIFI_LEGACY, OPENSSL_EVP_BYTES_TO_KEY when using " +
                        "algorithm ${encryptionMethod.algorithm}"
                String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'"
                Assert.assertTrue(message, keyLengthInvalidVR.toString().contains(expectedResult))
            }

            final def VALID_KDFS = [KeyDerivationFunction.NIFI_LEGACY, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY]
            VALID_KDFS.each { KeyDerivationFunction validKDF ->
                logger.info("Trying KDF ${validKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, validKDF.name())

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                Assert.assertEquals(0, results.size())
            }
        }
    }

    @Test
    public void testRoundTrip() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        final String RAW_KEY_HEX = "ab" * 16
        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, RAW_KEY_HEX)
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())

        def keyedCipherEMs = EncryptionMethod.values().findAll { it.isKeyedCipher() }

        keyedCipherEMs.each { EncryptionMethod encryptionMethod ->
            logger.info("Attempting {}", encryptionMethod.name())
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

            testRunner.enqueue(Paths.get("src/test/resources/hello.txt"))
            testRunner.clearTransferState()
            testRunner.run()

            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)

            MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
            testRunner.assertQueueEmpty()

            testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE)
            testRunner.enqueue(flowFile)
            testRunner.clearTransferState()
            testRunner.run()
            testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)

            logger.info("Successfully decrypted {}", encryptionMethod.name())

            flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
            flowFile.assertContentEquals(new File("src/test/resources/hello.txt"))
        }
    }

    @Test
    public void testShouldCheckMaximumLengthOfPasswordOnLimitedStrengthCryptoJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Only run on systems with limited strength crypto", !PasswordBasedEncryptor.supportsUnlimitedStrength())

        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name())
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_ALLOWED)

        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        // Use .find instead of .each to allow "breaks" using return false
        encryptionMethods.find { EncryptionMethod encryptionMethod ->
            def invalidPasswordLength = CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) + 1
            String tooLongPassword = "x" * invalidPasswordLength
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                return false   // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            testRunner.setProperty(EncryptContent.PASSWORD, tooLongPassword)
            logger.info("Attempting ${encryptionMethod.algorithm} with password of length ${invalidPasswordLength}")
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

            testRunner.clearTransferState()
            testRunner.enqueue(new byte[0])
            pc = (MockProcessContext) testRunner.getProcessContext()

            // Act
            results = pc.validate()

            // Assert
            logger.expected(results)
            Assert.assertEquals(1, results.size())
            ValidationResult passwordLengthVR = results.first()

            String expectedResult = "'Password' is invalid because Password length greater than ${invalidPasswordLength - 1} characters is not supported by" +
                    " this JVM due to lacking JCE Unlimited Strength Jurisdiction Policy files."
            String message = "'" + passwordLengthVR.toString() + "' contains '" + expectedResult + "'"
            Assert.assertTrue(message, passwordLengthVR.toString().contains(expectedResult))
        }
    }

    @Test
    public void testShouldCheckLengthOfPasswordWhenNotAllowed() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name())

        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength()
        boolean allowWeakCrypto = false
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_NOT_ALLOWED)

        // Use .find instead of .each to allow "breaks" using return false
        encryptionMethods.find { EncryptionMethod encryptionMethod ->
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            def shortPasswordLength = [PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1, CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1].min()
            String shortPassword = "x" * shortPasswordLength
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                return false   // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            testRunner.setProperty(EncryptContent.PASSWORD, shortPassword)
            logger.info("Attempting ${encryptionMethod.algorithm} with password of length ${shortPasswordLength}")
            logger.state("Limited strength crypto ${limitedStrengthCrypto} and allow weak crypto: ${allowWeakCrypto}")
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

            testRunner.clearTransferState()
            testRunner.enqueue(new byte[0])
            pc = (MockProcessContext) testRunner.getProcessContext()

            // Act
            results = pc.validate()

            // Assert
            logger.expected(results)
            Assert.assertEquals(1, results.size())
            ValidationResult passwordLengthVR = results.first()

            String expectedResult = "'Password' is invalid because Password length less than ${PasswordBasedEncryptor.getMinimumSafePasswordLength()} characters is potentially unsafe. " +
                    "See Admin Guide."
            String message = "'" + passwordLengthVR.toString() + "' contains '" + expectedResult + "'"
            Assert.assertTrue(message, passwordLengthVR.toString().contains(expectedResult))
        }
    }

    @Test
    public void testShouldNotCheckLengthOfPasswordWhenAllowed() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name())

        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength()
        boolean allowWeakCrypto = true
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_ALLOWED)

        // Use .find instead of .each to allow "breaks" using return false
        encryptionMethods.find { EncryptionMethod encryptionMethod ->
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            def shortPasswordLength = [PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1, CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1].min()
            String shortPassword = "x" * shortPasswordLength
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                return false   // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
            }

            testRunner.setProperty(EncryptContent.PASSWORD, shortPassword)
            logger.info("Attempting ${encryptionMethod.algorithm} with password of length ${shortPasswordLength}")
            logger.state("Limited strength crypto ${limitedStrengthCrypto} and allow weak crypto: ${allowWeakCrypto}")
            testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
            testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

            testRunner.clearTransferState()
            testRunner.enqueue(new byte[0])
            pc = (MockProcessContext) testRunner.getProcessContext()

            // Act
            results = pc.validate()

            // Assert
            Assert.assertEquals(results.toString(), 0, results.size())
        }
    }
}
