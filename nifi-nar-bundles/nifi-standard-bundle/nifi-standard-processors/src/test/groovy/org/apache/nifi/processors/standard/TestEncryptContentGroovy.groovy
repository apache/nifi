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

import groovy.time.TimeCategory
import groovy.time.TimeDuration
import org.apache.commons.codec.binary.Hex
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.apache.nifi.security.util.crypto.Argon2CipherProvider
import org.apache.nifi.security.util.crypto.Argon2SecureHasher
import org.apache.nifi.security.util.crypto.CipherUtility
import org.apache.nifi.security.util.crypto.KeyedEncryptor
import org.apache.nifi.security.util.crypto.PasswordBasedEncryptor
import org.apache.nifi.security.util.crypto.RandomIVPBECipherProvider
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

import javax.crypto.Cipher
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.Security
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.temporal.ChronoUnit

@RunWith(JUnit4.class)
class TestEncryptContentGroovy {
    private static final Logger logger = LoggerFactory.getLogger(TestEncryptContentGroovy.class)

    private static final String WEAK_CRYPTO_ALLOWED = EncryptContent.WEAK_CRYPTO_ALLOWED_NAME
    private static final String WEAK_CRYPTO_NOT_ALLOWED = EncryptContent.WEAK_CRYPTO_NOT_ALLOWED_NAME

    private static final List<EncryptionMethod> SUPPORTED_KEYED_ENCRYPTION_METHODS = EncryptionMethod.values().findAll { it.isKeyedCipher() && it != EncryptionMethod.AES_CBC_NO_PADDING }

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
    void testShouldValidateMaxKeySizeForAlgorithmsOnUnlimitedStrengthJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                CipherUtility.isUnlimitedStrengthCryptoSupported())

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
    void testShouldValidateMaxKeySizeForAlgorithmsOnLimitedStrengthJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Test is being skipped because this JVM supports unlimited strength crypto.",
                !CipherUtility.isUnlimitedStrengthCryptoSupported())

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
    void testShouldValidateKeyFormatAndSizeForAlgorithms() throws IOException {
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
    void testShouldValidateKDFWhenKeyedCipherSelected() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        final int VALID_KEY_LENGTH = 128
        final String VALID_KEY_HEX = "ab" * (VALID_KEY_LENGTH / 8)
        logger.info("Using key ${VALID_KEY_HEX} (${VALID_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        SUPPORTED_KEYED_ENCRYPTION_METHODS.each { EncryptionMethod encryptionMethod ->
            logger.info("Trying encryption method ${encryptionMethod.name()}")
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())

            // Scenario 1: Legacy KDF + keyed cipher -> validation error
            final def INVALID_KDFS = [KeyDerivationFunction.NIFI_LEGACY, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY]
            INVALID_KDFS.each { KeyDerivationFunction invalidKDF ->
                logger.info("Trying KDF ${invalidKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, invalidKDF.name())
                runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX)
                runner.removeProperty(EncryptContent.PASSWORD)

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                logger.expected(results)
                assert results.size() == 1
                ValidationResult keyLengthInvalidVR = results.first()

                String expectedResult = "'key-derivation-function' is invalid because Key Derivation Function is required to be BCRYPT, SCRYPT, PBKDF2, ARGON2, NONE when using " +
                        "algorithm ${encryptionMethod.algorithm}"
                String message = "'" + keyLengthInvalidVR.toString() + "' contains '" + expectedResult + "'"
                assert keyLengthInvalidVR.toString().contains(expectedResult)
            }

            // Scenario 2: No KDF + keyed cipher + raw-key-hex -> valid
            def none = KeyDerivationFunction.NONE
            logger.info("Trying KDF ${none.name()}")

            runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, none.name())
            runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX)
            runner.removeProperty(EncryptContent.PASSWORD)

            runner.enqueue(new byte[0])
            pc = (MockProcessContext) runner.getProcessContext()

            // Act
            results = pc.validate()

            // Assert
            assert results.isEmpty()

            // Scenario 3: Strong KDF + keyed cipher + password -> valid
            final def VALID_KDFS = [KeyDerivationFunction.BCRYPT, KeyDerivationFunction.SCRYPT, KeyDerivationFunction.PBKDF2, KeyDerivationFunction.ARGON2]
            VALID_KDFS.each { KeyDerivationFunction validKDF ->
                logger.info("Trying KDF ${validKDF.name()}")

                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, validKDF.name())
                runner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword")
                runner.removeProperty(EncryptContent.RAW_KEY_HEX)

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                assert results.isEmpty()
            }
        }
    }

    @Test
    void testKDFShouldDefaultToNone() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        String defaultKDF = pc.getProperty("key-derivation-function").getValue()

        // Assert
        assert defaultKDF == KeyDerivationFunction.NONE.name()
    }

    @Test
    void testEMShouldDefaultToAES_GCM() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        String defaultEM = pc.getProperty("Encryption Algorithm").getValue()

        // Assert
        assert defaultEM == EncryptionMethod.AES_GCM.name()
    }

    @Test
    void testShouldValidateKeyMaterialSourceWhenKeyedCipherSelected() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        logger.info("Testing keyed encryption methods: ${SUPPORTED_KEYED_ENCRYPTION_METHODS*.name()}")

        final int VALID_KEY_LENGTH = 128
        final String VALID_KEY_HEX = "ab" * (VALID_KEY_LENGTH / 8)
        logger.info("Using key ${VALID_KEY_HEX} (${VALID_KEY_HEX.length() * 4} bits)")

        final String VALID_PASSWORD = "thisIsABadPassword"
        logger.info("Using password ${VALID_PASSWORD} (${VALID_PASSWORD.length()} bytes)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        KeyDerivationFunction none = KeyDerivationFunction.NONE
        final def VALID_KDFS = KeyDerivationFunction.values().findAll { it.isStrongKDF() }

        // Scenario 1 - RKH w/ KDF NONE & em in [CBC, CTR, GCM] (no password)
        SUPPORTED_KEYED_ENCRYPTION_METHODS.each { EncryptionMethod kem ->
            logger.info("Trying encryption method ${kem.name()} with KDF ${none.name()}")
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, kem.name())
            runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, none.name())

            logger.info("Setting raw key hex: ${VALID_KEY_HEX}")
            runner.setProperty(EncryptContent.RAW_KEY_HEX, VALID_KEY_HEX)
            runner.removeProperty(EncryptContent.PASSWORD)

            runner.enqueue(new byte[0])
            pc = (MockProcessContext) runner.getProcessContext()

            // Act
            results = pc.validate()

            // Assert
            assert results.isEmpty()

            // Scenario 2 - PW w/ KDF in [BCRYPT, SCRYPT, PBKDF2, ARGON2] & em in [CBC, CTR, GCM] (no RKH)
            VALID_KDFS.each { KeyDerivationFunction kdf ->
                logger.info("Trying encryption method ${kem.name()} with KDF ${kdf.name()}")
                runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, kem.name())
                runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name())

                logger.info("Setting password: ${VALID_PASSWORD}")
                runner.removeProperty(EncryptContent.RAW_KEY_HEX)
                runner.setProperty(EncryptContent.PASSWORD, VALID_PASSWORD)

                runner.enqueue(new byte[0])
                pc = (MockProcessContext) runner.getProcessContext()

                // Act
                results = pc.validate()

                // Assert
                assert results.isEmpty()
            }
        }
    }

    @Test
    void testShouldValidateKDFWhenPBECipherSelected() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc
        final String PASSWORD = "short"

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }
        if (!CipherUtility.isUnlimitedStrengthCryptoSupported()) {
            // Remove all unlimited strength algorithms
            encryptionMethods.removeAll { it.unlimitedStrength }
        }

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.PASSWORD, PASSWORD)
        runner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_ALLOWED)

        encryptionMethods.each { EncryptionMethod encryptionMethod ->
            logger.info("Trying encryption method ${encryptionMethod.name()}")
            runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())

            final def INVALID_KDFS = [KeyDerivationFunction.NONE, KeyDerivationFunction.BCRYPT, KeyDerivationFunction.SCRYPT, KeyDerivationFunction.PBKDF2, KeyDerivationFunction.ARGON2]
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
    void testRoundTrip() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        final String RAW_KEY_HEX = "ab" * 16
        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, RAW_KEY_HEX)
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())

        SUPPORTED_KEYED_ENCRYPTION_METHODS.each { EncryptionMethod encryptionMethod ->
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
    void testDecryptAesCbcNoPadding() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        final String RAW_KEY_HEX = "ab" * 16
        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, RAW_KEY_HEX)
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, EncryptionMethod.AES_CBC_NO_PADDING.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE)

        final String content = "ExactBlockSizeRequiredForProcess"
        final byte[] bytes = content.getBytes(StandardCharsets.UTF_8)
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()

        final KeyedEncryptor encryptor = new KeyedEncryptor(EncryptionMethod.AES_CBC_NO_PADDING, Hex.decodeHex(RAW_KEY_HEX))
        encryptor.encryptionCallback.process(inputStream, outputStream)
        outputStream.close()

        final byte[] encrypted = outputStream.toByteArray()
        testRunner.enqueue(encrypted)
        testRunner.run()

        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        flowFile.assertContentEquals(content)
    }

    // TODO: Implement
    @Test
    void testArgon2EncryptionShouldWriteAttributesWithEncryptionMetadata() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        KeyDerivationFunction kdf = KeyDerivationFunction.ARGON2
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Attempting encryption with {}", encryptionMethod.name())

        testRunner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword")
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        String PLAINTEXT = "This is a plaintext message. "

        // Act
        testRunner.enqueue(PLAINTEXT)
        testRunner.clearTransferState()
        testRunner.run()

        // Assert
        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)
        logger.info("Successfully encrypted with {}", encryptionMethod.name())

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        testRunner.assertQueueEmpty()

        printFlowFileAttributes(flowFile.getAttributes())

        byte[] flowfileContentBytes = flowFile.getData()
        String flowfileContent = flowFile.getContent()

        int ivDelimiterStart = CipherUtility.findSequence(flowfileContentBytes, RandomIVPBECipherProvider.IV_DELIMITER)
        logger.info("IV delimiter starts at ${ivDelimiterStart}")

        final byte[] EXPECTED_KDF_SALT_BYTES = extractFullSaltFromCipherBytes(flowfileContentBytes)
        final String EXPECTED_KDF_SALT = new String(EXPECTED_KDF_SALT_BYTES)
        final String EXPECTED_SALT_HEX = extractRawSaltHexFromFullSalt(EXPECTED_KDF_SALT_BYTES, kdf)
        logger.info("Extracted expected raw salt (hex): ${EXPECTED_SALT_HEX}")

        final String EXPECTED_IV_HEX = Hex.encodeHexString(flowfileContentBytes[(ivDelimiterStart - 16)..<ivDelimiterStart] as byte[])

        printFlowFileAttributes(flowFile.getAttributes())

        // Assert the timestamp attribute was written and is accurate
        def diff = calculateTimestampDifference(new Date(), flowFile.getAttribute("encryptcontent.timestamp"))
        assert diff.toMilliseconds() < 1_000
        assert flowFile.getAttribute("encryptcontent.algorithm") == encryptionMethod.name()
        assert flowFile.getAttribute("encryptcontent.kdf") == kdf.name()
        assert flowFile.getAttribute("encryptcontent.action") == "encrypted"
        assert flowFile.getAttribute("encryptcontent.salt") == EXPECTED_SALT_HEX
        assert flowFile.getAttribute("encryptcontent.salt_length") == "16"
        assert flowFile.getAttribute("encryptcontent.kdf_salt") == EXPECTED_KDF_SALT
        assert (29..54)*.toString().contains(flowFile.getAttribute("encryptcontent.kdf_salt_length"))
        assert flowFile.getAttribute("encryptcontent.iv") == EXPECTED_IV_HEX
        assert flowFile.getAttribute("encryptcontent.iv_length") == "16"
        assert flowFile.getAttribute("encryptcontent.plaintext_length") == PLAINTEXT.size() as String
        assert flowFile.getAttribute("encryptcontent.cipher_text_length") == flowfileContentBytes.size() as String
    }

    static void printFlowFileAttributes(Map<String, String> attributes) {
        int maxLength = attributes.keySet()*.length().max()
        attributes.sort().each { attr, value ->
            logger.info("Attribute: ${attr.padRight(maxLength)}: ${value}")
        }
    }

    @Test
    void testKeyedEncryptionShouldWriteAttributesWithEncryptionMetadata() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        KeyDerivationFunction kdf = KeyDerivationFunction.NONE
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Attempting encryption with {}", encryptionMethod.name())

        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, "0123456789ABCDEFFEDCBA9876543210")
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        String PLAINTEXT = "This is a plaintext message. "

        // Act
        testRunner.enqueue(PLAINTEXT)
        testRunner.clearTransferState()
        testRunner.run()

        // Assert
        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)
        logger.info("Successfully encrypted with {}", encryptionMethod.name())

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        testRunner.assertQueueEmpty()

        printFlowFileAttributes(flowFile.getAttributes())

        byte[] flowfileContentBytes = flowFile.getData()
        String flowfileContent = flowFile.getContent()
        logger.info("Cipher text (${flowfileContentBytes.length}): ${Hex.encodeHexString(flowfileContentBytes)}")

        int ivDelimiterStart = CipherUtility.findSequence(flowfileContentBytes, RandomIVPBECipherProvider.IV_DELIMITER)
        logger.info("IV delimiter starts at ${ivDelimiterStart}")
        assert ivDelimiterStart == 16

        def diff = calculateTimestampDifference(new Date(), flowFile.getAttribute("encryptcontent.timestamp"))
        logger.info("Timestamp difference: ${diff}")

        // Assert the timestamp attribute was written and is accurate
        assert diff.toMilliseconds() < 1_000

        final String EXPECTED_IV_HEX = Hex.encodeHexString(flowfileContentBytes[0..<ivDelimiterStart] as byte[])
        final int EXPECTED_CIPHER_TEXT_LENGTH = CipherUtility.calculateCipherTextLength(PLAINTEXT.size(), 0)

        assert flowFile.getAttribute("encryptcontent.algorithm") == encryptionMethod.name()
        assert flowFile.getAttribute("encryptcontent.kdf") == kdf.name()
        assert flowFile.getAttribute("encryptcontent.action") == "encrypted"
        assert flowFile.getAttribute("encryptcontent.iv") == EXPECTED_IV_HEX
        assert flowFile.getAttribute("encryptcontent.iv_length") == "16"
        assert flowFile.getAttribute("encryptcontent.plaintext_length") == PLAINTEXT.size() as String
        assert flowFile.getAttribute("encryptcontent.cipher_text_length") == EXPECTED_CIPHER_TEXT_LENGTH as String
    }

    @Test
    void testKeyedDecryptionShouldWriteAttributesWithEncryptionMetadata() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        KeyDerivationFunction kdf = KeyDerivationFunction.NONE
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Attempting decryption with {}", encryptionMethod.name())

        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, "0123456789ABCDEFFEDCBA9876543210")
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        String PLAINTEXT = "This is a plaintext message. "

        testRunner.enqueue(PLAINTEXT)
        testRunner.clearTransferState()
        testRunner.run()

        MockFlowFile encryptedFlowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).first()
        byte[] cipherText = encryptedFlowFile.getData()

        int ivDelimiterStart = CipherUtility.findSequence(cipherText, RandomIVPBECipherProvider.IV_DELIMITER)
        logger.info("IV delimiter starts at ${ivDelimiterStart}")
        assert ivDelimiterStart == 16
        final String EXPECTED_IV_HEX = Hex.encodeHexString(cipherText[0..<ivDelimiterStart] as byte[])

        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE)
        testRunner.clearTransferState()
        testRunner.enqueue(cipherText)

        // Act
        testRunner.run()

        // Assert
        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)
        logger.info("Successfully decrypted with {}", encryptionMethod.name())

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        testRunner.assertQueueEmpty()

        printFlowFileAttributes(flowFile.getAttributes())

        byte[] flowfileContentBytes = flowFile.getData()
        String flowfileContent = flowFile.getContent()
        logger.info("Plaintext (${flowfileContentBytes.length}): ${Hex.encodeHexString(flowfileContentBytes)}")

        def diff = calculateTimestampDifference(new Date(), flowFile.getAttribute("encryptcontent.timestamp"))
        logger.info("Timestamp difference: ${diff}")

        // Assert the timestamp attribute was written and is accurate
        assert diff.toMilliseconds() < 1_000
        assert flowFile.getAttribute("encryptcontent.algorithm") == encryptionMethod.name()
        assert flowFile.getAttribute("encryptcontent.kdf") == kdf.name()
        assert flowFile.getAttribute("encryptcontent.action") == "decrypted"
        assert flowFile.getAttribute("encryptcontent.iv") == EXPECTED_IV_HEX
        assert flowFile.getAttribute("encryptcontent.iv_length") == "16"
        assert flowFile.getAttribute("encryptcontent.plaintext_length") == PLAINTEXT.size() as String
        assert flowFile.getAttribute("encryptcontent.cipher_text_length") == cipherText.length as String
    }

    @Test
    void testDifferentCompatibleConfigurations() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        KeyDerivationFunction argon2 = KeyDerivationFunction.ARGON2
        EncryptionMethod aesCbcEM = EncryptionMethod.AES_CBC
        logger.info("Attempting encryption with ${argon2} and ${aesCbcEM.name()}")
        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(aesCbcEM.algorithm)

        final String PASSWORD = "thisIsABadPassword"
        testRunner.setProperty(EncryptContent.PASSWORD, PASSWORD)
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, argon2.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, aesCbcEM.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        String PLAINTEXT = "This is a plaintext message. "

        testRunner.enqueue(PLAINTEXT)
        testRunner.clearTransferState()
        testRunner.run()

        MockFlowFile encryptedFlowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).first()
        byte[] fullCipherBytes = encryptedFlowFile.getData()
        printFlowFileAttributes(encryptedFlowFile.getAttributes())

        // Extract the KDF salt from the encryption metadata in the flowfile attribute
        String argon2Salt = encryptedFlowFile.getAttribute("encryptcontent.kdf_salt")
        Argon2SecureHasher a2sh = new Argon2SecureHasher(keyLength / 8 as int)
        byte[] fullSaltBytes = argon2Salt.getBytes(StandardCharsets.UTF_8)
        byte[] rawSaltBytes = Hex.decodeHex(encryptedFlowFile.getAttribute("encryptcontent.salt"))
        byte[] keyBytes = a2sh.hashRaw(PASSWORD.getBytes(StandardCharsets.UTF_8), rawSaltBytes)
        String keyHex = Hex.encodeHexString(keyBytes)
        logger.sanity("Derived key bytes: ${keyHex}")

        byte[] ivBytes = Hex.decodeHex(encryptedFlowFile.getAttribute("encryptcontent.iv"))
        logger.sanity("Extracted IV bytes: ${Hex.encodeHexString(ivBytes)}")

        // Sanity check the encryption
        Argon2CipherProvider a2cp = new Argon2CipherProvider()
        Cipher sanityCipher = a2cp.getCipher(aesCbcEM, PASSWORD, fullSaltBytes, ivBytes, CipherUtility.parseKeyLengthFromAlgorithm(aesCbcEM.algorithm), false)
        byte[] cipherTextBytes = fullCipherBytes[-32..-1]
        byte[] recoveredBytes = sanityCipher.doFinal(cipherTextBytes)
        logger.sanity("Recovered text: ${new String(recoveredBytes, StandardCharsets.UTF_8)}")

        // Act

        // Configure decrypting processor with raw key
        KeyDerivationFunction kdf = KeyDerivationFunction.NONE
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Attempting decryption with {}", encryptionMethod.name())

        testRunner.setProperty(EncryptContent.RAW_KEY_HEX, keyHex)
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, kdf.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE)
        testRunner.removeProperty(EncryptContent.PASSWORD)

        testRunner.enqueue(fullCipherBytes)
        testRunner.clearTransferState()
        testRunner.run()

        // Assert
        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)
        logger.info("Successfully decrypted with {}", encryptionMethod.name())

        MockFlowFile decryptedFlowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        testRunner.assertQueueEmpty()

        printFlowFileAttributes(decryptedFlowFile.getAttributes())

        byte[] flowfileContentBytes = decryptedFlowFile.getData()
        logger.info("Plaintext (${flowfileContentBytes.length}): ${new String(flowfileContentBytes, StandardCharsets.UTF_8)}")

        assert flowfileContentBytes == recoveredBytes
    }

    static TimeDuration calculateTimestampDifference(Date date, String timestamp) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z")
        final long dateMillis = date.toInstant().toEpochMilli()
        logger.info("Provided timestamp ${formatter.format(date)} -> (ms): ${dateMillis}")
        Date parsedTimestamp = formatter.parse(timestamp)
        long parsedTimestampMillis = parsedTimestamp.toInstant().toEpochMilli()
        logger.info("Parsed timestamp   ${timestamp} -> (ms): ${parsedTimestampMillis}")

        TimeCategory.minus(date, parsedTimestamp)
    }

    static byte[] extractFullSaltFromCipherBytes(byte[] cipherBytes) {
        int saltDelimiterStart = CipherUtility.findSequence(cipherBytes, RandomIVPBECipherProvider.SALT_DELIMITER)
        logger.info("Salt delimiter starts at ${saltDelimiterStart}")
        byte[] saltBytes = cipherBytes[0..<saltDelimiterStart]
        logger.info("Extracted full salt (${saltBytes.length}): ${new String(saltBytes, StandardCharsets.UTF_8)}")
        saltBytes
    }

    static String extractRawSaltHexFromFullSalt(byte[] fullSaltBytes, KeyDerivationFunction kdf) {
        logger.info("Full salt (${fullSaltBytes.length}): ${Hex.encodeHexString(fullSaltBytes)}")
        // Salt will be in Base64 (or Radix64) for strong KDFs
        byte[] rawSaltBytes = CipherUtility.extractRawSalt(fullSaltBytes, kdf)
        logger.info("Raw salt (${rawSaltBytes.length}): ${Hex.encodeHexString(rawSaltBytes)}")
        String rawSaltHex = Hex.encodeHexString(rawSaltBytes)
        logger.info("Extracted expected raw salt (hex): ${rawSaltHex}")
        rawSaltHex
    }

    @Test
    void testShouldCompareDate() {
        // Arrange
        Date now = new Date()
        logger.info("Now: ${now} -- ${now.toInstant().toEpochMilli()}")

        Instant fiveSecondsLater = now.toInstant().plus(5, ChronoUnit.SECONDS)
        Date fSLDate = Date.from(fiveSecondsLater)
        logger.info("FSL: ${fSLDate} -- ${fiveSecondsLater.toEpochMilli()}")

        // Convert entirely to String & parse back
        Instant tenSecondsLater = fiveSecondsLater.plusMillis(5000)
        Date tSLDate = Date.from(tenSecondsLater)
        logger.info("TSL: ${tSLDate} -- ${tenSecondsLater.toEpochMilli()}")

        // Java way ('y' is deterministic vs. 'Y' which is week-based and calendar & JVM dependent)
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z")
        String tslString = sdf.format(tSLDate)
        logger.info("TSL formatted: ${tslString}")

        // Parse back to date
        Date parsedTSLDate = sdf.parse(tslString)
        logger.info("TSL parsed: ${parsedTSLDate} -- ${parsedTSLDate.toInstant().toEpochMilli()}")

        // Act
        def fiveSecondDiff = TimeCategory.minus(fSLDate, now)
        logger.info(" FSL - now difference: ${fiveSecondDiff}")

        def tenSecondDiff = TimeCategory.minus(tSLDate, now)
        logger.info(" TSL - now difference: ${tenSecondDiff}")

        def parsedTenSecondDiff = TimeCategory.minus(parsedTSLDate, now)
        logger.info("PTSL - now difference: ${parsedTenSecondDiff}")

        // Assert
        assert fiveSecondDiff.seconds == 5
        assert tenSecondDiff.seconds == 10
        assert parsedTenSecondDiff.seconds == 10

        assert [fiveSecondDiff, tenSecondDiff, parsedTenSecondDiff].every { it.days == 0 }
    }

    @Test
    void testShouldCheckMaximumLengthOfPasswordOnLimitedStrengthCryptoJVM() throws IOException {
        // Arrange
        Assume.assumeTrue("Only run on systems with limited strength crypto", !CipherUtility.isUnlimitedStrengthCryptoSupported())

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
                return false
                // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
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
    void testShouldCheckLengthOfPasswordWhenNotAllowed() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name())

        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        boolean limitedStrengthCrypto = !CipherUtility.isUnlimitedStrengthCryptoSupported()
        boolean allowWeakCrypto = false
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_NOT_ALLOWED)

        // Use .find instead of .each to allow "breaks" using return false
        encryptionMethods.find { EncryptionMethod encryptionMethod ->
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            def shortPasswordLength = [PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1, CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1].min()
            String shortPassword = "x" * shortPasswordLength
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                return false
                // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
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
    void testShouldNotCheckLengthOfPasswordWhenAllowed() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NIFI_LEGACY.name())

        Collection<ValidationResult> results
        MockProcessContext pc

        def encryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }

        boolean limitedStrengthCrypto = !CipherUtility.isUnlimitedStrengthCryptoSupported()
        boolean allowWeakCrypto = true
        testRunner.setProperty(EncryptContent.ALLOW_WEAK_CRYPTO, WEAK_CRYPTO_ALLOWED)

        // Use .find instead of .each to allow "breaks" using return false
        encryptionMethods.find { EncryptionMethod encryptionMethod ->
            // Determine the minimum of the algorithm-accepted length or the global safe minimum to ensure only one validation result
            def shortPasswordLength = [PasswordBasedEncryptor.getMinimumSafePasswordLength() - 1, CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod) - 1].min()
            String shortPassword = "x" * shortPasswordLength
            if (encryptionMethod.isUnlimitedStrength() || encryptionMethod.isKeyedCipher()) {
                return false
                // cannot test unlimited strength in unit tests because it's not enabled by the JVM by default.
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

    @Test
    void testPGPPasswordShouldSupportExpressionLanguage() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE)
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, EncryptionMethod.PGP.name())
        testRunner.setProperty(EncryptContent.PRIVATE_KEYRING, "src/test/resources/TestEncryptContent/secring.gpg")

        Collection<ValidationResult> results
        MockProcessContext pc

        // Verify this is the correct password
        final String passphraseWithoutEL = "thisIsABadPassword"
        testRunner.setProperty(EncryptContent.PRIVATE_KEYRING_PASSPHRASE, passphraseWithoutEL)

        testRunner.clearTransferState()
        testRunner.enqueue(new byte[0])
        pc = (MockProcessContext) testRunner.getProcessContext()

        results = pc.validate()
        Assert.assertEquals(results.toString(), 0, results.size())

        final String passphraseWithEL = "\${literal('thisIsABadPassword')}"
        testRunner.setProperty(EncryptContent.PRIVATE_KEYRING_PASSPHRASE, passphraseWithEL)

        testRunner.clearTransferState()
        testRunner.enqueue(new byte[0])

        // Act
        results = pc.validate()

        // Assert
        Assert.assertEquals(results.toString(), 0, results.size())
    }

    @Test
    void testArgon2ShouldIncludeFullSalt() throws IOException {
        // Arrange
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptContent())
        testRunner.setProperty(EncryptContent.PASSWORD, "thisIsABadPassword")
        testRunner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.ARGON2.name())

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        logger.info("Attempting {}", encryptionMethod.name())
        testRunner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        testRunner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)

        // Act
        testRunner.enqueue(Paths.get("src/test/resources/hello.txt"))
        testRunner.clearTransferState()
        testRunner.run()

        // Assert
        testRunner.assertAllFlowFilesTransferred(EncryptContent.REL_SUCCESS, 1)

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncryptContent.REL_SUCCESS).get(0)
        testRunner.assertQueueEmpty()

        def flowFileContent = flowFile.getContent()
        logger.info("Flowfile content (${flowFile.getData().length}): ${Hex.encodeHexString(flowFile.getData())}")

        def fullSalt = flowFileContent.substring(0, flowFileContent.indexOf(new String(RandomIVPBECipherProvider.SALT_DELIMITER, StandardCharsets.UTF_8)))
        logger.info("Full salt (${fullSalt.size()}): ${fullSalt}")

        boolean isValidFormattedSalt = Argon2CipherProvider.isArgon2FormattedSalt(fullSalt)
        logger.info("Salt is Argon2 format: ${isValidFormattedSalt}")
        assert isValidFormattedSalt

        def FULL_SALT_LENGTH_RANGE = (49..57)
        boolean fullSaltIsValidLength = FULL_SALT_LENGTH_RANGE.contains(fullSalt.bytes.length)
        logger.info("Salt length (${fullSalt.length()}) in valid range (${FULL_SALT_LENGTH_RANGE})")
        assert fullSaltIsValidLength
    }
}
