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
package org.apache.nifi.security.kms

import org.apache.commons.lang3.SystemUtils
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.KeyManagementException
import java.security.SecureRandom
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class CryptoUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(CryptoUtilsTest.class)

    private static final String KEY_ID = "K1"
    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static
    final Set<PosixFilePermission> ALL_POSIX_ATTRS = PosixFilePermission.values() as Set<PosixFilePermission>

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
        tempFolder.create()
    }

    @After
    void tearDown() throws Exception {
        tempFolder?.delete()
    }

    @AfterClass
    static void tearDownOnce() throws Exception {

    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static boolean isRootUser() {
        ProcessBuilder pb = new ProcessBuilder(["id", "-u"])
        Process process = pb.start()
        InputStream responseStream = process.getInputStream()
        BufferedReader responseReader = new BufferedReader(new InputStreamReader(responseStream))
        responseReader.text.trim() == "0"
    }

    @Test
    void testShouldConcatenateByteArrays() {
        // Arrange
        byte[] bytes1 = "These are some bytes".getBytes(StandardCharsets.UTF_8)
        byte[] bytes2 = "These are some other bytes".getBytes(StandardCharsets.UTF_8)
        final byte[] EXPECTED_CONCATENATED_BYTES = ((bytes1 as List) << (bytes2 as List)).flatten() as byte[]
        logger.info("Expected concatenated bytes: ${Hex.toHexString(EXPECTED_CONCATENATED_BYTES)}")

        // Act
        byte[] concat = CryptoUtils.concatByteArrays(bytes1, bytes2)
        logger.info("  Actual concatenated bytes: ${Hex.toHexString(concat)}")

        // Assert
        assert concat == EXPECTED_CONCATENATED_BYTES
    }

    @Test
    void testShouldValidateStaticKeyProvider() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(staticProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX])
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert keyProviderIsValid
    }

    @Test
    void testShouldValidateLegacyStaticKeyProvider() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name.replaceFirst("security.kms", "provenance")
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(staticProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX])
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert keyProviderIsValid
    }

    @Test
    void testShouldNotValidateStaticKeyProviderMissingKeyId() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(staticProvider, providerLocation, null, [(KEY_ID): KEY_HEX])
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${null} / ${KEY_HEX} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !keyProviderIsValid
    }

    @Test
    void testShouldNotValidateStaticKeyProviderMissingKey() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(staticProvider, providerLocation, KEY_ID, null)
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${null} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !keyProviderIsValid
    }

    @Test
    void testShouldNotValidateStaticKeyProviderWithInvalidKey() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(staticProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX[0..<-2]])
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX[0..<-2]} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !keyProviderIsValid
    }

    @Test
    void testShouldValidateFileBasedKeyProvider() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(fileBasedProvider, providerLocation, KEY_ID, null)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${null} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert keyProviderIsValid
    }

    @Test
    void testShouldValidateLegacyFileBasedKeyProvider() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name.replaceFirst("security.kms", "provenance")
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(fileBasedProvider, providerLocation, KEY_ID, null)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${null} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert keyProviderIsValid
    }

    @Test
    void testShouldNotValidateMissingFileBasedKeyProvider() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = new File(tempFolder.root, "filebased_missing.kp")
        String providerLocation = fileBasedProviderFile.path
        logger.info("Created (no actual file) temporary file based key provider: ${providerLocation}")

        // Act
        String missingLocation = providerLocation
        boolean missingKeyProviderIsValid = CryptoUtils.isValidKeyProvider(fileBasedProvider, missingLocation, KEY_ID, null)
        logger.info("Key Provider ${fileBasedProvider} with location ${missingLocation} and keyId ${KEY_ID} / ${null} is ${missingKeyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !missingKeyProviderIsValid
    }

    @Test
    void testShouldNotValidateUnreadableFileBasedKeyProvider() {
        // Arrange
        Assume.assumeFalse("This test does not run on Windows", SystemUtils.IS_OS_WINDOWS)
        Assume.assumeFalse("This test does not run for root users", isRootUser())

        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Make it unreadable
        markFileUnreadable(fileBasedProviderFile)

        // Act
        boolean unreadableKeyProviderIsValid = CryptoUtils.isValidKeyProvider(fileBasedProvider, providerLocation, KEY_ID, null)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${null} is ${unreadableKeyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !unreadableKeyProviderIsValid

        // Make the file deletable so cleanup can occur
        markFileReadable(fileBasedProviderFile)
    }

    private static void markFileReadable(File fileBasedProviderFile) {
        if (SystemUtils.IS_OS_WINDOWS) {
            fileBasedProviderFile.setReadable(true, false)
        } else {
            Files.setPosixFilePermissions(fileBasedProviderFile.toPath(), ALL_POSIX_ATTRS)
        }
    }

    private static void markFileUnreadable(File fileBasedProviderFile) {
        if (SystemUtils.IS_OS_WINDOWS) {
            fileBasedProviderFile.setReadable(false, false)
        } else {
            Files.setPosixFilePermissions(fileBasedProviderFile.toPath(), [] as Set<PosixFilePermission>)
        }
    }

    @Test
    void testShouldNotValidateFileBasedKeyProviderMissingKeyId() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = tempFolder.newFile("missing_key_id.kp")
        String providerLocation = fileBasedProviderFile.path
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(fileBasedProvider, providerLocation, null, null)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${null} / ${null} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !keyProviderIsValid
    }

    @Test
    void testShouldNotValidateUnknownKeyProvider() {
        // Arrange
        String providerImplementation = "org.apache.nifi.provenance.ImaginaryKeyProvider"
        String providerLocation = null

        // Act
        boolean keyProviderIsValid = CryptoUtils.isValidKeyProvider(providerImplementation, providerLocation, KEY_ID, null)
        logger.info("Key Provider ${providerImplementation} with location ${providerLocation} and keyId ${KEY_ID} / ${null} is ${keyProviderIsValid ? "valid" : "invalid"}")

        // Assert
        assert !keyProviderIsValid
    }

    @Test
    void testShouldValidateKey() {
        // Arrange
        String validKey = KEY_HEX
        String validLowercaseKey = KEY_HEX.toLowerCase()

        String tooShortKey = KEY_HEX[0..<-2]
        String tooLongKey = KEY_HEX + KEY_HEX // Guaranteed to be 2x the max valid key length
        String nonHexKey = KEY_HEX.replaceFirst(/A/, "X")

        def validKeys = [validKey, validLowercaseKey]
        def invalidKeys = [tooShortKey, tooLongKey, nonHexKey]

        // If unlimited strength is available, also validate 128 and 196 bit keys
        if (isUnlimitedStrengthCryptoAvailable()) {
            validKeys << KEY_HEX_128
            validKeys << KEY_HEX_256[0..<48]
        } else {
            invalidKeys << KEY_HEX_256[0..<48]
            invalidKeys << KEY_HEX_256
        }

        // Act
        def validResults = validKeys.collect { String key ->
            logger.info("Validating ${key}")
            CryptoUtils.keyIsValid(key)
        }

        def invalidResults = invalidKeys.collect { String key ->
            logger.info("Validating ${key}")
            CryptoUtils.keyIsValid(key)
        }

        // Assert
        assert validResults.every()
        assert invalidResults.every { !it }
    }

    @Test
    void testShouldReadKeys() {
        // Arrange
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        String keyFileName = "keys.nkp"
        File keyFile = tempFolder.newFile(keyFileName)
        final int KEY_COUNT = 5
        List<String> lines = []
        KEY_COUNT.times { int i ->
            lines.add("key${i + 1}=${generateEncryptedKey(masterKey)}")
        }

        keyFile.text = lines.join("\n")

        logger.info("File contents: \n${keyFile.text}")

        // Act
        def readKeys = CryptoUtils.readKeys(keyFile.path, masterKey)
        logger.info("Read ${readKeys.size()} keys from ${keyFile.path}")

        // Assert
        assert readKeys.size() == KEY_COUNT
    }

    @Test
    void testShouldReadKeysWithDuplicates() {
        // Arrange
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        String keyFileName = "keys.nkp"
        File keyFile = tempFolder.newFile(keyFileName)
        final int KEY_COUNT = 3
        List<String> lines = []
        KEY_COUNT.times { int i ->
            lines.add("key${i + 1}=${generateEncryptedKey(masterKey)}")
        }

        lines.add("key3=${generateEncryptedKey(masterKey)}")

        keyFile.text = lines.join("\n")

        logger.info("File contents: \n${keyFile.text}")

        // Act
        def readKeys = CryptoUtils.readKeys(keyFile.path, masterKey)
        logger.info("Read ${readKeys.size()} keys from ${keyFile.path}")

        // Assert
        assert readKeys.size() == KEY_COUNT
    }

    @Test
    void testShouldReadKeysWithSomeMalformed() {
        // Arrange
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        String keyFileName = "keys.nkp"
        File keyFile = tempFolder.newFile(keyFileName)
        final int KEY_COUNT = 5
        List<String> lines = []
        KEY_COUNT.times { int i ->
            lines.add("key${i + 1}=${generateEncryptedKey(masterKey)}")
        }

        // Insert the malformed keys in the middle
        lines.add(2, "keyX1==${generateEncryptedKey(masterKey)}")
        lines.add(4, "=${generateEncryptedKey(masterKey)}")
        lines.add(6, "keyX3=non Base64-encoded data")

        keyFile.text = lines.join("\n")

        logger.info("File contents: \n${keyFile.text}")

        // Act
        def readKeys = CryptoUtils.readKeys(keyFile.path, masterKey)
        logger.info("Read ${readKeys.size()} keys from ${keyFile.path}")

        // Assert
        assert readKeys.size() == KEY_COUNT
    }

    @Test
    void testShouldNotReadKeysIfAllMalformed() {
        // Arrange
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        String keyFileName = "keys.nkp"
        File keyFile = tempFolder.newFile(keyFileName)
        final int KEY_COUNT = 5
        List<String> lines = []

        // All of these keys are malformed
        KEY_COUNT.times { int i ->
            lines.add("key${i + 1}=${generateEncryptedKey(masterKey)[0..<-4]}")
        }

        keyFile.text = lines.join("\n")

        logger.info("File contents: \n${keyFile.text}")

        // Act
        def msg = shouldFail(KeyManagementException) {
            def readKeys = CryptoUtils.readKeys(keyFile.path, masterKey)
            logger.info("Read ${readKeys.size()} keys from ${keyFile.path}")
        }

        // Assert
        assert msg.getMessage() == "The provided file contained no valid keys"
    }

    @Test
    void testShouldNotReadKeysIfEmptyOrMissing() {
        // Arrange
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        String keyFileName = "empty.nkp"
        File keyFile = tempFolder.newFile(keyFileName)
        logger.info("File contents: \n${keyFile.text}")

        // Act
        def missingMsg = shouldFail(KeyManagementException) {
            def readKeys = CryptoUtils.readKeys(keyFile.path, masterKey)
            logger.info("Read ${readKeys.size()} keys from ${keyFile.path}")
        }
        logger.expected("Missing file: ${missingMsg}")

        def emptyMsg = shouldFail(KeyManagementException) {
            def readKeys = CryptoUtils.readKeys(null, masterKey)
            logger.info("Read ${readKeys.size()} keys from ${null}")
        }
        logger.expected("Empty file: ${emptyMsg}")

        // Assert
        assert missingMsg.getMessage() == "The provided file contained no valid keys"
        assert emptyMsg.getMessage() == "The key provider file is not present and readable"
    }

    private static String generateEncryptedKey(SecretKey masterKey) {
        byte[] ivBytes = new byte[16]
        byte[] keyBytes = new byte[isUnlimitedStrengthCryptoAvailable() ? 32 : 16]

        SecureRandom sr = new SecureRandom()
        sr.nextBytes(ivBytes)
        sr.nextBytes(keyBytes)

        Cipher masterCipher = Cipher.getInstance("AES/GCM/NoPadding", "BC")
        masterCipher.init(Cipher.ENCRYPT_MODE, masterKey, new IvParameterSpec(ivBytes))
        byte[] cipherBytes = masterCipher.doFinal(keyBytes)

        Base64.encoder.encodeToString(CryptoUtils.concatByteArrays(ivBytes, cipherBytes))
    }

}
