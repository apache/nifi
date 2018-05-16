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
package org.apache.nifi.provenance

import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.kms.KeyProvider
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.KeyManagementException
import java.security.SecureRandom
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class AESProvenanceEventEncryptorTest {
    private static final Logger logger = LoggerFactory.getLogger(AESProvenanceEventEncryptorTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static KeyProvider mockKeyProvider
    private static AESKeyedCipherProvider mockCipherProvider

    private static String ORIGINAL_LOG_LEVEL

    private ProvenanceEventEncryptor encryptor

    @BeforeClass
    static void setUpOnce() throws Exception {
        ORIGINAL_LOG_LEVEL = System.getProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance")
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG")

        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        mockKeyProvider = [
                getKey   : { String keyId ->
                    logger.mock("Requesting key ID: ${keyId}")
                    new SecretKeySpec(Hex.decode(KEY_HEX), "AES")
                },
                keyExists: { String keyId ->
                    logger.mock("Checking existence of ${keyId}")
                    true
                }] as KeyProvider

        mockCipherProvider = [
                getCipher: { EncryptionMethod em, SecretKey key, byte[] ivBytes, boolean encryptMode ->
                    logger.mock("Getting cipher for ${em} with IV ${Hex.toHexString(ivBytes)} encrypt ${encryptMode}")
                    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding")
                    cipher.init((encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, key, new IvParameterSpec(ivBytes))
                    cipher
                }
        ] as AESKeyedCipherProvider
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        if (ORIGINAL_LOG_LEVEL) {
            System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", ORIGINAL_LOG_LEVEL)
        }
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    /**
     * Given arbitrary bytes, encrypt them and persist with the encryption metadata, then recover
     */
    @Test
    void testShouldEncryptAndDecryptArbitraryBytes() {
        // Arrange
        final byte[] SERIALIZED_BYTES = "This is a plaintext message.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes (${SERIALIZED_BYTES.size()}): ${Hex.toHexString(SERIALIZED_BYTES)}")

        encryptor = new AESProvenanceEventEncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"
        logger.info("Using record ID ${recordId} and key ID ${keyId}")

        // Act
        byte[] metadataAndCipherBytes = encryptor.encrypt(SERIALIZED_BYTES, recordId, keyId)
        logger.info("Encrypted data to: \n\t${Hex.toHexString(metadataAndCipherBytes)}")

        byte[] recoveredBytes = encryptor.decrypt(metadataAndCipherBytes, recordId)
        logger.info("Decrypted data to: \n\t${Hex.toHexString(recoveredBytes)}")

        // Assert
        assert recoveredBytes == SERIALIZED_BYTES
        logger.info("Decoded (usually would be serialized schema record): ${new String(recoveredBytes, StandardCharsets.UTF_8)}")
    }

    @Test
    void testShouldInitializeNullCipherProvider() {
        // Arrange
        encryptor = new AESProvenanceEventEncryptor()
        encryptor.setCipherProvider(null)
        assert !encryptor.aesKeyedCipherProvider
        
        // Act
        encryptor.initialize(mockKeyProvider)
        logger.info("Created ${encryptor}")

        // Assert
        assert encryptor.aesKeyedCipherProvider instanceof AESKeyedCipherProvider
    }

    @Test
    void testShouldFailOnMissingKeyId() {
        // Arrange
        final byte[] SERIALIZED_BYTES = "This is a plaintext message.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes (${SERIALIZED_BYTES.size()}): ${Hex.toHexString(SERIALIZED_BYTES)}")

        KeyProvider emptyKeyProvider = [
                getKey   : { String kid ->
                    throw new KeyManagementException("No key found for ${kid}")
                },
                keyExists: { String kid -> false }
        ] as KeyProvider

        encryptor = new AESProvenanceEventEncryptor()
        encryptor.initialize(emptyKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"
        logger.info("Using record ID ${recordId} and key ID ${keyId}")

        // Act
        def msg = shouldFail(EncryptionException) {
            byte[] metadataAndCipherBytes = encryptor.encrypt(SERIALIZED_BYTES, recordId, keyId)
            logger.info("Encrypted data to: \n\t${Hex.toHexString(metadataAndCipherBytes)}")
        }
        logger.expected(msg)

        // Assert
        assert msg.getMessage() == "The requested key ID is not available"
    }

    @Test
    void testShouldUseDifferentIVsForSequentialEncryptions() {
        // Arrange
        final byte[] SERIALIZED_BYTES = "This is a plaintext message.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes (${SERIALIZED_BYTES.size()}): ${Hex.toHexString(SERIALIZED_BYTES)}")

        encryptor = new AESProvenanceEventEncryptor()
        encryptor.initialize(mockKeyProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId1 = "R1"
        logger.info("Using record ID ${recordId1} and key ID ${keyId}")

        // Act
        byte[] metadataAndCipherBytes1 = encryptor.encrypt(SERIALIZED_BYTES, recordId1, keyId)
        logger.info("Encrypted data to: \n\t${Hex.toHexString(metadataAndCipherBytes1)}")
        EncryptionMetadata metadata1 = encryptor.extractEncryptionMetadata(metadataAndCipherBytes1)
        logger.info("Record ${recordId1} IV: ${Hex.toHexString(metadata1.ivBytes)}")

        byte[] recoveredBytes1 = encryptor.decrypt(metadataAndCipherBytes1, recordId1)
        logger.info("Decrypted data to: \n\t${Hex.toHexString(recoveredBytes1)}")

        String recordId2 = "R2"
        byte[] metadataAndCipherBytes2 = encryptor.encrypt(SERIALIZED_BYTES, recordId2, keyId)
        logger.info("Encrypted data to: \n\t${Hex.toHexString(metadataAndCipherBytes2)}")
        EncryptionMetadata metadata2 = encryptor.extractEncryptionMetadata(metadataAndCipherBytes2)
        logger.info("Record ${recordId2} IV: ${Hex.toHexString(metadata2.ivBytes)}")

        byte[] recoveredBytes2 = encryptor.decrypt(metadataAndCipherBytes2, recordId2)
        logger.info("Decrypted data to: \n\t${Hex.toHexString(recoveredBytes2)}")

        // Assert
        assert metadata1.ivBytes != metadata2.ivBytes

        assert recoveredBytes1 == SERIALIZED_BYTES
        assert recoveredBytes2 == SERIALIZED_BYTES
    }

    @Test
    void testShouldFailOnBadMetadata() {
        // Arrange
        final byte[] SERIALIZED_BYTES = "This is a plaintext message.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes (${SERIALIZED_BYTES.size()}): ${Hex.toHexString(SERIALIZED_BYTES)}")

        def strictMockKeyProvider = [
                getKey   : { String keyId ->
                    if (keyId != "K1") {
                        throw new KeyManagementException("No such key")
                    }
                    new SecretKeySpec(Hex.decode(KEY_HEX), "AES")
                },
                keyExists: { String keyId ->
                    keyId == "K1"
                }] as KeyProvider

        encryptor = new AESProvenanceEventEncryptor()
        encryptor.initialize(strictMockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"
        logger.info("Using record ID ${recordId} and key ID ${keyId}")

        final String ALGORITHM = "AES/GCM/NoPadding"
        final byte[] ivBytes = new byte[16]
        new SecureRandom().nextBytes(ivBytes)
        final String VERSION = "v1"

        // Perform the encryption independently of the encryptor
        SecretKey key = mockKeyProvider.getKey(keyId)
        Cipher cipher = new AESKeyedCipherProvider().getCipher(EncryptionMethod.AES_GCM, key, ivBytes, true)
        byte[] cipherBytes = cipher.doFinal(SERIALIZED_BYTES)

        int cipherBytesLength = cipherBytes.size()

        // Construct accurate metadata
        EncryptionMetadata goodMetadata = new EncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION, cipherBytesLength)
        logger.info("Created good encryption metadata: ${goodMetadata}")

        // Construct bad metadata instances
        EncryptionMetadata badKeyId = new EncryptionMetadata(keyId.reverse(), ALGORITHM, ivBytes, VERSION, cipherBytesLength)
        EncryptionMetadata badAlgorithm = new EncryptionMetadata(keyId, "ASE/GDM/SomePadding", ivBytes, VERSION, cipherBytesLength)
        EncryptionMetadata badIV = new EncryptionMetadata(keyId, ALGORITHM, new byte[16], VERSION, cipherBytesLength)
        EncryptionMetadata badVersion = new EncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION.reverse(), cipherBytesLength)
        EncryptionMetadata badCBLength = new EncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION, cipherBytesLength - 5)

        List badMetadata = [badKeyId, badAlgorithm, badIV, badVersion, badCBLength]

        // Form the proper cipherBytes
        byte[] completeGoodBytes = CryptoUtils.concatByteArrays([0x01] as byte[], encryptor.serializeEncryptionMetadata(goodMetadata), cipherBytes)

        byte[] recoveredGoodBytes = encryptor.decrypt(completeGoodBytes, recordId)
        logger.info("Recovered good bytes: ${Hex.toHexString(recoveredGoodBytes)}")

        final List EXPECTED_MESSAGES = ["The requested key ID (\\w+)? is not available",
                                        "Encountered an exception decrypting provenance record",
                                        "The event was encrypted with version ${VERSION.reverse()} which is not in the list of supported versions v1"]

        // Act
        badMetadata.eachWithIndex { EncryptionMetadata metadata, int i ->
            byte[] completeBytes = CryptoUtils.concatByteArrays([0x01] as byte[], encryptor.serializeEncryptionMetadata(metadata), cipherBytes)

            def msg = shouldFail(EncryptionException) {
                byte[] recoveredBytes = encryptor.decrypt(completeBytes, "R${i + 2}")
                logger.info("Recovered bad bytes: ${Hex.toHexString(recoveredBytes)}")
            }
            logger.expected(msg)

            // Assert
            assert EXPECTED_MESSAGES.any { msg.getMessage() =~ it }
        }
    }
}
