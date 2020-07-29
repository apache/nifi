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
package org.apache.nifi.security.repository.block.aes

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
import java.security.Security

@RunWith(JUnit4.class)
class RepositoryObjectAESGCMEncryptorTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryObjectAESGCMEncryptorTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String LOG_PACKAGE = "org.slf4j.simpleLogger.log.org.apache.nifi.security.repository.block.aes"

    private static KeyProvider mockKeyProvider
    private static AESKeyedCipherProvider mockCipherProvider

    private static String ORIGINAL_LOG_LEVEL

    private RepositoryObjectAESGCMEncryptor encryptor

    @BeforeClass
    static void setUpOnce() throws Exception {
        ORIGINAL_LOG_LEVEL = System.getProperty(LOG_PACKAGE)
        System.setProperty(LOG_PACKAGE, "DEBUG")

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
                }] as AESKeyedCipherProvider
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
            System.setProperty(LOG_PACKAGE, ORIGINAL_LOG_LEVEL)
        }
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    /**
     * Given arbitrary bytes, create an OutputStream, encrypt them, and persist with the (plaintext) encryption metadata, then recover
     */
    @Test
    void testShouldEncryptAndDecryptArbitraryBytes() {
        // Arrange
        final byte[] SERIALIZED_BYTES = "This is a plaintext message.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes (${SERIALIZED_BYTES.size()}): ${Hex.toHexString(SERIALIZED_BYTES)}")

        encryptor = new RepositoryObjectAESGCMEncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"
        logger.info("Using record ID ${recordId} and key ID ${keyId}")

        // Act
        byte[] encryptedBytes = encryptor.encrypt(SERIALIZED_BYTES, recordId, keyId)
        logger.info("Encrypted bytes: ${Hex.toHexString(encryptedBytes)}".toString())

        byte[] decryptedBytes = encryptor.decrypt(encryptedBytes, recordId)
        logger.info("Decrypted data to: \n\t${Hex.toHexString(decryptedBytes)}")

        // Assert
        assert decryptedBytes == SERIALIZED_BYTES
        logger.info("Decoded: ${new String(decryptedBytes, StandardCharsets.UTF_8)}")
    }

    /**
     * Test which demonstrates that multiple encryption and decryption calls each receive their own independent {@link RepositoryObjectEncryptionMetadata} instance.
     */
    @Test
    void testShouldEncryptAndDecryptMultiplePiecesOfContent() {
        // Arrange
        final byte[] SERIALIZED_BYTES_1 = "This is plaintext content 1.".getBytes(StandardCharsets.UTF_8)
        final byte[] SERIALIZED_BYTES_2 = "This is plaintext content 2.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes 1 (${SERIALIZED_BYTES_1.size()}): ${Hex.toHexString(SERIALIZED_BYTES_1)}")
        logger.info("Serialized bytes 2 (${SERIALIZED_BYTES_2.size()}): ${Hex.toHexString(SERIALIZED_BYTES_2)}")

        encryptor = new RepositoryObjectAESGCMEncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId1 = "R1"
        String recordId2 = "R2"

        // Act
        logger.info("Using record ID ${recordId1} and key ID ${keyId}")
        byte[] encryptedBytes1 = encryptor.encrypt(SERIALIZED_BYTES_1, recordId1, keyId)
        logger.info("Encrypted bytes 1: ${Hex.toHexString(encryptedBytes1)}".toString())

        logger.info("Using record ID ${recordId2} and key ID ${keyId}")
        byte[] encryptedBytes2 = encryptor.encrypt(SERIALIZED_BYTES_2, recordId2, keyId)
        logger.info("Encrypted bytes 2: ${Hex.toHexString(encryptedBytes2)}".toString())

        byte[] decryptedBytes1 = encryptor.decrypt(encryptedBytes1, recordId1)
        logger.info("Decrypted data 1 to: \n\t${Hex.toHexString(decryptedBytes1)}")

        byte[] decryptedBytes2 = encryptor.decrypt(encryptedBytes2, recordId2)
        logger.info("Decrypted data 2 to: \n\t${Hex.toHexString(decryptedBytes2)}")

        // Assert
        assert decryptedBytes1 == SERIALIZED_BYTES_1
        logger.info("Decoded 1: ${new String(decryptedBytes1, StandardCharsets.UTF_8)}")

        assert decryptedBytes2 == SERIALIZED_BYTES_2
        logger.info("Decoded 2: ${new String(decryptedBytes2, StandardCharsets.UTF_8)}")
    }

    /**
     * Test which demonstrates that encrypting and decrypting large blocks of content (~6 KB) works via block mechanism
     */
    @Test
    void testShouldEncryptAndDecryptLargeContent() {
        // Arrange
        final byte[] IMAGE_BYTES = new File("src/test/resources/nifi.png").readBytes()
        logger.info("Image bytes (${IMAGE_BYTES.size()}): src/test/resources/nifi.png")

        encryptor = new RepositoryObjectAESGCMEncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"

        // Act
        logger.info("Using record ID ${recordId} and key ID ${keyId}")
        byte[] encryptedBytes = encryptor.encrypt(IMAGE_BYTES, recordId, keyId)
        logger.info("Encrypted bytes (${encryptedBytes.size()}): ${Hex.toHexString(encryptedBytes)[0..<32]}...".toString())

        byte[] decryptedBytes = encryptor.decrypt(encryptedBytes, recordId)
        logger.info("Decrypted data to (${decryptedBytes.size()}): \n\t${Hex.toHexString(decryptedBytes)[0..<32]}...")

        // Assert
        assert decryptedBytes == IMAGE_BYTES
        logger.info("Decoded (binary PNG header): ${new String(decryptedBytes[0..<16] as byte[], StandardCharsets.UTF_8)}...")
    }

    /**
     * Test which demonstrates that multiple encryption and decryption calls each receive their own independent {@code RepositoryObjectEncryptionMetadata} instance and use independent keys.
     */
    @Test
    void testShouldEncryptAndDecryptMultiplePiecesOfContentWithDifferentKeys() {
        // Arrange
        final byte[] SERIALIZED_BYTES_1 = "This is plaintext content 1.".getBytes(StandardCharsets.UTF_8)
        final byte[] SERIALIZED_BYTES_2 = "This is plaintext content 2.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes 1 (${SERIALIZED_BYTES_1.size()}): ${Hex.toHexString(SERIALIZED_BYTES_1)}")
        logger.info("Serialized bytes 2 (${SERIALIZED_BYTES_2.size()}): ${Hex.toHexString(SERIALIZED_BYTES_2)}")

        // Set up a mock that can provide multiple keys
        KeyProvider mockMultipleKeyProvider = [
                getKey   : { String keyId ->
                    logger.mock("Requesting key ID: ${keyId}")
                    def keyHex = keyId == "K1" ? KEY_HEX : "AB" * 16
                    new SecretKeySpec(Hex.decode(keyHex), "AES")
                },
                keyExists: { String keyId ->
                    logger.mock("Checking existence of ${keyId}")
                    true
                }] as KeyProvider

        encryptor = new RepositoryObjectAESGCMEncryptor()
        encryptor.initialize(mockMultipleKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId1 = "K1"
        String keyId2 = "K2"
        String recordId1 = "R1"
        String recordId2 = "R2"

        // Act
        logger.info("Using record ID ${recordId1} and key ID ${keyId1}")
        byte[] encryptedBytes1 = encryptor.encrypt(SERIALIZED_BYTES_1, recordId1, keyId1)
        logger.info("Encrypted bytes 1: ${Hex.toHexString(encryptedBytes1)}".toString())

        logger.info("Using record ID ${recordId2} and key ID ${keyId2}")
        byte[] encryptedBytes2 = encryptor.encrypt(SERIALIZED_BYTES_2, recordId2, keyId2)
        logger.info("Encrypted bytes 2: ${Hex.toHexString(encryptedBytes2)}".toString())

        byte[] decryptedBytes1 = encryptor.decrypt(encryptedBytes1, recordId1)
        logger.info("Decrypted data 1 to: \n\t${Hex.toHexString(decryptedBytes1)}")

        byte[] decryptedBytes2 = encryptor.decrypt(encryptedBytes2, recordId2)
        logger.info("Decrypted data 2 to: \n\t${Hex.toHexString(decryptedBytes2)}")

        // Assert
        assert decryptedBytes1 == SERIALIZED_BYTES_1
        logger.info("Decoded 1: ${new String(decryptedBytes1, StandardCharsets.UTF_8)}")

        assert decryptedBytes2 == SERIALIZED_BYTES_2
        logger.info("Decoded 2: ${new String(decryptedBytes2, StandardCharsets.UTF_8)}")
    }
}
