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
package org.apache.nifi.security.repository.stream.aes

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
class RepositoryObjectAESCTREncryptorTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryObjectAESCTREncryptorTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String LOG_PACKAGE = "org.slf4j.simpleLogger.log.org.apache.nifi.security.repository.stream.aes"
    private static String ORIGINAL_LOG_LEVEL

    private static KeyProvider mockKeyProvider
    private static AESKeyedCipherProvider mockCipherProvider

    private RepositoryObjectAESCTREncryptor encryptor

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
                    Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding")
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

        encryptor = new RepositoryObjectAESCTREncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"
        logger.info("Using record ID ${recordId} and key ID ${keyId}")

        OutputStream encryptDestination = new ByteArrayOutputStream(256)

        // Act
        OutputStream encryptedOutputStream = encryptor.encrypt(encryptDestination, recordId, keyId)
        encryptedOutputStream.write(SERIALIZED_BYTES)
        encryptedOutputStream.flush()
        encryptedOutputStream.close()

        byte[] encryptedBytes = encryptDestination.toByteArray()
        logger.info("Encrypted bytes: ${Hex.toHexString(encryptedBytes)}".toString())

        InputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes)

        InputStream decryptedInputStream = encryptor.decrypt(encryptedInputStream, recordId)
        byte[] recoveredBytes = new byte[SERIALIZED_BYTES.length]
        decryptedInputStream.read(recoveredBytes)
        logger.info("Decrypted data to: \n\t${Hex.toHexString(recoveredBytes)}")

        // Assert
        assert recoveredBytes == SERIALIZED_BYTES
        logger.info("Decoded: ${new String(recoveredBytes, StandardCharsets.UTF_8)}")
    }

    /**
     * Test which demonstrates that normal mechanism of {@code OutputStream os = repository.write(contentClaim); os.write(content1); os.write(content2);} works because only one encryption metadata record is written (before {@code content1}). {@code content2} is written with the same recordId and keyId because the output stream is written to by the same {@code session.write()}
     */
    @Test
    void testShouldEncryptAndDecryptMultiplePiecesOfContent() {
        // Arrange
        final byte[] SERIALIZED_BYTES_1 = "This is plaintext content 1.".getBytes(StandardCharsets.UTF_8)
        final byte[] SERIALIZED_BYTES_2 = "This is plaintext content 2.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes 1 (${SERIALIZED_BYTES_1.size()}): ${Hex.toHexString(SERIALIZED_BYTES_1)}")
        logger.info("Serialized bytes 2 (${SERIALIZED_BYTES_2.size()}): ${Hex.toHexString(SERIALIZED_BYTES_2)}")

        encryptor = new RepositoryObjectAESCTREncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"

        OutputStream encryptDestination = new ByteArrayOutputStream(512)

        // Act
        logger.info("Using record ID ${recordId} and key ID ${keyId}")
        OutputStream encryptedOutputStream = encryptor.encrypt(encryptDestination, recordId, keyId)
        encryptedOutputStream.write(SERIALIZED_BYTES_1)
        encryptedOutputStream.write(SERIALIZED_BYTES_2)

        encryptedOutputStream.flush()
        encryptedOutputStream.close()

        byte[] encryptedBytes = encryptDestination.toByteArray()
        logger.info("Encrypted bytes: ${Hex.toHexString(encryptedBytes)}".toString())

        InputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes)

        InputStream decryptedInputStream = encryptor.decrypt(encryptedInputStream, recordId)
        byte[] recoveredBytes1 = new byte[SERIALIZED_BYTES_1.length]
        decryptedInputStream.read(recoveredBytes1)
        logger.info("Decrypted data 1 to: \n\t${Hex.toHexString(recoveredBytes1)}")

        byte[] recoveredBytes2 = new byte[SERIALIZED_BYTES_2.length]
        decryptedInputStream.read(recoveredBytes2)
        logger.info("Decrypted data 2 to: \n\t${Hex.toHexString(recoveredBytes2)}")

        // Assert
        assert recoveredBytes1 == SERIALIZED_BYTES_1
        logger.info("Decoded 1: ${new String(recoveredBytes1, StandardCharsets.UTF_8)}")

        assert recoveredBytes2 == SERIALIZED_BYTES_2
        logger.info("Decoded 2: ${new String(recoveredBytes2, StandardCharsets.UTF_8)}")
    }

    /**
     * Test which demonstrates that encrypting and decrypting large blocks of content (~6 KB) works via streaming mechanism
     */
    @Test
    void testShouldEncryptAndDecryptLargeContent() {
        // Arrange
        final byte[] IMAGE_BYTES = new File("src/test/resources/nifi.png").readBytes()
        logger.info("Image bytes (${IMAGE_BYTES.size()}): src/test/resources/nifi.png")

        // Arbitrary buffer size to force multiple writes
        final int BUFFER_SIZE = 256

        encryptor = new RepositoryObjectAESCTREncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId = "R1"

        // Create a stream with enough room for the content and some header & encryption overhead
        OutputStream encryptDestination = new ByteArrayOutputStream(6 * 1024 + 512)

        // Act
        logger.info("Using record ID ${recordId} and key ID ${keyId}")
        OutputStream encryptedOutputStream = encryptor.encrypt(encryptDestination, recordId, keyId)

        // Buffer the byte[] writing to the stream in chunks of BUFFER_SIZE
        for (int i = 0; i < IMAGE_BYTES.length; i+= BUFFER_SIZE) {
            int buf = Math.min(i+BUFFER_SIZE, IMAGE_BYTES.length)
            encryptedOutputStream.write((byte[]) (IMAGE_BYTES[i..<buf]))
            encryptedOutputStream.flush()
        }
        encryptedOutputStream.close()

        byte[] encryptedBytes = encryptDestination.toByteArray()
        logger.info("Encrypted bytes (${encryptedBytes.size()}): ${Hex.toHexString(encryptedBytes)}".toString())

        InputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes)

        InputStream decryptedInputStream = encryptor.decrypt(encryptedInputStream, recordId)
        byte[] recoveredBytes = decryptedInputStream.getBytes()
        logger.info("Decrypted data to (${recoveredBytes.size()}): \n\t${Hex.toHexString(recoveredBytes)}")

        // Assert
        assert recoveredBytes == IMAGE_BYTES
        logger.info("Decoded (binary PNG header): ${new String(recoveredBytes[0..<16] as byte[], StandardCharsets.UTF_8)}...")
    }

    /**
     * Test which demonstrates that if two {@code #encrypt()} calls are made, each piece of content is encrypted and decrypted independently, and each has its own encryption metadata persisted
     */
    @Test
    void testShouldEncryptAndDecryptMultiplePiecesOfContentIndependently() {
        // Arrange
        final byte[] SERIALIZED_BYTES_1 = "This is plaintext content 1.".getBytes(StandardCharsets.UTF_8)
        final byte[] SERIALIZED_BYTES_2 = "This is plaintext content 2.".getBytes(StandardCharsets.UTF_8)
        logger.info("Serialized bytes 1 (${SERIALIZED_BYTES_1.size()}): ${Hex.toHexString(SERIALIZED_BYTES_1)}")
        logger.info("Serialized bytes 2 (${SERIALIZED_BYTES_2.size()}): ${Hex.toHexString(SERIALIZED_BYTES_2)}")

        encryptor = new RepositoryObjectAESCTREncryptor()
        encryptor.initialize(mockKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId = "K1"
        String recordId1 = "R1"
        String recordId2 = "R2"

        OutputStream encryptDestination1 = new ByteArrayOutputStream(512)
        OutputStream encryptDestination2 = new ByteArrayOutputStream(512)

        // Act
        logger.info("Using record ID ${recordId1} and key ID ${keyId}")
        OutputStream encryptedOutputStream1 = encryptor.encrypt(encryptDestination1, recordId1, keyId)
        encryptedOutputStream1.write(SERIALIZED_BYTES_1)
        encryptedOutputStream1.flush()
        encryptedOutputStream1.close()

        logger.info("Using record ID ${recordId2} and key ID ${keyId}")
        OutputStream encryptedOutputStream2 = encryptor.encrypt(encryptDestination2, recordId2, keyId)
        encryptedOutputStream2.write(SERIALIZED_BYTES_2)
        encryptedOutputStream2.flush()
        encryptedOutputStream2.close()
        
        byte[] encryptedBytes1 = encryptDestination1.toByteArray()
        logger.info("Encrypted bytes 1: ${Hex.toHexString(encryptedBytes1)}".toString())

        byte[] encryptedBytes2 = encryptDestination2.toByteArray()
        logger.info("Encrypted bytes 2: ${Hex.toHexString(encryptedBytes2)}".toString())

        InputStream encryptedInputStream1 = new ByteArrayInputStream(encryptedBytes1)
        InputStream encryptedInputStream2 = new ByteArrayInputStream(encryptedBytes2)

        InputStream decryptedInputStream1 = encryptor.decrypt(encryptedInputStream1, recordId1)
        byte[] recoveredBytes1 = new byte[SERIALIZED_BYTES_1.length]
        decryptedInputStream1.read(recoveredBytes1)
        logger.info("Decrypted data 1 to: \n\t${Hex.toHexString(recoveredBytes1)}")

        InputStream decryptedInputStream2 = encryptor.decrypt(encryptedInputStream2, recordId2)
        byte[] recoveredBytes2 = new byte[SERIALIZED_BYTES_2.length]
        decryptedInputStream2.read(recoveredBytes2)
        logger.info("Decrypted data 2 to: \n\t${Hex.toHexString(recoveredBytes2)}")

        // Assert
        assert recoveredBytes1 == SERIALIZED_BYTES_1
        logger.info("Decoded 1: ${new String(recoveredBytes1, StandardCharsets.UTF_8)}")

        assert recoveredBytes2 == SERIALIZED_BYTES_2
        logger.info("Decoded 2: ${new String(recoveredBytes2, StandardCharsets.UTF_8)}")
    }

    /**
     * Test which demonstrates that if two {@code #encrypt()} calls are made *with different keys*, each piece of content is encrypted and decrypted independently, and each has its own encryption metadata persisted (including the key ID)
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


        encryptor = new RepositoryObjectAESCTREncryptor()
        encryptor.initialize(mockMultipleKeyProvider)
        encryptor.setCipherProvider(mockCipherProvider)
        logger.info("Created ${encryptor}")

        String keyId1 = "K1"
        String keyId2 = "K1"
        String recordId1 = "R1"
        String recordId2 = "R2"

        OutputStream encryptDestination1 = new ByteArrayOutputStream(512)
        OutputStream encryptDestination2 = new ByteArrayOutputStream(512)

        // Act
        logger.info("Using record ID ${recordId1} and key ID ${keyId1}")
        OutputStream encryptedOutputStream1 = encryptor.encrypt(encryptDestination1, recordId1, keyId1)
        encryptedOutputStream1.write(SERIALIZED_BYTES_1)
        encryptedOutputStream1.flush()
        encryptedOutputStream1.close()

        logger.info("Using record ID ${recordId2} and key ID ${keyId2}")
        OutputStream encryptedOutputStream2 = encryptor.encrypt(encryptDestination2, recordId2, keyId2)
        encryptedOutputStream2.write(SERIALIZED_BYTES_2)
        encryptedOutputStream2.flush()
        encryptedOutputStream2.close()

        byte[] encryptedBytes1 = encryptDestination1.toByteArray()
        logger.info("Encrypted bytes 1: ${Hex.toHexString(encryptedBytes1)}".toString())

        byte[] encryptedBytes2 = encryptDestination2.toByteArray()
        logger.info("Encrypted bytes 2: ${Hex.toHexString(encryptedBytes2)}".toString())

        InputStream encryptedInputStream1 = new ByteArrayInputStream(encryptedBytes1)
        InputStream encryptedInputStream2 = new ByteArrayInputStream(encryptedBytes2)

        InputStream decryptedInputStream1 = encryptor.decrypt(encryptedInputStream1, recordId1)
        byte[] recoveredBytes1 = new byte[SERIALIZED_BYTES_1.length]
        decryptedInputStream1.read(recoveredBytes1)
        logger.info("Decrypted data 1 to: \n\t${Hex.toHexString(recoveredBytes1)}")

        InputStream decryptedInputStream2 = encryptor.decrypt(encryptedInputStream2, recordId2)
        byte[] recoveredBytes2 = new byte[SERIALIZED_BYTES_2.length]
        decryptedInputStream2.read(recoveredBytes2)
        logger.info("Decrypted data 2 to: \n\t${Hex.toHexString(recoveredBytes2)}")

        // Assert
        assert recoveredBytes1 == SERIALIZED_BYTES_1
        logger.info("Decoded 1: ${new String(recoveredBytes1, StandardCharsets.UTF_8)}")

        assert recoveredBytes2 == SERIALIZED_BYTES_2
        logger.info("Decoded 2: ${new String(recoveredBytes2, StandardCharsets.UTF_8)}")
    }
}
