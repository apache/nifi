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
class AESProvenanceEventEncryptorTest {
    private static final Logger logger = LoggerFactory.getLogger(AESProvenanceEventEncryptorTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128
    private static final int IV_LENGTH = 16

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
}
