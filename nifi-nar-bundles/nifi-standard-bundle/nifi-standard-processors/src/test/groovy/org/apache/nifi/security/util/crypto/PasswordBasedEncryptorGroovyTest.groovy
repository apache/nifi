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
package org.apache.nifi.security.util.crypto

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.apache.nifi.stream.io.ByteArrayOutputStream
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.security.Security

class PasswordBasedEncryptorGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(PasswordBasedEncryptorGroovyTest.class)

    private static final String TEST_RESOURCES_PREFIX = "src/test/resources/TestEncryptContent/"
    private static final File plainFile = new File("${TEST_RESOURCES_PREFIX}/plain.txt")
    private static final File encryptedFile = new File("${TEST_RESOURCES_PREFIX}/salted_128_raw.asc")

    private static final String PASSWORD = "thisIsABadPassword"
    private static final String LEGACY_PASSWORD = "Hello, World!"

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
    void testShouldEncryptAndDecrypt() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message."
        logger.info("Plaintext: {}", PLAINTEXT)
        InputStream plainStream = new ByteArrayInputStream(PLAINTEXT.getBytes("UTF-8"))

        String shortPassword = "short"

        def encryptionMethodsAndKdfs = [
                (KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY): EncryptionMethod.MD5_128AES,
                (KeyDerivationFunction.NIFI_LEGACY)             : EncryptionMethod.MD5_128AES,
                (KeyDerivationFunction.BCRYPT)                  : EncryptionMethod.AES_CBC,
                (KeyDerivationFunction.SCRYPT)                  : EncryptionMethod.AES_CBC,
                (KeyDerivationFunction.PBKDF2)                  : EncryptionMethod.AES_CBC
        ]

        // Act
        encryptionMethodsAndKdfs.each { KeyDerivationFunction kdf, EncryptionMethod encryptionMethod ->
            OutputStream cipherStream = new ByteArrayOutputStream()
            OutputStream recoveredStream = new ByteArrayOutputStream()

            logger.info("Using ${kdf.name} and ${encryptionMethod.name()}")
            PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, shortPassword.toCharArray(), kdf)

            StreamCallback encryptionCallback = encryptor.getEncryptionCallback()
            StreamCallback decryptionCallback = encryptor.getDecryptionCallback()

            encryptionCallback.process(plainStream, cipherStream)

            final byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
            logger.info("Encrypted: {}", Hex.encodeHexString(cipherBytes))
            InputStream cipherInputStream = new ByteArrayInputStream(cipherBytes)
            decryptionCallback.process(cipherInputStream, recoveredStream)

            // Assert
            byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredStream).toByteArray()
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: {}\n\n", recovered)
            assert PLAINTEXT.equals(recovered)

            // This is necessary to run multiple iterations
            plainStream.reset()
        }
    }

    @Test
    void testShouldDecryptLegacyOpenSSLSaltedCipherText() throws Exception {
        // Arrange
        Assume.assumeTrue("Skipping test because unlimited strength crypto policy not installed", PasswordBasedEncryptor.supportsUnlimitedStrength())

        final String PLAINTEXT = new File("${TEST_RESOURCES_PREFIX}/plain.txt").text
        logger.info("Plaintext: {}", PLAINTEXT)
        byte[] cipherBytes = new File("${TEST_RESOURCES_PREFIX}/salted_128_raw.enc").bytes
        InputStream cipherStream = new ByteArrayInputStream(cipherBytes)
        OutputStream recoveredStream = new ByteArrayOutputStream()

        final EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES
        final KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY

        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)

        StreamCallback decryptionCallback = encryptor.getDecryptionCallback()
        logger.info("Cipher bytes: ${Hex.encodeHexString(cipherBytes)}")

        // Act
        decryptionCallback.process(cipherStream, recoveredStream)

        // Assert
        byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredStream).toByteArray()
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: {}", recovered)
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testShouldDecryptLegacyOpenSSLUnsaltedCipherText() throws Exception {
        // Arrange
        Assume.assumeTrue("Skipping test because unlimited strength crypto policy not installed", PasswordBasedEncryptor.supportsUnlimitedStrength())

        final String PLAINTEXT = new File("${TEST_RESOURCES_PREFIX}/plain.txt").text
        logger.info("Plaintext: {}", PLAINTEXT)
        byte[] cipherBytes = new File("${TEST_RESOURCES_PREFIX}/unsalted_128_raw.enc").bytes
        InputStream cipherStream = new ByteArrayInputStream(cipherBytes)
        OutputStream recoveredStream = new ByteArrayOutputStream()

        final EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES
        final KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY

        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)

        StreamCallback decryptionCallback = encryptor.getDecryptionCallback()
        logger.info("Cipher bytes: ${Hex.encodeHexString(cipherBytes)}")

        // Act
        decryptionCallback.process(cipherStream, recoveredStream)

        // Assert
        byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredStream).toByteArray()
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: {}", recovered)
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testShouldDecryptNiFiLegacySaltedCipherTextWithVariableSaltLength() throws Exception {
        // Arrange
        final String PLAINTEXT = new File("${TEST_RESOURCES_PREFIX}/plain.txt").text
        logger.info("Plaintext: {}", PLAINTEXT)

        final String PASSWORD = "short"
        logger.info("Password: ${PASSWORD}")

        /* The old NiFi legacy KDF code checked the algorithm block size and used it for the salt length.
         If the block size was not available, it defaulted to 8 bytes based on the default salt size. */

        def pbeEncryptionMethods = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }
        def encryptionMethodsByBlockSize = pbeEncryptionMethods.groupBy {
            Cipher cipher = Cipher.getInstance(it.algorithm, it.provider)
            cipher.getBlockSize()
        }

        logger.info("Grouped algorithms by block size: ${encryptionMethodsByBlockSize.collectEntries { k, v -> [k, v*.algorithm] }}")

        encryptionMethodsByBlockSize.each { int blockSize, List<EncryptionMethod> encryptionMethods ->
            encryptionMethods.each { EncryptionMethod encryptionMethod ->
                final int EXPECTED_SALT_SIZE = (blockSize > 0) ? blockSize : 8
                logger.info("Testing ${encryptionMethod.algorithm} with expected salt size ${EXPECTED_SALT_SIZE}")

                def legacySaltHex = "aa" * EXPECTED_SALT_SIZE
                byte[] legacySalt = Hex.decodeHex(legacySaltHex as char[])
                logger.info("Generated legacy salt ${legacySaltHex} (${legacySalt.length})")

                // Act

                // Encrypt using the raw legacy code
                NiFiLegacyCipherProvider legacyCipherProvider = new NiFiLegacyCipherProvider()
                Cipher legacyCipher = legacyCipherProvider.getCipher(encryptionMethod, PASSWORD, legacySalt, true)
                byte[] cipherBytes = legacyCipher.doFinal(PLAINTEXT.bytes)
                logger.info("Cipher bytes: ${Hex.encodeHexString(cipherBytes)}")

                byte[] completeCipherStreamBytes = CryptoUtils.concatByteArrays(legacySalt, cipherBytes)
                logger.info("Complete cipher stream: ${Hex.encodeHexString(completeCipherStreamBytes)}")

                InputStream cipherStream = new ByteArrayInputStream(completeCipherStreamBytes)
                OutputStream resultStream = new ByteArrayOutputStream()

                // Now parse and decrypt using PBE encryptor
                PasswordBasedEncryptor decryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD as char[], KeyDerivationFunction.NIFI_LEGACY)

                StreamCallback decryptCallback = decryptor.decryptionCallback
                decryptCallback.process(cipherStream, resultStream)

                logger.info("Decrypted: ${Hex.encodeHexString(resultStream.toByteArray())}")
                String recovered = new String(resultStream.toByteArray())
                logger.info("Recovered: ${recovered}")

                // Assert
                assert recovered == PLAINTEXT
            }
        }
    }
}