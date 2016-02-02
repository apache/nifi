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
package org.apache.nifi.processors.standard.util.crypto

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

public class PasswordBasedEncryptorGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(PasswordBasedEncryptorGroovyTest.class)

    private static final String TEST_RESOURCES_PREFIX = "src/test/resources/TestEncryptContent/"
    private static final File plainFile = new File("${TEST_RESOURCES_PREFIX}/plain.txt")
    private static final File encryptedFile = new File("${TEST_RESOURCES_PREFIX}/salted_128_raw.asc")

    private static final String PASSWORD = "thisIsABadPassword"
    private static final String LEGACY_PASSWORD = "Hello, World!"

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
    public void testShouldEncryptAndDecrypt() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message."
        logger.info("Plaintext: {}", PLAINTEXT)
        InputStream plainStream = new ByteArrayInputStream(PLAINTEXT.getBytes("UTF-8"))

        String shortPassword = "shortPassword"

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
    public void testShouldDecryptLegacyOpenSSLSaltedCipherText() throws Exception {
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
    public void testShouldDecryptLegacyOpenSSLUnsaltedCipherText() throws Exception {
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
}