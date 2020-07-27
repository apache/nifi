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
import org.apache.nifi.security.util.EncryptionMethod
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import java.security.Security

public class KeyedEncryptorGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(KeyedEncryptorGroovyTest.class)

    private static final String TEST_RESOURCES_PREFIX = "src/test/resources/TestEncryptContent/"
    private static final File plainFile = new File("${TEST_RESOURCES_PREFIX}/plain.txt")
    private static final File encryptedFile = new File("${TEST_RESOURCES_PREFIX}/unsalted_128_raw.asc")

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
    private static final SecretKey KEY = new SecretKeySpec(Hex.decodeHex(KEY_HEX as char[]), "AES")

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

        OutputStream cipherStream = new ByteArrayOutputStream()
        OutputStream recoveredStream = new ByteArrayOutputStream()

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using ${encryptionMethod.name()}")

        // Act
        KeyedEncryptor encryptor = new KeyedEncryptor(encryptionMethod, KEY)

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
    }

    @Test
    public void testShouldDecryptOpenSSLUnsaltedCipherTextWithKnownIV() throws Exception {
        // Arrange
        final String PLAINTEXT = new File("${TEST_RESOURCES_PREFIX}/plain.txt").text
        logger.info("Plaintext: {}", PLAINTEXT)
        byte[] cipherBytes = new File("${TEST_RESOURCES_PREFIX}/unsalted_128_raw.enc").bytes

        final String keyHex = "711E85689CE7AFF6F410AEA43ABC5446"
        final String ivHex = "842F685B84879B2E00F977C22B9E9A7D"

        InputStream cipherStream = new ByteArrayInputStream(cipherBytes)
        OutputStream recoveredStream = new ByteArrayOutputStream()

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        KeyedEncryptor encryptor = new KeyedEncryptor(encryptionMethod, new SecretKeySpec(Hex.decodeHex(keyHex as char[]), "AES"), Hex.decodeHex(ivHex as char[]))

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