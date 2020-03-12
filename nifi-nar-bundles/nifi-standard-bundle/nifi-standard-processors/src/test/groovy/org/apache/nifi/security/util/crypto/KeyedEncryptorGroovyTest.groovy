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
import org.apache.nifi.security.util.KeyDerivationFunction
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.Security

class KeyedEncryptorGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(KeyedEncryptorGroovyTest.class)

    private static final String TEST_RESOURCES_PREFIX = "src/test/resources/TestEncryptContent/"
    private static final File plainFile = new File("${TEST_RESOURCES_PREFIX}/plain.txt")
    private static final File encryptedFile = new File("${TEST_RESOURCES_PREFIX}/unsalted_128_raw.asc")

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"
    private static final SecretKey KEY = new SecretKeySpec(Hex.decodeHex(KEY_HEX as char[]), "AES")

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
    void testShouldDecryptOpenSSLUnsaltedCipherTextWithKnownIV() throws Exception {
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

    /**
     * This test demonstrates that if incoming cipher text was generated by a cipher using PBE with
     * KDF, the salt can be skipped and the cipher bytes can still be decrypted using keyed encryption.
     * @throws Exception
     */
    @Test
    void testShouldSkipSaltOnDecrypt() throws Exception {
        final String PASSWORD = "thisIsABadPassword"

        final String PLAINTEXT = "This is a plaintext message."
        logger.info("Plaintext: {}", PLAINTEXT)
        InputStream plainStream = new ByteArrayInputStream(PLAINTEXT.getBytes("UTF-8"))

        OutputStream cipherStream = new ByteArrayOutputStream()
        OutputStream recoveredStream = new ByteArrayOutputStream()

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.algorithm)
        logger.info("Using ${encryptionMethod.name()} with key length ${keyLength} bits")

        // The PBE encryptor encrypts the data and prepends the salt and IV
        PasswordBasedEncryptor passwordBasedEncryptor = new PasswordBasedEncryptor(EncryptionMethod.AES_CBC, PASSWORD.toCharArray(), KeyDerivationFunction.ARGON2)
        StreamCallback encryptionCallback = passwordBasedEncryptor.getEncryptionCallback()
        encryptionCallback.process(plainStream, cipherStream)

        final byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
        logger.info("Encrypted: {}", Hex.encodeHexString(cipherBytes))

        // Derive the decryption key manually from the provided salt & password
        String kdfSalt = passwordBasedEncryptor.flowfileAttributes.get("encryptcontent.kdf_salt")
        def costs = parseArgon2CostParamsFromSalt(kdfSalt)
        SecureHasher secureHasher = new Argon2SecureHasher(keyLength / 8 as int, costs.m, costs.p, costs.t)
        byte[] argon2DerivedKeyBytes = secureHasher.hashRaw(PASSWORD.getBytes(StandardCharsets.UTF_8), Argon2CipherProvider.extractRawSaltFromArgon2Salt(kdfSalt))
        logger.sanity("Derived key bytes: ${Hex.encodeHexString(argon2DerivedKeyBytes)}")

        // The keyed encryptor will attempt to decrypt the content, skipping the salt
        KeyedEncryptor keyedEncryptor = new KeyedEncryptor(encryptionMethod, argon2DerivedKeyBytes)
        StreamCallback decryptionCallback = keyedEncryptor.getDecryptionCallback()
        InputStream cipherInputStream = new ByteArrayInputStream(cipherBytes)

        // Act
        decryptionCallback.process(cipherInputStream, recoveredStream)

        // Assert
        byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredStream).toByteArray()
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: {}\n\n", recovered)
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testShouldParseCostParams() {
        // Arrange
        String argon2Salt = "\$argon2id\$v=19\$m=4096,t=3,p=1\$i8CIuSjrwdSuR42pb15AoQ"

        // Act
        def cost = parseArgon2CostParamsFromSalt(argon2Salt)
        logger.info("Parsed cost: ${cost}")

        // Assert
        assert cost == [m: 4096, t: 3, p: 1]
    }

    static Map<String, Integer> parseArgon2CostParamsFromSalt(String kdfSalt) {
        kdfSalt.tokenize("\$")[2].split(",").collectEntries {
            def l = it.split("=")
            [l.first(), Integer.valueOf(l.last())]
        }
    }
}