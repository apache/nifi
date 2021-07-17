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
import org.apache.nifi.processors.standard.TestEncryptContentGroovy
import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.apache.nifi.stream.io.ByteCountingInputStream
import org.apache.nifi.stream.io.ByteCountingOutputStream
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
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

            logger.info("Using ${kdf.kdfName} and ${encryptionMethod.name()}")
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

    /**
     * This test was added after observing an encryption which appended a single {@code 0x10} byte after the cipher text was written. All other bytes in the flowfile content were correct. The corresponding {@code DecryptContent} processor could not decrypt the content and manual decryption required truncating the final byte.
     * @throws Exception
     */
    @Test
    void testBcryptKDFShouldNotAddOutputBytes() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message." * 4
        logger.info("Plaintext: {}", PLAINTEXT)

        int saltLength = 29
        int saltDelimiterLength = 8
        int ivLength = 16
        int ivDelimiterLength = 6
        int plaintextBlockCount = (int) Math.ceil(PLAINTEXT.length() / 16.0)
        int cipherByteLength = (PLAINTEXT.length() % 16 == 0 ? plaintextBlockCount + 1 : plaintextBlockCount) * 16
        int EXPECTED_CIPHER_BYTE_COUNT = saltLength + saltDelimiterLength + ivLength + ivDelimiterLength + cipherByteLength
        logger.info("Expected total cipher byte count: ${EXPECTED_CIPHER_BYTE_COUNT}")

        InputStream plainStream = new ByteArrayInputStream(PLAINTEXT.getBytes("UTF-8"))

        String shortPassword = "short"

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        KeyDerivationFunction kdf = KeyDerivationFunction.BCRYPT

        // Act
        OutputStream cipherStream = new ByteArrayOutputStream()
        OutputStream recoveredStream = new ByteArrayOutputStream()

        logger.info("Using ${kdf.kdfName} and ${encryptionMethod.name()}")
        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, shortPassword.toCharArray(), kdf)

        StreamCallback encryptionCallback = encryptor.getEncryptionCallback()
        StreamCallback decryptionCallback = encryptor.getDecryptionCallback()

        encryptionCallback.process(plainStream, cipherStream)

        final byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
        logger.info("Encrypted (${cipherBytes.length}): ${Hex.encodeHexString(cipherBytes)}")
        assert cipherBytes.length == EXPECTED_CIPHER_BYTE_COUNT

        InputStream cipherInputStream = new ByteArrayInputStream(cipherBytes)
        decryptionCallback.process(cipherInputStream, recoveredStream)

        // Assert
        byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredStream).toByteArray()
        logger.info("Recovered (${recoveredBytes.length}): ${Hex.encodeHexString(recoveredBytes)}")
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: {}\n\n", recovered)
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testShouldDecryptLegacyOpenSSLSaltedCipherText() throws Exception {
        // Arrange
        Assume.assumeTrue("Skipping test because unlimited strength crypto policy not installed", CipherUtility.isUnlimitedStrengthCryptoSupported())

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
        Assume.assumeTrue("Skipping test because unlimited strength crypto policy not installed", CipherUtility.isUnlimitedStrengthCryptoSupported())

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

    @Test
    void testShouldWriteEncryptionMetadataAttributesForKDFs() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message. "
        logger.info("Plaintext: ${PLAINTEXT}")

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        def kdfs = KeyDerivationFunction.values().findAll { it.isStrongKDF() }

        // Act
        kdfs.each { KeyDerivationFunction kdf ->
            PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)
            StreamCallback encryptCallback = encryptor.getEncryptionCallback()

            // Reset the streams
            InputStream inputStream = new ByteArrayInputStream(PLAINTEXT.bytes)
            OutputStream cipherStream = new ByteArrayOutputStream()

            encryptCallback.process(inputStream, cipherStream)

            // Assert
            byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
            String cipherText = new String(cipherBytes, StandardCharsets.UTF_8)
            String cipherTextHex = Hex.encodeHexString(cipherBytes)
            logger.info("Cipher text (${cipherBytes.size()}): ${cipherTextHex}")

            int ivDelimiterStart = CipherUtility.findSequence(cipherBytes, RandomIVPBECipherProvider.IV_DELIMITER)
            logger.info("IV delimiter starts at ${ivDelimiterStart}")

            final byte[] EXPECTED_KDF_SALT_BYTES = TestEncryptContentGroovy.extractFullSaltFromCipherBytes(cipherBytes)
            final String EXPECTED_KDF_SALT = new String(EXPECTED_KDF_SALT_BYTES)
            final String EXPECTED_SALT_HEX = TestEncryptContentGroovy.extractRawSaltHexFromFullSalt(EXPECTED_KDF_SALT_BYTES, kdf)
            logger.info("Extracted expected raw salt (hex): ${EXPECTED_SALT_HEX}")

            final String EXPECTED_IV_HEX = Hex.encodeHexString(cipherBytes[(ivDelimiterStart - 16)..<ivDelimiterStart] as byte[])

            TestEncryptContentGroovy.printFlowFileAttributes(encryptor.flowfileAttributes)

            // Assert the timestamp attribute was written and is accurate
            def diff = TestEncryptContentGroovy.calculateTimestampDifference(new Date(), encryptor.flowfileAttributes.get("encryptcontent.timestamp"))
            assert diff.toMilliseconds() < 1_000
            assert encryptor.flowfileAttributes.get("encryptcontent.algorithm") == encryptionMethod.name()
            assert encryptor.flowfileAttributes.get("encryptcontent.kdf") == kdf.name()
            assert encryptor.flowfileAttributes.get("encryptcontent.action") == "encrypted"
            assert encryptor.flowfileAttributes.get("encryptcontent.salt") == EXPECTED_SALT_HEX
            assert encryptor.flowfileAttributes.get("encryptcontent.salt_length") == "16"
            assert encryptor.flowfileAttributes.get("encryptcontent.iv") == EXPECTED_IV_HEX
            assert encryptor.flowfileAttributes.get("encryptcontent.iv_length") == "16"
            assert encryptor.flowfileAttributes.get("encryptcontent.plaintext_length") == PLAINTEXT.size() as String
            assert encryptor.flowfileAttributes.get("encryptcontent.cipher_text_length") == cipherBytes.size() as String

            // PBKDF2 doesn't have a KDF salt, just the raw byte[16]
            if (kdf != KeyDerivationFunction.PBKDF2) {
                assert encryptor.flowfileAttributes.get("encryptcontent.kdf_salt") == EXPECTED_KDF_SALT
                assert (29..54)*.toString().contains(encryptor.flowfileAttributes.get("encryptcontent.kdf_salt_length"))
            }
        }
    }

    @Test
    void testPBKDF2ShouldWriteIterationsAsAttribute() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message. "
        logger.info("Plaintext: ${PLAINTEXT}")

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        KeyDerivationFunction kdf = KeyDerivationFunction.PBKDF2
        PBKDF2CipherProvider pbkdf2CipherProvider = new PBKDF2CipherProvider()
        final String EXPECTED_ITERATIONS = pbkdf2CipherProvider.getIterationCount() as String

        // Act
        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)
        StreamCallback encryptCallback = encryptor.getEncryptionCallback()

        // Reset the streams
        InputStream inputStream = new ByteArrayInputStream(PLAINTEXT.bytes)
        OutputStream cipherStream = new ByteArrayOutputStream()

        encryptCallback.process(inputStream, cipherStream)

        // Assert
        byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
        String cipherTextHex = Hex.encodeHexString(cipherBytes)
        logger.info("Cipher text (${cipherBytes.size()}): ${cipherTextHex}")

        TestEncryptContentGroovy.printFlowFileAttributes(encryptor.flowfileAttributes)

        assert encryptor.flowfileAttributes.get("encryptcontent.algorithm") == encryptionMethod.name()
        assert encryptor.flowfileAttributes.get("encryptcontent.kdf") == kdf.name()
        assert encryptor.flowfileAttributes.get("encryptcontent.action") == "encrypted"
        assert encryptor.flowfileAttributes.get("encryptcontent.pbkdf2_iterations") == EXPECTED_ITERATIONS
    }

    @Test
    void testBcryptDecryptShouldSupportLegacyKeyDerivationProcess() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message. "
        logger.info("Plaintext: ${PLAINTEXT}")

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        KeyDerivationFunction kdf = KeyDerivationFunction.BCRYPT
        BcryptCipherProvider bcryptCipherProvider = new BcryptCipherProvider()

        // Replicate PBE encryptor with manual legacy key derivation to encrypt
        final String PASSWORD = "shortPassword"
        final byte[] SALT = bcryptCipherProvider.generateSalt()
        String saltString = new String(SALT, StandardCharsets.UTF_8)
        logger.test("Using fixed Bcrypt salt: ${saltString}")

        // Determine the expected key bytes using the legacy key derivation process
        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher(bcryptCipherProvider.getWorkFactor(), bcryptCipherProvider.getDefaultSaltLength())
        byte[] rawSaltBytes = BcryptCipherProvider.extractRawSalt(saltString)
        byte[] hashOutputBytes = bcryptSecureHasher.hashRaw(PASSWORD.getBytes(StandardCharsets.UTF_8), rawSaltBytes)
        logger.test("Raw hash output (${hashOutputBytes.length}): ${Hex.encodeHexString(hashOutputBytes)}")

        MessageDigest sha512 = MessageDigest.getInstance("SHA-512", "BC")
        byte[] keyDigestBytes = sha512.digest(hashOutputBytes)
        logger.test("Key digest (${keyDigestBytes.length}): ${Hex.encodeHexString(keyDigestBytes)}")

        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.algorithm)
        byte[] derivedKeyBytes = Arrays.copyOf(keyDigestBytes, keyLength / 8 as int)
        logger.test("Derived key (${derivedKeyBytes.length}): ${Hex.encodeHexString(derivedKeyBytes)}")

        StreamCallback customEncryptCallback = { InputStream is, OutputStream os ->
            byte[] saltBytes = bcryptCipherProvider.generateSalt()
            ByteCountingInputStream bcis = new ByteCountingInputStream(is)
            ByteCountingOutputStream bcos = new ByteCountingOutputStream(os)
            bcryptCipherProvider.writeSalt(saltBytes, bcos)

            Cipher cipher = bcryptCipherProvider.getInitializedCipher(encryptionMethod, PASSWORD, saltBytes, new byte[16], keyLength, true, true)

            bcryptCipherProvider.writeIV(cipher.getIV(), bcos)
            CipherUtility.processStreams(cipher, bcis, bcos)
        } as StreamCallback

        // Reset the streams
        InputStream inputStream = new ByteArrayInputStream(PLAINTEXT.bytes)
        OutputStream cipherStream = new ByteArrayOutputStream()

        customEncryptCallback.process(inputStream, cipherStream)

        byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
        String cipherTextHex = Hex.encodeHexString(cipherBytes)
        logger.info("Cipher text (${cipherBytes.size()}): ${cipherTextHex}")

        // Act
        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)
        StreamCallback pbeDecryptCallback = encryptor.getDecryptionCallback()

        // Reset the streams
        InputStream cipherInputStream = new ByteArrayInputStream(cipherBytes)
        OutputStream recoveredOutputStream = new ByteArrayOutputStream()

        // Use PBE w/ Bcrypt to decrypt (and handle legacy key derivation process)
        pbeDecryptCallback.process(cipherInputStream, recoveredOutputStream)

        // Assert
        byte[] recoveredBytes = ((ByteArrayOutputStream) recoveredOutputStream).toByteArray()
        String recovered = new String(recoveredBytes, StandardCharsets.UTF_8)
        logger.info("Plaintext (${recoveredBytes.size()}): ${recovered}")

        // handle reader logic error (PKCS7 padding false positive) by explicitly testing legacy key derivation
        if (PLAINTEXT != recovered) {
            logger.warn("Explicit test of legacy key derivation logic.")
            InputStream inputStreamLegacy = new ByteArrayInputStream(cipherBytes)
            OutputStream outputStreamLegacy = new ByteArrayOutputStream()
            byte[] salt = bcryptCipherProvider.readSalt(inputStreamLegacy)
            byte[] iv = bcryptCipherProvider.readIV(inputStreamLegacy)
            Cipher cipherLegacy = bcryptCipherProvider.getLegacyDecryptCipher(encryptionMethod, PASSWORD, salt, iv, keyLength)
            CipherUtility.processStreams(cipherLegacy, inputStreamLegacy, outputStreamLegacy)
            String recoveredLegacy = new String(outputStreamLegacy.toByteArray(), StandardCharsets.UTF_8)
            assert recoveredLegacy == PLAINTEXT
        }
    }

    /**
     * This test was added to detect a non-deterministic problem with Scrypt expected salts being
     * 32 bytes. This was ultimately determined to be a problem with the Scrypt salt regex failing
     * to match salts containing a '+' in the first 12 characters. See
     * {@code ScryptCipherProviderGroovyTest#testShouldAcceptFormattedSaltWithPlus( )}.
     *
     * @throws Exception
     */
    @Test
    void testScryptSaltShouldBe16Bytes() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext message. "
        logger.info("Plaintext: ${PLAINTEXT}")

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        KeyDerivationFunction kdf = KeyDerivationFunction.SCRYPT

        // Act
        PasswordBasedEncryptor encryptor = new PasswordBasedEncryptor(encryptionMethod, PASSWORD.toCharArray(), kdf)
        StreamCallback encryptCallback = encryptor.getEncryptionCallback()

        // Reset the streams
        InputStream inputStream = new ByteArrayInputStream(PLAINTEXT.bytes)
        OutputStream cipherStream = new ByteArrayOutputStream()

        encryptCallback.process(inputStream, cipherStream)

        // Assert
        byte[] cipherBytes = ((ByteArrayOutputStream) cipherStream).toByteArray()
        String cipherText = new String(cipherBytes, StandardCharsets.UTF_8)
        String cipherTextHex = Hex.encodeHexString(cipherBytes)
        logger.info("Cipher text (${cipherBytes.size()}): ${cipherTextHex}")

        int ivDelimiterStart = CipherUtility.findSequence(cipherBytes, RandomIVPBECipherProvider.IV_DELIMITER)
        logger.info("IV delimiter starts at ${ivDelimiterStart}")

        final byte[] EXPECTED_KDF_SALT_BYTES = TestEncryptContentGroovy.extractFullSaltFromCipherBytes(cipherBytes)
        final String EXPECTED_KDF_SALT = new String(EXPECTED_KDF_SALT_BYTES)
        final String EXPECTED_SALT_HEX = TestEncryptContentGroovy.extractRawSaltHexFromFullSalt(EXPECTED_KDF_SALT_BYTES, kdf)
        logger.info("Extracted expected raw salt (hex): ${EXPECTED_SALT_HEX}")

        final String EXPECTED_IV_HEX = Hex.encodeHexString(cipherBytes[(ivDelimiterStart - 16)..<ivDelimiterStart] as byte[])

        TestEncryptContentGroovy.printFlowFileAttributes(encryptor.flowfileAttributes)

        // Assert the timestamp attribute was written and is accurate
        def diff = TestEncryptContentGroovy.calculateTimestampDifference(new Date(), encryptor.flowfileAttributes.get("encryptcontent.timestamp"))
        assert diff.toMilliseconds() < 1_000
        assert encryptor.flowfileAttributes.get("encryptcontent.algorithm") == encryptionMethod.name()
        assert encryptor.flowfileAttributes.get("encryptcontent.kdf") == kdf.name()
        assert encryptor.flowfileAttributes.get("encryptcontent.action") == "encrypted"
        assert encryptor.flowfileAttributes.get("encryptcontent.salt") == EXPECTED_SALT_HEX
        assert encryptor.flowfileAttributes.get("encryptcontent.salt_length") == "16"
        assert encryptor.flowfileAttributes.get("encryptcontent.iv") == EXPECTED_IV_HEX
        assert encryptor.flowfileAttributes.get("encryptcontent.iv_length") == "16"
        assert encryptor.flowfileAttributes.get("encryptcontent.plaintext_length") == PLAINTEXT.size() as String
        assert encryptor.flowfileAttributes.get("encryptcontent.cipher_text_length") == cipherBytes.size() as String

        assert encryptor.flowfileAttributes.get("encryptcontent.kdf_salt") == EXPECTED_KDF_SALT
        assert (29..54)*.toString().contains(encryptor.flowfileAttributes.get("encryptcontent.kdf_salt_length"))
    }
}