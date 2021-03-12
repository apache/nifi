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
package org.apache.nifi.security.util.crypto

import at.favre.lib.crypto.bcrypt.BCrypt
import at.favre.lib.crypto.bcrypt.Radix64Encoder
import org.apache.commons.codec.binary.Hex
import org.apache.nifi.security.util.EncryptionMethod
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail
import static org.junit.Assert.assertTrue

@RunWith(JUnit4.class)
class BcryptCipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProviderGroovyTest.class)

    private static final String PLAINTEXT = "ExactBlockSizeRequiredForProcess"

    private static List<EncryptionMethod> strongKDFEncryptionMethods

    private static final int DEFAULT_KEY_LENGTH = 128
    public static final String MICROBENCHMARK = "microbenchmark"
    private static ArrayList<Integer> AES_KEY_LENGTHS

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        strongKDFEncryptionMethods = EncryptionMethod.values().findAll { it.isCompatibleWithStrongKDFs() }

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        if (CipherUtility.isUnlimitedStrengthCryptoSupported()) {
            AES_KEY_LENGTHS = [128, 192, 256]
        } else {
            AES_KEY_LENGTHS = [128]
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT.equals(recovered)
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()
        final byte[] IV = Hex.decodeHex("01" * 16 as char[])

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT.equals(recovered)
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                CipherUtility.isUnlimitedStrengthCryptoSupported())

        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()

        final int LONG_KEY_LENGTH = 256

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, LONG_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, LONG_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT.equals(recovered)
        }
    }

    @Test
    void testHashPWShouldMatchTestVectors() {
        // Arrange
        final byte[] PASSWORD = 'abcdefghijklmnopqrstuvwxyz'.getBytes(StandardCharsets.UTF_8)
        final byte[] SALT = new Radix64Encoder.Default().decode('fVH8e28OQRj9tqiDXs1e1u'.getBytes(StandardCharsets.UTF_8))
        final String EXPECTED_HASH = '$2a$10$fVH8e28OQRj9tqiDXs1e1uxpsjN0c7II7YPKXua2NAKYvM6iQk7dq'
        final int WORK_FACTOR = 10

        // Act
        String libraryCalculatedHash = new String(BCrypt.withDefaults().hash(WORK_FACTOR, SALT, PASSWORD), StandardCharsets.UTF_8)
        logger.info("Generated ${libraryCalculatedHash}")

        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher(WORK_FACTOR)
        String secureHasherCalculatedHash = new String(bcryptSecureHasher.hashRaw(PASSWORD, SALT), StandardCharsets.UTF_8)
        logger.info("Generated ${secureHasherCalculatedHash}")

        // Assert
        assert secureHasherCalculatedHash == EXPECTED_HASH
        assert secureHasherCalculatedHash == EXPECTED_HASH
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        final int WORK_FACTOR = 10
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(WORK_FACTOR)

        final String PLAINTEXT = "This is a plaintext message."
        final String PASSWORD = "thisIsABadPassword"

        // These values can be generated by running `$ ./openssl_bcrypt` in the terminal
        // The Ruby bcrypt gem does not expose the custom Radix64 decoder, so maintain the R64 encoding from the output and decode here
        final byte[] SALT = new Radix64Encoder.Default().decode("LBVzJoPgh.85YCvnos4BKO".bytes)
        final byte[] IV = Hex.decodeHex("bae8a9d935748a75ff0e0bbd95a4f024" as char[])

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "\$2a\$10\$LBVzJoPgh.85YCvnos4BKOyYM.LRni6UbU4v/CEPBkmFIiigADJZi"
        logger.info("Full Hash: ${FULL_HASH}")
        final String HASH = FULL_HASH[-31..-1]
        logger.info("     Hash: ${HASH.padLeft(60, " ")}")
        logger.info(" B64 Salt: ${customB64Encode(SALT).padLeft(29, " ")}")

        String extractedSalt = FULL_HASH[7..<29]
        logger.info("Extracted Salt:   ${extractedSalt}")
        String extractedSaltHex = Hex.encodeHexString(customB64Decode(extractedSalt))
        logger.info("Extracted Salt (hex): ${extractedSaltHex}")
        logger.info(" Expected Salt (hex): ${Hex.encodeHexString(SALT)}")

        final String CIPHER_TEXT = "d232b68e7aa38242d195c54b8f360d8b8d6b7580b190ffdeef99f5fe460bd6b0"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")
        logger.info("External cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        // Sanity check
        Cipher rubyCipher = Cipher.getInstance(encryptionMethod.algorithm, "BC")
        def rubyKey = new SecretKeySpec(Hex.decodeHex("01ea96ccc48a1d045bd7f461721b94a8" as char[]), "AES")
        def ivSpec = new IvParameterSpec(IV)
        rubyCipher.init(Cipher.ENCRYPT_MODE, rubyKey, ivSpec)
        byte[] rubyCipherBytes = rubyCipher.doFinal(PLAINTEXT.bytes)
        logger.info("Expected cipher text: ${Hex.encodeHexString(rubyCipherBytes)}")
        rubyCipher.init(Cipher.DECRYPT_MODE, rubyKey, ivSpec)
        assert rubyCipher.doFinal(rubyCipherBytes) == PLAINTEXT.bytes
        assert rubyCipher.doFinal(cipherBytes) == PLAINTEXT.bytes
        logger.sanity("Decrypted external cipher text and generated cipher text successfully")

        // Sanity for hash generation
        final String FULL_SALT = FULL_HASH[0..<29]
        logger.sanity("Salt from external: ${FULL_SALT}")
        String generatedHash = new String(BCrypt.withDefaults().hash(WORK_FACTOR, BcryptCipherProvider.extractRawSalt(FULL_SALT), PASSWORD.bytes))
        logger.sanity("Generated hash: ${generatedHash}")
        assert generatedHash == FULL_HASH

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, FULL_SALT.bytes, IV, DEFAULT_KEY_LENGTH, false)
        byte[] recoveredBytes = cipher.doFinal(cipherBytes)
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered)
    }

    private static byte[] customB64Decode(String input) {
        customB64Decode(input.bytes)
    }

    private static byte[] customB64Decode(byte[] input) {
        new Radix64Encoder.Default().decode(input)
    }

    private static String customB64Encode(String input) {
        customB64Encode(input.bytes)
    }

    private static String customB64Encode(byte[] input) {
        new String(new Radix64Encoder.Default().encode(input), StandardCharsets.UTF_8)
    }


    @Test
    void testGetCipherShouldHandleFullSalt() throws Exception {
        // Arrange
        final int WORK_FACTOR = 10
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(WORK_FACTOR)

        final String PLAINTEXT = "This is a plaintext message."
        final String PASSWORD = "thisIsABadPassword"

        // These values can be generated by running `$ ./openssl_bcrypt.rb` in the terminal
        final byte[] IV = Hex.decodeHex("bae8a9d935748a75ff0e0bbd95a4f024" as char[])

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "\$2a\$10\$LBVzJoPgh.85YCvnos4BKOyYM.LRni6UbU4v/CEPBkmFIiigADJZi"
        logger.info("Full Hash: ${FULL_HASH}")
        final String FULL_SALT = FULL_HASH[0..<29]
        logger.info("     Salt: ${FULL_SALT}")
        final String HASH = FULL_HASH[-31..-1]
        logger.info("     Hash: ${HASH.padLeft(60, " ")}")

        String extractedSalt = FULL_HASH[7..<29]
        logger.info("Extracted Salt:   ${extractedSalt}")
        String extractedSaltHex = Hex.encodeHexString(customB64Decode(extractedSalt))
        logger.info("Extracted Salt (hex): ${extractedSaltHex}")

        final String CIPHER_TEXT = "d232b68e7aa38242d195c54b8f360d8b8d6b7580b190ffdeef99f5fe460bd6b0"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")
        logger.info("External cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, FULL_SALT.bytes, IV, DEFAULT_KEY_LENGTH, false)
        byte[] recoveredBytes = cipher.doFinal(cipherBytes)
        String recovered = new String(recoveredBytes, "UTF-8")
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered)
    }

    @Test
    void testGetCipherShouldHandleUnformedSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "thisIsABadPassword"

        final def INVALID_SALTS = ['$ab$00$acbdefghijklmnopqrstuv', 'bad_salt', '$3a$11$', 'x', '$2a$10$']

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")

        // Act
        INVALID_SALTS.each { String salt ->
            logger.info("Checking salt ${salt}")

            def msg = shouldFail(IllegalArgumentException) {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, salt.bytes, DEFAULT_KEY_LENGTH, true)
            }
            logger.expected(msg)

            // Assert
            assert msg =~ "The salt must be of the format \\\$2a\\\$10\\\$gUVbkVzp79H8YaCOsCVZNu\\. To generate a salt, use BcryptCipherProvider#generateSalt"
        }
    }

    String bytesToBitString(byte[] bytes) {
        bytes.collect {
            String.format("%8s", Integer.toBinaryString(it & 0xFF)).replace(' ', '0')
        }.join("")
    }

    String spaceString(String input, int blockSize = 4) {
        input.collect { it.padLeft(blockSize, " ") }.join("")
    }

    @Test
    void testGetCipherShouldRejectEmptySalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "thisIsABadPassword"

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}")

        // Two different errors -- one explaining the no-salt method is not supported, and the other for an empty byte[] passed

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, new byte[0], DEFAULT_KEY_LENGTH, true)
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "The salt must be of the format .* To generate a salt, use BcryptCipherProvider#generateSalt"
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()
        final byte[] IV = Hex.decodeHex("00" * 16 as char[])

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, false)
            }

            // Assert
            assert msg =~ "Cannot decrypt without a valid IV"
        }
    }

    @Test
    void testGetCipherShouldAcceptValidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()
        final byte[] IV = Hex.decodeHex("01" * 16 as char[])

        // Currently only AES ciphers are compatible with Bcrypt, so redundant to test all algorithms
        final def VALID_KEY_LENGTHS = AES_KEY_LENGTHS
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        VALID_KEY_LENGTHS.each { int keyLength ->
            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()} with key length ${keyLength}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, true)
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT.equals(recovered)
        }
    }

    @Test
    void testGetCipherShouldNotAcceptInvalidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()
        final byte[] IV = Hex.decodeHex("00" * 16 as char[])

        // Currently only AES ciphers are compatible with Bcrypt, so redundant to test all algorithms
        final def INVALID_KEY_LENGTHS = [-1, 40, 64, 112, 512]
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        INVALID_KEY_LENGTHS.each { int keyLength ->
            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()} with key length ${keyLength}")

            // Initialize a cipher for encryption
            def msg = shouldFail(IllegalArgumentException) {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, keyLength, true)
            }

            // Assert
            assert msg =~ "${keyLength} is not a valid key length for AES"
        }
    }

    @Test
    void testGenerateSaltShouldUseProvidedWorkFactor() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(11)
        int workFactor = cipherProvider.getWorkFactor()

        // Act
        final byte[] saltBytes = cipherProvider.generateSalt()
        String salt = new String(saltBytes)
        logger.info("Salt: ${salt}")

        // Assert
        assert salt =~ /^\$2[axy]\$\d{2}\$/
        assert salt.contains("\$${workFactor}\$")
    }

    /**
     * For {@code 1.12.0} the key derivation process was changed. Previously, the entire hash output
     * ({@code $2a$10$9XUQnxGEUsRdLqEhxY3xNujOQQkW3spKqxssi.Ox39VhhxB.z4496}) was fed to {@code SHA-512}
     * to stretch the hash output to a custom key length (128, 192, or 256 bits) because the Bcrypt hash
     * output length is fixed at 184 bits. The new key derivation process only feeds the <em>non-salt
     * hash output</em> (({@code jOQQkW3spKqxssi.Ox39VhhxB.z4496})) into the digest.
     * @throws Exception
     */
    @Test
    void testGetCipherShouldUseHashOutputOnlyToDeriveKey() throws Exception {
        // Arrange
        BcryptCipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()
        String saltString = new String(SALT, StandardCharsets.UTF_8)
        logger.info("Using fixed Bcrypt salt: ${saltString}")

        // Determine the expected key bytes using the new key derivation process
        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher(cipherProvider.getWorkFactor(), cipherProvider.getDefaultSaltLength())
        byte[] rawSaltBytes = BcryptCipherProvider.extractRawSalt(saltString)
        byte[] hashOutputBytes = bcryptSecureHasher.hashRaw(PASSWORD.getBytes(StandardCharsets.UTF_8), rawSaltBytes)
        logger.info("Raw hash output (${hashOutputBytes.length}): ${Hex.encodeHexString(hashOutputBytes)}")

        MessageDigest sha512 = MessageDigest.getInstance("SHA-512", "BC")
        byte[] keyDigestBytes = sha512.digest(hashOutputBytes[-31..-1] as byte[])
        logger.info("Key digest (${keyDigestBytes.length}): ${Hex.encodeHexString(keyDigestBytes)}")

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Expected key verification
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(em.getAlgorithm())
            byte[] derivedKeyBytes = Arrays.copyOf(keyDigestBytes, keyLength / 8 as int)
            logger.info("Derived key (${derivedKeyBytes.length}): ${Hex.encodeHexString(derivedKeyBytes)}")

            Cipher verificationCipher = Cipher.getInstance(em.getAlgorithm())
            verificationCipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(derivedKeyBytes, em.algorithm), new IvParameterSpec(iv))
            byte[] verificationBytes = verificationCipher.doFinal(cipherBytes)
            String verificationRecovered = new String(verificationBytes, StandardCharsets.UTF_8)
            logger.info("Verified: ${verificationRecovered}")

            // Assert
            assert PLAINTEXT == recovered
            assert PLAINTEXT == verificationRecovered
        }
    }

    @Test
    void testGetCipherShouldBeBackwardCompatibleWithFullHashKeyDerivation() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = cipherProvider.generateSalt()

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")

            // Initialize a cipher for encryption using the legacy key derivation process
            Cipher cipher = cipherProvider.getInitializedCipher(em, PASSWORD, SALT, new byte[0], DEFAULT_KEY_LENGTH, true, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getLegacyDecryptCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert PLAINTEXT == recovered
        }
    }

    @Test
    void testGetCipherShouldHandleNullSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4)

        final String PASSWORD = "shortPassword"
        final byte[] SALT = null
        final EncryptionMethod em = EncryptionMethod.AES_CBC

        // Act
        logger.info("Using algorithm: ${em.getAlgorithm()}")

        // Initialize a cipher for encryption
        def encryptMsg = shouldFail(IllegalArgumentException) {
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true)
            byte[] iv = cipher.getIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")
        }
        logger.expected("Encrypt error: ${encryptMsg}")

        byte[] cipherBytes = PLAINTEXT.reverse().getBytes(StandardCharsets.UTF_8)
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

        def decryptMsg = shouldFail(IllegalArgumentException) {
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, [0x00] * 16 as byte[], DEFAULT_KEY_LENGTH, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")
        }
        logger.expected("Decrypt error: ${decryptMsg}")

        // Assert
        assert encryptMsg =~ "The salt must be of the format"
        assert decryptMsg =~ "The salt must be of the format"
    }

    @Ignore("This test can be run on a specific machine to evaluate if the default work factor is sufficient")
    @Test
    void testDefaultConstructorShouldProvideStrongWorkFactor() {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider()

        // Values taken from http://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/ and http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt

        // Calculate the work factor to reach 500 ms
        int minimumWorkFactor = calculateMinimumWorkFactor()
        logger.info("Determined minimum safe work factor to be ${minimumWorkFactor}")

        // Act
        int workFactor = cipherProvider.getWorkFactor()
        logger.info("Default work factor ${workFactor}")

        // Assert
        assertTrue("The default work factor for BcryptCipherProvider is too weak. Please update the default value to a stronger level.", workFactor >= minimumWorkFactor)
    }

    /**
     * Returns the work factor required for a derivation to exceed 500 ms on this machine. Code adapted from http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
     *
     * @return the minimum bcrypt work factor
     */
    private static int calculateMinimumWorkFactor() {
        // High start-up cost, so run multiple times for better benchmarking
        final int RUNS = 10

        // Benchmark using a work factor of 5 (the second-lowest allowed)
        int workFactor = 5

        String salt = new BcryptCipherProvider(5).generateSalt()

        // Run once to prime the system
        double duration = time {
            BCrypt.hashpw(MICROBENCHMARK, salt)
        }
        logger.info("First run of work factor ${workFactor} took ${duration} ms (ignored)")

        def durations = []

        RUNS.times { int i ->
            duration = time {
                BCrypt.hashpw(MICROBENCHMARK, salt)
            }
            logger.info("Work factor ${workFactor} took ${duration} ms")
            durations << duration
        }

        duration = durations.sum() / durations.size()
        logger.info("Work factor ${workFactor} averaged ${duration} ms")

        // Increasing the work factor by 1 would double the run time
        // Keep increasing N until the estimated duration is over 500 ms
        while (duration < 500) {
            workFactor += 1
            duration *= 2
        }

        logger.info("Returning work factor ${workFactor} for ${duration} ms")

        return workFactor
    }

    private static double time(Closure c) {
        long start = System.nanoTime()
        c.call()
        long end = System.nanoTime()
        return (end - start) / 1_000_000.0
    }
}