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
package org.apache.nifi.encrypt

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.security.kms.CryptoUtils
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider
import org.apache.nifi.security.util.crypto.CipherUtility
import org.apache.nifi.security.util.crypto.KeyedCipherProvider
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor
import org.jasypt.encryption.pbe.config.PBEConfig
import org.jasypt.salt.SaltGenerator
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.security.SecureRandom
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class StringEncryptorTest {
    private static final Logger logger = LoggerFactory.getLogger(StringEncryptorTest.class)

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210"

    private static final List<EncryptionMethod> keyedEncryptionMethods = EncryptionMethod.values().findAll {
        it.keyedCipher
    }
    private static final List<EncryptionMethod> pbeEncryptionMethods = EncryptionMethod.values().findAll {
        it.algorithm =~ "PBE"
    }

    // Unlimited elements are removed in static initializer
    private static final List<EncryptionMethod> limitedPbeEncryptionMethods = pbeEncryptionMethods

    private static final SecretKey key = new SecretKeySpec(Hex.decodeHex(KEY_HEX as char[]), "AES")

    private static final String KEY = "nifi.sensitive.props.key"
    private static final String ALGORITHM = "nifi.sensitive.props.algorithm"
    private static final String PROVIDER = "nifi.sensitive.props.provider"

    private static final String DEFAULT_ALGORITHM = "PBEWITHMD5AND128BITAES-CBC-OPENSSL"
    private static final String DEFAULT_PROVIDER = "BC"
    private static final String DEFAULT_PASSWORD = "nififtw!"
    private static final String OTHER_PASSWORD = "thisIsABadPassword"
    private static
    final Map RAW_PROPERTIES = [(ALGORITHM): DEFAULT_ALGORITHM, (PROVIDER): DEFAULT_PROVIDER, (KEY): DEFAULT_PASSWORD]
    private static final NiFiProperties STANDARD_PROPERTIES = new StandardNiFiProperties(new Properties(RAW_PROPERTIES))

    private static final byte[] DEFAULT_SALT = new byte[8]
    private static final byte[] DEFAULT_IV = new byte[16]
    private static final int DEFAULT_ITERATION_COUNT = 0

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        limitedPbeEncryptionMethods.removeAll { it.algorithm =~ "SHA.*(CBC)?"}

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

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private
    static Cipher generatePBECipher(boolean encryptMode, EncryptionMethod em = EncryptionMethod.MD5_128AES, String password = DEFAULT_PASSWORD, byte[] salt = DEFAULT_SALT, int iterationCount = DEFAULT_ITERATION_COUNT) {
        // Initialize secret key from password
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray())
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(em.algorithm, em.provider)
        SecretKey tempKey = factory.generateSecret(pbeKeySpec)

        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, iterationCount)
        Cipher cipher = Cipher.getInstance(em.algorithm, em.provider)
        cipher.init((encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, tempKey, parameterSpec)
        cipher
    }

    private
    static Cipher generateKeyedCipher(boolean encryptMode, EncryptionMethod em = EncryptionMethod.MD5_128AES, String keyHex = KEY_HEX, byte[] iv = DEFAULT_IV) {
        SecretKey tempKey = new SecretKeySpec(Hex.decodeHex(keyHex as char[]), CipherUtility.parseCipherFromAlgorithm(em.algorithm))

        IvParameterSpec ivSpec = new IvParameterSpec(iv)
        Cipher cipher = Cipher.getInstance(em.algorithm, em.provider)
        cipher.init((encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, tempKey, ivSpec)
        cipher
    }

    @Test
    void testPBEncryptionShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : limitedPbeEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")
            NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties(RAW_PROPERTIES + [(ALGORITHM): em.algorithm]))
            StringEncryptor encryptor = StringEncryptor.createEncryptor(niFiProperties)

            String cipherText = encryptor.encrypt(plaintext)
            logger.info("Cipher text: ${cipherText}")

            String recovered = encryptor.decrypt(cipherText)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    @Test
    void testPBEncryptionShouldBeExternallyConsistent() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        for (EncryptionMethod em : pbeEncryptionMethods) {

            // Hard-coded 0x00 * 16
            byte[] salt = new byte[16]
            int iterationCount = DEFAULT_ITERATION_COUNT
            // DES/RC* algorithms use 8 byte salts and custom iteration counts
            if (em.algorithm =~ "DES|RC") {
                salt = new byte[8]
                iterationCount = 1000
            } else if (em.algorithm =~ "SHAA|SHA256") {
                // SHA-1/-256 use 16 byte salts but custom iteration counts
                iterationCount = 1000
            }
            logger.info("Using algorithm: ${em.getAlgorithm()} with ${salt.length} byte salt and ${iterationCount} iterations")

            // Encrypt the value manually
            Cipher cipher = generatePBECipher(true, em, DEFAULT_PASSWORD, salt, iterationCount)

            byte[] cipherBytes = cipher.doFinal(plaintext.bytes)
            byte[] saltAndCipherBytes = CryptoUtils.concatByteArrays(salt, cipherBytes)
            String cipherTextHex = Hex.encodeHexString(saltAndCipherBytes)
            logger.info("Cipher text: ${cipherTextHex}")

            NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties(RAW_PROPERTIES + [(ALGORITHM): em.algorithm]))
            StringEncryptor encryptor = StringEncryptor.createEncryptor(niFiProperties)

            // Act
            String recovered = encryptor.decrypt(cipherTextHex)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    /**
     * This test uses the Jasypt library {@see StandardPBEStringEncryptor} to encrypt raw messages as the legacy (pre-1.4.0) NiFi application did. Then the messages are decrypted with the "new"/current primitive implementation to ensure backward compatibility. This test method only exercises limited strength key sizes (even this is not technically accurate as the SHA KDF is restricted even when using 128-bit AES).
     *
     * @throws Exception
     */
    @Test
    void testLimitedPBEncryptionShouldBeConsistentWithLegacyEncryption() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        for (EncryptionMethod em : limitedPbeEncryptionMethods) {

            // Hard-coded 0x00 * 16
            byte[] salt = new byte[16]
            // DES/RC* algorithms use 8 byte salts
            if (em.algorithm =~ "DES|RC") {
                salt = new byte[8]
            }
            logger.info("Using algorithm: ${em.getAlgorithm()} with ${salt.length} byte salt")

            StandardPBEStringEncryptor legacyEncryptor = new StandardPBEStringEncryptor()
            SaltGenerator mockSaltGenerator = [generateSalt: { int l ->
                logger.mock("Generating ${l} byte salt")
                new byte[l]
            }, includePlainSaltInEncryptionResults         : {
                -> true
            }] as SaltGenerator
            PBEConfig mockConfig = [getAlgorithm             : { -> em.algorithm },
                                    getPassword              : { -> DEFAULT_PASSWORD },
                                    getKeyObtentionIterations: { -> 1000 },
                                    getProviderName          : { -> em.provider },
                                    getProvider              : { -> new BouncyCastleProvider() },
                                    getSaltGenerator         : { -> mockSaltGenerator }
            ] as PBEConfig
            legacyEncryptor.setConfig(mockConfig)
            legacyEncryptor.setStringOutputType("hexadecimal")

            String cipherText = legacyEncryptor.encrypt(plaintext)
            logger.info("Cipher text: ${cipherText}")

            NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties(RAW_PROPERTIES + [(ALGORITHM): em.algorithm]))
            StringEncryptor encryptor = StringEncryptor.createEncryptor(niFiProperties)

            // Act
            String recovered = encryptor.decrypt(cipherText)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    /**
     * This test uses the Jasypt library {@see StandardPBEStringEncryptor} to encrypt raw messages as the legacy (pre-1.4.0) NiFi application did. Then the messages are decrypted with the "new"/current primitive implementation to ensure backward compatibility. This test method exercises all strength key sizes.
     *
     * @throws Exception
     */
    @Test
    void testPBEncryptionShouldBeConsistentWithLegacyEncryption() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.", isUnlimitedStrengthCryptoAvailable())

        final String plaintext = "This is a plaintext message."

        for (EncryptionMethod em : pbeEncryptionMethods) {

            // Hard-coded 0x00 * 16
            byte[] salt = new byte[16]
            // DES/RC* algorithms use 8 byte salts
            if (em.algorithm =~ "DES|RC") {
                salt = new byte[8]
            }
            logger.info("Using algorithm: ${em.getAlgorithm()} with ${salt.length} byte salt")

            StandardPBEStringEncryptor legacyEncryptor = new StandardPBEStringEncryptor()
            SaltGenerator mockSaltGenerator = [generateSalt: { int l ->
                logger.mock("Generating ${l} byte salt")
                new byte[l]
            }, includePlainSaltInEncryptionResults         : {
                -> true
            }] as SaltGenerator
            PBEConfig mockConfig = [getAlgorithm             : { -> em.algorithm },
                                    getPassword              : { -> DEFAULT_PASSWORD },
                                    getKeyObtentionIterations: { -> 1000 },
                                    getProviderName          : { -> em.provider },
                                    getProvider              : { -> new BouncyCastleProvider() },
                                    getSaltGenerator         : { -> mockSaltGenerator }
            ] as PBEConfig
            legacyEncryptor.setConfig(mockConfig)
            legacyEncryptor.setStringOutputType("hexadecimal")

            String cipherText = legacyEncryptor.encrypt(plaintext)
            logger.info("Cipher text: ${cipherText}")

            NiFiProperties niFiProperties = new StandardNiFiProperties(new Properties(RAW_PROPERTIES + [(ALGORITHM): em.algorithm]))
            StringEncryptor encryptor = StringEncryptor.createEncryptor(niFiProperties)

            // Act
            String recovered = encryptor.decrypt(cipherText)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    @Test
    void testKeyedEncryptionShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        // Act
        for (EncryptionMethod em : keyedEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}")
            StringEncryptor encryptor = new StringEncryptor(em.algorithm, em.provider, Hex.decodeHex(KEY_HEX as char[]))

            String cipherText = encryptor.encrypt(plaintext)
            logger.info("Cipher text: ${cipherText}")

            String recovered = encryptor.decrypt(cipherText)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    @Test
    void testKeyedEncryptionShouldBeExternallyConsistent() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        for (EncryptionMethod em : keyedEncryptionMethods) {
            // IV is actually used for keyed encryption
            byte[] iv = Hex.decodeHex(("AA" * 16) as char[])
            logger.info("Using algorithm: ${em.getAlgorithm()} with ${iv.length} byte IV")

            // Encrypt the value manually
            Cipher cipher = generateKeyedCipher(true, em, KEY_HEX, iv)

            byte[] cipherBytes = cipher.doFinal(plaintext.bytes)
            byte[] ivAndCipherBytes = CryptoUtils.concatByteArrays(iv, cipherBytes)
            String cipherTextHex = Hex.encodeHexString(ivAndCipherBytes)
            logger.info("Cipher text: ${cipherTextHex}")

            StringEncryptor encryptor = new StringEncryptor(em.algorithm, em.provider, Hex.decodeHex(KEY_HEX.chars))

            // Act
            String recovered = encryptor.decrypt(cipherTextHex)
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext == recovered
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()

        final String plaintext = "This is a plaintext message."

        // Act
        keyedEncryptionMethods.each { EncryptionMethod em ->
            logger.info("Using algorithm: ${em.getAlgorithm()}")
            byte[] iv = cipherProvider.generateIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, iv, true)

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

            cipher = cipherProvider.getCipher(em, key, iv, false)
            byte[] recoveredBytes = cipher.doFinal(cipherBytes)
            String recovered = new String(recoveredBytes, "UTF-8")
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered)
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.", isUnlimitedStrengthCryptoAvailable())

        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider()
        final List<Integer> LONG_KEY_LENGTHS = [192, 256]

        final String plaintext = "This is a plaintext message."

        SecureRandom secureRandom = new SecureRandom()

        // Act
        keyedEncryptionMethods.each { EncryptionMethod em ->
            // Re-use the same IV for the different length keys to ensure the encryption is different
            byte[] iv = cipherProvider.generateIV()
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            LONG_KEY_LENGTHS.each { int keyLength ->
                logger.info("Using algorithm: ${em.getAlgorithm()} with key length ${keyLength}")

                // Generate a key
                byte[] keyBytes = new byte[keyLength / 8]
                secureRandom.nextBytes(keyBytes)
                SecretKey localKey = new SecretKeySpec(keyBytes, "AES")
                logger.info("Key: ${Hex.encodeHexString(keyBytes)} ${keyBytes.length}")

                // Initialize a cipher for encryption
                Cipher cipher = cipherProvider.getCipher(em, localKey, iv, true)

                byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"))
                logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}")

                cipher = cipherProvider.getCipher(em, localKey, iv, false)
                byte[] recoveredBytes = cipher.doFinal(cipherBytes)
                String recovered = new String(recoveredBytes, "UTF-8")
                logger.info("Recovered: ${recovered}")

                // Assert
                assert plaintext.equals(recovered)
            }
        }
    }

    @Test
    void testStringEncryptorShouldNotBeFinal() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        StringEncryptor mockEncryptor = [encrypt: { String pt -> pt.reverse() },
                                         decrypt: { String ct -> ct.reverse() }] as StringEncryptor

        // Act
        String cipherText = mockEncryptor.encrypt(plaintext)
        logger.info("Encrypted ${plaintext} to ${cipherText}")
        String recovered = mockEncryptor.decrypt(cipherText)
        logger.info("Decrypted ${cipherText} to ${recovered}")

        // Assert
        assert recovered == plaintext
        assert cipherText != plaintext
    }

    @Test
    void testStringEncryptorShouldNotOperateIfNotInitialized() throws Exception {
        // Arrange
        final String plaintext = "This is a plaintext message."

        StringEncryptor uninitializedEncryptor = new StringEncryptor()

        // Act
        def encryptMsg = shouldFail(EncryptionException) {
            String cipherText = uninitializedEncryptor.encrypt(plaintext)
            logger.info("Encrypted ${plaintext} to ${cipherText}")
        }
        def decryptMsg = shouldFail(EncryptionException) {
            String recovered = uninitializedEncryptor.decrypt(plaintext)
            logger.info("Decrypted ${plaintext} to ${recovered}")
        }

        // Assert
        assert encryptMsg =~ "encryptor is not initialized"
        assert decryptMsg =~ "encryptor is not initialized"
    }

    @Test
    void testStringEncryptorShouldDetermineIfInitialized() throws Exception {
        // Arrange
        StringEncryptor uninitializedEncryptor = new StringEncryptor()
        EncryptionMethod em = EncryptionMethod.MD5_128AES
        StringEncryptor initializedEncryptor = new StringEncryptor(em.algorithm, em.provider, DEFAULT_PASSWORD)

        // Act
        boolean uninitializedIsInitialized = uninitializedEncryptor.isInitialized()
        logger.info("Uninitialized encryptor is initialized: ${uninitializedIsInitialized}")
        boolean initializedIsInitialized = initializedEncryptor.isInitialized()
        logger.info("Initialized encryptor is initialized: ${initializedIsInitialized}")

        // Assert
        assert !uninitializedIsInitialized
        assert initializedIsInitialized
    }
}
