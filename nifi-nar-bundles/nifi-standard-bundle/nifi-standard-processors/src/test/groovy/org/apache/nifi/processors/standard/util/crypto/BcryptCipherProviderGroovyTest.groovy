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
package org.apache.nifi.processors.standard.util.crypto

import org.apache.commons.codec.binary.Hex
import org.apache.nifi.security.util.EncryptionMethod
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.*
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mindrot.jbcrypt.BCrypt
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
public class BcryptCipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(BcryptCipherProviderGroovyTest.class);

    private static List<EncryptionMethod> strongKDFEncryptionMethods

    private static final int DEFAULT_KEY_LENGTH = 128;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        strongKDFEncryptionMethods = EncryptionMethod.values().findAll { it.isCompatibleWithStrongKDFs() }

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
    public void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = cipherProvider.generateSalt().bytes

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = cipherProvider.generateSalt().bytes
        final byte[] IV = Hex.decodeHex("00" * 16 as char[]);

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                PasswordBasedEncryptor.supportsUnlimitedStrength());

        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = cipherProvider.generateSalt().bytes

        final int LONG_KEY_LENGTH = 256

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, LONG_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, LONG_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testHashPWShouldMatchTestVectors() {
        // Arrange
        final String PASSWORD = 'abcdefghijklmnopqrstuvwxyz'
        final String SALT = '$2a$10$fVH8e28OQRj9tqiDXs1e1u'
        final String EXPECTED_HASH = '$2a$10$fVH8e28OQRj9tqiDXs1e1uxpsjN0c7II7YPKXua2NAKYvM6iQk7dq'
//        final int WORK_FACTOR = 10

        // Act
        String calculatedHash = BCrypt.hashpw(PASSWORD, SALT)
        logger.info("Generated ${calculatedHash}")

        // Assert
        assert calculatedHash == EXPECTED_HASH
    }

    @Ignore("BouncyCastle implementation matches OpenBSD instead of paper-based BCrypt")
    @Test
    public void testBCGenerateShouldMatchTestVectors() {
        // Arrange
        final String PASSWORD = 'abcdefghijklmnopqrstuvwxyz'
        final String SALT = 'fVH8e28OQRj9tqiDXs1e1u'
        final String EXPECTED_HASH = '$2a$10$fVH8e28OQRj9tqiDXs1e1uxpsjN0c7II7YPKXua2NAKYvM6iQk7dq'
        final int WORK_FACTOR = 10

        // Act
        byte[] calculatedHashBytes = org.bouncycastle.crypto.generators.BCrypt.generate(PASSWORD.bytes, Base64.decoder.decode(SALT), WORK_FACTOR)
        String calculatedHash = Base64.encoder.withoutPadding().encodeToString(calculatedHashBytes)
        logger.info("Generated ${calculatedHash}")

        // Assert
        assert "\$2a\$${WORK_FACTOR}\$${SALT}${calculatedHash}" as String == EXPECTED_HASH
    }

    @Test
    public void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PLAINTEXT = "This is a plaintext message.";
        final String PASSWORD = "thisIsABadPassword";

        // These values can be generated by running `$ ./openssl_bcrypt` in the terminal
        final byte[] SALT = Hex.decodeHex("81455b915ce9efd1fc61a08eb0255936" as char[]);
        final byte[] IV = Hex.decodeHex("41a51e0150df6a1f72826b36c6371f3f" as char[]);

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "\$2a\$10\$gUVbkVzp79H8YaCOsCVZNuz/d759nrMKzjuviaS5/WdcKHzqngGKi"
        logger.info("Full Hash: ${FULL_HASH}")
        final String HASH = FULL_HASH[-31..-1]
        logger.info("     Hash: ${HASH.padLeft(60, " ")}")
        logger.info(" B64 Salt: ${Base64.encoder.withoutPadding().encodeToString(SALT).padLeft(29, " ")}")

        String extractedSalt = FULL_HASH[7..<29]
        logger.info("Extracted Salt:   ${extractedSalt}")
        String extractedSaltHex = Hex.encodeHexString(Base64.decoder.decode(extractedSalt))
        logger.info("Extracted Salt (hex): ${extractedSaltHex}")
        logger.info(" Expected Salt (hex): ${Hex.encodeHexString(SALT)}")

        final String CIPHER_TEXT = "3a226ba2b3c8fe559acb806620001246db289375ba8075a68573478b56a69f15"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");
        logger.info("External cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

        // Sanity check
        Cipher rubyCipher = Cipher.getInstance(encryptionMethod.algorithm, "BC")
        def rubyKey = new SecretKeySpec(Hex.decodeHex("724cd9e1b0b9e87c7f7e7d7b270bca07" as char[]), "AES")
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
        String generatedHash = BCrypt.hashpw(PASSWORD, FULL_SALT)
        logger.sanity("Generated hash: ${generatedHash}")
        assert generatedHash == FULL_HASH

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, FULL_SALT.bytes, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered);
    }

    // TODO: May be redundant
    @Test
    public void testGetCipherShouldHandleFullSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PLAINTEXT = "This is a plaintext message.";
        final String PASSWORD = "thisIsABadPassword";

        // These values can be generated by running `$ ./openssl_bcrypt.rb` in the terminal
        final byte[] IV = Hex.decodeHex("41a51e0150df6a1f72826b36c6371f3f" as char[]);

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "\$2a\$10\$gUVbkVzp79H8YaCOsCVZNuz/d759nrMKzjuviaS5/WdcKHzqngGKi"
        logger.info("Full Hash: ${FULL_HASH}")
        final String FULL_SALT = FULL_HASH[0..<29]
        logger.info("     Salt: ${FULL_SALT}")
        final String HASH = FULL_HASH[-31..-1]
        logger.info("     Hash: ${HASH.padLeft(60, " ")}")

        String extractedSalt = FULL_HASH[7..<29]
        logger.info("Extracted Salt:   ${extractedSalt}")
        String extractedSaltHex = Hex.encodeHexString(Base64.decoder.decode(extractedSalt))
        logger.info("Extracted Salt (hex): ${extractedSaltHex}")

        final String CIPHER_TEXT = "3a226ba2b3c8fe559acb806620001246db289375ba8075a68573478b56a69f15"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");
        logger.info("External cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, FULL_SALT.bytes, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered);
    }

    String bytesToBitString(byte[] bytes) {
        bytes.collect {
            String.format("%8s", Integer.toBinaryString(it & 0xFF)).replace(' ', '0')
        }.join("")
    }

    String spaceString(String input, int blockSize = 4) {
        input.collect { it.padLeft(blockSize, " ")}.join("")
    }

    @Test
    public void testGetCipherShouldHandleEmptySalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PASSWORD = "thisIsABadPassword";

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, DEFAULT_KEY_LENGTH, true);
        }

        // Assert
        assert msg =~ "The salt cannot be empty\\. To generate a salt, use BcryptCipherProvider#generateSalt"
    }

    @Test
    public void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(10);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = cipherProvider.generateSalt().bytes
        final byte[] IV = Hex.decodeHex("00" * 16 as char[]);

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            def msg = shouldFail(IllegalArgumentException) {
                cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, false);
            }

            // Assert
            assert msg =~ "Cannot decrypt without an IV"
        }
    }

    // TODO: Test default constructor
}