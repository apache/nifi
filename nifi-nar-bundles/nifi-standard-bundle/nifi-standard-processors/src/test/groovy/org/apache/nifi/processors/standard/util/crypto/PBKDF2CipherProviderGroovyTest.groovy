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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
public class PBKDF2CipherProviderGroovyTest {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProviderGroovyTest.class);

    private static List<EncryptionMethod> strongKDFEncryptionMethods

    private static final int DEFAULT_KEY_LENGTH = 128;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        strongKDFEncryptionMethods = EncryptionMethod.values().findAll { it.isCompatibleWithStrongKDFs() }
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
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

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
    public void testGetCipherWithoutSaltShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PASSWORD = "shortPassword";

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            // No method for just IV because signature would conflict with salt method
            cipher = cipherProvider.getCipher(em, PASSWORD, new byte[0], iv, DEFAULT_KEY_LENGTH, false);
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
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());
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

        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

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
    public void testShouldResolveVariousPRFs() throws Exception {
        // Arrange
        final List<String> PRFS = ["SHA-256", "SHA-1", "MD5", "SHA-384", "SHA-512"]
        RandomIVPBECipherProvider cipherProvider

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());
        final byte[] IV = Hex.decodeHex("00" * 16 as char[]);

        final String plaintext = "This is a plaintext message.";
        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Act
        PRFS.each { String prf ->
            logger.info("Using ${prf}")
            cipherProvider = new PBKDF2CipherProvider(prf, 1000);
            logger.info("Resolved PRF to ${cipherProvider.getPRFName()}")

            logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
            logger.info("IV: ${Hex.encodeHexString(IV)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testShouldResolveDefaultPRF() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());
        final byte[] IV = Hex.decodeHex("00" * 16 as char[]);

        final String plaintext = "This is a plaintext message.";
        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        final PBKDF2CipherProvider SHA256_PROVIDER = new PBKDF2CipherProvider("SHA-256", 1000)

        String prf = "sha768"
        logger.info("Using ${prf}")

        // Act
        cipherProvider = new PBKDF2CipherProvider(prf, 1000);
        logger.info("Resolved PRF to ${cipherProvider.getPRFName()}")
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");

        // Initialize a cipher for encryption
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
        logger.info("IV: ${Hex.encodeHexString(IV)}")

        byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

        cipher = SHA256_PROVIDER.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");
        logger.info("Recovered: ${recovered}")

        // Assert
        assert plaintext.equals(recovered);
    }

    @Test
    public void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PLAINTEXT = "This is a plaintext message.";
        final String PASSWORD = "thisIsABadPassword";

        // These values can be generated by running `$ ./openssl_pbkdf2.rb` in the terminal
        final byte[] SALT = Hex.decodeHex("ae2481bee3d8b5d5b732bf464ea2ff01" as char[]);
        final byte[] IV = Hex.decodeHex("26db997dcd18472efd74dabe5ff36853" as char[]);

        final String CIPHER_TEXT = "92edbabae06add6275a1d64815755a9ba52afc96e2c1a316d3abbe1826e96f6c"
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT as char[])

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC
        logger.info("Using algorithm: ${encryptionMethod.getAlgorithm()}");
        logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");
        logger.info("Recovered: ${recovered}")

        // Assert
        assert PLAINTEXT.equals(recovered);
    }

    @Test
    public void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", 1000);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());
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