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
package org.apache.nifi.security.util.crypto;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.crypto.Cipher;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PBKDF2CipherProviderTest {
    private static final String PLAINTEXT = "ExactBlockSizeRequiredForProcess";
    private static final String SHORT_PASSWORD = "shortPassword";
    private static final String BAD_PASSWORD = "thisIsABadPassword";
    private static List<EncryptionMethod> strongKDFEncryptionMethods;

    public static final String MICROBENCHMARK = "microbenchmark";
    private static final int DEFAULT_KEY_LENGTH = 128;
    private static final int TEST_ITERATION_COUNT = 1000;
    private final String DEFAULT_PRF = "SHA-512";
    private final String SALT_HEX = "0123456789ABCDEFFEDCBA9876543210";
    private final String IV_HEX = StringUtils.repeat("01", 16);
    private static List<Integer> AES_KEY_LENGTHS;

    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        strongKDFEncryptionMethods = Arrays.stream(EncryptionMethod.values())
                .filter(EncryptionMethod::isCompatibleWithStrongKDFs)
                .collect(Collectors.toList());

        AES_KEY_LENGTHS = Arrays.asList(128, 192, 256);
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());

        final int MAX_LENGTH = 15;
        final List<byte[]> INVALID_IVS = new ArrayList<>();
        for (int length = 0; length <= MAX_LENGTH; length++) {
            INVALID_IVS.add(new byte[length]);
        }

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final byte[] badIV : INVALID_IVS) {
            // Encrypt should print a warning about the bad IV but overwrite it
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, badIV, DEFAULT_KEY_LENGTH, true);

            // Decrypt should fail
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, badIV, DEFAULT_KEY_LENGTH, false));

            // Assert
            assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());

        final int LONG_KEY_LENGTH = 256;

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, LONG_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, iv, LONG_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testShouldRejectEmptyPRF() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider;

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;
        String prf = "";

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT));

        // Assert
        assertTrue(iae.getMessage().contains("Cannot resolve empty PRF"));
    }

    @Test
    void testShouldResolveDefaultPRF() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider;

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        final PBKDF2CipherProvider SHA512_PROVIDER = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        String prf = "sha768";

        // Act
        cipherProvider = new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT);

        // Initialize a cipher for encryption
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

        byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

        cipher = SHA512_PROVIDER.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    @Test
    void testShouldResolveVariousPRFs() throws Exception {
        // Arrange
        final List<String> PRFS = Arrays.asList("SHA-1", "MD5", "SHA-256", "SHA-384", "SHA-512");
        RandomIVPBECipherProvider cipherProvider;

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final String prf : PRFS) {
            cipherProvider = new PBKDF2CipherProvider(prf, TEST_ITERATION_COUNT);

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider("SHA-256", TEST_ITERATION_COUNT);

        final String PLAINTEXT = "This is a plaintext message.";

        // These values can be generated by running `$ ./openssl_pbkdf2.rb` in the terminal
        final byte[] SALT = Hex.decodeHex("ae2481bee3d8b5d5b732bf464ea2ff01".toCharArray());
        final byte[] IV = Hex.decodeHex("26db997dcd18472efd74dabe5ff36853".toCharArray());

        final String CIPHER_TEXT = "92edbabae06add6275a1d64815755a9ba52afc96e2c1a316d3abbe1826e96f6c";
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT.toCharArray());

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    @Test
    void testGetCipherShouldHandleDifferentPRFs() throws Exception {
        // Arrange
        RandomIVPBECipherProvider sha256CP = new PBKDF2CipherProvider("SHA-256", TEST_ITERATION_COUNT);
        RandomIVPBECipherProvider sha512CP = new PBKDF2CipherProvider("SHA-512", TEST_ITERATION_COUNT);

        final String BAD_PASSWORD = "thisIsABadPassword";
        final byte[] SALT = new byte[16];
        Arrays.fill(SALT, (byte) 0x11);
        final byte[] IV = new byte[16];
        Arrays.fill(IV, (byte) 0x22);

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        Cipher sha256Cipher = sha256CP.getCipher(encryptionMethod, BAD_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
        byte[] sha256CipherBytes = sha256Cipher.doFinal(PLAINTEXT.getBytes());

        Cipher sha512Cipher = sha512CP.getCipher(encryptionMethod, BAD_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);
        byte[] sha512CipherBytes = sha512Cipher.doFinal(PLAINTEXT.getBytes());

        // Assert
        assertFalse(Arrays.equals(sha512CipherBytes, sha256CipherBytes));

        Cipher sha256DecryptCipher = sha256CP.getCipher(encryptionMethod, BAD_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] sha256RecoveredBytes = sha256DecryptCipher.doFinal(sha256CipherBytes);
        assertArrayEquals(PLAINTEXT.getBytes(), sha256RecoveredBytes);

        Cipher sha512DecryptCipher = sha512CP.getCipher(encryptionMethod, BAD_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
        byte[] sha512RecoveredBytes = sha512DecryptCipher.doFinal(sha512CipherBytes);
        assertArrayEquals(PLAINTEXT.getBytes(), sha512RecoveredBytes);
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, false));

            // Assert
            assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final List<String> INVALID_SALTS = Arrays.asList("pbkdf2", "$3a$11$", "x", "$2a$10$", "", null);

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final String salt : INVALID_SALTS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, salt != null ? salt.getBytes(): null, DEFAULT_KEY_LENGTH, true));

            // Assert
            assertTrue(iae.getMessage().contains("The salt must be at least 16 bytes. To generate a salt, use PBKDF2CipherProvider#generateSalt"));
        }
    }

    @Test
    void testGetCipherShouldAcceptValidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        // Currently only AES ciphers are compatible with PBKDF2, so redundant to test all algorithms
        final List<Integer> VALID_KEY_LENGTHS = AES_KEY_LENGTHS;
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final int keyLength : VALID_KEY_LENGTHS) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherShouldNotAcceptInvalidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        final byte[] SALT = Hex.decodeHex(SALT_HEX.toCharArray());
        final byte[] IV = Hex.decodeHex(IV_HEX.toCharArray());

        // Currently only AES ciphers are compatible with PBKDF2, so redundant to test all algorithms
        final List<Integer> VALID_KEY_LENGTHS = Arrays.asList(-1, 40, 64, 112, 512);
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final int keyLength : VALID_KEY_LENGTHS) {
            // Initialize a cipher for encryption
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, true));

            // Assert
            assertTrue(iae.getMessage().contains(keyLength + " is not a valid key length for AES"));
        }
    }

    @EnabledIfSystemProperty(named = "nifi.test.unstable", matches = "true")
    @Test
    void testDefaultConstructorShouldProvideStrongIterationCount() throws Exception {
        // Arrange
        PBKDF2CipherProvider cipherProvider = new PBKDF2CipherProvider();

        // Values taken from http://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/ and http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt

        // Calculate the iteration count to reach 500 ms
        int minimumIterationCount = calculateMinimumIterationCount();

        // Act
        int iterationCount = cipherProvider.getIterationCount();

        // Assert
        assertTrue(iterationCount >= minimumIterationCount, "The default iteration count for PBKDF2CipherProvider is too weak. Please update the default value to a stronger level.");
    }

    /**
     * Returns the iteration count required for a derivation to exceed 500 ms on this machine using the default PRF.
     * Code adapted from http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
     *
     * @return the minimum iteration count
     */
    private static int calculateMinimumIterationCount() throws Exception {
        // High start-up cost, so run multiple times for better benchmarking
        final int RUNS = 10;

        // Benchmark using an iteration count of 10k
        int iterationCount = 10_000;

        final byte[] SALT = new byte[16];
        Arrays.fill(SALT, (byte) 0x00);
        final byte[] IV = new byte[16];
        Arrays.fill(IV, (byte) 0x01);

        String defaultPrf = new PBKDF2CipherProvider().getPRFName();
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(defaultPrf, iterationCount);

        long start;
        long end;
        double duration;

        // Run once to prime the system
        start = System.nanoTime();
        cipherProvider.getCipher(EncryptionMethod.AES_CBC, MICROBENCHMARK, SALT, IV, DEFAULT_KEY_LENGTH, false);
        end = System.nanoTime();
        getTime(start, end);

        final List<Double> durations = new ArrayList<>();

        for (int i = 0; i < RUNS; i++) {
            start = System.nanoTime();
            cipherProvider.getCipher(EncryptionMethod.AES_CBC, String.format("%s%s", MICROBENCHMARK, i), SALT, IV, DEFAULT_KEY_LENGTH, false);
            end = System.nanoTime();
            duration = getTime(start, end);
            durations.add(duration);
        }

        duration = durations.stream().mapToDouble(Double::doubleValue).sum() / durations.size();

        // Keep increasing iteration count until the estimated duration is over 500 ms
        while (duration < 500) {
            iterationCount *= 2;
            duration *= 2;
        }

        return iterationCount;
    }

    private static double getTime(final long start, final long end) {
        return (end - start) / 1_000_000.0;
    }

    @Test
    void testGenerateSaltShouldProvideValidSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new PBKDF2CipherProvider(DEFAULT_PRF, TEST_ITERATION_COUNT);

        // Act
        byte[] salt = cipherProvider.generateSalt();

        // Assert
        assertEquals(16, salt.length);
        byte[] notExpected = new byte[16];
        Arrays.fill(notExpected, (byte) 0x00);
        assertFalse(Arrays.equals(notExpected, salt));
    }
}
