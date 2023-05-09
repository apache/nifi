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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.scrypt.Scrypt;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScryptCipherProviderTest {
    private static final String PLAINTEXT = "ExactBlockSizeRequiredForProcess";
    private static final String SHORT_PASSWORD = "shortPassword";
    private static final String BAD_PASSWORD = "thisIsABadPassword";

    private static List<EncryptionMethod> strongKDFEncryptionMethods;

    private static final int DEFAULT_KEY_LENGTH = 128;
    public static final String MICROBENCHMARK = "microbenchmark";
    private static List<Integer> AES_KEY_LENGTHS;

    RandomIVPBECipherProvider cipherProvider;

    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        strongKDFEncryptionMethods = Arrays.stream(EncryptionMethod.values())
                .filter(EncryptionMethod::isCompatibleWithStrongKDFs)
                .collect(Collectors.toList());

        AES_KEY_LENGTHS = Arrays.asList(128, 192, 256);
    }

    @BeforeEach
    void setUp() throws Exception {
        // Very fast parameters to test for correctness rather than production values
        cipherProvider = new ScryptCipherProvider(4, 1, 1);
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final byte[] SALT = cipherProvider.generateSalt();

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
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("01", 16).toCharArray());

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
        // Arrange
        final byte[] SALT = cipherProvider.generateSalt();

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
    void testScryptShouldSupportExternalCompatibility() throws Exception {
        // Arrange

        // Default values are N=2^14, r=8, p=1, but the provided salt will contain the parameters used
        cipherProvider = new ScryptCipherProvider();

        final String PLAINTEXT = "This is a plaintext message.";
        final int DK_LEN = 128;

        // These values can be generated by running `$ ./openssl_scrypt.rb` in the terminal
        final byte[] SALT = Hex.decodeHex("f5b8056ea6e66edb8d013ac432aba24a".toCharArray());
        final byte[] IV = Hex.decodeHex("76a00f00878b8c3db314ae67804c00a1".toCharArray());

        final String CIPHER_TEXT = "604188bf8e9137bc1b24a0ab01973024bc5935e9ae5fedf617bdca028c63c261";
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT.toCharArray());

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Sanity check
        String rubyKeyHex = "a8efbc0a709d3f89b6bb35b05fc8edf5";
        Cipher rubyCipher = Cipher.getInstance(encryptionMethod.getAlgorithm(), "BC");
        final SecretKeySpec rubyKey = new SecretKeySpec(Hex.decodeHex(rubyKeyHex.toCharArray()), "AES");
        final IvParameterSpec ivSpec = new IvParameterSpec(IV);
        rubyCipher.init(Cipher.ENCRYPT_MODE, rubyKey, ivSpec);
        byte[] rubyCipherBytes = rubyCipher.doFinal(PLAINTEXT.getBytes());
        rubyCipher.init(Cipher.DECRYPT_MODE, rubyKey, ivSpec);
        assertArrayEquals(PLAINTEXT.getBytes(), rubyCipher.doFinal(rubyCipherBytes));
        assertArrayEquals(PLAINTEXT.getBytes(), rubyCipher.doFinal(cipherBytes));

        // n$r$p$hex_salt_SL$hex_hash_HL
        final String FULL_HASH = "400$8$24$f5b8056ea6e66edb8d013ac432aba24a$a8efbc0a709d3f89b6bb35b05fc8edf5";

        final String[] fullHashArr = FULL_HASH.split("\\$");
        final String nStr = fullHashArr[0];
        final String rStr = fullHashArr[1];
        final String pStr = fullHashArr[2];
        final String saltHex = fullHashArr[3];
        final String hashHex = fullHashArr[4];
        final int n = Integer.valueOf(nStr, 16);
        final int r = Integer.valueOf(rStr, 16);
        final int p = Integer.valueOf(pStr, 16);

        // Form Java-style salt with cost params from Ruby-style
        String javaSalt = Scrypt.formatSalt(Hex.decodeHex(saltHex.toCharArray()), n, r, p);

        // Convert hash from hex to Base64
        String base64Hash = CipherUtility.encodeBase64NoPadding(Hex.decodeHex(hashHex.toCharArray()));
        assertEquals(hashHex, Hex.encodeHexString(Base64.decodeBase64(base64Hash)));

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, javaSalt.getBytes(), IV, DK_LEN, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    @Test
    void testGetCipherShouldHandleSaltWithoutParameters() throws Exception {
        // Arrange

        // To help Groovy resolve implementation private methods not known at interface level
        final ScryptCipherProvider cipherProvider = (ScryptCipherProvider) this.cipherProvider;

        final byte[] SALT = new byte[cipherProvider.getDefaultSaltLength()];
        new SecureRandom().nextBytes(SALT);

        final String EXPECTED_FORMATTED_SALT = cipherProvider.formatSaltForScrypt(SALT);

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act

        // Initialize a cipher for encryption
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
        byte[] iv = cipher.getIV();

        byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

        // Manually initialize a cipher for decrypt with the expected salt
        byte[] parsedSalt = new byte[cipherProvider.getDefaultSaltLength()];
        final List<Integer> params = new ArrayList<>();
        cipherProvider.parseSalt(EXPECTED_FORMATTED_SALT, parsedSalt, params);
        final int n = params.get(0);
        final int r = params.get(1);
        final int p = params.get(2);
        byte[] keyBytes = Scrypt.deriveScryptKey(SHORT_PASSWORD.getBytes(), parsedSalt, n, r, p, DEFAULT_KEY_LENGTH);
        SecretKey key = new SecretKeySpec(keyBytes, "AES");
        Cipher manualCipher = Cipher.getInstance(encryptionMethod.getAlgorithm(), encryptionMethod.getProvider());
        manualCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
        byte[] recoveredBytes = manualCipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    @Test
    void testGetCipherShouldNotAcceptInvalidSalts() throws Exception {
        // Arrange
        final List<String> INVALID_SALTS = Arrays.asList("bad_sal", "$3a$11$", "x", "$2a$10$");
        final String LENGTH_MESSAGE = "The raw salt must be greater than or equal to 8 bytes";

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final String salt : INVALID_SALTS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, salt.getBytes(), DEFAULT_KEY_LENGTH, true));

            // Assert
            assertTrue(iae.getMessage().contains(LENGTH_MESSAGE));
        }
    }

    @Test
    void testGetCipherShouldHandleUnformattedSalts() throws Exception {
        // Arrange
        final List<String> RECOVERABLE_SALTS = Arrays.asList("$ab$00$acbdefghijklmnopqrstuv", "$4$1$1$0123456789abcdef", "$400$1$1$abcdefghijklmnopqrstuv");

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final String salt : RECOVERABLE_SALTS) {
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, salt.getBytes(), DEFAULT_KEY_LENGTH, true);

            // Assert
            assertNotNull(cipher);
        }
    }

    @Test
    void testGetCipherShouldRejectEmptySalt() throws Exception {
        // Arrange
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, new byte[0], DEFAULT_KEY_LENGTH, true));

        // Assert
        assertTrue(iae.getMessage().contains("The salt cannot be empty. To generate a salt, use ScryptCipherProvider#generateSalt"));
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("00", 16).toCharArray());

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
    void testGetCipherShouldAcceptValidKeyLengths() throws Exception {
        // Arrange
        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("01", 16).toCharArray());

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
        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("00", 16).toCharArray());

        // Even though Scrypt can derive keys of arbitrary length, it will fail to validate if the underlying cipher does not support it
        final List<Integer> INVALID_KEY_LENGTHS = Arrays.asList(-1, 40, 64, 112, 512);
        // Currently only AES ciphers are compatible with Scrypt, so redundant to test all algorithms
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final int keyLength : INVALID_KEY_LENGTHS) {
            // Initialize a cipher for encryption
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, true));

            // Assert
            assertTrue(iae.getMessage().contains(keyLength + " is not a valid key length for AES"));
        }
    }

    @Test
    void testScryptShouldNotAcceptInvalidPassword() {
        // Arrange
        String emptyPassword = "";
        final byte[] salt = new byte[16];
        Arrays.fill(salt, (byte) 0x01);

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, emptyPassword, salt, DEFAULT_KEY_LENGTH, true));

        // Assert
        assertTrue(iae.getMessage().contains("Encryption with an empty password is not supported"));
    }

    @Test
    void testGenerateSaltShouldUseProvidedParameters() throws Exception {
        // Arrange
        ScryptCipherProvider cipherProvider = new ScryptCipherProvider(8, 2, 2);
        int n = cipherProvider.getN();
        int r = cipherProvider.getR();
        int p = cipherProvider.getP();

        // Act
        final String salt = new String(cipherProvider.generateSalt());

        // Assert
        final Matcher matcher = Pattern.compile("^(?i)\\$s0\\$[a-f0-9]{5,16}\\$").matcher(salt);
        assertTrue(matcher.find());
        String params = Scrypt.encodeParams(n, r, p);
        assertTrue(salt.contains(String.format("$%s$", params)));
    }

    @Test
    void testShouldParseSalt() throws Exception {
        // Arrange
        ScryptCipherProvider cipherProvider = (ScryptCipherProvider) this.cipherProvider;

        final byte[] EXPECTED_RAW_SALT = Hex.decodeHex("f5b8056ea6e66edb8d013ac432aba24a".toCharArray());
        final int EXPECTED_N = 1024;
        final int EXPECTED_R = 8;
        final int EXPECTED_P = 36;

        final String FORMATTED_SALT = "$s0$a0824$9bgFbqbmbtuNATrEMquiSg";

        byte[] rawSalt = new byte[16];
        final List<Integer> params = new ArrayList<>();

        // Act
        cipherProvider.parseSalt(FORMATTED_SALT, rawSalt, params);

        // Assert
        assertArrayEquals(EXPECTED_RAW_SALT, rawSalt);
        assertEquals(EXPECTED_N, params.get(0));
        assertEquals(EXPECTED_R, params.get(1));
        assertEquals(EXPECTED_P, params.get(2));
    }

    @Test
    void testShouldVerifyPBoundary() throws Exception {
        // Arrange
        final int r = 8;
        final int p = 1;

        // Act
        boolean valid = ScryptCipherProvider.isPValid(r, p);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailPBoundary() throws Exception {
        // Arrange
        // The p upper bound is calculated with the formula below, when r = 8:
        // pBoundary = ((Math.pow(2,32))-1) * (32.0/(r * 128)), where pBoundary = 134217727.96875;
        final Map<Integer, Integer> costParameters = new HashMap<>();
        costParameters.put(8, 134217729);
        costParameters.put(128, 8388608);
        costParameters.put(4096, 0);

        // Act and Assert
        costParameters.entrySet().forEach(entry ->
                assertFalse(ScryptCipherProvider.isPValid(entry.getKey(), entry.getValue()))
        );
    }

    @Test
    void testShouldVerifyRValue() throws Exception {
        // Arrange
        final int r = 8;

        // Act
        boolean valid = ScryptCipherProvider.isRValid(r);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailRValue() throws Exception {
        // Arrange
        final int r = 0;

        // Act
        boolean valid = ScryptCipherProvider.isRValid(r);

        // Assert
        assertFalse(valid);
    }

    @Test
    void testShouldValidateScryptCipherProviderPBoundary() throws Exception {
        // Arrange
        final int n = 64;
        final int r = 8;
        final int p = 1;

        // Act
        ScryptCipherProvider testCipherProvider = new ScryptCipherProvider(n, r, p);

        // Assert
        assertNotNull(testCipherProvider);
    }

    @Test
    void testShouldCatchInvalidP() throws Exception {
        // Arrange
        final int n = 64;
        final int r = 8;
        final int p = 0;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> new ScryptCipherProvider(n, r, p));

        // Assert
        assertTrue(iae.getMessage().contains("Invalid p value exceeds p boundary"));
    }

    @Test
    void testShouldCatchInvalidR() throws Exception {
        // Arrange
        final int n = 64;
        final int r = 0;
        final int p = 0;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> new ScryptCipherProvider(n, r, p));

        // Assert
        assertTrue(iae.getMessage().contains("Invalid r value; must be greater than 0"));
    }

    @Test
    void testShouldAcceptFormattedSaltWithPlus() throws Exception {
        // Arrange
        final String FULL_SALT_WITH_PLUS = "$s0$e0801$smJD8vwWI3+uQCHYz2yg0+";

        // Act
        boolean isScryptSalt = ScryptCipherProvider.isScryptFormattedSalt(FULL_SALT_WITH_PLUS);

        // Assert
        assertTrue(isScryptSalt);
    }

    @EnabledIfSystemProperty(named = "nifi.test.unstable", matches = "true",
            disabledReason = "This test can be run on a specific machine to evaluate if the default parameters are sufficient")
    @Test
    void testDefaultConstructorShouldProvideStrongParameters() {
        // Arrange
        ScryptCipherProvider testCipherProvider = new ScryptCipherProvider();

        /** See this Stack Overflow answer for a good visualization of the interplay between N, r, p <a href="http://stackoverflow.com/a/30308723" rel="noopener">http://stackoverflow.com/a/30308723</a> */

        // Act
        int n = testCipherProvider.getN();
        int r = testCipherProvider.getR();
        int p = testCipherProvider.getP();

        // Calculate the parameters to reach 500 ms
        final List<Integer> minParameters = calculateMinimumParameters(r, p, 1024 * 1024 * 1024);
        final int minimumN = minParameters.get(0);

        // Assert
        assertTrue(n >= minimumN, "The default parameters for ScryptCipherProvider are too weak. Please update the default values to a stronger level.");
    }

    /**
     * Returns the parameters required for a derivation to exceed 500 ms on this machine. Code adapted from http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
     *
     * @param r the block size in bytes
     * @param p the parallelization factor
     * @param maxHeapSize the maximum heap size to use in bytes (defaults to 1 GB)
     *
     * @return the minimum scrypt parameters as [N, r, p]
     */
    private static List<Integer> calculateMinimumParameters(final int r, final int p, final int maxHeapSize) {
        // High start-up cost, so run multiple times for better benchmarking
        final int RUNS = 10;

        // Benchmark using N=2^4
        int n = (int) Math.pow(2, 4);
        int dkLen = 128;

        assertTrue(Scrypt.calculateExpectedMemory(n, r, p) <= maxHeapSize);

        byte[] salt = new byte[Scrypt.getDefaultSaltLength()];
        new SecureRandom().nextBytes(salt);

        long start;
        long end;
        double duration;

        // Run once to prime the system
        Scrypt.scrypt(MICROBENCHMARK, salt, n, r, p, dkLen);

        final List<Double> durations = new ArrayList<>();

        for (int i = 0; i < RUNS; i++) {
            start = System.nanoTime();
            Scrypt.scrypt(MICROBENCHMARK, salt, n, r, p, dkLen);
            end = System.nanoTime();
            duration = getTime(start, end);

            durations.add(duration);
        }

        duration = durations.stream().mapToDouble(Double::doubleValue).sum() / durations.size();

        // Doubling N would double the run time
        // Keep increasing N until the estimated duration is over 500 ms
        while (duration < 500) {
            n *= 2;
            duration *= 2;
        }

        return Arrays.asList(n, r, p);
    }

    private static double getTime(final long start, final long end) {
        return (end - start) / 1_000_000.0;
    }
}
