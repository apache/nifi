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
package org.apache.nifi.security.util.scrypt;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.crypto.scrypt.Scrypt;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ScryptTest {
    private static final String PASSWORD = "shortPassword";
    private static final String SALT_HEX = "0123456789ABCDEFFEDCBA9876543210";
    private static byte[] SALT_BYTES;

    // Small values to test for correctness, not timing
    private static final int N = (int) Math.pow(2, 4);
    private static final int R = 1;
    private static final int P = 1;
    private static final int DK_LEN = 128;
    private static final long TWO_GIGABYTES = 2048L * 1024 * 1024;

    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        SALT_BYTES = Hex.decodeHex(SALT_HEX.toCharArray());
    }

    @Test
    void testDeriveScryptKeyShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final List<byte[]> allKeys = new ArrayList<>();
        final int RUNS = 10;

        // Act
        for (int i = 0; i < RUNS; i++) {
            byte[] keyBytes = Scrypt.deriveScryptKey(PASSWORD.getBytes(), SALT_BYTES, N, R, P, DK_LEN);
            allKeys.add(keyBytes);
        }

        // Assert
        assertEquals(RUNS, allKeys.size());
        allKeys.forEach(key -> assertArrayEquals(allKeys.get(0), key));
    }

    /**
     * This test ensures that the local implementation of Scrypt is compatible with the reference implementation from the Colin Percival paper.
     */
    @Test
    void testDeriveScryptKeyShouldMatchTestVectors() throws DecoderException, GeneralSecurityException {
        // Arrange

        // These values are taken from Colin Percival's scrypt paper: https://www.tarsnap.com/scrypt/scrypt.pdf
        final byte[] HASH_2 = Hex.decodeHex("fdbabe1c9d3472007856e7190d01e9fe" +
                "7c6ad7cbc8237830e77376634b373162" +
                "2eaf30d92e22a3886ff109279d9830da" +
                "c727afb94a83ee6d8360cbdfa2cc0640");

        final byte[] HASH_3 = Hex.decodeHex("7023bdcb3afd7348461c06cd81fd38eb" +
                "fda8fbba904f8e3ea9b543f6545da1f2" +
                "d5432955613f0fcf62d49705242a9af9" +
                "e61e85dc0d651e40dfcf017b45575887");

        final List<TestVector> TEST_VECTORS = new ArrayList<>();
        TEST_VECTORS.add(new TestVector(
                "password",
                "NaCl",
                1024,
                8,
                16,
                64 * 8,
                HASH_2
        ));
        TEST_VECTORS.add(new TestVector(
                "pleaseletmein",
                "SodiumChloride",
                16384,
                8,
                1,
                64 * 8,
                HASH_3
        ));

        // Act
        for (final TestVector params: TEST_VECTORS) {
            long memoryInBytes = Scrypt.calculateExpectedMemory(params.getN(), params.getR(), params.getP());

            byte[] calculatedHash = Scrypt.deriveScryptKey(params.getPassword().getBytes(), params.getSalt().getBytes(), params.getN(), params.getR(), params.getP(), params.getDkLen());

            // Assert
            assertArrayEquals(params.hash, calculatedHash);
        }
    }

    /**
     * This test ensures that the local implementation of Scrypt is compatible with the reference implementation from the Colin Percival paper. The test vector requires ~1GB {@code byte[]}
     * and therefore the Java heap must be at least 1GB. Because nifi/pom.xml has a {@code surefire} rule which appends {@code -Xmx1G}
     * to the Java options, this overrides any IDE options. To ensure the heap is properly set, using the {@code groovyUnitTest} profile will re-append {@code -Xmx3072m} to the Java options.
     */
    @Test
    void testDeriveScryptKeyShouldMatchExpensiveTestVector() throws Exception {
        // Arrange
        long totalMemory = Runtime.getRuntime().totalMemory();
        assumeTrue(totalMemory >= TWO_GIGABYTES, "Test is being skipped due to JVM heap size. Please run with -Xmx3072m to set sufficient heap size");

        // These values are taken from Colin Percival's scrypt paper: https://www.tarsnap.com/scrypt/scrypt.pdf
        final byte[] HASH = Hex.decodeHex("2101cb9b6a511aaeaddbbe09cf70f881" +
                "ec568d574a2ffd4dabe5ee9820adaa47" +
                "8e56fd8f4ba5d09ffa1c6d927c40f4c3" +
                "37304049e8a952fbcbf45c6fa77a41a4".toCharArray());

        // This test vector requires 2GB heap space and approximately 10 seconds on a consumer machine
        String password = "pleaseletmein";
        String salt = "SodiumChloride";
        int n = 1048576;
        int r = 8;
        int p = 1;
        int dkLen = 64 * 8;

        // Act
        long memoryInBytes = Scrypt.calculateExpectedMemory(n, r, p);

        byte[] calculatedHash = Scrypt.deriveScryptKey(password.getBytes(), salt.getBytes(), n, r, p, dkLen);

        // Assert
        assertArrayEquals(HASH, calculatedHash);
    }

    @EnabledIfSystemProperty(named = "nifi.test.unstable", matches = "true")
    @Test
    void testShouldCauseOutOfMemoryError() {
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 10; i <= 31; i++) {
            int length = (int) Math.pow(2, i);
            byte[] bytes = new byte[length];
            secureRandom.nextBytes(bytes);
        }
    }

    @Test
    void testDeriveScryptKeyShouldSupportExternalCompatibility() throws Exception {
        // Arrange

        // These values can be generated by running `$ ./openssl_scrypt.rb` in the terminal
        final String EXPECTED_KEY_HEX = "a8efbc0a709d3f89b6bb35b05fc8edf5";
        String password = "thisIsABadPassword";
        String saltHex = "f5b8056ea6e66edb8d013ac432aba24a";
        int n = 1024;
        int r = 8;
        int p = 36;
        int dkLen = 16 * 8;

        // Act
        long memoryInBytes = Scrypt.calculateExpectedMemory(n, r, p);

        byte[] calculatedHash = Scrypt.deriveScryptKey(password.getBytes(), Hex.decodeHex(saltHex.toCharArray()), n, r, p, dkLen);

        // Assert
        assertArrayEquals(Hex.decodeHex(EXPECTED_KEY_HEX.toCharArray()), calculatedHash);
    }

    @Test
    void testScryptShouldBeInternallyConsistent() throws Exception {
        // Arrange
        final List<String> allHashes = new ArrayList<>();
        final int RUNS = 10;

        // Act
        for (int i = 0; i < RUNS; i++) {
            String hash = Scrypt.scrypt(PASSWORD, SALT_BYTES, N, R, P, DK_LEN);
            allHashes.add(hash);
        }

        // Assert
        assertEquals(RUNS, allHashes.size());
        allHashes.forEach(hash -> assertEquals(allHashes.get(0), hash));
    }

    @Test
    void testScryptShouldGenerateValidSaltIfMissing() {
        // Arrange

        // The generated salt should be byte[16], encoded as 22 Base64 chars
        final String EXPECTED_SALT_PATTERN = "\\$.+\\$[0-9a-zA-Z\\/+]{22}\\$.+";

        // Act
        String calculatedHash = Scrypt.scrypt(PASSWORD, N, R, P, DK_LEN);

        // Assert
        System.out.println(calculatedHash);
        final Matcher matcher = Pattern.compile(EXPECTED_SALT_PATTERN).matcher(calculatedHash);
        assertTrue(matcher.matches());
    }

    @Test
    void testScryptShouldNotAcceptInvalidN() throws Exception {
        // Arrange

        final int MAX_N = Integer.MAX_VALUE / 128 / R ;

        // N must be a power of 2 > 1 and < Integer.MAX_VALUE / 128 / r
        final List<Integer> INVALID_NS = Arrays.asList(-2, 0, 1, 3, 4096 - 1, MAX_N);

        // Act
        for (final int invalidN : INVALID_NS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> Scrypt.deriveScryptKey(PASSWORD.getBytes(), SALT_BYTES, invalidN, R, P, DK_LEN));

            // Assert
            assertTrue(iae.getMessage().contains("N must be a power of 2 greater than 1")
                    || iae.getMessage().contains("Parameter N is too large"));
        }
    }

    @Test
    void testScryptShouldAcceptValidR() throws Exception {
        // Arrange

        // Use a large p value to allow r to exceed MAX_R without normal N exceeding MAX_N
        int largeP = 1024;
        final int maxR = 16384;

        // r must be in (0..Integer.MAX_VALUE / 128 / p)
        final List<Integer> INVALID_RS = Arrays.asList(0, maxR);
        // Act
        for (final int invalidR : INVALID_RS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> Scrypt.deriveScryptKey(PASSWORD.getBytes(), SALT_BYTES, N, invalidR, largeP, DK_LEN));

            // Assert
            assertTrue(iae.getMessage().contains("Parameter r must be 1 or greater")
                    || iae.getMessage().contains("Parameter r is too large"));
        }
    }

    @Test
    void testScryptShouldNotAcceptInvalidP() throws Exception {
        // Arrange
        final int MAX_P = (int) (Math.ceil(Integer.MAX_VALUE / 128.0));

        // p must be in (0..Integer.MAX_VALUE / 128)
        final List<Integer> INVALID_PS = Arrays.asList(0, MAX_P);

        // Act
        for (final int invalidP : INVALID_PS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> Scrypt.deriveScryptKey(PASSWORD.getBytes(), SALT_BYTES, N, R, invalidP, DK_LEN));

            // Assert
            assertTrue(iae.getMessage().contains("Parameter p must be 1 or greater")
                    || iae.getMessage().contains("Parameter p is too large"));
        }
    }

    @Test
    void testCheckShouldValidateCorrectPassword() throws Exception {
        // Arrange
        final String PASSWORD = "thisIsABadPassword";
        final String EXPECTED_HASH = Scrypt.scrypt(PASSWORD, N, R, P, DK_LEN);

        // Act
        boolean matches = Scrypt.check(PASSWORD, EXPECTED_HASH);

        // Assert
        assertTrue(matches);
    }

    @Test
    void testCheckShouldNotValidateIncorrectPassword() throws Exception {
        // Arrange
        final String PASSWORD = "thisIsABadPassword";
        final String INCORRECT_PASSWORD = "incorrectPassword";
        final String EXPECTED_HASH = Scrypt.scrypt(PASSWORD, N, R, P, DK_LEN);

        // Act
        boolean matches = Scrypt.check(INCORRECT_PASSWORD, EXPECTED_HASH);

        // Assert
        assertFalse(matches);
    }

    @Test
    void testCheckShouldNotAcceptInvalidPassword() throws Exception {
        // Arrange
        final String HASH = "$s0$a0801$abcdefghijklmnopqrstuv$abcdefghijklmnopqrstuv";

        // Even though the spec allows for empty passwords, the JCE does not, so extend enforcement of that to the user boundary
        final List<String> INVALID_PASSWORDS = Arrays.asList("", null);

        // Act
        for (final String invalidPassword : INVALID_PASSWORDS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> Scrypt.check(invalidPassword, HASH));

            // Assert
            assertTrue(iae.getMessage().contains("Password cannot be empty"));
        }
    }

    @Test
    void testCheckShouldNotAcceptInvalidHash() throws Exception {
        // Arrange
        final String PASSWORD = "thisIsABadPassword";

        // Even though the spec allows for empty salts, the JCE does not, so extend enforcement of that to the user boundary
        final List<String> INVALID_HASHES = Arrays.asList("", null, "$s0$a0801$", "$s0$a0801$abcdefghijklmnopqrstuv$");

        // Act
        for (final String invalidHash : INVALID_HASHES) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> Scrypt.check(PASSWORD, invalidHash));

            // Assert
            assertTrue(iae.getMessage().contains("Hash cannot be empty")
                    || iae.getMessage().contains("Hash is not properly formatted"));
        }
    }

    @Test
    void testVerifyHashFormatShouldDetectValidHash() throws Exception {
        // Arrange
        final List<String> VALID_HASHES = Arrays.asList(
                "$s0$40801$AAAAAAAAAAAAAAAAAAAAAA$gLSh7ChbHdOIMvZ74XGjV6qF65d9qvQ8n75FeGnM8YM",
                "$s0$40801$ABCDEFGHIJKLMNOPQRSTUQ$hxU5g0eH6sRkBqcsiApI8jxvKRT+2QMCenV0GToiMQ8",
                "$s0$40801$eO+UUcKYL2gnpD51QCc+gnywQ7Eg9tZeLMlf0XXr2zc$99aTTB39TJo69aZCONQmRdyWOgYsDi+1MI+8D0EgMNM",
                "$s0$40801$AAAAAAAAAAAAAAAAAAAAAA$Gk7K9YmlsWbd8FS7e4RKVWnkg9vlsqYnlD593pJ71gg",
                "$s0$40801$ABCDEFGHIJKLMNOPQRSTUQ$Ri78VZbrp2cCVmGh2a9Nbfdov8LPnFb49MYyzPCaXmE",
                "$s0$40801$eO+UUcKYL2gnpD51QCc+gnywQ7Eg9tZeLMlf0XXr2zc$rZIrP2qdIY7LN4CZAMgbCzl3YhXz6WhaNyXJXqFIjaI",
                "$s0$40801$AAAAAAAAAAAAAAAAAAAAAA$GxH68bGykmPDZ6gaPIGOONOT2omlZ7cd0xlcZ9UsY/0",
                "$s0$40801$ABCDEFGHIJKLMNOPQRSTUQ$KLGZjWlo59sbCbtmTg5b4k0Nu+biWZRRzhPhN7K5kkI",
                "$s0$40801$eO+UUcKYL2gnpD51QCc+gnywQ7Eg9tZeLMlf0XXr2zc$6Ql6Efd2ac44ERoV31CL3Q0J3LffNZKN4elyMHux99Y",
                // Uncommon but technically valid
                "$s0$F0801$AAAAAAAAAAA$A",
                "$s0$40801$ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP$A",
                "$s0$40801$ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP$ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP",
                "$s0$40801$ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP$" +
                        "ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP",
                "$s0$F0801$AAAAAAAAAAA$A",
                "$s0$F0801$AAAAAAAAAAA$A",
                "$s0$F0801$AAAAAAAAAAA$A",
                "$s0$F0801$AAAAAAAAAAA$A",
                "$s0$F0801$AAAAAAAAAAA$A"
        );

        // Act
        for (final String validHash : VALID_HASHES) {
            boolean isValidHash = Scrypt.verifyHashFormat(validHash);

            // Assert
            assertTrue(isValidHash);
        }
    }

    @Test
    void testVerifyHashFormatShouldDetectInvalidHash() throws Exception {
        // Arrange

        // Even though the spec allows for empty salts, the JCE does not, so extend enforcement of that to the user boundary
        final List<String> INVALID_HASHES = Arrays.asList("", null, "$s0$a0801$", "$s0$a0801$abcdefghijklmnopqrstuv$");

        // Act
        for (final String invalidHash : INVALID_HASHES) {
            boolean isValidHash = Scrypt.verifyHashFormat(invalidHash);

            // Assert
            assertFalse(isValidHash);
        }
    }

    private class TestVector {
        private String password;
        private String salt;
        private int n;
        private int r;
        private int p;
        private int dkLen;
        private byte[] hash;

        public TestVector(String password, String salt, int n, int r, int p, int dkLen, byte[] hash) {
            this.password = password;
            this.salt = salt;
            this.n = n;
            this.r = r;
            this.p = p;
            this.dkLen = dkLen;
            this.hash = hash;
        }

        public String getPassword() {
            return password;
        }

        public String getSalt() {
            return salt;
        }

        public int getN() {
            return n;
        }

        public int getR() {
            return r;
        }

        public int getP() {
            return p;
        }

        public int getDkLen() {
            return dkLen;
        }

        public byte[] getHash() {
            return hash;
        }
    }
}
