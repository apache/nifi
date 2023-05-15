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

import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Argon2SecureHasherTest {
    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int hashLength = 32;
        int memory = 8;
        int parallelism = 4;
        int iterations = 4;

        int testIterations = 10;
        byte[] inputBytes = "This is a sensitive value".getBytes();

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919";

        Argon2SecureHasher a2sh = new Argon2SecureHasher(hashLength, memory, parallelism, iterations);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = a2sh.hashRaw(inputBytes);
            String hashHex = new String(Hex.encode(hash));
            results.add(hashHex);
        }

        // Assert
        results.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        int hashLength = 32;
        int memory = 8;
        int parallelism = 4;
        int iterations = 4;

        int testIterations = 10;
        byte[] inputBytes = "This is a sensitive value".getBytes();

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919";

        Argon2SecureHasher a2sh = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = a2sh.hashRaw(inputBytes);
            String hashHex = new String(Hex.encode(hash));
            results.add(hashHex);
        }

        // Assert
        assertTrue(results.stream().distinct().collect(Collectors.toList()).size() == results.size());
        results.forEach(result -> assertNotEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldHandleArbitrarySalt() {
        // Arrange
        int hashLength = 32;
        int memory = 8;
        int parallelism = 4;
        int iterations = 4;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919";
        final String EXPECTED_HASH_BASE64 = "pzpHH1GykAkBoAuB53C5wd/FlWArt67GTNJ3VKQXSRk";
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX);

        // Static salt instance
        Argon2SecureHasher staticSaltHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations);
        Argon2SecureHasher arbitrarySaltHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16);

        final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);
        final String DIFFERENT_STATIC_SALT = "Diff Static Salt";

        // Act
        byte[] staticSaltHash = staticSaltHasher.hashRaw(inputBytes);
        byte[] arbitrarySaltHash = arbitrarySaltHasher.hashRaw(inputBytes, STATIC_SALT);
        byte[] differentArbitrarySaltHash = arbitrarySaltHasher.hashRaw(inputBytes, DIFFERENT_STATIC_SALT.getBytes(StandardCharsets.UTF_8));
        byte[] differentSaltHash = arbitrarySaltHasher.hashRaw(inputBytes);

        String staticSaltHashHex = staticSaltHasher.hashHex(input);
        String arbitrarySaltHashHex = arbitrarySaltHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8));
        String differentArbitrarySaltHashHex = arbitrarySaltHasher.hashHex(input, DIFFERENT_STATIC_SALT);
        String differentSaltHashHex = arbitrarySaltHasher.hashHex(input);

        String staticSaltHashBase64 = staticSaltHasher.hashBase64(input);
        String arbitrarySaltHashBase64 = arbitrarySaltHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8));
        String differentArbitrarySaltHashBase64 = arbitrarySaltHasher.hashBase64(input, DIFFERENT_STATIC_SALT);
        String differentSaltHashBase64 = arbitrarySaltHasher.hashBase64(input);

        // Assert
        assertArrayEquals(EXPECTED_HASH_BYTES, staticSaltHash);
        assertArrayEquals(EXPECTED_HASH_BYTES, arbitrarySaltHash);
        assertFalse(Arrays.equals(EXPECTED_HASH_BYTES, differentArbitrarySaltHash));
        assertFalse(Arrays.equals(EXPECTED_HASH_BYTES, differentSaltHash));

        assertEquals(EXPECTED_HASH_HEX, staticSaltHashHex);
        assertEquals(EXPECTED_HASH_HEX, arbitrarySaltHashHex);
        assertNotEquals(EXPECTED_HASH_HEX, differentArbitrarySaltHashHex);
        assertNotEquals(EXPECTED_HASH_HEX, differentSaltHashHex);

        assertEquals(EXPECTED_HASH_BASE64, staticSaltHashBase64);
        assertEquals(EXPECTED_HASH_BASE64, arbitrarySaltHashBase64);
        assertNotEquals(EXPECTED_HASH_BASE64, differentArbitrarySaltHashBase64);
        assertNotEquals(EXPECTED_HASH_BASE64, differentSaltHashBase64);
    }

    @Test
    void testShouldValidateArbitrarySalt() {
        // Arrange
        int hashLength = 32;
        int memory = 8;
        int parallelism = 4;
        int iterations = 4;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        // Static salt instance
        Argon2SecureHasher secureHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16);
        final byte[] STATIC_SALT = "bad_sal".getBytes();

        // Act
        assertThrows(IllegalArgumentException.class, () ->
            new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 7)
        );

        assertThrows(RuntimeException.class, () -> secureHasher.hashRaw(inputBytes, STATIC_SALT));
        assertThrows(RuntimeException.class, () -> secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
        assertThrows(RuntimeException.class, () -> secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_HEX = "0c2920c52f28e0a2c77d006ec6138c8dc59580881468b85541cf886abdebcf18";

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3);

        // Act
        String hashHex = a2sh.hashHex(input);

        // Assert
        assertEquals(EXPECTED_HASH_HEX, hashHex);
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_B64 = "DCkgxS8o4KLHfQBuxhOMjcWVgIgUaLhVQc+Iar3rzxg";

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3);

        // Act
        String hashB64 = a2sh.hashBase64(input);

        // Assert
        assertEquals(EXPECTED_HASH_B64, hashB64);
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = Arrays.asList(null, "");

        final String EXPECTED_HASH_HEX = "8e5625a66b94ed9d31c1496d7f9ff49249cf05d6753b50ba0e2bf2a1108973dd";
        final String EXPECTED_HASH_B64 = "jlYlpmuU7Z0xwUltf5/0kknPBdZ1O1C6DivyoRCJc90";

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3);

        final List<String> hexResults = new ArrayList<>();
        final List<String> b64Results = new ArrayList<>();

        // Act
        for (final String input : inputs) {
            String hashHex = a2sh.hashHex(input);
            hexResults.add(hashHex);

            String hashB64 = a2sh.hashBase64(input);
            b64Results.add(hashB64);
        }

        // Assert
        hexResults.forEach(hexResult -> assertEquals(EXPECTED_HASH_HEX, hexResult));
        b64Results.forEach(b64Result -> assertEquals(EXPECTED_HASH_B64, b64Result));
    }

    /**
     * This test can have the minimum time threshold updated to determine if the performance
     * is still sufficient compared to the existing threat model.
     */
    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true")
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100; //_000
        byte[] inputBytes = "This is a sensitive value".getBytes();

        Argon2SecureHasher a2sh = new Argon2SecureHasher(16, (int) Math.pow(2, 16), 8, 5);

        final List<String> results = new ArrayList<>();
        final List<Long> resultDurations = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            long startNanos = System.nanoTime();
            byte[] hash = a2sh.hashRaw(inputBytes);
            long endNanos = System.nanoTime();
            long durationNanos = endNanos - startNanos;

            String hashHex = new String(Hex.encode(hash));

            results.add(hashHex);
            resultDurations.add(durationNanos);
        }

        // Assert
        final long MIN_DURATION_NANOS = 500_000_000; // 500 ms
        assertTrue(Collections.min(resultDurations) > MIN_DURATION_NANOS);
        assertTrue(resultDurations.stream().mapToLong(Long::longValue).sum() / testIterations > MIN_DURATION_NANOS);
    }

    @Test
    void testShouldVerifyHashLengthBoundary() throws Exception {
        // Arrange
        final int hashLength = 128;

        // Act
        boolean valid = Argon2SecureHasher.isHashLengthValid(hashLength);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailHashLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> hashLengths = Arrays.asList(-8, 0, 1, 2);

        // Act & Assert
        for (final int hashLength: hashLengths) {
            assertFalse(Argon2SecureHasher.isHashLengthValid(hashLength));
        }
    }

    @Test
    void testShouldVerifyMemorySizeBoundary() throws Exception {
        // Arrange
        final int memory = 2048;

        // Act
        boolean valid = Argon2SecureHasher.isMemorySizeValid(memory);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailMemorySizeBoundary() throws Exception {
        // Arrange
        final List<Integer> memorySizes = Arrays.asList(-12, 0, 1, 6);

        // Act & Assert
        for (final int memory : memorySizes) {
            assertFalse(Argon2SecureHasher.isMemorySizeValid(memory));
        }
    }

    @Test
    void testShouldVerifyParallelismBoundary() throws Exception {
        // Arrange
        final int parallelism = 4;

        // Act
        boolean valid = Argon2SecureHasher.isParallelismValid(parallelism);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailParallelismBoundary() throws Exception {
        // Arrange
        final List<Integer> parallelisms = Arrays.asList(-8, 0, 16777220, 16778000);

        // Act & Assert
        for (final int parallelism : parallelisms) {
            assertFalse(Argon2SecureHasher.isParallelismValid(parallelism));
        }
    }

    @Test
    void testShouldVerifyIterationsBoundary() throws Exception {
        // Arrange
        final int iterations = 4;

        // Act
        boolean valid = Argon2SecureHasher.isIterationsValid(iterations);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailIterationsBoundary() throws Exception {
        // Arrange
        final List<Integer> iterationCounts = Arrays.asList(-50, -1, 0);

        // Act & Assert
        for (final int iterations: iterationCounts) {
            assertFalse(Argon2SecureHasher.isIterationsValid(iterations));
        }
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(0, 64);

        // Act and Assert
        Argon2SecureHasher argon2SecureHasher = new Argon2SecureHasher();
        saltLengths.forEach(saltLength ->
            assertTrue(argon2SecureHasher.isSaltLengthValid(saltLength))
        );
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(-16, 4);

        // Act and Assert
        Argon2SecureHasher argon2SecureHasher = new Argon2SecureHasher();
        saltLengths.forEach(saltLength -> assertFalse(argon2SecureHasher.isSaltLengthValid(saltLength)));
    }

    @Test
    void testShouldCreateHashOfDesiredLength() throws Exception {
        // Arrange
        final List<Integer> hashLengths = Arrays.asList(16, 32);

        final String PASSWORD = "password";
        final byte[] SALT = new byte[16];
        Arrays.fill(SALT, (byte) '\0');
        final byte[] EXPECTED_HASH = Hex.decode("411c9c87e7c91d8c8eacc418665bd2e1");

        // Act
        Map<Integer, byte[]> results = hashLengths
                .stream()
                .collect(
                        Collectors.toMap(
                            Function.identity(),
                            hashLength -> {
                                Argon2SecureHasher ash = new Argon2SecureHasher(hashLength, 8, 1, 3);
                                final byte[] hash = ash.hashRaw(PASSWORD.getBytes(), SALT);
                                return hash;
                            }
                        )
                );

        // Assert
        assertFalse(Arrays.equals(Arrays.copyOf(results.get(16), 16), Arrays.copyOf(results.get(32), 16)));
        // Demonstrates that internal hash truncation is not supported
//        assert results.every { int k, byte[] v -> v[0..15] as byte[] == EXPECTED_HASH}
    }
}
