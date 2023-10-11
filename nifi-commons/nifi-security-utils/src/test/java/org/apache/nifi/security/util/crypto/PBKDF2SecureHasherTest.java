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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PBKDF2SecureHasherTest {
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int cost = 10_000;
        int dkLength = 32;
        byte[] inputBytes = "This is a sensitive value".getBytes();
        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511";

        // Act
        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher(cost, dkLength);
        List<String> results = Stream.iterate(0, n -> n + 1)
                .limit(10)
                .map(iteration -> {
                    byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes);
                    return new String(Hex.encode(hash));
                })
                .collect(Collectors.toList());

        // Assert
        results.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        String prf = "SHA512";
        int cost = 10_000;
        int saltLength = 16;
        int dkLength = 32;
        byte[] inputBytes = "This is a sensitive value".getBytes();
        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511";

        //Act
        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength);
        List<String> results = Stream.iterate(0, n -> n + 1)
                .limit(10)
                .map(iteration -> {
                    byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes);
                    return new String(Hex.encode(hash));
                })
                .collect(Collectors.toList());

        // Assert
        assertEquals(results.stream().distinct().collect(Collectors.toList()).size(), results.size());
        results.forEach(result -> assertNotEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldHandleArbitrarySalt() {
        // Arrange
        String prf = "SHA512";
        int cost = 10_000;
        int saltLength = 16;
        int dkLength = 32;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511";
        final String EXPECTED_HASH_BASE64 = "LEem2AG3Hgh/lHkgecQIgK6ikBO//9CrlLG8ES6lJRE";
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX);

        PBKDF2SecureHasher staticSaltHasher = new PBKDF2SecureHasher(cost, dkLength);
        PBKDF2SecureHasher arbitrarySaltHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength);

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
        // Assert
        String prf = "SHA512";
        int cost = 10_000;
        int saltLength = 16;
        int dkLength = 32;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        // Static salt instance
        PBKDF2SecureHasher secureHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength);
        byte[] STATIC_SALT = "bad_sal".getBytes();

        assertThrows(IllegalArgumentException.class, () -> new PBKDF2SecureHasher(prf, cost, 7, dkLength));
        assertThrows(RuntimeException.class, () -> secureHasher.hashRaw(inputBytes, STATIC_SALT));
        assertThrows(RuntimeException.class, () -> secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
        assertThrows(RuntimeException.class, () -> secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_HEX = "8f67110e87d225366e2d79ad251d2cf48f8cb15845800452e0e2cff09f95ef1c";

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();

        // Act
        String hashHex = pbkdf2SecureHasher.hashHex(input);

        // Assert
        assertEquals(EXPECTED_HASH_HEX, hashHex);
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_BASE64 = "j2cRDofSJTZuLXmtJR0s9I+MsVhFgARS4OLP8J+V7xw";

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();

        // Act
        String hashB64 = pbkdf2SecureHasher.hashBase64(input);

        // Assert
        assertEquals(EXPECTED_HASH_BASE64, hashB64);
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = Arrays.asList(null, "");

        final String EXPECTED_HASH_HEX = "7f2d8d8c7aaa45471f6c05a8edfe0a3f75fe01478cc965c5dce664e2ac6f5d0a";
        final String EXPECTED_HASH_BASE64 = "fy2NjHqqRUcfbAWo7f4KP3X+AUeMyWXF3OZk4qxvXQo";

        // Act
        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();
        List<String> hexResults = inputs.stream()
                .map(input -> pbkdf2SecureHasher.hashHex(input))
                .collect(Collectors.toList());
        List<String> B64Results = inputs.stream()
                .map(input -> pbkdf2SecureHasher.hashBase64(input))
                .collect(Collectors.toList());

        // Assert
        hexResults.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
        B64Results.forEach(result -> assertEquals(EXPECTED_HASH_BASE64, result));
    }

    /**
     * This test can have the minimum time threshold updated to determine if the performance
     * is still sufficient compared to the existing threat model.
     */
    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true")
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100;
        byte[] inputBytes = "This is a sensitive value".getBytes();

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();

        final List<String> results = new ArrayList<>();
        final List<Long> resultDurations = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            long startNanos = System.nanoTime();
            byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes);
            long endNanos = System.nanoTime();
            long durationNanos = endNanos - startNanos;

            String hashHex = Arrays.toString(Hex.encode(hash));

            results.add(hashHex);
            resultDurations.add(durationNanos);
        }

        // Assert
        final long MIN_DURATION_NANOS = 75_000_000; // 75 ms
        assertTrue(Collections.min(resultDurations) > MIN_DURATION_NANOS);
        assertTrue(resultDurations.stream().mapToLong(Long::longValue).sum() / testIterations > MIN_DURATION_NANOS);
    }

    @Test
    void testShouldVerifyIterationCountBoundary() throws Exception {
        // Arrange
        final List<Integer> validIterationCounts = Arrays.asList(1, 1000, 1_000_000);

        // Act & Assert
        for (final int iterationCount : validIterationCounts) {
            assertTrue(PBKDF2SecureHasher.isIterationCountValid(iterationCount));
        }
    }

    @Test
    void testShouldFailIterationCountBoundary() throws Exception {
        // Arrange
        List<Integer> invalidIterationCounts = Arrays.asList(-1, 0, Integer.MAX_VALUE + 1);

        // Act and Assert
        invalidIterationCounts.forEach(i -> assertFalse(PBKDF2SecureHasher.isIterationCountValid(i)));
    }

    @Test
    void testShouldVerifyDKLengthBoundary() throws Exception {
        // Arrange
        List<Integer> validHLengths = Arrays.asList(32, 64);

        // 1 and MAX_VALUE are the length boundaries, inclusive
        List<Integer> validDKLengths = Arrays.asList(1, 1000, 1_000_000, Integer.MAX_VALUE);

        // Act and Assert
        validHLengths.forEach(hLen -> {
            validDKLengths.forEach(dkLength -> {
                assertTrue(PBKDF2SecureHasher.isDKLengthValid(hLen, dkLength));
            });
        });
    }

    @Test
    void testShouldFailDKLengthBoundary() throws Exception {
        // Arrange
        List<Integer> validHLengths = Arrays.asList(32, 64);

        // MAX_VALUE + 1 will become MIN_VALUE because of signed integer math
        List<Integer> invalidDKLengths = Arrays.asList(-1, 0, Integer.MAX_VALUE + 1, Integer.valueOf(Integer.MAX_VALUE * 2 - 1));

        // Act and Assert
        validHLengths.forEach(hLen -> {
            invalidDKLengths.forEach(dkLength -> {
                assertFalse(PBKDF2SecureHasher.isDKLengthValid(hLen, dkLength));
            });
        });
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        List<Integer> saltLengths = Arrays.asList(0, 16, 64);

        // Act and Assert
        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();
        saltLengths.forEach(saltLength -> assertTrue(pbkdf2SecureHasher.isSaltLengthValid(saltLength)));
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        List<Integer> saltLengths = Arrays.asList(-8, 1, Integer.MAX_VALUE + 1);

        // Act and Assert
        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher();
        saltLengths.forEach(saltLength -> assertFalse(pbkdf2SecureHasher.isSaltLengthValid(saltLength)));
    }
}
