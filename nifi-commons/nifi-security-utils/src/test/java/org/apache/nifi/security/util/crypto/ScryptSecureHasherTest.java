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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScryptSecureHasherTest {
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);
    private static final String SENSITIVE_VALUE = "This is a sensitive value";

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int n = 1024;
        int r = 8;
        int p = 2;
        int dkLength = 32;

        int testIterations = 10;
        byte[] inputBytes = SENSITIVE_VALUE.getBytes();

        final String EXPECTED_HASH_HEX = "a67fd2f4b3aa577b8ecdb682e60b4451a84611dcbbc534bce17616056ef8965d";

        ScryptSecureHasher scryptSH = new ScryptSecureHasher(n, r, p, dkLength);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = scryptSH.hashRaw(inputBytes);
            String hashHex = new String(Hex.encode(hash));
            results.add(hashHex);
        }

        // Assert
        results.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        int n = 1024;
        int r = 8;
        int p = 2;
        int dkLength = 128;

        int testIterations = 10;
        byte[] inputBytes = SENSITIVE_VALUE.getBytes();

        final String EXPECTED_HASH_HEX = "a67fd2f4b3aa577b8ecdb682e60b4451";

        ScryptSecureHasher scryptSH = new ScryptSecureHasher(n, r, p, dkLength, 16);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = scryptSH.hashRaw(inputBytes);
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
        int n = 1024;
        int r = 8;
        int p = 2;
        int dkLength = 32;

        final String input = SENSITIVE_VALUE;
        byte[] inputBytes = input.getBytes();

        final String EXPECTED_HASH_HEX = "a67fd2f4b3aa577b8ecdb682e60b4451a84611dcbbc534bce17616056ef8965d";
        final String EXPECTED_HASH_BASE64 = "pn/S9LOqV3uOzbaC5gtEUahGEdy7xTS84XYWBW74ll0";
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX);

        // Static salt instance
        ScryptSecureHasher staticSaltHasher = new ScryptSecureHasher(n, r, p, dkLength);
        ScryptSecureHasher arbitrarySaltHasher = new ScryptSecureHasher(n, r, p, dkLength, 16);

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
        int n = 1024;
        int r = 8;
        int p = 2;
        int dkLength = 32;

        final String input = SENSITIVE_VALUE;
        byte[] inputBytes = input.getBytes();

        // Static salt instance
        ScryptSecureHasher secureHasher = new ScryptSecureHasher(n, r, p, dkLength, 16);
        final byte[] STATIC_SALT = "bad_sal".getBytes();

        assertThrows(IllegalArgumentException.class, () -> new ScryptSecureHasher(n, r, p, dkLength, 7));
        assertThrows(RuntimeException.class, () -> secureHasher.hashRaw(inputBytes, STATIC_SALT));
        assertThrows(RuntimeException.class, () -> secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
        assertThrows(RuntimeException.class, () -> secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = SENSITIVE_VALUE;

        final String EXPECTED_HASH_HEX = "6a9c827815fe0718af5e336811fc78dd719c8d9505e015283239b9bf1d24ee71";

        SecureHasher scryptSH = new ScryptSecureHasher();

        // Act
        String hashHex = scryptSH.hashHex(input);

        // Assert
        assertEquals(EXPECTED_HASH_HEX, hashHex);
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = SENSITIVE_VALUE;

        final String EXPECTED_HASH_BASE64 = "apyCeBX+BxivXjNoEfx43XGcjZUF4BUoMjm5vx0k7nE";

        SecureHasher scryptSH = new ScryptSecureHasher();

        // Act
        String hashB64 = scryptSH.hashBase64(input);

        // Assert
        assertEquals(EXPECTED_HASH_BASE64, hashB64);
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = Arrays.asList(null, "");

        final String EXPECTED_HASH_HEX = "";
        final String EXPECTED_HASH_BASE64 = "";

        ScryptSecureHasher scryptSH = new ScryptSecureHasher();

        final List<String> hexResults = new ArrayList<>();
        final List<String> B64Results = new ArrayList<>();

        // Act
        for (final String input : inputs) {
            String hashHex = scryptSH.hashHex(input);
            hexResults.add(hashHex);

            String hashB64 = scryptSH.hashBase64(input);
            B64Results.add(hashB64);
        }

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
        byte[] inputBytes = SENSITIVE_VALUE.getBytes();

        ScryptSecureHasher scryptSH = new ScryptSecureHasher();

        final List<String> results = new ArrayList<>();
        final List<Long> resultDurations = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            long startNanos = System.nanoTime();
            byte[] hash = scryptSH.hashRaw(inputBytes);
            long endNanos = System.nanoTime();
            long durationNanos = endNanos - startNanos;

            String hashHex = new String(Hex.encode(hash));

            results.add(hashHex);
            resultDurations.add(durationNanos);
        }

        // Assert
        final long MIN_DURATION_NANOS = 75_000_000; // 75 ms
        assertTrue(Collections.min(resultDurations) > MIN_DURATION_NANOS);
        assertTrue(resultDurations.stream().mapToLong(Long::longValue).sum() / testIterations > MIN_DURATION_NANOS);
    }

    @Test
    void testShouldVerifyRBoundary() throws Exception {
        // Arrange
        final int r = 32;

        // Act
        boolean valid = ScryptSecureHasher.isRValid(r);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailRBoundary() throws Exception {
        // Arrange
        List<Integer> rValues = Arrays.asList(-8, 0, 2147483647);

        // Act and Assert
        rValues.forEach(rValue -> assertFalse(ScryptSecureHasher.isRValid(rValue)));
    }

    @Test
    void testShouldVerifyNBoundary() throws Exception {
        // Arrange
        final Integer n = 16385;
        final int r = 8;

        // Act and Assert
        assertTrue(ScryptSecureHasher.isNValid(n, r));
    }

    @Test
    void testShouldFailNBoundary() throws Exception {
        // Arrange
        final Map<Integer, Integer> costParameters = new HashMap<>();
        costParameters.put(-8, 8);
        costParameters.put(0, 32);

        //Act and Assert
        costParameters.entrySet().forEach(entry ->
                assertFalse(ScryptSecureHasher.isNValid(entry.getKey(), entry.getValue()))
        );
    }

    @Test
    void testShouldVerifyPBoundary() throws Exception {
        // Arrange
        final List<Integer> ps = Arrays.asList(1, 8, 1024);
        final List<Integer> rs = Arrays.asList(8, 1024, 4096);

        // Act and Assert
        ps.forEach(p ->
                rs.forEach(r ->
                        assertTrue(ScryptSecureHasher.isPValid(p, r))
                )
        );
    }

    @Test
    void testShouldFailIfPBoundaryExceeded() throws Exception {
        // Arrange
        final List<Integer> ps = Arrays.asList(4096 * 64, 1024 * 1024);
        final List<Integer> rs = Arrays.asList(4096, 1024 * 1024);

        // Act and Assert
        ps.forEach(p ->
                rs.forEach(r ->
                        assertFalse(ScryptSecureHasher.isPValid(p, r))
                )
        );
    }

    @Test
    void testShouldVerifyDKLengthBoundary() throws Exception {
        // Arrange
        final Integer dkLength = 64;

        // Act
        boolean valid = ScryptSecureHasher.isDKLengthValid(dkLength);

        // Assert
        assertTrue(valid);
    }

    @Test
    void testShouldFailDKLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> dKLengths = Arrays.asList(-8, 0, 2147483647);

        // Act and Assert
        dKLengths.forEach(dKLength ->
                assertFalse(ScryptSecureHasher.isDKLengthValid(dKLength))
        );
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(0, 64);

        // Act and Assert
        ScryptSecureHasher scryptSecureHasher = new ScryptSecureHasher();
        saltLengths.forEach(saltLength ->
                assertTrue(scryptSecureHasher.isSaltLengthValid(saltLength))
        );
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(-8, 1, 2147483647);

        // Act and Assert
        ScryptSecureHasher scryptSecureHasher = new ScryptSecureHasher();
        saltLengths.forEach(saltLength ->
                assertFalse(scryptSecureHasher.isSaltLengthValid(saltLength))
        );
    }
}
