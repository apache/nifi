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

import at.favre.lib.crypto.bcrypt.Radix64Encoder;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BcryptSecureHasherTest {

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int cost = 4;

        int testIterations = 10;
        byte[] inputBytes = "This is a sensitive value".getBytes();

        final String EXPECTED_HASH_HEX = "24326124303424526b6a4559512f526245447959554b6553304471622e596b4c5331655a2e6c61586550484c69464d783937564c566d47354250454f";

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher(cost);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = bcryptSH.hashRaw(inputBytes);
            String hashHex = new String(Hex.encode(hash));
            results.add(hashHex);
        }

        // Assert
        results.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        int cost = 4;
        int saltLength = 16;

        int testIterations = 10;
        byte[] inputBytes = "This is a sensitive value".getBytes();

        final String EXPECTED_HASH_HEX = "24326124303424546d6c47615342546447463061574d6755324673642e38675a347a6149356d6b4d50594c542e344e68337962455a4678384b676a75";

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher(cost, saltLength);

        final List<String> results = new ArrayList<>();

        // Act
        for (int i = 0; i < testIterations; i++) {
            byte[] hash = bcryptSH.hashRaw(inputBytes);
            String hashHex = new String(Hex.encode(hash));
            results.add(hashHex);
        }

        // Assert
        assertEquals(results.size(), results.stream().distinct().collect(Collectors.toList()).size());
        results.forEach(result -> assertNotEquals(EXPECTED_HASH_HEX, result));
    }

    @Test
    void testShouldHandleArbitrarySalt() {
        // Arrange
        int cost = 4;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        final String EXPECTED_HASH_HEX = "24326124303424526b6a4559512f526245447959554b6553304471622e596b4c5331655a2e6c61586550484c69464d783937564c566d47354250454f";
        final String EXPECTED_HASH_BASE64 = "JDJhJDA0JFJrakVZUS9SYkVEeVlVS2VTMERxYi5Za0xTMWVaLmxhWGVQSExpRk14OTdWTFZtRzVCUEVP";
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX);

        // Static salt instance
        BcryptSecureHasher staticSaltHasher = new BcryptSecureHasher(cost);
        BcryptSecureHasher arbitrarySaltHasher = new BcryptSecureHasher(cost, 16);

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
        int cost = 4;

        final String input = "This is a sensitive value";
        byte[] inputBytes = input.getBytes();

        // Static salt instance
        BcryptSecureHasher secureHasher = new BcryptSecureHasher(cost, 16);
        final byte[] STATIC_SALT = "bad_sal".getBytes();

        assertThrows(IllegalArgumentException.class, () -> new BcryptSecureHasher(cost, 7));

        assertThrows(RuntimeException.class, () -> secureHasher.hashRaw(inputBytes, STATIC_SALT));
        assertThrows(RuntimeException.class, () -> secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
        assertThrows(RuntimeException.class, () -> secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8)));
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_HEX = "24326124313224526b6a4559512f526245447959554b6553304471622e5852696135344d4e356c5a44515243575874516c4c696d476669635a776871";

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher();

        // Act
        String hashHex = bcryptSH.hashHex(input);

        // Assert
        assertEquals(EXPECTED_HASH_HEX, hashHex);
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value";

        final String EXPECTED_HASH_BASE64 = "JDJhJDEyJFJrakVZUS9SYkVEeVlVS2VTMERxYi5YUmlhNTRNTjVsWkRRUkNXWHRRbExpbUdmaWNad2hx";

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher();

        // Act
        String hashB64 = bcryptSH.hashBase64(input);

        // Assert
        assertEquals(EXPECTED_HASH_BASE64, hashB64);
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = Arrays.asList(null, "");

        final String EXPECTED_HASH_HEX = "";
        final String EXPECTED_HASH_BASE64 = "";

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher();

        final List<String> hexResults = new ArrayList<>();
        final List<String> b64Results = new ArrayList<>();

        // Act
        for (final String input : inputs) {
            String hashHex = bcryptSH.hashHex(input);
            hexResults.add(hashHex);

            String hashB64 = bcryptSH.hashBase64(input);
            b64Results.add(hashB64);
        }

        // Assert
        hexResults.forEach(result -> assertEquals(EXPECTED_HASH_HEX, result));
        b64Results.forEach(result -> assertEquals(EXPECTED_HASH_BASE64, result));
    }

    @Test
    void testShouldVerifyCostBoundary() throws Exception {
        // Arrange
        final int cost = 14;

        // Act and Assert
        assertTrue(BcryptSecureHasher.isCostValid(cost));
    }

    @Test
    void testShouldFailCostBoundary() throws Exception {
        // Arrange
        final List<Integer> costFactors = Arrays.asList(-8, 0, 40);

        // Act and Assert
        costFactors.forEach(costFactor -> assertFalse(BcryptSecureHasher.isCostValid(costFactor)));
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(0, 16);

        // Act and Assert
        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher();
        saltLengths.forEach(saltLength -> assertTrue(bcryptSecureHasher.isSaltLengthValid(saltLength)));
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        final List<Integer> saltLengths = Arrays.asList(-8, 1);

        // Act and Assert
        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher();
        saltLengths.forEach(saltLength -> assertFalse(bcryptSecureHasher.isSaltLengthValid(saltLength)));
    }

    @Test
    void testShouldConvertRadix64ToBase64() {
        // Arrange
        final String INPUT_RADIX_64 = "mm7MiKjvXVYCujVUlKRKiu";
        final byte[] EXPECTED_BYTES = new Radix64Encoder.Default().decode(INPUT_RADIX_64.getBytes());

        // Uses standard Base64 library but removes padding chars
        final String EXPECTED_MIME_B64 = Base64.getEncoder().encodeToString(EXPECTED_BYTES).replaceAll("=", "");

        // Act
        String convertedBase64 = BcryptSecureHasher.convertBcryptRadix64ToMimeBase64(INPUT_RADIX_64);

        String convertedRadix64 = BcryptSecureHasher.convertMimeBase64ToBcryptRadix64(convertedBase64);

        // Assert
        assertEquals(EXPECTED_MIME_B64, convertedBase64);
        assertEquals(INPUT_RADIX_64, convertedRadix64);
    }

    @Test
    void testConvertRadix64ToBase64ShouldHandlePeriod() {
        // Arrange
        final String INPUT_RADIX_64 = "75x373yP7atxMD3pVgsdO.";
        final byte[] EXPECTED_BYTES = new Radix64Encoder.Default().decode(INPUT_RADIX_64.getBytes());

        // Uses standard Base64 library but removes padding chars
        final String EXPECTED_MIME_B64 = Base64.getEncoder().encodeToString(EXPECTED_BYTES).replaceAll("=", "");

        // Act
        String convertedBase64 = BcryptSecureHasher.convertBcryptRadix64ToMimeBase64(INPUT_RADIX_64);

        String convertedRadix64 = BcryptSecureHasher.convertMimeBase64ToBcryptRadix64(convertedBase64);

        // Assert
        assertEquals(EXPECTED_MIME_B64, convertedBase64);
        assertEquals(INPUT_RADIX_64, convertedRadix64);
    }
}

