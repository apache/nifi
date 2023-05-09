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

import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CipherUtilityTest {
    private static final Pattern KEY_LENGTH_PATTERN = Pattern.compile("([\\d]+)BIT");
    // TripleDES must precede DES for automatic grouping precedence
    private static final List<String> CIPHERS = Arrays.asList("AES", "TRIPLEDES", "DES", "RC2", "RC4", "RC5", "TWOFISH");
    private static final List<String> SYMMETRIC_ALGORITHMS = Arrays.stream(EncryptionMethod.values())
            .map(it -> it.getAlgorithm())
            .filter(algorithm -> algorithm.startsWith("PBE") || algorithm.startsWith("AES"))
            .collect(Collectors.toList());
    private static final Map<String, List<String>> ALGORITHMS_MAPPED_BY_CIPHER = SYMMETRIC_ALGORITHMS
            .stream()
            .collect(Collectors.groupingBy(algorithm -> CIPHERS.stream().filter(cipher -> algorithm.contains(cipher)).findFirst().get()));

    // Manually mapped as of 03/21/21 1.13.0
    private static final Map<Integer, List<String>> ALGORITHMS_MAPPED_BY_KEY_LENGTH = new HashMap<>();
    static {
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(40, Arrays.asList("PBEWITHSHAAND40BITRC2-CBC",
                "PBEWITHSHAAND40BITRC4"));
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(64, Arrays.asList("PBEWITHMD5ANDDES",
                "PBEWITHSHA1ANDDES"));
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(112, Arrays.asList("PBEWITHSHAAND2-KEYTRIPLEDES-CBC",
                "PBEWITHSHAAND3-KEYTRIPLEDES-CBC"));
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(128, Arrays.asList("PBEWITHMD5AND128BITAES-CBC-OPENSSL",
                "PBEWITHMD5ANDRC2",
                "PBEWITHSHA1ANDRC2",
                "PBEWITHSHA256AND128BITAES-CBC-BC",
                "PBEWITHSHAAND128BITAES-CBC-BC",
                "PBEWITHSHAAND128BITRC2-CBC",
                "PBEWITHSHAAND128BITRC4",
                "PBEWITHSHAANDTWOFISH-CBC",
                "AES/CBC/NoPadding",
                "AES/CBC/PKCS7Padding",
                "AES/CTR/NoPadding",
                "AES/GCM/NoPadding"));
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(192, Arrays.asList("PBEWITHMD5AND192BITAES-CBC-OPENSSL",
                "PBEWITHSHA256AND192BITAES-CBC-BC",
                "PBEWITHSHAAND192BITAES-CBC-BC",
                "AES/CBC/NoPadding",
                "AES/CBC/PKCS7Padding",
                "AES/CTR/NoPadding",
                "AES/GCM/NoPadding"));
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.put(256, Arrays.asList("PBEWITHMD5AND256BITAES-CBC-OPENSSL",
                "PBEWITHSHA256AND256BITAES-CBC-BC",
                "PBEWITHSHAAND256BITAES-CBC-BC",
                "AES/CBC/NoPadding",
                "AES/CBC/PKCS7Padding",
                "AES/CTR/NoPadding",
                "AES/GCM/NoPadding"));
    }

    @BeforeAll
    static void setUpOnce() {
        Security.addProvider(new BouncyCastleProvider());

        // Fix because TRIPLEDES -> DESede
        final List<String> tripleDESAlgorithms = ALGORITHMS_MAPPED_BY_CIPHER.remove("TRIPLEDES");
        ALGORITHMS_MAPPED_BY_CIPHER.put("DESede", tripleDESAlgorithms);
    }

    @Test
    void testShouldParseCipherFromAlgorithm() {
        // Arrange
        final Map<String, List<String>> EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_CIPHER;

        // Act
        for (final String algorithm: SYMMETRIC_ALGORITHMS) {
            String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm);

            // Assert
            assertTrue(EXPECTED_ALGORITHMS.get(cipher).contains(algorithm));
        }
    }

    @Test
    void testShouldParseKeyLengthFromAlgorithm() {
        // Arrange
        final Map<Integer, List<String>> EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_KEY_LENGTH;

        // Act
        for (final String algorithm: SYMMETRIC_ALGORITHMS) {
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(algorithm);

            // Assert
            assertTrue(EXPECTED_ALGORITHMS.get(keyLength).contains(algorithm));
        }
    }

    @Test
    void testShouldDetermineValidKeyLength() {
        // Arrange

        // Act
        for (final Map.Entry<Integer, List<String>> entry : ALGORITHMS_MAPPED_BY_KEY_LENGTH.entrySet()) {
            final int keyLength = entry.getKey();
            final List<String> algorithms = entry.getValue();
            for (final String algorithm : algorithms) {
                // Assert

                assertTrue(CipherUtility.isValidKeyLength(keyLength, CipherUtility.parseCipherFromAlgorithm(algorithm)));
            }
        }
    }

    @Test
    void testShouldDetermineInvalidKeyLength() {
        // Arrange

        // Act
        for (final Map.Entry<Integer, List<String>> entry : ALGORITHMS_MAPPED_BY_KEY_LENGTH.entrySet()) {
            final int keyLength = entry.getKey();
            final List<String> algorithms = entry.getValue();
            for (final String algorithm : algorithms) {
                final List<Integer> invalidKeyLengths = new ArrayList<>(Arrays.asList(-1, 0, 1));
                final Matcher matcher = Pattern.compile("RC\\d").matcher(algorithm);
                if (matcher.find()) {
                    invalidKeyLengths.add(39);
                    invalidKeyLengths.add(2049);
                } else {
                    invalidKeyLengths.add(keyLength + 1);
                }

                // Assert
                invalidKeyLengths.forEach(invalidKeyLength -> assertFalse(CipherUtility.isValidKeyLength(invalidKeyLength, CipherUtility.parseCipherFromAlgorithm(algorithm))));
            }
        }
    }

    @Test
    void testShouldDetermineValidKeyLengthForAlgorithm() {
        // Arrange

        // Act
        for (final Map.Entry<Integer, List<String>> entry : ALGORITHMS_MAPPED_BY_KEY_LENGTH.entrySet()) {
            final int keyLength = entry.getKey();
            final List<String> algorithms = entry.getValue();
            for (final String algorithm : algorithms) {

                // Assert
                assertTrue(CipherUtility.isValidKeyLengthForAlgorithm(keyLength, algorithm));
            }
        }
    }

    @Test
    void testShouldDetermineInvalidKeyLengthForAlgorithm() {
        // Arrange

        // Act
        for (final Map.Entry<Integer, List<String>> entry : ALGORITHMS_MAPPED_BY_KEY_LENGTH.entrySet()) {
            final int keyLength = entry.getKey();
            final List<String> algorithms = entry.getValue();
            for (final String algorithm : algorithms) {
                final List<Integer> invalidKeyLengths = new ArrayList<>(Arrays.asList(-1, 0, 1));
                final Matcher matcher = Pattern.compile("RC\\d").matcher(algorithm);
                if (matcher.find()) {
                    invalidKeyLengths.add(39);
                    invalidKeyLengths.add(2049);
                } else {
                    invalidKeyLengths.add(keyLength + 1);
                }

                // Assert
                invalidKeyLengths.forEach(invalidKeyLength -> assertFalse(CipherUtility.isValidKeyLengthForAlgorithm(invalidKeyLength, algorithm)));
            }
        }

        // Extra hard-coded checks
        String algorithm = "PBEWITHSHA256AND256BITAES-CBC-BC";
        int invalidKeyLength = 192;
        assertFalse(CipherUtility.isValidKeyLengthForAlgorithm(invalidKeyLength, algorithm));
    }

    @Test
    void testShouldGetValidKeyLengthsForAlgorithm() {
        // Arrange
        final List<Integer> rcKeyLengths = IntStream.rangeClosed(40, 2048)
                .boxed().collect(Collectors.toList());
        final Map<String, List<Integer>> CIPHER_KEY_SIZES = new HashMap<>();
        CIPHER_KEY_SIZES.put("AES", Arrays.asList(128, 192, 256));
        CIPHER_KEY_SIZES.put("DES", Arrays.asList(56, 64));
        CIPHER_KEY_SIZES.put("DESede", Arrays.asList(56, 64, 112, 128, 168, 192));
        CIPHER_KEY_SIZES.put("RC2", rcKeyLengths);
        CIPHER_KEY_SIZES.put("RC4", rcKeyLengths);
        CIPHER_KEY_SIZES.put("RC5", rcKeyLengths);
        CIPHER_KEY_SIZES.put("TWOFISH", Arrays.asList(128, 192, 256));

        final List<String> SINGLE_KEY_SIZE_ALGORITHMS = Arrays.stream(EncryptionMethod.values())
                .map(encryptionMethod -> encryptionMethod.getAlgorithm())
                .filter(algorithm -> parseActualKeyLengthFromAlgorithm(algorithm) != -1)
                .collect(Collectors.toList());
        final List<String> MULTIPLE_KEY_SIZE_ALGORITHMS = Arrays.stream(EncryptionMethod.values())
                .map(encryptionMethod -> encryptionMethod.getAlgorithm())
                .filter(algorithm -> !algorithm.contains("PGP"))
                .collect(Collectors.toList());
        MULTIPLE_KEY_SIZE_ALGORITHMS.removeAll(SINGLE_KEY_SIZE_ALGORITHMS);

        // Act
        for (final String algorithm : SINGLE_KEY_SIZE_ALGORITHMS) {
            final List<Integer> EXPECTED_KEY_SIZES = Arrays.asList(CipherUtility.parseKeyLengthFromAlgorithm(algorithm));

            final List<Integer> validKeySizes = CipherUtility.getValidKeyLengthsForAlgorithm(algorithm);

            // Assert
            assertEquals(EXPECTED_KEY_SIZES, validKeySizes);
        }

        // Act
        for (final String algorithm : MULTIPLE_KEY_SIZE_ALGORITHMS) {
            final String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm);
            final List<Integer> EXPECTED_KEY_SIZES = CIPHER_KEY_SIZES.get(cipher);

            final List<Integer> validKeySizes = CipherUtility.getValidKeyLengthsForAlgorithm(algorithm);

            // Assert
            assertEquals(EXPECTED_KEY_SIZES, validKeySizes);
        }
    }

    @Test
    void testShouldFindSequence() {
        // Arrange
        byte[] license = ("Licensed to the Apache Software Foundation (ASF) under one or more " +
                        "contributor license agreements.  See the NOTICE file distributed with " +
                        "this work for additional information regarding copyright ownership. " +
                        "The ASF licenses this file to You under the Apache License, Version 2.0 " +
                        "(the \"License\"); you may not use this file except in compliance with " +
                        "the License.  You may obtain a copy of the License at ".getBytes()).getBytes();

        byte[] apache = "Apache".getBytes();
        byte[] software = "Software".getBytes();
        byte[] asf = "ASF".getBytes();
        byte[] kafka = "Kafka".getBytes();

        // Act
        int apacheIndex = CipherUtility.findSequence(license, apache);

        int softwareIndex = CipherUtility.findSequence(license, software);

        int asfIndex = CipherUtility.findSequence(license, asf);

        int kafkaIndex = CipherUtility.findSequence(license, kafka);

        // Assert
        assertEquals(16, apacheIndex);
        assertEquals(23, softwareIndex);
        assertEquals(44, asfIndex);
        assertEquals(-1, kafkaIndex);
    }

    @Test
    void testShouldExtractRawSalt() {
        // Arrange
        final byte[] PLAIN_SALT = new byte[16];
        Arrays.fill(PLAIN_SALT, (byte) 0xab);

        String ARGON2_SALT = Argon2CipherProvider.formSalt(PLAIN_SALT, 8, 1, 1);
        String BCRYPT_SALT = BcryptCipherProvider.formatSaltForBcrypt(PLAIN_SALT, 10);
        String SCRYPT_SALT = ScryptCipherProvider.formatSaltForScrypt(PLAIN_SALT, 10, 1, 1);

        // Act
        final Map<Object, byte[]> results = Arrays.stream(KeyDerivationFunction.values())
                .filter(kdf -> !kdf.isStrongKDF())
                .collect(Collectors.toMap(Function.identity(), kdf -> CipherUtility.extractRawSalt(PLAIN_SALT, kdf)));

        results.put(KeyDerivationFunction.ARGON2, CipherUtility.extractRawSalt(ARGON2_SALT.getBytes(), KeyDerivationFunction.ARGON2));
        results.put(KeyDerivationFunction.BCRYPT, CipherUtility.extractRawSalt(BCRYPT_SALT.getBytes(), KeyDerivationFunction.BCRYPT));
        results.put(KeyDerivationFunction.SCRYPT, CipherUtility.extractRawSalt(SCRYPT_SALT.getBytes(), KeyDerivationFunction.SCRYPT));
        results.put(KeyDerivationFunction.PBKDF2, CipherUtility.extractRawSalt(PLAIN_SALT, KeyDerivationFunction.PBKDF2));

        // Assert
        results.values().forEach(v -> assertArrayEquals(PLAIN_SALT, v));
    }

    private static int parseActualKeyLengthFromAlgorithm(final String algorithm) {
        Matcher matcher = KEY_LENGTH_PATTERN.matcher(algorithm);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }
}
