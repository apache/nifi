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
package org.apache.nifi.security.util.crypto

import org.apache.nifi.security.util.EncryptionMethod
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class CipherUtilityGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CipherUtilityGroovyTest.class)

    // TripleDES must precede DES for automatic grouping precedence
    private static final List<String> CIPHERS = ["AES", "TRIPLEDES", "DES", "RC2", "RC4", "RC5", "TWOFISH"]
    private static final List<String> SYMMETRIC_ALGORITHMS = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") || it.algorithm.startsWith("AES") }*.algorithm
    private static final Map<String, List<String>> ALGORITHMS_MAPPED_BY_CIPHER = SYMMETRIC_ALGORITHMS.groupBy { String algorithm -> CIPHERS.find { algorithm.contains(it) } }

    // Manually mapped as of 01/19/16 0.5.0
    private static final Map<Integer, List<String>> ALGORITHMS_MAPPED_BY_KEY_LENGTH = [
            (40) : ["PBEWITHSHAAND40BITRC2-CBC",
                    "PBEWITHSHAAND40BITRC4"],
            (64) : ["PBEWITHMD5ANDDES",
                    "PBEWITHSHA1ANDDES"],
            (112): ["PBEWITHSHAAND2-KEYTRIPLEDES-CBC",
                    "PBEWITHSHAAND3-KEYTRIPLEDES-CBC"],
            (128): ["PBEWITHMD5AND128BITAES-CBC-OPENSSL",
                    "PBEWITHMD5ANDRC2",
                    "PBEWITHSHA1ANDRC2",
                    "PBEWITHSHA256AND128BITAES-CBC-BC",
                    "PBEWITHSHAAND128BITAES-CBC-BC",
                    "PBEWITHSHAAND128BITRC2-CBC",
                    "PBEWITHSHAAND128BITRC4",
                    "PBEWITHSHAANDTWOFISH-CBC",
                    "AES/CBC/PKCS7Padding",
                    "AES/CTR/NoPadding",
                    "AES/GCM/NoPadding"],
            (192): ["PBEWITHMD5AND192BITAES-CBC-OPENSSL",
                    "PBEWITHSHA256AND192BITAES-CBC-BC",
                    "PBEWITHSHAAND192BITAES-CBC-BC",
                    "AES/CBC/PKCS7Padding",
                    "AES/CTR/NoPadding",
                    "AES/GCM/NoPadding"],
            (256): ["PBEWITHMD5AND256BITAES-CBC-OPENSSL",
                    "PBEWITHSHA256AND256BITAES-CBC-BC",
                    "PBEWITHSHAAND256BITAES-CBC-BC",
                    "AES/CBC/PKCS7Padding",
                    "AES/CTR/NoPadding",
                    "AES/GCM/NoPadding"]
    ]

    @BeforeClass
    static void setUpOnce() {
        Security.addProvider(new BouncyCastleProvider());

        // Fix because TRIPLEDES -> DESede
        def tripleDESAlgorithms = ALGORITHMS_MAPPED_BY_CIPHER.remove("TRIPLEDES")
        ALGORITHMS_MAPPED_BY_CIPHER.put("DESede", tripleDESAlgorithms)

        logger.info("Mapped algorithms: ${ALGORITHMS_MAPPED_BY_CIPHER}")
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testShouldParseCipherFromAlgorithm() {
        // Arrange
        final def EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_CIPHER

        // Act
        SYMMETRIC_ALGORITHMS.each { String algorithm ->
            String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm)
            logger.info("Extracted ${cipher} from ${algorithm}")

            // Assert
            assert EXPECTED_ALGORITHMS.get(cipher).contains(algorithm)
        }
    }

    @Test
    void testShouldParseKeyLengthFromAlgorithm() {
        // Arrange
        final def EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_KEY_LENGTH

        // Act
        SYMMETRIC_ALGORITHMS.each { String algorithm ->
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(algorithm)
            logger.info("Extracted ${keyLength} from ${algorithm}")

            // Assert
            assert EXPECTED_ALGORITHMS.get(keyLength).contains(algorithm)
        }
    }

    @Test
    void testShouldDetermineValidKeyLength() {
        // Arrange

        // Act
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.each { int keyLength, List<String> algorithms ->
            algorithms.each { String algorithm ->
                logger.info("Checking ${keyLength} for ${algorithm}")

                // Assert
                assert CipherUtility.isValidKeyLength(keyLength, CipherUtility.parseCipherFromAlgorithm(algorithm))
            }
        }
    }

    @Test
    void testShouldDetermineInvalidKeyLength() {
        // Arrange

        // Act
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.each { int keyLength, List<String> algorithms ->
            algorithms.each { String algorithm ->
                def invalidKeyLengths = [-1, 0, 1]
                if (algorithm =~ "RC\\d") {
                    invalidKeyLengths += [39, 2049]
                } else {
                    invalidKeyLengths += keyLength + 1
                }
                logger.info("Checking ${invalidKeyLengths.join(", ")} for ${algorithm}")

                // Assert
                invalidKeyLengths.each { int invalidKeyLength ->
                    assert !CipherUtility.isValidKeyLength(invalidKeyLength, CipherUtility.parseCipherFromAlgorithm(algorithm))
                }
            }
        }
    }

    @Test
    void testShouldDetermineValidKeyLengthForAlgorithm() {
        // Arrange

        // Act
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.each { int keyLength, List<String> algorithms ->
            algorithms.each { String algorithm ->
                logger.info("Checking ${keyLength} for ${algorithm}")

                // Assert
                assert CipherUtility.isValidKeyLengthForAlgorithm(keyLength, algorithm)
            }
        }
    }

    @Test
    void testShouldDetermineInvalidKeyLengthForAlgorithm() {
        // Arrange

        // Act
        ALGORITHMS_MAPPED_BY_KEY_LENGTH.each { int keyLength, List<String> algorithms ->
            algorithms.each { String algorithm ->
                def invalidKeyLengths = [-1, 0, 1]
                if (algorithm =~ "RC\\d") {
                    invalidKeyLengths += [39, 2049]
                } else {
                    invalidKeyLengths += keyLength + 1
                }
                logger.info("Checking ${invalidKeyLengths.join(", ")} for ${algorithm}")

                // Assert
                invalidKeyLengths.each { int invalidKeyLength ->
                    assert !CipherUtility.isValidKeyLengthForAlgorithm(invalidKeyLength, algorithm)
                }
            }
        }

        // Extra hard-coded checks
        String algorithm = "PBEWITHSHA256AND256BITAES-CBC-BC"
        int invalidKeyLength = 192
        logger.info("Checking ${invalidKeyLength} for ${algorithm}")
        assert !CipherUtility.isValidKeyLengthForAlgorithm(invalidKeyLength, algorithm)
    }

    @Test
    void testShouldGetValidKeyLengthsForAlgorithm() {
        // Arrange

        def rcKeyLengths = (40..2048).asList()
        def CIPHER_KEY_SIZES = [
                AES    : [128, 192, 256],
                DES    : [56, 64],
                DESede : [56, 64, 112, 128, 168, 192],
                RC2    : rcKeyLengths,
                RC4    : rcKeyLengths,
                RC5    : rcKeyLengths,
                TWOFISH: [128, 192, 256]
        ]

        def SINGLE_KEY_SIZE_ALGORITHMS = EncryptionMethod.values()*.algorithm.findAll { CipherUtility.parseActualKeyLengthFromAlgorithm(it) != -1 }
        logger.info("Single key size algorithms: ${SINGLE_KEY_SIZE_ALGORITHMS}")
        def MULTIPLE_KEY_SIZE_ALGORITHMS = EncryptionMethod.values()*.algorithm - SINGLE_KEY_SIZE_ALGORITHMS
        MULTIPLE_KEY_SIZE_ALGORITHMS.removeAll { it.contains("PGP") }
        logger.info("Multiple key size algorithms: ${MULTIPLE_KEY_SIZE_ALGORITHMS}")

        // Act
        SINGLE_KEY_SIZE_ALGORITHMS.each { String algorithm ->
            def EXPECTED_KEY_SIZES = [CipherUtility.parseKeyLengthFromAlgorithm(algorithm)]

            def validKeySizes = CipherUtility.getValidKeyLengthsForAlgorithm(algorithm)
            logger.info("Checking ${algorithm} ${validKeySizes} against expected ${EXPECTED_KEY_SIZES}")

            // Assert
            assert validKeySizes == EXPECTED_KEY_SIZES
        }

        // Act
        MULTIPLE_KEY_SIZE_ALGORITHMS.each { String algorithm ->
            String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm)
            def EXPECTED_KEY_SIZES = CIPHER_KEY_SIZES[cipher]

            def validKeySizes = CipherUtility.getValidKeyLengthsForAlgorithm(algorithm)
            logger.info("Checking ${algorithm} ${validKeySizes} against expected ${EXPECTED_KEY_SIZES}")

            // Assert
            assert validKeySizes == EXPECTED_KEY_SIZES
        }
    }
}
