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

import org.apache.kerby.util.Hex
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class Argon2SecureHasherTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(Argon2SecureHasherTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    private static byte[] decodeHex(String hex) {
        Hex.decode(hex?.replaceAll("[^0-9a-fA-F]", ""))
    }

    @Ignore("Cannot override static salt")
    @Test
    void testShouldMatchReferenceVectors() {
        // Arrange
        int hashLength = 32
        int memory = 32
        int parallelism = 4
        int iterations = 3
        logger.info("Generating Argon2 hash for hash length: ${hashLength} B, mem: ${memory} KiB, parallelism: ${parallelism}, iterations: ${iterations}")

        Argon2SecureHasher a2sh = new Argon2SecureHasher(hashLength, memory, parallelism, iterations)
        // Override the static salt for the published test vector
//        a2sh.staticSalt = [0x02] * 16

        // Act
        byte[] hash = a2sh.hashRaw([0x01] * 32 as byte[])
        logger.info("Generated hash: ${Hex.encode(hash)}")

        // Assert
        assert hash == decodeHex("0d 64 0d f5 8d 78 76 6c 08 c0 37 a3 4a 8b 53 c9 d0 " +
                "1e f0 45 2d 75 b6 5e b5 25 20 e9 6b 01 e6 59")

        // Clean up
//        Argon2SecureHasher.staticSalt = "NiFi Static Salt".bytes
    }

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int hashLength = 32
        int memory = 8
        int parallelism = 4
        int iterations = 4
        logger.info("Generating Argon2 hash for hash length: ${hashLength} B, mem: ${memory} KiB, parallelism: ${parallelism}, iterations: ${iterations}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919"

        Argon2SecureHasher a2sh = new Argon2SecureHasher(hashLength, memory, parallelism, iterations)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = a2sh.hashRaw(inputBytes)
            String hashHex = Hex.encode(hash)
            logger.info("Generated hash: ${hashHex}")
            results << hashHex
        }

        // Assert
        assert results.every { it == EXPECTED_HASH_HEX }
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        int hashLength = 32
        int memory = 8
        int parallelism = 4
        int iterations = 4
        logger.info("Generating Argon2 hash for hash length: ${hashLength} B, mem: ${memory} KiB, parallelism: ${parallelism}, iterations: ${iterations}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919"

        Argon2SecureHasher a2sh = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = a2sh.hashRaw(inputBytes)
            String hashHex = Hex.encode(hash)
            logger.info("Generated hash: ${hashHex}")
            results << hashHex
        }

        // Assert
        assert results.unique().size() == results.size()
        assert results.every { it != EXPECTED_HASH_HEX }
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_HEX = "0c2920c52f28e0a2c77d006ec6138c8dc59580881468b85541cf886abdebcf18"

        Argon2SecureHasher a2sh = new Argon2SecureHasher()

        // Act
        String hashHex = a2sh.hashHex(input)
        logger.info("Generated hash: ${hashHex}")

        // Assert
        assert hashHex == EXPECTED_HASH_HEX
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_B64 = "DCkgxS8o4KLHfQBuxhOMjcWVgIgUaLhVQc+Iar3rzxg="

        Argon2SecureHasher a2sh = new Argon2SecureHasher()

        // Act
        String hashB64 = a2sh.hashBase64(input)
        logger.info("Generated hash: ${hashB64}")

        // Assert
        assert hashB64 == EXPECTED_HASH_B64
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = [null, ""]

        final String EXPECTED_HASH_HEX = "8e5625a66b94ed9d31c1496d7f9ff49249cf05d6753b50ba0e2bf2a1108973dd"
        final String EXPECTED_HASH_B64 = "jlYlpmuU7Z0xwUltf5/0kknPBdZ1O1C6DivyoRCJc90="

        Argon2SecureHasher a2sh = new Argon2SecureHasher()

        def hexResults = []
        def b64Results = []

        // Act
        inputs.each { String input ->
            String hashHex = a2sh.hashHex(input)
            logger.info("Generated hash: ${hashHex}")
            hexResults << hashHex

            String hashB64 = a2sh.hashBase64(input)
            logger.info("Generated hash: ${hashB64}")
            b64Results << hashB64
        }

        // Assert
        assert hexResults.every { it == EXPECTED_HASH_HEX }
        assert b64Results.every { it == EXPECTED_HASH_B64 }
    }

    /**
     * This test can have the minimum time threshold updated to determine if the performance
     * is still sufficient compared to the existing threat model.
     */
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        Argon2SecureHasher a2sh = new Argon2SecureHasher()

        def results = []
        def resultDurations = []

        // Act
        testIterations.times { int i ->
            long startNanos = System.nanoTime()
            byte[] hash = a2sh.hashRaw(inputBytes)
            long endNanos = System.nanoTime()
            long durationNanos = endNanos - startNanos

            String hashHex = Hex.encode(hash)
            logger.info("Generated hash: ${hashHex} in ${durationNanos} ns")

            results << hashHex
            resultDurations << durationNanos
        }

        // Assert
        final long MIN_DURATION_NANOS = 5_000_000 // 5 ms
        assert resultDurations.min() > MIN_DURATION_NANOS
        assert resultDurations.sum() / testIterations > MIN_DURATION_NANOS
    }
}
