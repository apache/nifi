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

import java.nio.charset.StandardCharsets
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
    void testShouldHandleArbitrarySalt() {
        // Arrange
        int hashLength = 32
        int memory = 8
        int parallelism = 4
        int iterations = 4
        logger.info("Generating Argon2 hash for hash length: ${hashLength} B, mem: ${memory} KiB, parallelism: ${parallelism}, iterations: ${iterations}")

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        final String EXPECTED_HASH_HEX = "a73a471f51b2900901a00b81e770b9c1dfc595602bb7aec64cd27754a4174919"
        logger.info("Expected Hash Hex length: ${EXPECTED_HASH_HEX.length()}")
        final String EXPECTED_HASH_BASE64 = "pzpHH1GykAkBoAuB53C5wd/FlWArt67GTNJ3VKQXSRk"
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX)

        // Static salt instance
        Argon2SecureHasher staticSaltHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations)
        Argon2SecureHasher arbitrarySaltHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16)

        final byte[] STATIC_SALT = AbstractSecureHasher.STATIC_SALT
        final String DIFFERENT_STATIC_SALT = "Diff Static Salt"

        // Act
        byte[] staticSaltHash = staticSaltHasher.hashRaw(inputBytes)
        byte[] arbitrarySaltHash = arbitrarySaltHasher.hashRaw(inputBytes, STATIC_SALT)
        byte[] differentArbitrarySaltHash = arbitrarySaltHasher.hashRaw(inputBytes, DIFFERENT_STATIC_SALT.getBytes(StandardCharsets.UTF_8))
        byte[] differentSaltHash = arbitrarySaltHasher.hashRaw(inputBytes)

        String staticSaltHashHex = staticSaltHasher.hashHex(input)
        String arbitrarySaltHashHex = arbitrarySaltHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        String differentArbitrarySaltHashHex = arbitrarySaltHasher.hashHex(input, DIFFERENT_STATIC_SALT)
        String differentSaltHashHex = arbitrarySaltHasher.hashHex(input)

        String staticSaltHashBase64 = staticSaltHasher.hashBase64(input)
        String arbitrarySaltHashBase64 = arbitrarySaltHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        String differentArbitrarySaltHashBase64 = arbitrarySaltHasher.hashBase64(input, DIFFERENT_STATIC_SALT)
        String differentSaltHashBase64 = arbitrarySaltHasher.hashBase64(input)

        // Assert
        assert staticSaltHash == EXPECTED_HASH_BYTES
        assert arbitrarySaltHash == EXPECTED_HASH_BYTES
        assert differentArbitrarySaltHash != EXPECTED_HASH_BYTES
        assert differentSaltHash != EXPECTED_HASH_BYTES

        assert staticSaltHashHex == EXPECTED_HASH_HEX
        assert arbitrarySaltHashHex == EXPECTED_HASH_HEX
        assert differentArbitrarySaltHashHex != EXPECTED_HASH_HEX
        assert differentSaltHashHex != EXPECTED_HASH_HEX

        assert staticSaltHashBase64 == EXPECTED_HASH_BASE64
        assert arbitrarySaltHashBase64 == EXPECTED_HASH_BASE64
        assert differentArbitrarySaltHashBase64 != EXPECTED_HASH_BASE64
        assert differentSaltHashBase64 != EXPECTED_HASH_BASE64
    }

    @Test
    void testShouldValidateArbitrarySalt() {
        // Arrange
        int hashLength = 32
        int memory = 8
        int parallelism = 4
        int iterations = 4
        logger.info("Generating Argon2 hash for hash length: ${hashLength} B, mem: ${memory} KiB, parallelism: ${parallelism}, iterations: ${iterations}")

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        // Static salt instance
        Argon2SecureHasher secureHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 16)
        final byte[] STATIC_SALT = "bad_sal".bytes

        // Act
        def initializeMsg = shouldFail(IllegalArgumentException) {
            Argon2SecureHasher invalidSaltLengthHasher = new Argon2SecureHasher(hashLength, memory, parallelism, iterations, 7)
        }
        logger.expected(initializeMsg)

        def arbitrarySaltRawMsg = shouldFail {
            byte[] arbitrarySaltRaw = secureHasher.hashRaw(inputBytes, STATIC_SALT)
        }

        def arbitrarySaltHexMsg = shouldFail {
            byte[] arbitrarySaltHex = secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        }

        def arbitrarySaltBase64Msg = shouldFail {
            byte[] arbitraySaltBase64 = secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        }

        def results = [arbitrarySaltRawMsg, arbitrarySaltHexMsg, arbitrarySaltBase64Msg]

        // Assert
        assert results.every { it =~ /The salt length \(7 bytes\) is invalid/ }
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_HEX = "0c2920c52f28e0a2c77d006ec6138c8dc59580881468b85541cf886abdebcf18"

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3)

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

        final String EXPECTED_HASH_B64 = "DCkgxS8o4KLHfQBuxhOMjcWVgIgUaLhVQc+Iar3rzxg"

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3)

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
        final String EXPECTED_HASH_B64 = "jlYlpmuU7Z0xwUltf5/0kknPBdZ1O1C6DivyoRCJc90"

        Argon2SecureHasher a2sh = new Argon2SecureHasher(32, 4096, 1, 3)

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
    @Ignore("Long running test")
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100 //_000
        byte[] inputBytes = "This is a sensitive value".bytes

        Argon2SecureHasher a2sh = new Argon2SecureHasher(16, 2**16, 8, 5)

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

        def milliDurations = [resultDurations.min(), resultDurations.max(), resultDurations.sum()/resultDurations.size()].collect { it / 1_000_000 }
        logger.info("Min/Max/Avg durations in ms: ${milliDurations}")

        // Assert
        final long MIN_DURATION_NANOS = 500_000_000 // 500 ms
        assert resultDurations.min() > MIN_DURATION_NANOS
        assert resultDurations.sum() / testIterations > MIN_DURATION_NANOS
    }

    @Test
    void testShouldVerifyHashLengthBoundary() throws Exception {
        // Arrange
        final int hashLength = 128

        // Act
        boolean valid = Argon2SecureHasher.isHashLengthValid(hashLength)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailHashLengthBoundary() throws Exception {
        // Arrange
        def hashLengths = [-8, 0, 1, 2]

        // Act
        def results = hashLengths.collect { hashLength ->
            def isValid = Argon2SecureHasher.isHashLengthValid(hashLength)
            [hashLength, isValid]
        }

        // Assert
        results.each { hashLength, isHashLengthValid ->
            logger.info("For hashLength value ${hashLength}, hashLength is ${isHashLengthValid ? "valid" : "invalid"}")
            assert !isHashLengthValid
        }
    }

    @Test
    void testShouldVerifyMemorySizeBoundary() throws Exception {
        // Arrange
        final int memory = 2048

        // Act
        boolean valid = Argon2SecureHasher.isMemorySizeValid(memory)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailMemorySizeBoundary() throws Exception {
        // Arrange
        def memorySizes = [-12, 0, 1, 6]

        // Act
        def results = memorySizes.collect { memory ->
            def isValid = Argon2SecureHasher.isMemorySizeValid(memory)
            [memory, isValid]
        }

        // Assert
        results.each { memory, isMemorySizeValid ->
            logger.info("For memory size ${memory}, memory is ${isMemorySizeValid ? "valid" : "invalid"}")
            assert !isMemorySizeValid
        }
    }

    @Test
    void testShouldVerifyParallelismBoundary() throws Exception {
        // Arrange
        final int parallelism = 4

        // Act
        boolean valid = Argon2SecureHasher.isParallelismValid(parallelism)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailParallelismBoundary() throws Exception {
        // Arrange
        def parallelisms = [-8, 0, 16777220, 16778000]

        // Act
        def results = parallelisms.collect { parallelism ->
            def isValid = Argon2SecureHasher.isParallelismValid(parallelism)
            [parallelism, isValid]
        }

        // Assert
        results.each { parallelism, isParallelismValid ->
            logger.info("For parallelization factor ${parallelism}, parallelism is ${isParallelismValid ? "valid" : "invalid"}")
            assert !isParallelismValid
        }
    }

    @Test
    void testShouldVerifyIterationsBoundary() throws Exception {
        // Arrange
        final int iterations = 4

        // Act
        boolean valid = Argon2SecureHasher.isIterationsValid(iterations)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailIterationsBoundary() throws Exception {
        // Arrange
        def iterationCounts = [-50, -1, 0]

        // Act
        def results = iterationCounts.collect { iterations ->
            def isValid = Argon2SecureHasher.isIterationsValid(iterations)
            [iterations, isValid]
        }

        // Assert
        results.each { iterations, isIterationsValid ->
            logger.info("For iteration counts ${iterations}, iteration is ${isIterationsValid ? "valid" : "invalid"}")
            assert !isIterationsValid
        }
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [0, 64]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new Argon2SecureHasher().isSaltLengthValid(saltLength)
            [saltLength, isValid]
        }

        // Assert
        results.each { saltLength, isSaltLengthValid ->
            logger.info("For salt length ${saltLength}, saltLength is ${isSaltLengthValid ? "valid" : "invalid"}")
            assert isSaltLengthValid
        }
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [-16, 4]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new Argon2SecureHasher().isSaltLengthValid(saltLength)
            [saltLength, isValid]
        }

        // Assert
        results.each { saltLength, isSaltLengthValid ->
            logger.info("For salt length ${saltLength}, saltLength is ${isSaltLengthValid ? "valid" : "invalid"}")
            assert !isSaltLengthValid
        }
    }

    @Test
    void testShouldCreateHashOfDesiredLength() throws Exception {
        // Arrange
        def hashLengths = [16, 32]

        final String PASSWORD = "password"
        final byte[] SALT = [0x00] * 16
        final byte[] EXPECTED_HASH = Hex.decode("411c9c87e7c91d8c8eacc418665bd2e1")

        // Act
        Map<Integer, byte[]> results = hashLengths.collectEntries { hashLength ->
            Argon2SecureHasher ash = new Argon2SecureHasher(hashLength, 8, 1, 3)
            def hash = ash.hashRaw(PASSWORD.bytes, SALT)
            logger.info("Hashed password ${PASSWORD} with salt ${Hex.encode(SALT)} to ${Hex.encode(hash)}".toString())
            [hashLength, hash]
        }

        // Assert
        assert results[16][0..15] != results[32][0..15]
        // Demonstrates that internal hash truncation is not supported
//        assert results.every { int k, byte[] v -> v[0..15] as byte[] == EXPECTED_HASH}
    }
}
