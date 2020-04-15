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

import at.favre.lib.crypto.bcrypt.Radix64Encoder
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
class BcryptSecureHasherTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(BcryptSecureHasher)

    @BeforeClass
    static void setupOnce() throws Exception {
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

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int cost = 4
        logger.info("Generating Bcrypt hash for cost factor: ${cost}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "24326124303424526b6a4559512f526245447959554b6553304471622e596b4c5331655a2e6c61586550484c69464d783937564c566d47354250454f"

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher(cost)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = bcryptSH.hashRaw(inputBytes)
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
        int cost = 4
        int saltLength = 16
        logger.info("Generating Bcrypt hash for cost factor: ${cost}, salt length: ${saltLength}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "24326124303424546d6c47615342546447463061574d6755324673642e38675a347a6149356d6b4d50594c542e344e68337962455a4678384b676a75"

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher(cost, saltLength)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = bcryptSH.hashRaw(inputBytes)
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
        int cost = 4
        logger.info("Generating Bcrypt hash for cost factor: ${cost}")

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        final String EXPECTED_HASH_HEX = "24326124303424526b6a4559512f526245447959554b6553304471622e596b4c5331655a2e6c61586550484c69464d783937564c566d47354250454f"
        final String EXPECTED_HASH_BASE64 = "JDJhJDA0JFJrakVZUS9SYkVEeVlVS2VTMERxYi5Za0xTMWVaLmxhWGVQSExpRk14OTdWTFZtRzVCUEVP"
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX)

        // Static salt instance
        BcryptSecureHasher staticSaltHasher = new BcryptSecureHasher(cost)
        BcryptSecureHasher arbitrarySaltHasher = new BcryptSecureHasher(cost, 16)

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
        int cost = 4
        logger.info("Generating Bcrypt hash for cost factor: ${cost}")

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        // Static salt instance
        BcryptSecureHasher secureHasher = new BcryptSecureHasher(cost, 16)
        final byte[] STATIC_SALT = "bad_sal".bytes

        // Act
        def initializationMsg = shouldFail(IllegalArgumentException) {
            BcryptSecureHasher invalidSaltLengthHasher = new BcryptSecureHasher(cost, 7)
        }
        logger.expected(initializationMsg)

        def arbitrarySaltRawMsg = shouldFail {
            byte[] arbitrarySaltHash = secureHasher.hashRaw(inputBytes, STATIC_SALT)
        }

        def arbitrarySaltHexMsg = shouldFail {
            byte[] arbitrarySaltHashHex = secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        }

        def arbitrarySaltBase64Msg = shouldFail {
            byte[] arbitrarySaltBase64 = secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8))
        }

        def results = [arbitrarySaltRawMsg, arbitrarySaltHexMsg, arbitrarySaltBase64Msg]

        // Assert
        assert results.every { it =~ /The salt length \(7 bytes\) is invalid/ }
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_HEX = "24326124313224526b6a4559512f526245447959554b6553304471622e5852696135344d4e356c5a44515243575874516c4c696d476669635a776871"

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher()

        // Act
        String hashHex = bcryptSH.hashHex(input)
        logger.info("Generated hash: ${hashHex}")

        // Assert
        assert hashHex == EXPECTED_HASH_HEX
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_BASE64 = "JDJhJDEyJFJrakVZUS9SYkVEeVlVS2VTMERxYi5YUmlhNTRNTjVsWkRRUkNXWHRRbExpbUdmaWNad2hx"

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher()

        // Act
        String hashB64 = bcryptSH.hashBase64(input)
        logger.info("Generated hash: ${hashB64}")

        // Assert
        assert hashB64 == EXPECTED_HASH_BASE64
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = [null, ""]

        final String EXPECTED_HASH_HEX = ""
        final String EXPECTED_HASH_BASE64 = ""

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher()

        def hexResults = []
        def B64Results = []

        // Act
        inputs.each { String input ->
            String hashHex = bcryptSH.hashHex(input)
            logger.info("Generated hex-encoded hash: ${hashHex}")
            hexResults << hashHex

            String hashB64 = bcryptSH.hashBase64(input)
            logger.info("Generated B64-encoded hash: ${hashB64}")
            B64Results << hashB64
        }

        // Assert
        assert hexResults.every { it == EXPECTED_HASH_HEX }
        assert B64Results.every { it == EXPECTED_HASH_BASE64 }
    }

    /**
     * This test can have the minimum time threshold updated to determine if the performance
     * is still sufficient compared to the existing threat model.
     */
    @Ignore("Long running test")
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100
        byte[] inputBytes = "This is a sensitive value".bytes

        BcryptSecureHasher bcryptSH = new BcryptSecureHasher()

        def results = []
        def resultDurations = []

        // Act
        testIterations.times { int i ->
            long startNanos = System.nanoTime()
            byte[] hash = bcryptSH.hashRaw(inputBytes)
            long endNanos = System.nanoTime()
            long durationNanos = endNanos - startNanos

            String hashHex = Hex.encode(hash)
            logger.info("Generated hash: ${hashHex} in ${durationNanos} ns")

            results << hashHex
            resultDurations << durationNanos
        }

        // Assert
        final long MIN_DURATION_NANOS = 75_000_000 // 75 ms
        assert resultDurations.min() > MIN_DURATION_NANOS
        assert resultDurations.sum() / testIterations > MIN_DURATION_NANOS
    }

    @Test
    void testShouldVerifyCostBoundary() throws Exception {
        // Arrange
        final int cost = 14

        // Act
        boolean valid = BcryptSecureHasher.isCostValid(cost)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailCostBoundary() throws Exception {
        // Arrange
        def costFactors = [-8, 0, 40]

        // Act
        def results = costFactors.collect { costFactor ->
            def isValid = BcryptSecureHasher.isCostValid(costFactor)
            [costFactor, isValid]
        }

        // Assert
        results.each { costFactor, isCostValid ->
            logger.info("For cost factor ${costFactor}, cost is ${isCostValid ? "valid" : "invalid"}")
            assert !isCostValid
        }
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [0, 16]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new BcryptSecureHasher().isSaltLengthValid(saltLength)
            [saltLength, isValid]
        }

        // Assert
        results.each { saltLength, isSaltLengthValid ->
            assert { it == isSaltLengthValid }
        }
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [-8, 1]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new BcryptSecureHasher().isSaltLengthValid(saltLength)
            [saltLength, isValid]
        }

        // Assert
        results.each { saltLength, isSaltLengthValid ->
            logger.info("For Salt Length value ${saltLength}, saltLength is ${isSaltLengthValid ? "valid" : "invalid"}")
            assert !isSaltLengthValid
        }
    }

    @Test
    void testShouldConvertRadix64ToBase64() {
        // Arrange
        final String INPUT_RADIX_64 = "mm7MiKjvXVYCujVUlKRKiu"
        final byte[] EXPECTED_BYTES = new Radix64Encoder.Default().decode(INPUT_RADIX_64.bytes)
        logger.info("Plain bytes: ${Hex.encode(EXPECTED_BYTES)}")

        // Uses standard Base64 library but removes padding chars
        final String EXPECTED_MIME_B64 = Base64.encoder.encodeToString(EXPECTED_BYTES).replaceAll(/=/, '')

        // Act
        String convertedBase64 = BcryptSecureHasher.convertBcryptRadix64ToMimeBase64(INPUT_RADIX_64)
        logger.info("Converted (R64) ${INPUT_RADIX_64} to (B64) ${convertedBase64}")

        String convertedRadix64 = BcryptSecureHasher.convertMimeBase64ToBcryptRadix64(convertedBase64)
        logger.info("Converted (B64) ${convertedBase64} to (R64) ${convertedRadix64}")

        // Assert
        assert convertedBase64 == EXPECTED_MIME_B64
        assert convertedRadix64 == INPUT_RADIX_64
    }

    @Test
    void testConvertRadix64ToBase64ShouldHandlePeriod() {
        // Arrange
        final String INPUT_RADIX_64 = "75x373yP7atxMD3pVgsdO."
        final byte[] EXPECTED_BYTES = new Radix64Encoder.Default().decode(INPUT_RADIX_64.bytes)
        logger.info("Plain bytes: ${Hex.encode(EXPECTED_BYTES)}")

        // Uses standard Base64 library but removes padding chars
        final String EXPECTED_MIME_B64 = Base64.encoder.encodeToString(EXPECTED_BYTES).replaceAll(/=/, '')

        // Act
        String convertedBase64 = BcryptSecureHasher.convertBcryptRadix64ToMimeBase64(INPUT_RADIX_64)
        logger.info("Converted (R64) ${INPUT_RADIX_64} to (B64) ${convertedBase64}")

        String convertedRadix64 = BcryptSecureHasher.convertMimeBase64ToBcryptRadix64(convertedBase64)
        logger.info("Converted (B64) ${convertedBase64} to (R64) ${convertedRadix64}")

        // Assert
        assert convertedBase64 == EXPECTED_MIME_B64
        assert convertedRadix64 == INPUT_RADIX_64
    }
}

