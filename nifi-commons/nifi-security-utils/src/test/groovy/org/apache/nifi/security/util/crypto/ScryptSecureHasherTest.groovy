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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class ScryptSecureHasherTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ScryptSecureHasherTest)

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
        int n = 1024
        int r = 8
        int p = 2
        int dkLength = 32
        logger.info("Generating Scrypt hash for iterations: ${n}, mem: ${r} B, parallelism: ${p}, desired key length: ${dkLength}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "a67fd2f4b3aa577b8ecdb682e60b4451a84611dcbbc534bce17616056ef8965d"

        ScryptSecureHasher scryptSH = new ScryptSecureHasher(n, r, p, dkLength)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = scryptSH.hashRaw(inputBytes)
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
        int n = 1024
        int r = 8
        int p = 2
        int dkLength = 128
        logger.info("Generating Scrypt hash for iterations: ${n}, mem: ${r} B, parallelism: ${p}, desired key length: ${dkLength}")

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "a67fd2f4b3aa577b8ecdb682e60b4451"

        ScryptSecureHasher scryptSH = new ScryptSecureHasher(n, r, p, dkLength, 16)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = scryptSH.hashRaw(inputBytes)
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

        final String EXPECTED_HASH_HEX = "6a9c827815fe0718af5e336811fc78dd719c8d9505e015283239b9bf1d24ee71"

        SecureHasher scryptSH = new ScryptSecureHasher()

        // Act
        String hashHex = scryptSH.hashHex(input)
        logger.info("Generated hash: ${hashHex}")

        // Assert
        assert hashHex == EXPECTED_HASH_HEX
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_BASE64 = "apyCeBX+BxivXjNoEfx43XGcjZUF4BUoMjm5vx0k7nE="

        SecureHasher scryptSH = new ScryptSecureHasher()

        // Act
        String hashB64 = scryptSH.hashBase64(input)
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

        ScryptSecureHasher scryptSH = new ScryptSecureHasher()

        def hexResults = []
        def B64Results = []

        // Act
        inputs.each { String input ->
            String hashHex = scryptSH.hashHex(input)
            logger.info("Generated hex-encoded hash: ${hashHex}")
            hexResults << hashHex

            String hashB64 = scryptSH.hashBase64(input)
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
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100
        byte[] inputBytes = "This is a sensitive value".bytes

        ScryptSecureHasher scryptSH = new ScryptSecureHasher()

        def results = []
        def resultDurations = []

        // Act
        testIterations.times { int i ->
            long startNanos = System.nanoTime()
            byte[] hash = scryptSH.hashRaw(inputBytes)
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
    void testShouldVerifyRBoundary() throws Exception {
        // Arrange
        final int r = 32

        // Act
        boolean valid = ScryptSecureHasher.isRValid(r)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailRBoundary() throws Exception {
        // Arrange
        def rValues = [-8, 0, 2147483647]

        // Act
        def results = rValues.collect { rValue ->
            def isValid = ScryptSecureHasher.isRValid(rValue)
            [rValue, isValid]
        }

        // Assert
        results.each { rValue, isRValid ->
            logger.info("For R value ${rValue}, R is ${isRValid ? "valid" : "invalid"}")
            assert !isRValid
        }
    }

    @Test
    void testShouldVerifyNBoundary() throws Exception {
        // Arrange
        final Integer n = 16385
        final int r = 8

        // Act
        boolean valid = ScryptSecureHasher.isNValid(n, r)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailNBoundary() throws Exception {
        // Arrange
        Map costParameters = [(-8): 8, 0: 32]

        // Act
        def results = costParameters.collect { n, p ->
            def isValid = ScryptSecureHasher.isNValid(n, p)
            [n, isValid]
        }

        // Assert
        results.each { n, isNValid ->
            logger.info("For N value ${n}, N is ${isNValid ? "valid" : "invalid"}")
            assert !isNValid
        }
    }

    @Test
    void testShouldVerifyPBoundary() throws Exception {
        // Arrange
        final List<Integer> ps = [1, 8, 1024]
        final List<Integer> rs = [8, 1024, 4096]

        // Act
        def pResults = ps.collectEntries { int p ->
            def rResults = rs.collectEntries { int r ->
                boolean valid = ScryptSecureHasher.isPValid(p, r)
                logger.info("p ${p} is valid for r ${r}: ${valid}")
                [r, valid]
            }
            [p, rResults]
        }

        // Assert
        pResults.each { p, rResult ->
            assert rResult.every { r, isValid -> isValid }
        }
    }

    @Test
    void testShouldFailIfPBoundaryExceeded() throws Exception {
        // Arrange
        final List<Integer> ps = [4096 * 64, 1024 * 1024]
        final List<Integer> rs = [4096, 1024 * 1024]

        // Act
        def pResults = ps.collectEntries { int p ->
            def rResults = rs.collectEntries { int r ->
                boolean valid = ScryptSecureHasher.isPValid(p, r)
                logger.info("p ${p} is valid for r ${r}: ${valid}")
                [r, valid]
            }
            [p, rResults]
        }

        // Assert
        pResults.each { p, rResult ->
            assert rResult.every { r, isValid -> !isValid }
        }
    }

    @Test
    void testShouldVerifyDKLengthBoundary() throws Exception {
        // Arrange
        final Integer dkLength = 64

        // Act
        boolean valid = ScryptSecureHasher.isDKLengthValid(dkLength)

        // Assert
        assert valid
    }

    @Test
    void testShouldFailDKLengthBoundary() throws Exception {
        // Arrange
        def dKLengths = [-8, 0, 2147483647]

        // Act
        def results = dKLengths.collect { dKLength ->
            def isValid = ScryptSecureHasher.isDKLengthValid(dKLength)
            [dKLength, isValid]
        }

        // Assert
        results.each { dKLength, isDKLengthValid ->
            logger.info("For Desired Key Length value ${dKLength}, dkLength is ${isDKLengthValid ? "valid" : "invalid"}")
            assert !isDKLengthValid
        }
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [0, 64]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = ScryptSecureHasher.isSaltLengthValid(saltLength)
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
        def saltLengths = [-8, 1, 2147483647]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = ScryptSecureHasher.isSaltLengthValid(saltLength)
            [saltLength, isValid]
        }

        // Assert
        results.each { saltLength, isSaltLengthValid ->
            logger.info("For Salt Length value ${saltLength}, saltLength is ${isSaltLengthValid ? "valid" : "invalid"}")
            assert !isSaltLengthValid
        }
    }

}
