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

import org.bouncycastle.util.encoders.Hex
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

import java.nio.charset.StandardCharsets

import static org.junit.jupiter.api.Assertions.assertThrows

class PBKDF2SecureHasherTest {

    @Test
    void testShouldBeDeterministicWithStaticSalt() {
        // Arrange
        int cost = 10_000
        int dkLength = 32

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511"

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher(cost, dkLength)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes)
            String hashHex = new String(Hex.encode(hash))
            results << hashHex
        }

        // Assert
        assert results.every { it == EXPECTED_HASH_HEX }
    }

    @Test
    void testShouldBeDifferentWithRandomSalt() {
        // Arrange
        String prf = "SHA512"
        int cost = 10_000
        int saltLength = 16
        int dkLength = 32

        int testIterations = 10
        byte[] inputBytes = "This is a sensitive value".bytes

        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511"

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength)

        def results = []

        // Act
        testIterations.times { int i ->
            byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes)
            String hashHex = Hex.encode(hash)
            results << hashHex
        }

        // Assert
        assert results.unique().size() == results.size()
        assert results.every { it != EXPECTED_HASH_HEX }
    }

    @Test
    void testShouldHandleArbitrarySalt() {
        // Arrange
        String prf = "SHA512"
        int cost = 10_000
        int saltLength = 16
        int dkLength = 32

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        final String EXPECTED_HASH_HEX = "2c47a6d801b71e087f94792079c40880aea29013bfffd0ab94b1bc112ea52511"
        final String EXPECTED_HASH_BASE64 = "LEem2AG3Hgh/lHkgecQIgK6ikBO//9CrlLG8ES6lJRE"
        final byte[] EXPECTED_HASH_BYTES = Hex.decode(EXPECTED_HASH_HEX)

        PBKDF2SecureHasher staticSaltHasher = new PBKDF2SecureHasher(cost, dkLength)
        PBKDF2SecureHasher arbitrarySaltHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength)

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
        // Assert
        String prf = "SHA512"
        int cost = 10_000
        int saltLength = 16
        int dkLength = 32

        def input = "This is a sensitive value"
        byte[] inputBytes = input.bytes

        // Static salt instance
        PBKDF2SecureHasher secureHasher = new PBKDF2SecureHasher(prf, cost, saltLength, dkLength)
        byte[] STATIC_SALT = "bad_sal".bytes

        assertThrows(IllegalArgumentException.class, { -> new PBKDF2SecureHasher(prf, cost, 7, dkLength) })
        assertThrows(RuntimeException.class, { -> secureHasher.hashRaw(inputBytes, STATIC_SALT) })
        assertThrows(RuntimeException.class, { -> secureHasher.hashHex(input, new String(STATIC_SALT, StandardCharsets.UTF_8)) })
        assertThrows(RuntimeException.class, { -> secureHasher.hashBase64(input, new String(STATIC_SALT, StandardCharsets.UTF_8)) })
    }

    @Test
    void testShouldFormatHex() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_HEX = "8f67110e87d225366e2d79ad251d2cf48f8cb15845800452e0e2cff09f95ef1c"

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher()

        // Act
        String hashHex = pbkdf2SecureHasher.hashHex(input)

        // Assert
        assert hashHex == EXPECTED_HASH_HEX
    }

    @Test
    void testShouldFormatBase64() {
        // Arrange
        String input = "This is a sensitive value"

        final String EXPECTED_HASH_BASE64 = "j2cRDofSJTZuLXmtJR0s9I+MsVhFgARS4OLP8J+V7xw"

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher()

        // Act
        String hashB64 = pbkdf2SecureHasher.hashBase64(input)

        // Assert
        assert hashB64 == EXPECTED_HASH_BASE64
    }

    @Test
    void testShouldHandleNullInput() {
        // Arrange
        List<String> inputs = [null, ""]

        final String EXPECTED_HASH_HEX = "7f2d8d8c7aaa45471f6c05a8edfe0a3f75fe01478cc965c5dce664e2ac6f5d0a"
        final String EXPECTED_HASH_BASE64 = "fy2NjHqqRUcfbAWo7f4KP3X+AUeMyWXF3OZk4qxvXQo"

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher()

        def hexResults = []
        def B64Results = []

        // Act
        inputs.each { String input ->
            String hashHex = pbkdf2SecureHasher.hashHex(input)
            hexResults << hashHex

            String hashB64 = pbkdf2SecureHasher.hashBase64(input)
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
    @EnabledIfSystemProperty(named = "nifi.test.performance", matches = "true")
    @Test
    void testDefaultCostParamsShouldBeSufficient() {
        // Arrange
        int testIterations = 100
        byte[] inputBytes = "This is a sensitive value".bytes

        PBKDF2SecureHasher pbkdf2SecureHasher = new PBKDF2SecureHasher()

        def results = []
        def resultDurations = []

        // Act
        testIterations.times { int i ->
            long startNanos = System.nanoTime()
            byte[] hash = pbkdf2SecureHasher.hashRaw(inputBytes)
            long endNanos = System.nanoTime()
            long durationNanos = endNanos - startNanos

            String hashHex = Hex.encode(hash)

            results << hashHex
            resultDurations << durationNanos
        }

        // Assert
        final long MIN_DURATION_NANOS = 75_000_000 // 75 ms
        assert resultDurations.min() > MIN_DURATION_NANOS
        assert resultDurations.sum() / testIterations > MIN_DURATION_NANOS
    }

    @Test
    void testShouldVerifyIterationCountBoundary() throws Exception {
        // Arrange
        def validIterationCounts = [1, 1000, 1_000_000]

        // Act
        def results = validIterationCounts.collect { int i ->
            boolean valid = PBKDF2SecureHasher.isIterationCountValid(i)
            valid
        }

        // Assert
        assert results.every()
    }

    @Test
    void testShouldFailIterationCountBoundary() throws Exception {
        // Arrange
        def invalidIterationCounts = [-1, 0, Integer.MAX_VALUE + 1]

        // Act
        def results = invalidIterationCounts.collect { i ->
            boolean valid = PBKDF2SecureHasher.isIterationCountValid(i)
            valid
        }

        // Assert
        results.each { valid ->
            assert !valid
        }
    }

    @Test
    void testShouldVerifyDKLengthBoundary() throws Exception {
        // Arrange
        def validHLengths = [32, 64]

        // 1 and MAX_VALUE are the length boundaries, inclusive
        def validDKLengths = [1, 1000, 1_000_000, Integer.MAX_VALUE]

        // Act
        def results = validHLengths.collectEntries { int hLen ->
            def dkResults = validDKLengths.collect { int dkLength ->
                boolean valid = PBKDF2SecureHasher.isDKLengthValid(hLen, dkLength)
                valid
            }
            [hLen, dkResults]
        }

        // Assert
        results.each { int hLen, def dkResults ->
            assert dkResults.every()
        }
    }

    @Test
    void testShouldFailDKLengthBoundary() throws Exception {
        // Arrange
        def validHLengths = [32, 64]

        // MAX_VALUE + 1 will become MIN_VALUE because of signed integer math
        def invalidDKLengths = [-1, 0, Integer.MAX_VALUE + 1, new Integer(Integer.MAX_VALUE * 2 - 1)]

        // Act
        def results = validHLengths.collectEntries { int hLen ->
            def dkResults = invalidDKLengths.collect { int dkLength ->
                boolean valid = PBKDF2SecureHasher.isDKLengthValid(hLen, dkLength)
                valid
            }
            [hLen, dkResults]
        }

        // Assert
        results.each { int hLen, def dkResults ->
            assert dkResults.every { boolean valid -> !valid }
        }
    }

    @Test
    void testShouldVerifySaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [0, 16, 64]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new PBKDF2SecureHasher().isSaltLengthValid(saltLength)
            isValid
        }

        // Assert
        assert results.every()
    }

    @Test
    void testShouldFailSaltLengthBoundary() throws Exception {
        // Arrange
        def saltLengths = [-8, 1, Integer.MAX_VALUE + 1]

        // Act
        def results = saltLengths.collect { saltLength ->
            def isValid = new PBKDF2SecureHasher().isSaltLengthValid(saltLength)
            isValid
        }

        // Assert
        results.each { assert !it }
    }
}
