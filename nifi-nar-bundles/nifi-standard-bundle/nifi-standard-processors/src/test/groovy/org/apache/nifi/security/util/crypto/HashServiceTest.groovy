/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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


import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class HashServiceTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(HashServiceTest.class)

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

    // TODO: Test test vectors

    // TODO: Test various convenience methods

    // TODO: Test ASCII encoding is different from UTF-8

    // TODO: Test UTF-8 explicit vs. default charset

    // TODO: Test null algorithm and null value for each public method
    @Test
    void testShouldRejectNullAlgorithm() {
        // Arrange
        final String KNOWN_VALUE = "apachenifi"

        Closure threeArgString = { -> HashService.hashValue(null, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgString = { -> HashService.hashValue(null, KNOWN_VALUE) }
        Closure threeArgStringRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE, StandardCharsets.UTF_8) }
        Closure twoArgStringRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE) }
        Closure twoArgBytesRaw = { -> HashService.hashValueRaw(null, KNOWN_VALUE.bytes) }

        def scenarios = [threeArgString   : threeArgString,
                         twoArgString     : twoArgString,
                         threeArgStringRaw: threeArgStringRaw,
                         twoArgStringRaw  : twoArgStringRaw,
                         twoArgBytesRaw   : twoArgBytesRaw,
        ]

        // Act
        scenarios.each { String name, Closure closure ->
            def msg = shouldFail(IllegalArgumentException) {
                closure.call()
            }
            logger.expected("${name.padLeft(20)}: ${msg}")

            // Assert
            assert msg =~ "The hash algorithm cannot be null"
        }
    }

    @Test
    void testShouldRejectNullValue() {
        // Arrange
        final HashAlgorithm algorithm = HashAlgorithm.SHA256

        Closure threeArgString = { -> HashService.hashValue(algorithm, null, StandardCharsets.UTF_8) }
        Closure twoArgString = { -> HashService.hashValue(algorithm, null) }
        Closure threeArgStringRaw = { -> HashService.hashValueRaw(algorithm, null, StandardCharsets.UTF_8) }
        Closure twoArgStringRaw = { -> HashService.hashValueRaw(algorithm, null as String) }
        Closure twoArgBytesRaw = { -> HashService.hashValueRaw(algorithm, null as byte[]) }

        def scenarios = [threeArgString   : threeArgString,
                         twoArgString     : twoArgString,
                         threeArgStringRaw: threeArgStringRaw,
                         twoArgStringRaw  : twoArgStringRaw,
                         twoArgBytesRaw   : twoArgBytesRaw,
        ]

        // Act
        scenarios.each { String name, Closure closure ->
            def msg = shouldFail(IllegalArgumentException) {
                closure.call()
            }
            logger.expected("${name.padLeft(20)}: ${msg}")

            // Assert
            assert msg =~ "The value cannot be null"
        }
    }
}
