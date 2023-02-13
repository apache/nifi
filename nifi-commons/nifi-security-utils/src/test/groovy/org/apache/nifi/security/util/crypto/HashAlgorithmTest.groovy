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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class HashAlgorithmTest {
    private static final Logger logger = LoggerFactory.getLogger(HashAlgorithmTest.class)


    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Test
    void testDetermineBrokenAlgorithms() throws Exception {
        // Arrange
        def algorithms = HashAlgorithm.values()

        // Act
        def brokenAlgorithms = algorithms.findAll { !it.isStrongAlgorithm() }
        logger.info("Broken algorithms: ${brokenAlgorithms}")

        // Assert
        assertEquals([HashAlgorithm.MD2, HashAlgorithm.MD5, HashAlgorithm.SHA1], brokenAlgorithms)
    }

    @Test
    void testShouldBuildAllowableValueDescription() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        // Act
        def descriptions = algorithms.collect { HashAlgorithm algorithm ->
            algorithm.buildAllowableValueDescription()
        }

        // Assert
        descriptions.forEach(description -> assertTrue((description =~ /.* \(\d+ byte output\).*/).find()) )

        descriptions.stream()
        .filter(description -> (description =~ "MD2|MD5|SHA-1").find() )
        .forEach(description -> assertTrue(description.contains("WARNING")))
    }

    @Test
    void testDetermineBlake2Algorithms() {
        def algorithms = HashAlgorithm.values()

        // Act
        def blake2Algorithms = algorithms.findAll { it.isBlake2() }
        logger.info("Blake2 algorithms: ${blake2Algorithms}")

        // Assert
        assertEquals([HashAlgorithm.BLAKE2_160, HashAlgorithm.BLAKE2_256, HashAlgorithm.BLAKE2_384, HashAlgorithm.BLAKE2_512], blake2Algorithms)
    }

    @Test
    void testShouldMatchAlgorithmByName() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        // Act
        algorithms.each { HashAlgorithm algorithm ->
            def transformedNames = [algorithm.name, algorithm.name.toUpperCase(), algorithm.name.toLowerCase()]
            logger.info("Trying with names: ${transformedNames}")

            transformedNames.each { String name ->
                HashAlgorithm found = HashAlgorithm.fromName(name)

                // Assert
                assertEquals(name.toUpperCase(), found.name)
            }
        }
    }
}
