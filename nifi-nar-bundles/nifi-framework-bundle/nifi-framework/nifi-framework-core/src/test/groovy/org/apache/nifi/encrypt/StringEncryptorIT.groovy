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
package org.apache.nifi.encrypt


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
class StringEncryptorIT {
    private static final Logger logger = LoggerFactory.getLogger(StringEncryptorIT.class)

    private static final String DEFAULT_PROVIDER = "BC"

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

    private static long time(Closure code) {
        long startNanos = System.nanoTime()
        code.run()
        long stopNanos = System.nanoTime()
        return stopNanos - startNanos
    }

    /**
     * Checks the custom algorithm (Argon2+AES-G/CM) doesn't derive the key (a slow process) on every encrypt/decrypt operation.
     *
     * @throws Exception
     */
    @Test
    void testCustomAlgorithmShouldOnlyDeriveKeyOnce() throws Exception {
        // Arrange
        final String CUSTOM_ALGORITHM = "NIFI_ARGON2_AES_GCM_256"
        final String PASSWORD = "nifiPassword123"
        final String plaintext = "some sensitive flow value"

        int testIterations = 100 //_000

        def results = []
        def resultDurations = []

        StringEncryptor encryptor
        long createNanos = time {
            encryptor = StringEncryptor.createEncryptor(CUSTOM_ALGORITHM, DEFAULT_PROVIDER, PASSWORD)
        }
        logger.info("Created encryptor: ${encryptor} in ${createNanos / 1_000_000} ms")

        // Prime the cipher with one encrypt/decrypt cycle
        String primeCT = encryptor.encrypt(plaintext)
        encryptor.decrypt(primeCT)

        // Act
        testIterations.times { int i ->
            // Encrypt the value
            def ciphertext
            long encryptNanos = time {
                ciphertext = encryptor.encrypt(plaintext)
            }
            logger.info("Encrypted plaintext in ${encryptNanos / 1_000_000} ms")

            def recovered
            long durationNanos = time {
                recovered = encryptor.decrypt(ciphertext)
            }
            logger.info("Recovered ciphertext to ${recovered} in ${durationNanos / 1_000_000} ms")

            results << recovered
            resultDurations << encryptNanos
            resultDurations << durationNanos
        }

        def milliDurations = [resultDurations.min(), resultDurations.max(), resultDurations.sum() / resultDurations.size()].collect { it / 1_000_000 }
        logger.info("Min/Max/Avg durations in ms: ${milliDurations}")
    }
}
