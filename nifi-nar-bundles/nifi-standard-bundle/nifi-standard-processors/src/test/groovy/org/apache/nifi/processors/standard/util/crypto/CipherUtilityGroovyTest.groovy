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
package org.apache.nifi.processors.standard.util.crypto

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
    private static final List<String> PBE_ALGORITHMS = EncryptionMethod.values().findAll { it.algorithm.startsWith("PBE") }*.algorithm
    private static final Map<String, List<String>> ALGORITHMS_MAPPED_BY_CIPHER = PBE_ALGORITHMS.groupBy { String algorithm -> CIPHERS.find { algorithm.contains(it) } }

    // Manually mapped as of 01/13/16 0.5.0
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
                    "PBEWITHSHAANDTWOFISH-CBC"],
            (192): ["PBEWITHMD5AND192BITAES-CBC-OPENSSL",
                    "PBEWITHSHA256AND192BITAES-CBC-BC",
                    "PBEWITHSHAAND192BITAES-CBC-BC"],
            (256): ["PBEWITHMD5AND256BITAES-CBC-OPENSSL",
                    "PBEWITHSHA256AND256BITAES-CBC-BC",
                    "PBEWITHSHAAND256BITAES-CBC-BC"]
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
    void testParseCipherFromAlgorithm() {
        // Arrange
        final def EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_CIPHER

        // Act
        PBE_ALGORITHMS.each { String algorithm ->
            String cipher = CipherUtility.parseCipherFromAlgorithm(algorithm)
            logger.info("Extracted ${cipher} from ${algorithm}")

            // Assert
            assert EXPECTED_ALGORITHMS.get(cipher).contains(algorithm)
        }
    }

    @Test
    void testParseKeyLengthFromAlgorithm() {
        // Arrange
        final def EXPECTED_ALGORITHMS = ALGORITHMS_MAPPED_BY_KEY_LENGTH

        // Act
        PBE_ALGORITHMS.each { String algorithm ->
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(algorithm)
            logger.info("Extracted ${keyLength} from ${algorithm}")

            // Assert
            assert EXPECTED_ALGORITHMS.get(keyLength).contains(algorithm)
        }
    }
}
