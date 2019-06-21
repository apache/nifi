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
package org.apache.nifi.properties.sensitive.aes


import org.apache.nifi.properties.sensitive.SensitivePropertyMetadata
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.security.Security

@RunWith(JUnit4.class)
class AESSensitivePropertyMetadataTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyMetadataTest.class)

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

    @AfterClass
    static void tearDownOnce() {

    }

    /**
     * Returns the list of acceptable key lengths in bits based on the current JCE policies.
     *
     * @return 128 , [192, 256]
     */
    static List<Integer> getValidKeyLengths() {
        Cipher.getMaxAllowedKeyLength("AES") > 128 ? [128, 192, 256] : [128]
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange
        String algorithmAndMode = "aes/gcm"
        List keyLengths = getValidKeyLengths()
        logger.info("Detected valid key lengths: ${keyLengths}")

        // Act
       keyLengths.each { int kl ->
           SensitivePropertyMetadata spm = new AESSensitivePropertyMetadata(algorithmAndMode, kl)
           logger.info("Created ${spm}")

           // Assert
           assert spm instanceof AESSensitivePropertyMetadata
           assert spm.algorithmAndMode == algorithmAndMode
           assert spm.keyLength == kl
           assert spm.identifier == "${algorithmAndMode}/${kl}"
       }
    }

    @Test
    void testFromIdentifierShouldParseValidIdentifiers() throws Exception {
        // Arrange

        // Candidate for generative testing
        List algorithmAndMode = ["aes/gcm", "AES/GCM", "aEs/GcM"]
        List keyLengths = getValidKeyLengths()
        logger.info("Detected valid key lengths: ${keyLengths}")

        // Act
        keyLengths.each { int kl ->
            algorithmAndMode.each { String alg ->
                SensitivePropertyMetadata spm = AESSensitivePropertyMetadata.fromIdentifier("${alg}/${kl}")
                logger.info("Created ${spm}")

                // Assert
                assert spm instanceof AESSensitivePropertyMetadata
                assert spm.algorithmAndMode == "aes/gcm"
                assert spm.keyLength == kl
                assert spm.identifier == "aes/gcm/${kl}"
            }
        }
    }

    @Test
    void testFromIdentifierShouldBlockInvalidAlgorithms() throws Exception {
        // Arrange
        String algorithmAndMode = "aes/gcm".reverse()
        List keyLengths = getValidKeyLengths()
        logger.info("Detected valid key lengths: ${keyLengths}")

        // Act
        keyLengths.each { int kl ->
            def msg = shouldFail(SensitivePropertyProtectionException) {
                SensitivePropertyMetadata spm = AESSensitivePropertyMetadata.fromIdentifier("${algorithmAndMode}/${kl}")
                logger.info("Created ${spm}")
            }
            logger.expected(msg)

            // Assert
            assert msg =~ "The only algorithm and mode supported is aes/gcm"
        }
    }

    @Test
    void testFromIdentifierShouldBlockInvalidIdentifiers() throws Exception {
        // Arrange
        String algorithmAndMode = "aes_gcm_"
        List keyLengths = getValidKeyLengths()
        logger.info("Detected valid key lengths: ${keyLengths}")

        // Act
        keyLengths.each { int kl ->
            def msg = shouldFail(SensitivePropertyProtectionException) {
                SensitivePropertyMetadata spm = AESSensitivePropertyMetadata.fromIdentifier("${algorithmAndMode}${kl}")
                logger.info("Created ${spm}")
            }
            logger.expected(msg)

            // Assert
            assert msg =~ "Could not parse \\w+ to algorithm & mode and key length"
        }
    }

    @Test
    void testFromIdentifierShouldBlockInvalidKeyLengths() throws Exception {
        // Arrange
        Assume.assumeFalse("This test should only run on limited strength installs", validKeyLengths.contains(256))

        String algorithmAndMode = "aes/gcm"
        List invalidKeyLengths = [192, 256]
        logger.info("Invalid key lengths: ${invalidKeyLengths}")

        // Act
        invalidKeyLengths.each { int kl ->
            def msg = shouldFail(SensitivePropertyProtectionException) {
                SensitivePropertyMetadata spm = AESSensitivePropertyMetadata.fromIdentifier("${algorithmAndMode}/${kl}")
                logger.info("Created ${spm}")
            }
            logger.expected(msg)

            // Assert
            assert msg =~ "Key length \\d+ not valid \\[128\\]"
        }
    }
}
