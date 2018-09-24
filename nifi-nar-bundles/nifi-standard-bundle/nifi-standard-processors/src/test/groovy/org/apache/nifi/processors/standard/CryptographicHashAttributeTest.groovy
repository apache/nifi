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
package org.apache.nifi.processors.standard


import org.apache.nifi.security.util.crypto.HashAlgorithm
import org.apache.nifi.security.util.crypto.HashService
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class CryptographicHashAttributeTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CryptographicHashAttributeTest.class)


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

    @Test
    void testShouldCalculateHashOfPresentAttribute() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes for username and date
        def attributes = [
                username: "alopresto",
                date    : new Date().format("YYYY-MM-dd HH:mm:ss.SSS Z")
        ]
        def attributeKeys = attributes.keySet()

        algorithms.each { HashAlgorithm algorithm ->
            final EXPECTED_USERNAME_HASH = HashService.hashValue(algorithm, attributes["username"])
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["username"]}) = ${EXPECTED_USERNAME_HASH}")
            final EXPECTED_DATE_HASH = HashService.hashValue(algorithm, attributes["date"])
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["date"]}) = ${EXPECTED_DATE_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            // Add the desired dynamic properties
            attributeKeys.each { String attr ->
                runner.setProperty(attr, "${attr}_${algorithm.name}")
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashedUsername = flowFile.getAttribute("username_${algorithm.name}")
            logger.info("flowfile.username_${algorithm.name} = ${hashedUsername}")
            String hashedDate = flowFile.getAttribute("date_${algorithm.name}")
            logger.info("flowfile.date_${algorithm.name} = ${hashedDate}")

            assert hashedUsername == EXPECTED_USERNAME_HASH
            assert hashedDate == EXPECTED_DATE_HASH
        }
    }

    @Test
    void testShouldCalculateHashOfMissingAttribute() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes for username (empty string) and date (null)
        def attributes = [
                username: "",
                date    : null
        ]
        def attributeKeys = attributes.keySet()

        algorithms.each { HashAlgorithm algorithm ->
            final EXPECTED_USERNAME_HASH = HashService.hashValue(algorithm, attributes["username"])
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["username"]}) = ${EXPECTED_USERNAME_HASH}")
            final EXPECTED_DATE_HASH = null
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["date"]}) = ${EXPECTED_DATE_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            // Add the desired dynamic properties
            attributeKeys.each { String attr ->
                runner.setProperty(attr, "${attr}_${algorithm.name}")
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashedUsername = flowFile.getAttribute("username_${algorithm.name}")
            logger.info("flowfile.username_${algorithm.name} = ${hashedUsername}")
            String hashedDate = flowFile.getAttribute("date_${algorithm.name}")
            logger.info("flowfile.date_${algorithm.name} = ${hashedDate}")

            assert hashedUsername == EXPECTED_USERNAME_HASH
            assert hashedDate == EXPECTED_DATE_HASH
        }
    }

    @Test
    void testShouldRouteToFailureOnProhibitedMissingAttribute() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes for username (empty string) and date (null)
        def attributes = [
                username: "",
                date    : null
        ]
        def attributeKeys = attributes.keySet()

        algorithms.each { HashAlgorithm algorithm ->
            final EXPECTED_USERNAME_HASH = HashService.hashValue(algorithm, attributes["username"])
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["username"]}) = ${EXPECTED_USERNAME_HASH}")
            final EXPECTED_DATE_HASH = null
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["date"]}) = ${EXPECTED_DATE_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            // Set to fail if there are missing attributes
            runner.setProperty(CryptographicHashAttribute.PARTIAL_ATTR_ROUTE_POLICY, CryptographicHashAttribute.PartialAttributePolicy.PROHIBIT.name())

            // Add the desired dynamic properties
            attributeKeys.each { String attr ->
                runner.setProperty(attr, "${attr}_${algorithm.name}")
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 1)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 0)

            final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_FAILURE)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowFiles.first()
            logger.info("Failed flowfile has attributes ${flowFile.attributes}")
            attributeKeys.each { String missingAttribute ->
                flowFile.assertAttributeNotExists("${missingAttribute}_${algorithm.name}")
            }
        }
    }

    @Test
    void testShouldRouteToFailureOnEmptyAttributes() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes for username (empty string) and date (null)
        def attributes = [
                username: "",
                date    : null
        ]
        def attributeKeys = attributes.keySet()

        algorithms.each { HashAlgorithm algorithm ->
            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            // Set to fail if all attributes are missing
            runner.setProperty(CryptographicHashAttribute.FAIL_WHEN_EMPTY, "true")

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 1)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 0)

            final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_FAILURE)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowFiles.first()
            logger.info("Failed flowfile has attributes ${flowFile.attributes}")
            attributeKeys.each { String missingAttribute ->
                flowFile.assertAttributeNotExists("${missingAttribute}_${algorithm.name}")
            }
        }
    }

    @Test
    void testShouldRouteToSuccessOnAllowPartial() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes for username (empty string) and date (null)
        def attributes = [
                username: ""
        ]
        def attributeKeys = attributes.keySet()

        algorithms.each { HashAlgorithm algorithm ->
            final EXPECTED_USERNAME_HASH = HashService.hashValue(algorithm, attributes["username"])
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["username"]}) = ${EXPECTED_USERNAME_HASH}")
            final EXPECTED_DATE_HASH = null
            logger.expected("${algorithm.name.padLeft(11)}(${attributes["date"]}) = ${EXPECTED_DATE_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            // Set to fail if there are missing attributes
            runner.setProperty(CryptographicHashAttribute.PARTIAL_ATTR_ROUTE_POLICY, CryptographicHashAttribute.PartialAttributePolicy.ALLOW.name())

            // Add the desired dynamic properties
            attributeKeys.each { String attr ->
                runner.setProperty(attr, "${attr}_${algorithm.name}")
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowFiles.first()
            logger.info("Successful flowfile has attributes ${flowFile.attributes}")
            attributeKeys.each { String attribute ->
                flowFile.assertAttributeExists("${attribute}_${algorithm.name}")
            }
        }
    }

    @Test
    void testShouldCalculateHashWithVariousCharacterEncodings() {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashAttribute())

        // Create attributes
        def attributes = [test_attribute: "apachenifi"]
        def attributeKeys = attributes.keySet()

        HashAlgorithm algorithm = HashAlgorithm.MD5

        List<Charset> charsets = [StandardCharsets.UTF_8, StandardCharsets.UTF_16, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE]

        final def EXPECTED_MD5_HASHES = [
                "utf_8"   : "a968b5ec1d52449963dcc517789baaaf",
                "utf_16"  : "b8413d18f7e64042bb0322a1cd61eba2",
                "utf_16be": "b8413d18f7e64042bb0322a1cd61eba2",
                "utf_16le": "91c3b67f9f8ae77156f21f271cc09121",
        ]
        EXPECTED_MD5_HASHES.each { k, hash ->
            logger.expected("MD5(${k.padLeft(9)}(${attributes["test_attribute"]})) = ${hash}")
        }

        charsets.each { Charset charset ->
            // Calculate the expected hash value given the character set
            final EXPECTED_HASH = HashService.hashValue(algorithm, attributes["test_attribute"], charset)
            logger.expected("${algorithm.name}(${attributes["test_attribute"]}, ${charset.name()}) = ${EXPECTED_HASH}")

            // Sanity check
            assert EXPECTED_HASH == EXPECTED_MD5_HASHES[translateEncodingToMapKey(charset.name())]

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the properties
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.name)

            logger.info("Setting character set to ${charset.name()}")
            runner.setProperty(CryptographicHashAttribute.CHARACTER_SET, charset.name())

            // Add the desired dynamic properties
            attributeKeys.each { String attr ->
                runner.setProperty(attr, "${attr}_${algorithm.name}")
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes)

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashedAttribute = flowFile.getAttribute("test_attribute_${algorithm.name}")
            logger.info("flowfile.test_attribute_${algorithm.name} = ${hashedAttribute}")

            assert hashedAttribute == EXPECTED_HASH
        }
    }

    static String translateEncodingToMapKey(String charsetName) {
        charsetName.toLowerCase().replaceAll(/[-\/]/, '_')
    }
}
