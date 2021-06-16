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

import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class CryptographicHashContentTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CryptographicHashContentTest.class)

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
    void testShouldCalculateHashOfPresentContent() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        // Generate some long content (90 KB)
        final String LONG_CONTENT = "apachenifi " * 8192

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent())

        algorithms.each { HashAlgorithm algorithm ->
            final String EXPECTED_CONTENT_HASH = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(LONG_CONTENT.bytes))
            logger.info("Expected ${algorithm.name.padLeft(11)}: ${EXPECTED_CONTENT_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.name)

            // Insert the content in the mock flowfile
            runner.enqueue(LONG_CONTENT.getBytes(StandardCharsets.UTF_8),
                    [size: LONG_CONTENT.length() as String])

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashAttribute = "content_${algorithm.name}"
            flowFile.assertAttributeExists(hashAttribute)

            String hashedContent = flowFile.getAttribute(hashAttribute)
            logger.info("flowfile.${hashAttribute} = ${hashedContent}")

            assert hashedContent == EXPECTED_CONTENT_HASH
        }
    }

    @Test
    void testShouldCalculateHashOfEmptyContent() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final String EMPTY_CONTENT = ""

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent())

        algorithms.each { HashAlgorithm algorithm ->
            final String EXPECTED_CONTENT_HASH = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(EMPTY_CONTENT.bytes))
            logger.info("Expected ${algorithm.name.padLeft(11)}: ${EXPECTED_CONTENT_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.name)

            // Insert the content in the mock flowfile
            runner.enqueue(EMPTY_CONTENT.getBytes(StandardCharsets.UTF_8), [size: "0"])

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashAttribute = "content_${algorithm.name}"
            flowFile.assertAttributeExists(hashAttribute)

            String hashedContent = flowFile.getAttribute(hashAttribute)
            logger.info("flowfile.${hashAttribute} = ${hashedContent}")

            assert hashedContent == EXPECTED_CONTENT_HASH
        }
    }

    /**
     * This test works because {@link MockFlowFile} uses the actual internal {@code data.size} for {@code getSize ( )}, while {@code StandardFlowFileRecord} uses a separate {@code size} field. May need to use {@code flowfile.getContentClaim ( ) .getLength ( )}.
     */
    @Test
    void testShouldCalculateHashOfContentWithIncorrectSizeAttribute() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final String NON_EMPTY_CONTENT = "apachenifi"

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent())

        algorithms.each { HashAlgorithm algorithm ->
            final String EXPECTED_CONTENT_HASH = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(NON_EMPTY_CONTENT.bytes))
            logger.info("Expected ${algorithm.name.padLeft(11)}: ${EXPECTED_CONTENT_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.name)

            // Insert the content in the mock flowfile (with the wrong size attribute)
            runner.enqueue(NON_EMPTY_CONTENT.getBytes(StandardCharsets.UTF_8), [size: "0"])

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0)
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1)

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.first()
            String hashAttribute = "content_${algorithm.name}"
            flowFile.assertAttributeExists(hashAttribute)

            String hashedContent = flowFile.getAttribute(hashAttribute)
            logger.info("flowfile.${hashAttribute} = ${hashedContent}")

            assert hashedContent == EXPECTED_CONTENT_HASH
        }
    }

    @Test
    void testShouldOverwriteExistingAttribute() {
        // Arrange
        final String NON_EMPTY_CONTENT = "apachenifi"
        final String OLD_HASH_ATTRIBUTE_VALUE = "OLD VALUE"

        HashAlgorithm algorithm = HashAlgorithm.SHA256

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent())

        final String EXPECTED_CONTENT_HASH = HashService.hashValue(algorithm, NON_EMPTY_CONTENT)
        logger.info("Expected ${algorithm.name.padLeft(11)}: ${EXPECTED_CONTENT_HASH}")

        // Set the algorithm
        logger.info("Setting hash algorithm to ${algorithm.name}")
        runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.name)

        // Insert the content in the mock flowfile (with an existing attribute)
        def oldAttributes = [("content_${algorithm.name}".toString()): OLD_HASH_ATTRIBUTE_VALUE]
        runner.enqueue(NON_EMPTY_CONTENT.getBytes(StandardCharsets.UTF_8),
                oldAttributes)

        // Act
        runner.run(1)

        // Assert
        runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0)
        runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1)

        final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS)

        // Extract the generated attributes from the flowfile
        MockFlowFile flowFile = successfulFlowfiles.first()
        String hashAttribute = "content_${algorithm.name}"
        flowFile.assertAttributeExists(hashAttribute)

        String hashedContent = flowFile.getAttribute(hashAttribute)
        logger.info("flowfile.${hashAttribute} = ${hashedContent}")

        assert hashedContent != OLD_HASH_ATTRIBUTE_VALUE
        assert hashedContent == EXPECTED_CONTENT_HASH
    }

    @Test
    void testShouldRouteToFailureOnEmptyContent() {
        // Arrange
        def algorithms = HashAlgorithm.values()

        final String EMPTY_CONTENT = ""

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent())

        algorithms.each { HashAlgorithm algorithm ->
            final String EXPECTED_CONTENT_HASH = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(EMPTY_CONTENT.bytes))
            logger.info("Expected ${algorithm.name.padLeft(11)}: ${EXPECTED_CONTENT_HASH}")

            // Reset the processor
            runner.clearProperties()
            runner.clearProvenanceEvents()
            runner.clearTransferState()

            // Set the failure property
            logger.info("Setting fail when empty to true")
            runner.setProperty(CryptographicHashContent.FAIL_WHEN_EMPTY, "true")

            // Set the algorithm
            logger.info("Setting hash algorithm to ${algorithm.name}")
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.name)

            // Insert the content in the mock flowfile
            runner.enqueue(EMPTY_CONTENT.getBytes(StandardCharsets.UTF_8))

            // Act
            runner.run(1)

            // Assert
            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 1)
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 0)

            final List<MockFlowFile> failedFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_FAILURE)

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowfiles.first()
            String hashAttribute = "content_${algorithm.name}"
            flowFile.assertAttributeNotExists(hashAttribute)
        }
    }
}
