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
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.crypto.HashAlgorithm;
import org.apache.nifi.security.util.crypto.HashService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class CryptographicHashContentTest {
    private TestRunner runner;

    @BeforeEach
    void setupRunner() {
        runner = TestRunners.newTestRunner(new CryptographicHashContent());
    }

    @Test
    void testShouldCalculateHashOfPresentContent() throws IOException {
        // Generate some long content (90 KB)
        final String longContent = StringUtils.repeat("apachenifi ", 8192);

        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            final String expectedContentHash = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(longContent.getBytes()));

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.getName());

            // Insert the content in the mock flowfile
            runner.enqueue(longContent.getBytes(StandardCharsets.UTF_8),
                    Collections.singletonMap("size", String.valueOf(longContent.length())));

            runner.run(1);

            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);
            String hashAttribute = String.format("content_%s", algorithm.getName());
            flowFile.assertAttributeExists(hashAttribute);
            flowFile.assertAttributeEquals(hashAttribute, expectedContentHash);
        }
    }

    @Test
    void testShouldCalculateHashOfEmptyContent() throws IOException {
        final String emptyContent = "";

        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            final String expectedContentHash = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(emptyContent.getBytes()));

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.getName());

            // Insert the content in the mock flowfile
            runner.enqueue(emptyContent.getBytes(StandardCharsets.UTF_8), Collections.singletonMap("size", "0"));

            runner.run(1);

            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);
            String hashAttribute = String.format("content_%s", algorithm.getName());
            flowFile.assertAttributeExists(hashAttribute);

            String hashedContent = flowFile.getAttribute(hashAttribute);

            assertEquals(expectedContentHash, hashedContent);
        }
    }

    /**
     * This test works because {@link MockFlowFile} uses the actual internal {@code data.size} for {@code getSize ( )},
     * while {@code StandardFlowFileRecord} uses a separate {@code size} field. May need to use {@code flowfile.getContentClaim ( ) .getLength ( )}.
     */
    @Test
    void testShouldCalculateHashOfContentWithIncorrectSizeAttribute() throws IOException {
        final String nonEmptyContent = "apachenifi";

        final TestRunner runner = TestRunners.newTestRunner(new CryptographicHashContent());

        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            final String expectedContentHash = HashService.hashValueStreaming(algorithm, new ByteArrayInputStream(nonEmptyContent.getBytes()));

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.getName());

            // Insert the content in the mock flowfile (with the wrong size attribute)
            runner.enqueue(nonEmptyContent.getBytes(StandardCharsets.UTF_8), Collections.singletonMap("size", "0"));

            runner.run(1);

            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);
            String hashAttribute = String.format("content_%s", algorithm.getName());
            flowFile.assertAttributeExists(hashAttribute);
            flowFile.assertAttributeEquals(hashAttribute, expectedContentHash);
        }
    }

    @Test
    void testShouldOverwriteExistingAttribute() {
        final String nonEmptyContent = "apachenifi";
        final String oldHashAttributeValue = "OLD VALUE";

        HashAlgorithm algorithm = HashAlgorithm.SHA256;

        final String expectedContentHash = HashService.hashValue(algorithm, nonEmptyContent);

        // Set the algorithm
        runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.getName());

        // Insert the content in the mock flowfile (with an existing attribute)
        final Map<String, String> oldAttributes = Collections.singletonMap(String.format("content_%s", algorithm.getName()),
                oldHashAttributeValue);
        runner.enqueue(nonEmptyContent.getBytes(StandardCharsets.UTF_8),
                oldAttributes);

        runner.run(1);

        runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 0);
        runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 1);

        final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_SUCCESS);

        // Extract the generated attributes from the flowfile
        MockFlowFile flowFile = successfulFlowfiles.get(0);
        String hashAttribute = String.format("content_%s", algorithm.getName());
        flowFile.assertAttributeExists(hashAttribute);

        String hashedContent = flowFile.getAttribute(hashAttribute);

        assertNotEquals(oldHashAttributeValue, hashedContent);
        assertEquals(expectedContentHash, hashedContent);
    }

    @Test
    void testShouldRouteToFailureOnEmptyContent() {
        final String emptyContent = "";

        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the failure property
            runner.setProperty(CryptographicHashContent.FAIL_WHEN_EMPTY, "true");

            // Set the algorithm
            runner.setProperty(CryptographicHashContent.HASH_ALGORITHM, algorithm.getName());

            // Insert the content in the mock flowfile
            runner.enqueue(emptyContent.getBytes(StandardCharsets.UTF_8));

            runner.run(1);

            runner.assertTransferCount(CryptographicHashContent.REL_FAILURE, 1);
            runner.assertTransferCount(CryptographicHashContent.REL_SUCCESS, 0);

            final List<MockFlowFile> failedFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashContent.REL_FAILURE);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowfiles.get(0);
            String hashAttribute = String.format("content_%s", algorithm.getName());
            flowFile.assertAttributeNotExists(hashAttribute);
        }
    }
}
