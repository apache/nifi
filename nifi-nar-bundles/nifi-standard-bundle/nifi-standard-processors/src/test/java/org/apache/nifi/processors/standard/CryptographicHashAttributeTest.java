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

import org.apache.nifi.security.util.crypto.HashAlgorithm;
import org.apache.nifi.security.util.crypto.HashService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CryptographicHashAttributeTest {
    private TestRunner runner;

    @BeforeAll
    static void setUpOnce() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeEach
    void setupRunner() {
        runner = TestRunners.newTestRunner(new CryptographicHashAttribute());
    }

    @Test
    void testShouldCalculateHashOfPresentAttribute() {
        // Create attributes for username and date
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("username", "alopresto");
        attributes.put("date", ZonedDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSS Z")));

        final Set<String> attributeKeys = attributes.keySet();

        for (final HashAlgorithm algorithm : HashAlgorithm.values()) {
            final String expectedUsernameHash = HashService.hashValue(algorithm, attributes.get("username"));
            final String expectedDateHash = HashService.hashValue(algorithm, attributes.get("date"));

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());

            // Add the desired dynamic properties
            for (final String attr: attributeKeys) {
                runner.setProperty(attr, String.format("%s_%s", attr, algorithm.getName()));
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);

            flowFile.assertAttributeEquals(String.format("username_%s", algorithm.getName()), expectedUsernameHash);
            flowFile.assertAttributeEquals(String.format("date_%s", algorithm.getName()), expectedDateHash);
        }
    }

    @Test
    void testShouldCalculateHashOfMissingAttribute() {
        // Create attributes for username (empty string) and date (null)
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("username", "");
        attributes.put("date", null);

        final Set<String> attributeKeys = attributes.keySet();

        for (final HashAlgorithm algorithm: HashAlgorithm.values()) {
            final String expectedUsernameHash = HashService.hashValue(algorithm, attributes.get("username"));
            final String expectedDateHash = null;

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());

            // Add the desired dynamic properties
            for (final String attr: attributeKeys) {
                runner.setProperty(attr, String.format("%s_%s", attr, algorithm.getName()));
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);

            flowFile.assertAttributeEquals(String.format("username_%s", algorithm.getName()), expectedUsernameHash);
            flowFile.assertAttributeEquals(String.format("date_%s", algorithm.getName()), expectedDateHash);
        }
    }

    @Test
    void testShouldRouteToFailureOnProhibitedMissingAttribute() {
        // Create attributes for username (empty string) and date (null)
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("username", "");
        attributes.put("date", null);

        final Set<String> attributeKeys = attributes.keySet();

        for (final HashAlgorithm algorithm: HashAlgorithm.values()) {
            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());

            // Set to fail if there are missing attributes
            runner.setProperty(CryptographicHashAttribute.PARTIAL_ATTR_ROUTE_POLICY, CryptographicHashAttribute.PartialAttributePolicy.PROHIBIT.name());

            // Add the desired dynamic properties
            for (final String attr: attributeKeys) {
                runner.setProperty(attr, String.format("%s_%s", attr, algorithm.getName()));
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 1);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 0);

            final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_FAILURE);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowFiles.get(0);
            for (final String missingAttribute: attributeKeys) {
                flowFile.assertAttributeNotExists(String.format("%s_%s", missingAttribute, algorithm.getName()));
            }
        }
    }

    @Test
    void testShouldRouteToFailureOnEmptyAttributes() {
        // Create attributes for username (empty string) and date (null)
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("username", "");
        attributes.put("date", null);

        final Set<String> attributeKeys = attributes.keySet();

        for (final HashAlgorithm algorithm: HashAlgorithm.values()) {
            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());

            // Set to fail if all attributes are missing
            runner.setProperty(CryptographicHashAttribute.FAIL_WHEN_EMPTY, "true");

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 1);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 0);

            final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_FAILURE);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = failedFlowFiles.get(0);
            for (final String missingAttribute: attributeKeys) {
                flowFile.assertAttributeNotExists(String.format("%s_%s", missingAttribute, algorithm.getName()));
            }
        }
    }

    @Test
    void testShouldRouteToSuccessOnAllowPartial() {
        // Create attributes for username (empty string) and date (null)
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("username", "");

        final Set<String> attributeKeys = attributes.keySet();

        for (final HashAlgorithm algorithm: HashAlgorithm.values()) {
            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the algorithm
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());

            // Set to fail if there are missing attributes
            runner.setProperty(CryptographicHashAttribute.PARTIAL_ATTR_ROUTE_POLICY, CryptographicHashAttribute.PartialAttributePolicy.ALLOW.name());

            // Add the desired dynamic properties
            for (final String attr: attributeKeys) {
                runner.setProperty(attr, String.format("%s_%s", attr, algorithm.getName()));
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowFiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowFiles.get(0);
            for (final String attribute: attributeKeys) {
                flowFile.assertAttributeExists(String.format("%s_%s", attribute, algorithm.getName()));
            }
        }
    }

    @Test
    void testShouldCalculateHashWithVariousCharacterEncodings() {
        // Create attributes
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("test_attribute", "apachenifi");
        final Set<String> attributeKeys = attributes.keySet();

        final HashAlgorithm algorithm = HashAlgorithm.MD5;

        final List<Charset> charsets = Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.UTF_16, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE);

        final Map<String, String> EXPECTED_MD5_HASHES = new HashMap<>();
        EXPECTED_MD5_HASHES.put(StandardCharsets.UTF_8.name(), "a968b5ec1d52449963dcc517789baaaf");
        EXPECTED_MD5_HASHES.put(StandardCharsets.UTF_16.name(), "b8413d18f7e64042bb0322a1cd61eba2");
        EXPECTED_MD5_HASHES.put(StandardCharsets.UTF_16BE.name(), "b8413d18f7e64042bb0322a1cd61eba2");
        EXPECTED_MD5_HASHES.put(StandardCharsets.UTF_16LE.name(), "91c3b67f9f8ae77156f21f271cc09121");

        for (final Charset charset: charsets) {
            // Calculate the expected hash value given the character set
            final String EXPECTED_HASH = HashService.hashValue(algorithm, attributes.get("test_attribute"), charset);

            // Sanity check
            assertEquals(EXPECTED_HASH, EXPECTED_MD5_HASHES.get(charset.name()));

            // Reset the processor
            runner.clearProperties();
            runner.clearProvenanceEvents();
            runner.clearTransferState();

            // Set the properties
            runner.setProperty(CryptographicHashAttribute.HASH_ALGORITHM, algorithm.getName());
            runner.setProperty(CryptographicHashAttribute.CHARACTER_SET, charset.name());

            // Add the desired dynamic properties
            for (final String attr: attributeKeys) {
                runner.setProperty(attr, String.format("%s_%s", attr, algorithm.getName()));
            }

            // Insert the attributes in the mock flowfile
            runner.enqueue(new byte[0], attributes);

            runner.run(1);

            runner.assertTransferCount(CryptographicHashAttribute.REL_FAILURE, 0);
            runner.assertTransferCount(CryptographicHashAttribute.REL_SUCCESS, 1);

            final List<MockFlowFile> successfulFlowfiles = runner.getFlowFilesForRelationship(CryptographicHashAttribute.REL_SUCCESS);

            // Extract the generated attributes from the flowfile
            MockFlowFile flowFile = successfulFlowfiles.get(0);

            flowFile.assertAttributeEquals(String.format("test_attribute_%s", algorithm.getName()), EXPECTED_HASH);
        }
    }
}
