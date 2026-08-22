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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.DetectBase64Content.DetectionScope;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDetectBase64Content {

    private static final String DEFAULT_ATTRIBUTE = "content.base64";

    private static final String CUSTOM_ATTRIBUTE = "encoded";

    private static final String TRUE = "true";

    private static final String FALSE = "false";

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(DetectBase64Content.class);
    }

    @Test
    void testDefaultConfigurationValid() {
        runner.assertValid();
    }

    @Test
    void testZeroSampleSizeInvalid() {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "0 B");
        runner.assertNotValid();
    }

    @Test
    void testSampleSizeValid() {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "1 MB");
        runner.assertValid();
    }

    @Test
    void testDefaultDetectionScopeEvaluatesEntireContent() {
        final String content = "QUJD".repeat(4096) + "!!! not base64 !!!";
        runner.enqueue(content);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "QQ==",
            "QUJD",
            "YWJjZGVmZ2hpams=",
            "MTIzNDU2Nzg5MA==",
            "c3RyaW5nIHdpdGggc3BhY2Vz"
    })
    void testBase64Detected(final String content) {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        assertResult(TRUE, DEFAULT_ATTRIBUTE);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not base64!",
            "QUJD$",
            "QUJ",
            "QUJDR",
            "Q=QJD",
            "QUJD===",
            "{\"field\": \"value\"}",
            "The quick brown fox jumps over the lazy dog"
    })
    void testBase64NotDetected(final String content) {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @ParameterizedTest
    @EnumSource(DetectionScope.class)
    void testEmptyContentNotDetected(final DetectionScope detectionScope) {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, detectionScope);
        runner.enqueue(new byte[0]);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @ParameterizedTest
    @EnumSource(DetectionScope.class)
    void testLineSeparatorsOnlyNotDetected(final DetectionScope detectionScope) {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, detectionScope);
        runner.enqueue("\r\n\r\n");
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testLineSeparatorsDetected() {
        final String content = "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXow\r\nYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXow\r\n";
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        assertResult(TRUE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testBinaryContentNotDetected() {
        final byte[] content = new byte[4096];
        new Random(0).nextBytes(content);
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testEncodedBinaryContentDetected() {
        final byte[] binary = new byte[8192];
        new Random(0).nextBytes(binary);
        final byte[] encoded = Base64.getEncoder().encode(binary);

        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(encoded);
        runner.run();

        assertResult(TRUE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testCustomAttributeName() {
        runner.setProperty(DetectBase64Content.BASE64_ATTRIBUTE_NAME, CUSTOM_ATTRIBUTE);
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue("QUJD");
        runner.run();

        final MockFlowFile flowFile = assertResult(TRUE, CUSTOM_ATTRIBUTE);
        flowFile.assertAttributeNotExists(DEFAULT_ATTRIBUTE);
    }

    @Test
    void testCustomAttributeNameExpressionLanguage() {
        runner.setProperty(DetectBase64Content.BASE64_ATTRIBUTE_NAME, "${attribute.name}");
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue("QUJD", Map.of("attribute.name", CUSTOM_ATTRIBUTE));
        runner.run();

        assertResult(TRUE, CUSTOM_ATTRIBUTE);
    }

    @Test
    void testSampleScopeIgnoresContentBeyondSampleSize() {
        final String content = "QUJD".repeat(16) + "!!! not base64 !!!";
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "16 B");
        runner.enqueue(content);
        runner.run();

        assertResult(TRUE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testEntireContentScopeEvaluatesContentBeyondSampleSize() {
        final String content = "QUJD".repeat(16) + "!!! not base64 !!!";
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testSampleScopeDetectsNonBase64WithinSampleSize() {
        final String content = "!!! not base64 !!!" + "QUJD".repeat(16);
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "16 B");
        runner.enqueue(content);
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testSampleScopeAppliesLengthCheckWhenContentSmallerThanSampleSize() {
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "4 KB");
        runner.enqueue("QUJDR");
        runner.run();

        assertResult(FALSE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testSampleSizeExpressionLanguage() {
        final String content = "QUJD".repeat(16) + "!!! not base64 !!!";
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.SAMPLE);
        runner.setProperty(DetectBase64Content.SAMPLE_SIZE, "${sample.size}");
        runner.enqueue(content, Map.of("sample.size", "16 B"));
        runner.run();

        assertResult(TRUE, DEFAULT_ATTRIBUTE);
    }

    @Test
    void testContentNotModified() {
        final String content = "QUJD";
        runner.setProperty(DetectBase64Content.DETECTION_SCOPE, DetectionScope.ENTIRE_CONTENT);
        runner.enqueue(content);
        runner.run();

        final MockFlowFile flowFile = assertResult(TRUE, DEFAULT_ATTRIBUTE);
        flowFile.assertContentEquals(content, StandardCharsets.UTF_8);
    }

    private MockFlowFile assertResult(final String expected, final String attributeName) {
        runner.assertAllFlowFilesTransferred(DetectBase64Content.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DetectBase64Content.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(expected, flowFile.getAttribute(attributeName));
        return flowFile;
    }
}
