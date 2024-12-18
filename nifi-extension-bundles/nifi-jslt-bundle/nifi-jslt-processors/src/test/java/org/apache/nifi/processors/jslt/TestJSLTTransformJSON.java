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
package org.apache.nifi.processors.jslt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.processors.jslt.JSLTTransformJSON.TransformationStrategy.ENTIRE_FLOWFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJSLTTransformJSON {

    private TestRunner runner = TestRunners.newTestRunner(new JSLTTransformJSON());

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(new JSLTTransformJSON());
    }

    @Test
    public void testBadInput() {
        final String inputFlowFile = "I am not JSON";
        final String transform = getResource("simpleTransform.json");
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(inputFlowFile);

        runner.run();

        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 0);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 1);
    }

    @Test
    public void testInvalidJSLTTransform() {
        final TestRunner runner = TestRunners.newTestRunner(new JSLTTransformJSON());
        final String invalidTransform = "invalid";
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, invalidTransform);
        runner.assertNotValid();
    }

    @Test
    public void testTransformFilePath() {
        final URL transformUrl = Objects.requireNonNull(getClass().getResource("/simpleTransform.json"));
        final String transformPath = transformUrl.getPath();

        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transformPath);
        runner.setProperty(JSLTTransformJSON.TRANSFORMATION_STRATEGY, ENTIRE_FLOWFILE.getValue());
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());

        final String json = getResource("input.json");
        runner.enqueue(json);

        assertRunSuccess();
    }

    @Test
    public void testSimpleJSLT() {
        runTransform("input.json", "simpleTransform.json", "simpleOutput.json");
    }

    @Test
    public void testTransform() {
        runTransform("input.json", "dynamicKeyTransform.json", "dynamicKeyTransformOutput.json");
    }

    // This test verifies the capability of JSLT to perform a "cardinality ONE" operation (i.e. get first element if array) like JOLT has
    @Test
    public void testCardinality() {
        runTransform("input.json", "cardinalityTransform.json", "cardinalityOutput.json");
    }

    @Test
    public void testExpressionLanguageTransform() {
        final String inputFlowFile = getResource("input.json");
        final String transform = getResource("expressionLanguageTransform.json");
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.TRANSFORMATION_STRATEGY, ENTIRE_FLOWFILE.getValue());
        runner.assertValid();
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        Map<String, String> attrs = new HashMap<>();
        attrs.put("rating.range", "RatingRange");
        attrs.put("rating.quality", ".rating.quality.value");
        runner.enqueue(inputFlowFile, attrs);

        final MockFlowFile flowFile = assertRunSuccess();
        final String expectedOutput = getResource("simpleOutput.json");
        assertContentEquals(flowFile, expectedOutput);
    }

    @Test
    public void testArrayJSLT() {
        runTransform("inputArray.json", "arrayTransform.json", "arrayOutput.json");
    }

    @Test
    public void testArrayJSLTPerObject() {
        final String transform = getResource("arrayTransformPerObject.json");
        final String json = getResource("inputArray.json");
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(json);

        final MockFlowFile flowFile = assertRunSuccess();

        final String expectedOutput = getResource("arrayOutput.json");
        assertContentEquals(flowFile, expectedOutput);
    }

    @Test
    public void testJSLTNoOutput() throws IOException {
        final String input = "{\"a\":1}";
        final String transform = ".b";
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.TRANSFORMATION_STRATEGY, ENTIRE_FLOWFILE.getValue());
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(input);

        final MockFlowFile flowFile = assertRunSuccess();
        flowFile.assertContentEquals(new byte[0]);
    }

    @Test
    public void testTransformWithNullValueRemoved() {
        runner.setProperty(JSLTTransformJSON.RESULT_FILTER, ". != null and . != {} and . != []");
        runTransform("inputWithNull.json", "simpleTransform.json", "simpleOutputWithoutNull.json");
    }

    @Test
    public void testTransformWithNullValueIncluded() {
        runner.setProperty(JSLTTransformJSON.RESULT_FILTER, ". != {} and . != []");
        runTransform("inputWithNull.json", "simpleTransform.json", "simpleOutputWithNull.json");
    }

    @Test
    public void testTransformWithNoFilter() {
        runner.setProperty(JSLTTransformJSON.RESULT_FILTER, "true");
        runTransform("inputWithNull.json", "simpleTransform.json", "simpleOutputWithNull.json");
    }

    // This test verifies transformCache cleanup does not throw an exception
    @Test
    public void testShutdown() {
        runner.stop();
    }

    private void runTransform(final String inputFileName, final String transformFileName, final String outputFileName) {
        setTransformEnqueueJson(transformFileName, inputFileName);

        final MockFlowFile flowFile = assertRunSuccess();

        final String expectedOutput = getResource(outputFileName);
        assertContentEquals(flowFile, expectedOutput);
    }

    private void assertContentEquals(final MockFlowFile flowFile, final String expectedJson) {
        try {
            final JsonNode flowFileNode = objectMapper.readTree(flowFile.getContent());
            final JsonNode expectedNode = objectMapper.readTree(expectedJson);
            assertEquals(expectedNode, flowFileNode);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void setTransformEnqueueJson(final String transformFileName, final String jsonFileName) {
        final String transform = getResource(transformFileName);
        final String json = getResource(jsonFileName);
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.TRANSFORMATION_STRATEGY, ENTIRE_FLOWFILE.getValue());
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(json);
    }

    private MockFlowFile assertRunSuccess() {
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        return runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).getFirst();
    }

    private String getResource(final String fileName) {
        final String path = String.format("/%s", fileName);
        try (
                final InputStream inputStream = Objects.requireNonNull(getClass().getResourceAsStream(path), "Resource not found");
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
                ) {
            StreamUtils.copy(inputStream, outputStream);
            return outputStream.toString();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}