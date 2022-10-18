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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TestJSLTTransformJSON {

    private TestRunner runner = TestRunners.newTestRunner(new JSLTTransformJSON());

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
        runner.assertValid();
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        Map<String, String> attrs = new HashMap<>();
        attrs.put("rating.range", "RatingRange");
        attrs.put("rating.quality", ".rating.quality.value");
        runner.enqueue(inputFlowFile, attrs);

        final MockFlowFile flowFile = assertRunSuccess();
        final String expectedOutput = getResource("simpleOutput.json");
        flowFile.assertContentEquals(expectedOutput);
    }

    @Test
    public void testArrayJSLT() {
        runTransform("inputArray.json", "arrayTransform.json", "arrayOutput.json");
    }

    @Test
    public void testJSLTNoOutput() throws IOException {
        final String input = "{\"a\":1}";
        final String transform = ".b";
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(input);

        final MockFlowFile flowFile = assertRunSuccess();
        flowFile.assertContentEquals(new byte[0]);
    }

    private void runTransform(final String inputFileName, final String transformFileName, final String outputFileName) {
        setTransformEnqueueJson(transformFileName, inputFileName);

        final MockFlowFile flowFile = assertRunSuccess();

        final String expectedOutput = getResource(outputFileName);
        flowFile.assertContentEquals(expectedOutput);
    }

    private void setTransformEnqueueJson(final String transformFileName, final String jsonFileName) {
        final String transform = getResource(transformFileName);
        final String json = getResource(jsonFileName);
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, Boolean.TRUE.toString());
        runner.enqueue(json);
    }

    private MockFlowFile assertRunSuccess() {
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        return runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).iterator().next();
    }

    private String getResource(final String fileName) {
        final String path = String.format("/%s", fileName);
        try (
                final InputStream inputStream = Objects.requireNonNull(getClass().getResourceAsStream(path), "Resource not found");
                final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            final String resource = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            return translateNewLines(resource);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /*
     * Translate newlines (expected to be in *nix format to be in the codebase) to the system's line separator (to support Windows, e.g.)
     */
    private String translateNewLines(final String text) {
        final Pattern pattern = Pattern.compile("\n", Pattern.MULTILINE);
        return pattern.matcher(text).replaceAll(System.lineSeparator());
    }
}