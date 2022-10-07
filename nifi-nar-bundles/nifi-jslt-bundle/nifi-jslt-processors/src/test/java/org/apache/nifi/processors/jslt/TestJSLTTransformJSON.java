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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TestJSLTTransformJSON {

    private final static Path JSON_INPUT = Paths.get("src/test/resources/input.json");
    private TestRunner runner = TestRunners.newTestRunner(new JSLTTransformJSON());

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(new JSLTTransformJSON());
    }

    @Test
    public void testBadInput() throws IOException {
        final String inputFlowFile = "I am not JSON";
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/simpleTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
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
    public void testSimpleJSLT() throws IOException {
        final String inputFlowFile = new String(Files.readAllBytes(JSON_INPUT));
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/simpleTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        runner.enqueue(inputFlowFile);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/simpleOutput.json")));
        flowFile.assertContentEquals(translateNewLines(expectedOutput));
    }

    @Test
    public void testTransform() throws IOException {
        final String inputFlowFile = new String(Files.readAllBytes(JSON_INPUT));
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/dynamicKeyTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        runner.enqueue(inputFlowFile);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/dynamicKeyTransformOutput.json")));
        flowFile.assertContentEquals(translateNewLines(expectedOutput));
    }

    // This test verifies the capability of JSLT to perform a "cardinality ONE" operation (i.e. get first element if array) like JOLT has
    @Test
    public void testCardinality() throws IOException {
        final String inputFlowFile = new String(Files.readAllBytes(JSON_INPUT));
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/cardinalityTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        runner.enqueue(inputFlowFile);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/cardinalityOutput.json")));
        flowFile.assertContentEquals(translateNewLines(expectedOutput));
    }

    @Test
    public void testExpressionLanguageTransform() throws IOException {
        final String inputFlowFile = new String(Files.readAllBytes(JSON_INPUT));
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/expressionLanguageTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.assertValid();
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("rating.range", "RatingRange");
        attrs.put("rating.quality", ".rating.quality.value");
        runner.enqueue(inputFlowFile, attrs);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/simpleOutput.json")));
        flowFile.assertContentEquals(translateNewLines(expectedOutput));
    }

    @Test
    public void testArrayJSLT() throws IOException {
        final String inputFlowFile = new String(Files.readAllBytes(Paths.get("src/test/resources/inputArray.json")));
        final String transform = new String(Files.readAllBytes(Paths.get("src/test/resources/arrayTransform.json")));
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        runner.enqueue(inputFlowFile);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/arrayOutput.json")));
        flowFile.assertContentEquals(translateNewLines(expectedOutput));
    }

    @Test
    public void testJSLTNoOutput() throws IOException {
        final String input = "{\"a\":1}";
        final String transform = ".b";
        runner.setProperty(JSLTTransformJSON.JSLT_TRANSFORM, transform);
        runner.setProperty(JSLTTransformJSON.PRETTY_PRINT, "true");
        runner.enqueue(input);
        runner.run();
        runner.assertTransferCount(JSLTTransformJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(JSLTTransformJSON.REL_FAILURE, 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(JSLTTransformJSON.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(new byte[0]);
    }

    /*
     * Translate newlines (expected to be in *nix format to be in the codebase) to the system's line separator (to support Windows, e.g.)
     */
    private String translateNewLines(final String text) {
        final String lineSeparator = System.getProperty("line.separator");
        final Pattern pattern = Pattern.compile("\n", Pattern.MULTILINE);
        final String translated = pattern.matcher(text).replaceAll(lineSeparator);
        return translated;
    }
}