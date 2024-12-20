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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;

public class TestSplitJson {

    private static final Path JSON_SNIPPET = Paths.get("src/test/resources/TestJson/json-sample.json");
    private static final Path XML_SNIPPET = Paths.get("src/test/resources/TestXml/xml-snippet.xml");

    @Test
    public void testInvalidJsonPath() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$..");
        testRunner.assertNotValid();
    }

    @Test
    public void testInvalidJsonDocument() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SplitJson.REL_FAILURE, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(SplitJson.REL_FAILURE).getFirst();
        // Verify that the content was unchanged
        out.assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testSplit_nonArrayResult() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[0]._id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = SplitJson.REL_FAILURE;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).getFirst();
        out.assertContentEquals(JSON_SNIPPET);
    }

    @Test
    public void testSplit_arrayResult_oneValue() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[0].range[?(@ == 0)]");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        testRunner.assertTransferCount(SplitJson.REL_ORIGINAL, 1);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst().assertAttributeEquals(FRAGMENT_COUNT.key(), "1");
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, 1);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst().assertContentEquals(JSON_SNIPPET);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).getFirst().assertContentEquals("0");
    }

    @Test
    public void testSplit_arrayResult_multipleValues() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[0].range");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        int numSplitsExpected = 10;

        testRunner.assertTransferCount(SplitJson.REL_ORIGINAL, 1);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst().assertAttributeEquals(FRAGMENT_COUNT.key(), String.valueOf(numSplitsExpected));
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, numSplitsExpected);
        final MockFlowFile originalOut = testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst();
        originalOut.assertContentEquals(JSON_SNIPPET);
    }

    @Test
    public void testSplit_change_jsonpath() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[0].range");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        int numSplitsExpected = 10;

        testRunner.assertTransferCount(SplitJson.REL_ORIGINAL, 1);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst().assertAttributeEquals(FRAGMENT_COUNT.key(), String.valueOf(numSplitsExpected));
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, numSplitsExpected);
        final MockFlowFile originalOut = testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst();
        originalOut.assertContentEquals(JSON_SNIPPET);

        // Change JsonPath Expression, verify it is being applied correctly
        testRunner.clearTransferState();
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[*].name");

        testRunner.enqueue(JSON_SNIPPET, Collections.singletonMap(CoreAttributes.FILENAME.key(), "test.json"));
        testRunner.run();

        testRunner.assertTransferCount(SplitJson.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst();
        originalFlowFile.assertAttributeExists(FRAGMENT_ID.key());
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT.key(), "7");
        originalFlowFile.assertContentEquals(JSON_SNIPPET);
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, 7);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).getFirst();
        flowFile.assertContentEquals("{\"first\":\"Shaffer\",\"last\":\"Pearson\"}");
    }

    @Test
    public void testSplit_arrayResult_nonScalarValues() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$[*].name");

        testRunner.enqueue(JSON_SNIPPET, Map.of(CoreAttributes.FILENAME.key(), "test.json"));
        testRunner.run();

        testRunner.assertTransferCount(SplitJson.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(SplitJson.REL_ORIGINAL).getFirst();
        originalFlowFile.assertAttributeExists(FRAGMENT_ID.key());
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT.key(), "7");
        originalFlowFile.assertContentEquals(JSON_SNIPPET);
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, 7);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).getFirst();
        flowFile.assertContentEquals("{\"first\":\"Shaffer\",\"last\":\"Pearson\"}");
        flowFile.assertAttributeEquals(FRAGMENT_COUNT.key(), "7");
        flowFile.assertAttributeEquals(FRAGMENT_INDEX.key(), "0");
        flowFile.assertAttributeEquals(SEGMENT_ORIGINAL_FILENAME.key(), "test.json");

        flowFile = testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).get(6);
        flowFile.assertAttributeEquals(FRAGMENT_COUNT.key(), "7");
        flowFile.assertAttributeEquals(FRAGMENT_INDEX.key(), "6");
        flowFile.assertAttributeEquals(SEGMENT_ORIGINAL_FILENAME.key(), "test.json");
    }

    @Test
    public void testSplit_pathNotFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$.nonexistent");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        testRunner.assertTransferCount(SplitJson.REL_FAILURE, 1);
        testRunner.getFlowFilesForRelationship(SplitJson.REL_FAILURE).getFirst().assertContentEquals(JSON_SNIPPET);
    }

    @Test
    public void testSplit_pathToNullValue() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$.nullField");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("{\"stringField\": \"String Value\", \"nullField\": null}".getBytes(StandardCharsets.UTF_8));
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(SplitJson.REL_FAILURE, 1);
    }

    @Test
    public void testSplit_pathToArrayWithNulls_emptyStringRepresentation() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$.arrayOfNulls");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("{\"stringField\": \"String Value\", \"arrayOfNulls\": [null, null, null]}".getBytes(StandardCharsets.UTF_8));
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();

        /* assert that three files were transferred to split and each is empty */
        int expectedFiles = 3;
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, expectedFiles);
        for (int i = 0; i < expectedFiles; i++) {
            testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).get(i).assertContentEquals("");
        }
    }

    @Test
    public void testSplit_pathToArrayWithNulls_nullStringRepresentation() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$.arrayOfNulls");
        testRunner.setProperty(SplitJson.NULL_VALUE_DEFAULT_REPRESENTATION,
                AbstractJsonPathProcessor.NULL_STRING_OPTION);

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("{\"stringField\": \"String Value\", \"arrayOfNulls\": [null, null, null]}".getBytes(StandardCharsets.UTF_8));
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();

        /* assert that three files were transferred to split and each has the word null in it */
        int expectedFiles = 3;
        testRunner.assertTransferCount(SplitJson.REL_SPLIT, expectedFiles);
        for (int i = 0; i < expectedFiles; i++) {
            testRunner.getFlowFilesForRelationship(SplitJson.REL_SPLIT).get(i).assertContentEquals("null");
        }
    }

    @Test
    public void testSplit_pathToInputStringNullValue() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitJson());
        testRunner.setProperty(SplitJson.ARRAY_JSON_PATH_EXPRESSION, "$.*");
        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("null".getBytes(StandardCharsets.UTF_8));
            }
        });
        testRunner.enqueue(ff);
        testRunner.run();
        testRunner.assertTransferCount(SplitJson.REL_FAILURE, 1);
    }
}
