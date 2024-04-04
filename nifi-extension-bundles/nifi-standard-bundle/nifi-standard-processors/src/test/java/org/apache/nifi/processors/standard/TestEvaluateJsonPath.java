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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestEvaluateJsonPath {

    private static final Path JSON_SNIPPET = Paths.get("src/test/resources/TestJson/json-sample.json");
    private static final Path XML_SNIPPET = Paths.get("src/test/resources/TestXml/xml-snippet.xml");

    @Test
    void testInvalidJsonPath() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("invalid.jsonPath", "$..");

        testRunner.assertNotValid();
    }

    @Test
    void testUpgradeToJsonPath24() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        List<String> badInputs = Arrays.asList("LoremIpsum []", "LoremIpsum[]", "$..", "$.xyz.");
        for (String bad : badInputs) {
            testRunner.setProperty("DefinitelyNotJsonPath", bad);

            testRunner.enqueue(JSON_SNIPPET);
            testRunner.assertNotValid();
        }

        /*
         * Not sure why these passed with v2, but they do. These are weird strings that oddly worked before.
         */
        List<String> testWhatUsedToPass = Arrays.asList("LoremIpsum@$Q#$^Q$%Q#", "TestTest['sdfadsf']#$%#$^#$^.xyz");
        for (String old : testWhatUsedToPass) {
            testRunner.setProperty("DefinitelyNotJsonPath", old);

            testRunner.enqueue(JSON_SNIPPET);
            testRunner.assertValid();
        }

        /*
         * Test some obviously good JsonPath strings
         */
        List<String> goodStrings = Arrays.asList("$", "$.xyz", "$['xyz']", "$.*['xyz']");
        for (String good : goodStrings) {
            testRunner.setProperty("DefinitelyNotJsonPath", good);

            testRunner.enqueue(JSON_SNIPPET);
            testRunner.assertValid();
        }

        testRunner.setProperty("DefinitelyNotJsonPath", "   ");
        testRunner.assertNotValid();
    }

    @Test
    void testInvalidJsonDocument() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateJsonPath.REL_FAILURE, 1);
    }

    @Test
    void testInvalidConfiguration_destinationContent_twoPaths() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name");

        testRunner.assertNotValid();
    }

    @Test
    void testInvalidConfiguration_invalidJsonPath_space() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty("JsonPath1", "$[0]. _id");
        testRunner.assertNotValid();
    }

    @Test
    void testConfiguration_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name"); // cannot be converted to scalar

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_FAILURE;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
    }

    @Test
    void testExtractPath_destinationAttribute() throws Exception {
        String jsonPathAttrKey = "JsonPath";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(jsonPathAttrKey, "$[0]._id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertEquals("54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathAttrKey), "Transferred flow file did not have the correct result");
    }

    @Test
    void testExtractPath_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_JSON);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertEquals("54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathIdAttrKey), "Transferred flow file did not have the correct result for id attribute");
        assertEquals("{\"first\":\"Shaffer\",\"last\":\"Pearson\"}", out.getAttribute(jsonPathNameAttrKey), "Transferred flow file did not have the correct result for name attribute");
    }

    @Test
    void testExtractPath_destinationAttributes_twoPaths_notFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.PATH_NOT_FOUND, EvaluateJsonPath.PATH_NOT_FOUND_WARN);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id.nonexistent");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertEquals(StringUtils.EMPTY, out.getAttribute(jsonPathIdAttrKey), "Transferred flow file did not have the correct result for id attribute");
        assertEquals(StringUtils.EMPTY, out.getAttribute(jsonPathNameAttrKey), "Transferred flow file did not have the correct result for name attribute");
    }

    @Test
    void testExtractPath_destinationAttributes_twoPaths_oneFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.PATH_NOT_FOUND, EvaluateJsonPath.PATH_NOT_FOUND_IGNORE);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertEquals("54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathIdAttrKey), "Transferred flow file did not have the correct result for id attribute");
        assertEquals(StringUtils.EMPTY, out.getAttribute(jsonPathNameAttrKey), "Transferred flow file did not have the correct result for name attribute");
    }

    @Test
    void testExtractPath_destinationAttributes_twoPaths_oneFound_skipMissing() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.PATH_NOT_FOUND, EvaluateJsonPath.PATH_NOT_FOUND_SKIP);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertEquals("54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathIdAttrKey), "Transferred flow file did not have the correct result for id attribute");
        out.assertAttributeNotExists(jsonPathNameAttrKey);
    }

    @Test
    void testExtractPath_destinationContent() throws Exception {
        String jsonPathAttrKey = "JsonPath";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty(jsonPathAttrKey, "$[0]._id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        testRunner.getFlowFilesForRelationship(expectedRel).get(0).assertContentEquals("54df94072d5dbf7dc6340cc5");
    }

    @Test
    void testExtractPath_destinationContent_indefiniteResult() throws Exception {
        String jsonPathAttrKey = "friends.indefinite.id.list";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty(jsonPathAttrKey, "$[0].friends.[*].id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        testRunner.getFlowFilesForRelationship(expectedRel).get(0).assertContentEquals("[0,1,2]");
    }

    @Test
    void testExtractPath_destinationContent_indefiniteResult_operators() throws Exception {
        String jsonPathAttrKey = "friends.indefinite.id.list";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty(jsonPathAttrKey, "$[0].friends[?(@.id < 3)].id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        testRunner.getFlowFilesForRelationship(expectedRel).get(0).assertContentEquals("[0,1,2]");
    }

    @Test
    void testRouteUnmatched_destinationContent_noMatch() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty("jsonPath", "$[0].nonexistent.key");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_NO_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        testRunner.getFlowFilesForRelationship(expectedRel).get(0).assertContentEquals(JSON_SNIPPET);
    }

    @Test
    void testRouteFailure_returnTypeScalar_resultArray() throws Exception {
        String jsonPathAttrKey = "friends.indefinite.id.list";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_SCALAR);
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty(jsonPathAttrKey, "$[0].friends[?(@.id < 3)].id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_FAILURE;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        testRunner.getFlowFilesForRelationship(expectedRel).get(0).assertContentEquals(JSON_SNIPPET);
    }

    @Test
    void testNullInput() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_JSON);
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("stringField", "$.stringField");
        testRunner.setProperty("missingField", "$.missingField");
        testRunner.setProperty("nullField", "$.nullField");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("{\"stringField\": \"String Value\", \"nullField\": null}".getBytes(StandardCharsets.UTF_8));
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(EvaluateJsonPath.REL_MATCH, 1);

        FlowFile output = testRunner.getFlowFilesForRelationship(EvaluateJsonPath.REL_MATCH).get(0);

        String validFieldValue = output.getAttribute("stringField");
        assertEquals("String Value", validFieldValue);

        String missingValue = output.getAttribute("missingField");
        assertEquals("", missingValue, "Missing Value");

        String nullValue = output.getAttribute("nullField");
        assertEquals("", nullValue, "Null Value");
    }

    @Test
    void testNullInput_nullStringRepresentation() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_JSON);
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.NULL_VALUE_DEFAULT_REPRESENTATION, AbstractJsonPathProcessor.NULL_STRING_OPTION);
        testRunner.setProperty("stringField", "$.stringField");
        testRunner.setProperty("missingField", "$.missingField");
        testRunner.setProperty("nullField", "$.nullField");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.write(ff, out -> {
            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                outputStream.write("{\"stringField\": \"String Value\", \"nullField\": null}".getBytes(StandardCharsets.UTF_8));
            }
        });

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(EvaluateJsonPath.REL_MATCH, 1);

        FlowFile output = testRunner.getFlowFilesForRelationship(EvaluateJsonPath.REL_MATCH).get(0);

        String validFieldValue = output.getAttribute("stringField");
        assertEquals("String Value", validFieldValue);

        String missingValue = output.getAttribute("missingField");
        assertEquals("", missingValue, "Missing Value");

        String nullValue = output.getAttribute("nullField");
        assertEquals("null", nullValue, "Null Value");
    }

    @Test
    void testHandleAsciiControlCharacters() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_JSON);

        final String jsonPathControlCharKey = "evaluatejson.controlcharacterpath";

        testRunner.setProperty(jsonPathControlCharKey, "$.jinxing_json.object.property");

        testRunner.enqueue(Paths.get("src/test/resources/TestJson/control-characters.json"));
        testRunner.run();

        final Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        assertNotNull(out.getAttribute(jsonPathControlCharKey), "Transferred flow file did not have the correct result for id attribute");
    }
}
