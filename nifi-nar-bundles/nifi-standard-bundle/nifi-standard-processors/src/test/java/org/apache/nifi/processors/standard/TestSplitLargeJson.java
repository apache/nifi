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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.util.SimpleJsonPath;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;

public class TestSplitLargeJson {

    private static final Path WEATHER_JSON = Paths.get("src/test/resources/TestSplitLargeJson/weather.json");
    private static final Path MISC_JSON    = Paths.get("src/test/resources/TestSplitLargeJson/misc.json");
    private static final Path SAMPLE_XML   = Paths.get("src/test/resources/TestSplitLargeJson/sample.xml");

    @Test
    public void testInvalidJsonPaths() {
        String[] badValues = { //some of these are normally OK json-paths, but not for streaming mode
            "$..", "$.asdf[:2]", "$.store..price", "$.book[?(@.isbn)]", "$.book[0].", "$.store/price"
        };
        for (String bad : badValues) {
            boolean failed = false;
            try {
                SimpleJsonPath.of(bad);
            } catch (Exception e) {
                failed = true;
            }
            Assert.assertTrue(String.format("Path should be invalid but was valid: %s", bad), failed);
        }
    }

    @Test
    public void testValidJsonPaths() {
        String[] okValues = {
            "$", "$.asdf", "$.asdf[1]", "$.asdf.qwer", "$['address']['city']", "$['phoneNumbers'][0]",
            "$.store.book[*].author", "$[0].blah.*[4].asdf"
        };
        for (String ok : okValues) {
            try {
                SimpleJsonPath.of(ok);
            } catch (Exception e) {
                Assert.fail(String.format("Path should be valid but was not: %s (%s)", ok, e.getMessage()));
            }
        }
    }

    @Test
    public void testProcessorValidation() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitLargeJson());
        testRunner.setProperty(SplitLargeJson.JSON_PATH_EXPRESSION, "badpath!");
        testRunner.assertNotValid();
    }

    @Test
    public void invalidJson() throws Exception {
        final TestRunner testRunner = newTestRunner(SAMPLE_XML, "$");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SplitLargeJson.REL_FAILURE, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(SplitLargeJson.REL_FAILURE).get(0);
        out.assertContentEquals(SAMPLE_XML);
    }

    @Test
    public void contiguousScalar() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[0].name");
        testRunner.run();

        Relationship expectedRel = SplitLargeJson.REL_FAILURE;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        out.assertContentEquals(WEATHER_JSON);
    }

    @Test
    public void contiguousScalar2() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[0]['name']");
        testRunner.run();

        Relationship expectedRel = SplitLargeJson.REL_FAILURE;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        out.assertContentEquals(WEATHER_JSON);
    }

    @Test
    public void contiguousArrayOfObject() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[1].weather");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, WEATHER_JSON, "{\"main\":\"Mist\",\"description\":\"mist\"}", testRunner);
        checkSplit(1, WEATHER_JSON, "{\"main\":\"Fog\",\"description\":\"fog\"}",   testRunner);
    }

    @Test
    public void contiguousObject() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[1].main");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 5, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 5);
        checkSplit(1, WEATHER_JSON, "{\"humidity\":93}", testRunner);
    }

    @Test
    public void rootSplitArray() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$");
        testRunner.run();
        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
    }

    @Test
    public void rootSplitObject() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$");
        testRunner.run();
        checkOriginal(MISC_JSON, 5, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 5);
        checkSplit(0, MISC_JSON, "{\"autumn\":\"leaves\"}", testRunner);
    }

    @Test
    public void impliedRootSplitArray() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*]");
        testRunner.run();
        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
    }

    @Test
    public void impliedRootSplitObject() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.*");
        testRunner.run();
        checkOriginal(MISC_JSON, 5, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 5);
        checkSplit(0, MISC_JSON, "{\"autumn\":\"leaves\"}", testRunner);
    }

    @Test
    public void disjointSetOfObjects() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*].main");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(1, WEATHER_JSON,
                "{\"temp\":56.14,\"humidity\":93,\"temp_min\":50,\"temp_max\":62.6,\"something\":[4,5,6]}",
                testRunner);
    }

    @Test
    public void disjointSetOfArrays() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*].weather");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, WEATHER_JSON, "[{\"main\":\"Snow\",\"description\":\"light snow\"}]", testRunner);
    }

    @Test
    public void contiguousArrayOfArray() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.two-d");
        testRunner.run();

        checkOriginal(MISC_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(2, MISC_JSON, "[2,0,1,8]", testRunner);
    }

    @Test
    public void contiguousMixedArray() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.mixed-arr");
        testRunner.run();

        checkOriginal(MISC_JSON, 7, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 7);
        checkSplit(0, MISC_JSON, "[true]", testRunner);
        checkSplit(1, MISC_JSON, "[false]", testRunner);
        checkSplit(2, MISC_JSON, "[null]", testRunner);
        checkSplit(3, MISC_JSON, "[23]", testRunner);
        checkSplit(4, MISC_JSON, "[3.14]", testRunner);
        checkSplit(5, MISC_JSON, "[\"pi\"]", testRunner);
        checkSplit(6, MISC_JSON, "{\"g\":3927}", testRunner);
    }

    @Test
    public void arrayOffsetFromWildcard() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.fence.*[1]");
        testRunner.run();
        
        checkOriginal(MISC_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, MISC_JSON, "[\"brown\"]", testRunner);
        checkSplit(1, MISC_JSON, "[\"hinges\"]", testRunner);
    }

    @Test
    public void disjointEmbeddedStar() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.nest.*.red");
        testRunner.run();

        checkOriginal(MISC_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, MISC_JSON, "{\"red\":3}", testRunner);
        checkSplit(1, MISC_JSON, "{\"red\":7}", testRunner);
    }

    @Test
    public void disjointEmbeddedStar2() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.nest[*]['red']");
        testRunner.run();

        checkOriginal(MISC_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, MISC_JSON, "{\"red\":3}", testRunner);
        checkSplit(1, MISC_JSON, "{\"red\":7}", testRunner);
    }

    @Test
    public void disjointEmbeddedStar3() throws Exception {
        final TestRunner testRunner = newTestRunner(MISC_JSON, "$.nest.arr[*]['orange']");
        testRunner.run();

        checkOriginal(MISC_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, MISC_JSON, "{\"orange\":9}", testRunner);
        checkSplit(1, MISC_JSON, "{\"orange\":2}", testRunner);
    }

    @Test
    public void disjointSetOfScalarsArrContext() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*].main.something[1]");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, WEATHER_JSON, "[2]", testRunner);
        checkSplit(1, WEATHER_JSON, "[5]", testRunner);
        checkSplit(2, WEATHER_JSON, "[8]", testRunner);
    }

    @Test
    public void disjointSetOfScalarsArrContext2() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*]['main']['something'][1]");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, WEATHER_JSON, "[2]", testRunner);
        checkSplit(1, WEATHER_JSON, "[5]", testRunner);
        checkSplit(2, WEATHER_JSON, "[8]", testRunner);
    }

    @Test
    public void disjointSetOfScalarsObjContext() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitLargeJson());
        testRunner.setProperty(SplitLargeJson.JSON_PATH_EXPRESSION, "$[*].name");
        final String filename = "test.json";

        testRunner.enqueue(WEATHER_JSON, new HashMap<String, String>() {
            {
                put(CoreAttributes.FILENAME.key(), filename);
            }
        });
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, filename, "{\"name\":\"Seattle\"}", testRunner);
        checkSplit(1, filename, "{\"name\":\"Washington, DC\"}", testRunner);
        checkSplit(2, filename, "{\"name\":\"Miami\"}", testRunner);
    }

    @Test
    public void pathNotFound() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$.nonexistent");
        testRunner.run();

        testRunner.assertTransferCount(SplitLargeJson.REL_FAILURE, 1);
        testRunner.getFlowFilesForRelationship(SplitLargeJson.REL_FAILURE).get(0).assertContentEquals(WEATHER_JSON);
    }

    @Test
    public void contiguousDotStarEnd() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[0].coord.*");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 2, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 2);
        checkSplit(0, WEATHER_JSON, "{\"lon\":-122.33}", testRunner);
    }

    @Test
    public void contiguousBracketStarEnd() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[0].weather[*]");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 1, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 1);
        checkSplit(0, WEATHER_JSON, "{\"main\":\"Snow\",\"description\":\"light snow\"}", testRunner);
    }

    @Test
    public void disjointDotStarEnd() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*].coord.*");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, WEATHER_JSON, "{\"lon\":-122.33,\"lat\":47.61}", testRunner);
    }

    @Test
    public void disjointBracketStarEnd() throws Exception {
        final TestRunner testRunner = newTestRunner(WEATHER_JSON, "$[*].weather[*]");
        testRunner.run();

        checkOriginal(WEATHER_JSON, 3, testRunner);
        testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 3);
        checkSplit(0, WEATHER_JSON, "[{\"main\":\"Snow\",\"description\":\"light snow\"}]", testRunner);
    }

    private TestRunner newTestRunner(Path testFile, String path) throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitLargeJson());
        testRunner.setProperty(SplitLargeJson.JSON_PATH_EXPRESSION, path);
        testRunner.enqueue(testFile);
        return testRunner;
    }

    private void printSplit(TestRunner testRunner, int i) {
        MockFlowFile split = testRunner.getFlowFilesForRelationship(SplitLargeJson.REL_SPLIT).get(i);
        System.out.printf("\nSplit %d:\n", i);
        System.out.println(new String(testRunner.getContentAsByteArray(split)));
    }

    private void checkOriginal(Path path, int numSplitsExpected, TestRunner testRunner) throws IOException {
        testRunner.assertTransferCount(SplitLargeJson.REL_ORIGINAL, 1);
        final MockFlowFile orig = testRunner.getFlowFilesForRelationship(SplitLargeJson.REL_ORIGINAL).get(0);
        orig.assertAttributeExists(FRAGMENT_ID.key());
        orig.assertAttributeEquals(FRAGMENT_COUNT.key(), String.valueOf(numSplitsExpected));
        orig.assertContentEquals(path);
    }

    private void checkSplit(int i, Path path, String content, TestRunner testRunner) {
        checkSplit(i, path.getFileName().toString(), content, testRunner);
    }

    private void checkSplit(int i, String fileName, String content, TestRunner testRunner) {
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(SplitLargeJson.REL_SPLIT).get(i);
        flowFile.assertContentEquals(content);
        flowFile.assertAttributeEquals(FRAGMENT_INDEX.key(), String.valueOf(i));
        flowFile.assertAttributeEquals(SEGMENT_ORIGINAL_FILENAME.key(), fileName);
    }

}
