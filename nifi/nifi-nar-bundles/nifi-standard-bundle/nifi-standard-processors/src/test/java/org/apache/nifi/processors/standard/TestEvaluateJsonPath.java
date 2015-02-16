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

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestEvaluateJsonPath {

    private static final Path JSON_SNIPPET = Paths.get("src/test/resources/TestJson/json-sample.json");
    private static final Path XML_SNIPPET = Paths.get("src/test/resources/TestXml/xml-snippet.xml");

    @Test(expected = AssertionError.class)
    public void testInvalidJsonPath() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("invalid.jsonPath", "$..");

        Assert.fail("An improper JsonPath expression was not detected as being invalid.");
    }

    @Test
    public void testInvalidJsonDocument() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateJsonPath.REL_FAILURE, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateJsonPath.REL_FAILURE).get(0);
    }


    @Test(expected = AssertionError.class)
    public void testInvalidConfiguration_destinationContent_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_CONTENT);
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Assert.fail("Processor incorrectly ran with an invalid configuration of multiple paths specified as attributes for a destination of content.");
    }

    @Test
    public void testConfiguration_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();
    }

    @Test
    public void testExtractPath_destinationAttribute() throws Exception {
        String jsonPathAttrKey = "JsonPath";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(jsonPathAttrKey, "$[0]._id");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result", "54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathAttrKey));
    }

    @Test
    public void testExtractPath_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result for id attribute", "54df94072d5dbf7dc6340cc5", out.getAttribute(jsonPathIdAttrKey));
        Assert.assertEquals("Transferred flow file did not have the correct result for name attribute", "{\"first\":\"Shaffer\",\"last\":\"Pearson\"}", out.getAttribute(jsonPathNameAttrKey));
    }

    @Test
    public void testExtractPath_destinationAttributes_twoPaths_notFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.DESTINATION, EvaluateJsonPath.DESTINATION_ATTRIBUTE);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id.nonexistent");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(JSON_SNIPPET);
        testRunner.run();

        Relationship expectedRel = EvaluateJsonPath.REL_MATCH;

        testRunner.assertAllFlowFilesTransferred(expectedRel, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(expectedRel).get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result for id attribute", "", out.getAttribute(jsonPathIdAttrKey));
        Assert.assertEquals("Transferred flow file did not have the correct result for name attribute", "", out.getAttribute(jsonPathNameAttrKey));
    }

    @Test
    public void testExtractPath_destinationContent() throws Exception {
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
}
