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
package org.apache.nifi.processors.xmlcalabash;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.Assert;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;

public class TestPipelineXml {

    @Test
    public void testBadXPL() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        final ValidationResult vr = testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/bad.xpl");
        testRunner.assertNotValid();
        Assert.assertTrue(vr.getExplanation().indexOf("default value on a required option") >= 0);
    }

    @Test
    public void testOptionUsingDefault() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes1.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testPropertyOverrideOptionDefault() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.setProperty("attributeValB", "attrB");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes2.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testPropertyAsExpression() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.setProperty("attributeValA", "${filename}");
        testRunner.setProperty("attributeValB", "attrB");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes3.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfig() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        testRunner.setProperty(PipelineXml.BASE_URI, "http://example.com");
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes1.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfigNonFileInput() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        testRunner.setProperty(PipelineXml.BASE_URI, "http://example.com");
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty("attributeValA", "attrA");
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/input.xml"));
        testRunner.enqueue(bytes);
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes1.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfigNoBaseURI() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        //testRunner.setProperty(PipelineXml.BASE_URI, "http://example.com");
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.assertNotValid();
    }

    @Test
    public void testConfigBaseURINoScheme() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        testRunner.setProperty(PipelineXml.BASE_URI, "/path/to/file"); // missing a scheme
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.assertNotValid();
    }

    @Test
    public void testConfigBaseURISyntaxError() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        testRunner.setProperty(PipelineXml.BASE_URI, "$invalid:::"); // missing a scheme
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.assertNotValid();
    }

    @Test
    public void testBothConfigAndFile() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        testRunner.setProperty(PipelineXml.BASE_URI, "http://example.com");
        testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.assertNotValid();
    }

    @Test
    public void testNeighterConfigNorFile() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        //String config = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes.xpl")));
        //testRunner.setProperty(PipelineXml.XML_PIPELINE_CONFIG, config);
        //testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.assertNotValid();
    }

    @Test
    public void testMissingRequiredOption() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PipelineXml.REL_PIPELINE_FAILURE);
        final MockFlowFile failure = testRunner.getFlowFilesForRelationship(PipelineXml.REL_PIPELINE_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/input.xml")));
        failure.assertContentEquals(expectedContent);
    }

    @Test
    public void testMultipleOutputs() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/multipleOutputs.xpl");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
        testRunner.assertTransferCount("a", 1);
        testRunner.assertTransferCount("b", 2);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile a1 = testRunner.getFlowFilesForRelationship("a").get(0);
        final MockFlowFile b1 = testRunner.getFlowFilesForRelationship("b").get(0);
        final MockFlowFile b2 = testRunner.getFlowFilesForRelationship("b").get(1);
        a1.assertContentEquals("<a>Foo</a>");
        b1.assertContentEquals("<b>Bar</b>");
        b2.assertContentEquals("<b>Baz</b>");
        a1.assertAttributeEquals(FRAGMENT_INDEX.key(), "0");
        b1.assertAttributeEquals(FRAGMENT_INDEX.key(), "1");
        b2.assertAttributeEquals(FRAGMENT_INDEX.key(), "2");
        a1.assertAttributeEquals(FRAGMENT_COUNT.key(), "3");
        b1.assertAttributeEquals(FRAGMENT_COUNT.key(), "3");
        b2.assertAttributeEquals(FRAGMENT_COUNT.key(), "3");
    }

    @Test
    public void testNoOutputs() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/noOutputs.xpl");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"));
        testRunner.run();
    }

    @Test
    public void testNoPrimaryInputPort() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        final ValidationResult vr = testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/noPrimaryPort.xpl");
        testRunner.assertNotValid();
        Assert.assertTrue(vr.getExplanation().indexOf("primary non-parameter input port") >= 0);
    }

    @Test
    public void testInputNotXML() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        final ValidationResult vr = testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/noOutputs.xpl");
        testRunner.enqueue("not valid xml");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PipelineXml.REL_PIPELINE_FAILURE);
        final MockFlowFile failure = testRunner.getFlowFilesForRelationship(PipelineXml.REL_PIPELINE_FAILURE).get(0);
        failure.assertContentEquals("not valid xml");
    }

    @Test
    public void testBaseURIExpression() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("absolute.path", "/path/to/file");
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.setProperty("attributeValA", "attrA");
        testRunner.enqueue(Paths.get("src/test/resources/input.xml"), attributes);
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes1.xml")));
        result.assertContentEquals(expectedContent);
    }

    @Test
    public void testNonFileBaseInputFlowFile() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(PipelineXml.class);
        testRunner.setProperty(PipelineXml.XML_PIPELINE_FILE, "src/test/resources/addAttributes.xpl");
        testRunner.setProperty("attributeValA", "attrA");
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/input.xml"));
        testRunner.enqueue(bytes);
        testRunner.run();
        testRunner.assertTransferCount("result", 1);
        testRunner.assertTransferCount("original xml", 1);
        final MockFlowFile result = testRunner.getFlowFilesForRelationship("result").get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/addAttributes1.xml")));
        result.assertContentEquals(expectedContent);
    }

}
