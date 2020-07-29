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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.xpath.XPathFactoryConfigurationException;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestEvaluateXPath {

    private static final Path XML_SNIPPET = Paths.get("src/test/resources/TestXml/xml-snippet.xml");
    private static final Path XML_SNIPPET_EMBEDDED_DOCTYPE = Paths.get("src/test/resources/TestXml/xml-snippet-embedded-doctype.xml");
    private static final Path XML_SNIPPET_NONEXISTENT_DOCTYPE = Paths.get("src/test/resources/TestXml/xml-snippet-external-doctype.xml");

    @Test
    public void testAsAttribute() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xpath.result1", "/");
        testRunner.setProperty("xpath.result2", "/*:bundle/node/subNode/value/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        out.assertAttributeEquals("xpath.result2", "Hello");
        assertTrue(out.getAttribute("xpath.result1").contains("Hello"));
    }

    @Test
    public void testCheckIfElementExists() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xpath.result1", "/");
        testRunner.setProperty("xpath.result.exist.1", "boolean(/*:bundle/node)");
        testRunner.setProperty("xpath.result.exist.2", "boolean(/*:bundle/node2)");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        assertTrue(out.getAttribute("xpath.result1").contains("Hello"));
        out.assertAttributeEquals("xpath.result.exist.1", "true");
        out.assertAttributeEquals("xpath.result.exist.2", "false");
    }

    @Test
    public void testUnmatched() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty("xpath.result.exist.2", "/*:bundle/node2");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_NO_MATCH, 1);
        testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_NO_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test(expected = java.lang.AssertionError.class)
    public void testMultipleXPathForContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_AUTO);
        testRunner.setProperty("some.property.1", "/*:bundle/node/subNode[1]");
        testRunner.setProperty("some.property.2", "/*:bundle/node/subNode[2]");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();
    }

    @Test
    public void testWriteToContent() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, "UTF-8");
        assertTrue(outXml.contains("subNode"));
        assertTrue(outXml.contains("Hello"));
    }

    @Test
    public void testFailureIfContentMatchesMultipleNodes() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_FAILURE, 1);
    }

    @Test
    public void testWriteStringToContent() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_STRING);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, "UTF-8");
        assertTrue(outXml.trim().equals("Hello"));
    }

    @Test
    public void testWriteNodeSetToAttribute() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_ATTRIBUTE);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_NODESET);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final String outXml = out.getAttribute("some.property");
        assertTrue(outXml.contains("subNode"));
        assertTrue(outXml.contains("Hello"));
    }

    @Test
    public void testSuccessForEmbeddedDocTypeValidation() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_STRING);
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "true");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_EMBEDDED_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, "UTF-8");
        assertTrue(outXml.trim().equals("Hello"));
    }

    @Test
    public void testSuccessForEmbeddedDocTypeValidationDisabled() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_STRING);
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_EMBEDDED_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, "UTF-8");
        assertTrue(outXml.trim().equals("Hello"));
    }

    @Test
    public void testFailureForExternalDocTypeWithDocTypeValidationEnabled() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_STRING);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_NONEXISTENT_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_FAILURE, 1);
    }

    @Test
    public void testSuccessForExternalDocTypeWithDocTypeValidationDisabled() throws XPathFactoryConfigurationException, IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXPath());
        testRunner.setProperty(EvaluateXPath.DESTINATION, EvaluateXPath.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXPath.RETURN_TYPE, EvaluateXPath.RETURN_TYPE_STRING);
        testRunner.setProperty(EvaluateXPath.VALIDATE_DTD, "false");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_NONEXISTENT_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXPath.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXPath.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, "UTF-8");
        assertTrue(outXml.trim().equals("Hello"));
    }

}
