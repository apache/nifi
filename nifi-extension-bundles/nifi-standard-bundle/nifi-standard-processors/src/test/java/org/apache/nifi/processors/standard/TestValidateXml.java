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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestValidateXml {

    private static final String VALID_XML = "<ns:bundle xmlns:ns=\"http://namespace/1\"><node><subNode><value>Hello</value></subNode>" +
            "<subNode><value>World!</value></subNode></node></ns:bundle>";
    private static final String INVALID_XML = "<this>is an invalid</xml>";
    private static final String NONCOMPLIANT_XML = "<ns:bundle xmlns:ns=\"http://namespace/1\"><this>is good XML, but violates schema</this></ns:bundle>";

    @Test
    public void testValid() throws IOException {
        // Valid XML in FF content, XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "src/test/resources/TestXml/XmlBundle.xsd");

        runner.enqueue(Paths.get("src/test/resources/TestXml/xml-snippet.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_VALID, 1);
    }

    @Test
    public void testInvalid() {
        // Invalid XML in FF content, XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "src/test/resources/TestXml/XmlBundle.xsd");

        runner.enqueue(INVALID_XML);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertErrorAttributeContainsStableErrorKeyword(runner);

        runner.clearTransferState();
        runner.enqueue(NONCOMPLIANT_XML);

        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertErrorAttributeContainsStableErrorKeyword(runner);
    }

    private void assertErrorAttributeContainsStableErrorKeyword(TestRunner runner) {
        String errorAttribute = runner.getFlowFilesForRelationship(ValidateXml.REL_INVALID).get(0).getAttribute(ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertTrue(errorAttribute.contains("lineNumber"));
    }

    @Test
    public void testValidEL() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "${my.schema}");
        runner.setEnvironmentVariableValue("my.schema", "src/test/resources/TestXml/XmlBundle.xsd");

        runner.enqueue(Paths.get("src/test/resources/TestXml/xml-snippet.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_VALID, 1);
    }

    @Test
    public void testInvalidEL() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "${my.schema}");

        runner.enqueue(INVALID_XML);
        assertThrows(AssertionError.class, () -> {
            runner.run();
        });
    }

    @Test
    public void testValidXMLAttributeWithSchema()  {
        // Valid XML in FF attribute, XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "src/test/resources/TestXml/XmlBundle.xsd");
        runner.setProperty(ValidateXml.XML_SOURCE_ATTRIBUTE, "xml.attribute");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("xml.attribute", VALID_XML);

        runner.enqueue("XML is in attribute, not content", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_VALID, 1);
    }

    @Test
    public void testInvalidXMLAttributeWithSchema() {
        // Invalid XML in FF attribute, XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.SCHEMA_FILE, "src/test/resources/TestXml/XmlBundle.xsd");
        runner.setProperty(ValidateXml.XML_SOURCE_ATTRIBUTE, "xml.attribute");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("xml.attribute", INVALID_XML);

        runner.enqueue("flowfile content is irrelevant", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertErrorAttributeContainsStableErrorKeyword(runner);

        runner.clearTransferState();
        attributes.clear();
        attributes.put("xml.attribute", NONCOMPLIANT_XML);

        runner.enqueue("flowfile content is irrelevant", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertErrorAttributeContainsStableErrorKeyword(runner);
    }

    @Test
    public void testValidXMLAttributeStructure() {
        // Valid XML in FF attribute, no XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.XML_SOURCE_ATTRIBUTE, "xml.attribute");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("xml.attribute", VALID_XML);

        runner.enqueue("XML is in attribute, not content", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_VALID, 1);
    }

    @Test
    public void testInvalidXMLAttributeStructure() {
        // Invalid XML in FF attribute, no XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());
        runner.setProperty(ValidateXml.XML_SOURCE_ATTRIBUTE, "xml.attribute");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("xml.attribute", INVALID_XML);

        runner.enqueue("XML is in attribute, not content", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        String errorAttribute = runner.getFlowFilesForRelationship(ValidateXml.REL_INVALID).get(0).getAttribute(ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertTrue(errorAttribute.contains("ParseError"));
    }

    @Test
    public void testValidXMLContentStructure() throws IOException {
        // Valid XML in FF content, no XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());

        runner.enqueue(Paths.get("src/test/resources/TestXml/xml-snippet.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_VALID, 1);
    }

    @Test
    public void testInvalidXMLContentStructure() {
        // Invalid XML in FF content, no XSD provided
        final TestRunner runner = TestRunners.newTestRunner(new ValidateXml());

        runner.enqueue(INVALID_XML);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateXml.REL_INVALID, 1);
        runner.assertAllFlowFilesContainAttribute(ValidateXml.REL_INVALID, ValidateXml.ERROR_ATTRIBUTE_KEY);
        String errorAttribute = runner.getFlowFilesForRelationship(ValidateXml.REL_INVALID).get(0).getAttribute(ValidateXml.ERROR_ATTRIBUTE_KEY);
        assertTrue(errorAttribute.contains("ParseError"));
    }
}
