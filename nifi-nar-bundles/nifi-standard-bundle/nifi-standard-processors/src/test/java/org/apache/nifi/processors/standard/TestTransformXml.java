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

import org.apache.nifi.lookup.SimpleKeyValueLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTransformXml {

    @Test
    public void testStylesheetNotFound() {
        final TestRunner controller = TestRunners.newTestRunner(TransformXml.class);
        controller.setProperty(TransformXml.XSLT_FILE_NAME, "/no/path/to/math.xsl");
        controller.assertNotValid();
    }

    @Test
    public void testNonXmlContent() {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/math.xsl");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("not xml".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        final MockFlowFile original = runner.getFlowFilesForRelationship(TransformXml.REL_FAILURE).get(0);
        original.assertContentEquals("not xml");
    }

    @Test
    public void testTransformMath() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/math.xsl");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformCsv() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/tokens.xsl");
        runner.setProperty("uuid_0", "${uuid_0}");
        runner.setProperty("uuid_1", "${uuid_1}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid_0", "uuid_0");
        attributes.put("uuid_1", "uuid_1");

        StringBuilder builder = new StringBuilder();
        builder.append("<data>\n");

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream("src/test/resources/TestTransformXml/tokens.csv")))){


            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            builder.append("</data>");
            String data = builder.toString();
            runner.enqueue(data.getBytes(), attributes);
            runner.run();

            runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
            final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
            final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/tokens.xml")));

            transformed.assertContentEquals(expectedContent);
        }
    }

    @Test
    public void testTransformExpressionLanguage() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "${xslt.path}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xslt.path", "src/test/resources/TestTransformXml/math.xsl");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformNoCache() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.CACHE_SIZE, "0");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/math.xsl");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformBothControllerFileNotValid() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/math.xsl");

        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(TransformXml.XSLT_CONTROLLER, "simple-key-value-lookup-service");

        runner.assertNotValid();
    }

    @Test
    public void testTransformNoneControllerFileNotValid() {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.CACHE_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testTransformControllerNoKey() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());

        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(TransformXml.XSLT_CONTROLLER, "simple-key-value-lookup-service");

        runner.assertNotValid();
    }

    @Test
    public void testTransformWithController() throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());

        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "math", "<xsl:stylesheet version=\"2.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                + "<xsl:param name=\"header\" /><xsl:template match=\"doc\">"
                + "<HTML><H1><xsl:value-of select=\"$header\"/></H1><HR/>"
                + "<P>Should say \"1\": <xsl:value-of select=\"5 mod 2\"/></P>"
                + "<P>Should say \"1\": <xsl:value-of select=\"n1 mod n2\"/></P>"
                + "<P>Should say \"-1\": <xsl:value-of select=\"div mod mod\"/></P>"
                + "<P><xsl:value-of select=\"div or ((mod)) | or\"/></P>"
                + "</HTML></xsl:template></xsl:stylesheet>");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(TransformXml.XSLT_CONTROLLER, "simple-key-value-lookup-service");
        runner.setProperty(TransformXml.XSLT_CONTROLLER_KEY, "${xslt}");
        runner.setProperty("header", "Test for mod");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xslt", "math");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformWithXsltNotFoundInController() throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());

        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(TransformXml.XSLT_CONTROLLER, "simple-key-value-lookup-service");
        runner.setProperty(TransformXml.XSLT_CONTROLLER_KEY, "${xslt}");
        runner.setProperty("header", "Test for mod");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xslt", "math");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
    }

    @Test
    public void testTransformWithControllerNoCache() throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());

        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "math", "<xsl:stylesheet version=\"2.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                + "<xsl:param name=\"header\" /><xsl:template match=\"doc\">"
                + "<HTML><H1><xsl:value-of select=\"$header\"/></H1><HR/>"
                + "<P>Should say \"1\": <xsl:value-of select=\"5 mod 2\"/></P>"
                + "<P>Should say \"1\": <xsl:value-of select=\"n1 mod n2\"/></P>"
                + "<P>Should say \"-1\": <xsl:value-of select=\"div mod mod\"/></P>"
                + "<P><xsl:value-of select=\"div or ((mod)) | or\"/></P>"
                + "</HTML></xsl:template></xsl:stylesheet>");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(TransformXml.XSLT_CONTROLLER, "simple-key-value-lookup-service");
        runner.setProperty(TransformXml.XSLT_CONTROLLER_KEY, "${xslt}");
        runner.setProperty(TransformXml.CACHE_SIZE, "0");
        runner.setProperty("header", "Test for mod");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xslt", "math");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformSecureProcessingEnabledXmlWithEntity() {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/doc-node.xsl");
        runner.setProperty(TransformXml.INDENT_OUTPUT, Boolean.FALSE.toString());

        final String input = "<!DOCTYPE doc [<!ENTITY uri SYSTEM \"http://127.0.0.1\" >]><doc>&uri;</doc>";
        runner.enqueue(input);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);

        final String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><doc xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"/>";
        transformed.assertContentEquals(expected);
    }

    @Test
    public void testTransformSecureProcessingEnabledXslWithEntity() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/doctype-entity-file-uri.xsl");
        runner.setProperty(TransformXml.INDENT_OUTPUT, Boolean.FALSE.toString());

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/doctype-entity-file-uri.xsl"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);

        transformed.assertContentEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    }

    @Test
    public void testNonMatchingTemplateTag() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/nonMatchingEndTag.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        MockComponentLog logger = runner.getLogger();
        assertFalse(logger.getErrorMessages().isEmpty());
        String firstMessage = logger.getErrorMessages().get(0).getMsg();
        assertTrue(firstMessage.contains("xsl:template"));
    }

    @Test
    public void testMessageTerminate() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test message terminate");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/employeeMessageTerminate.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/employee.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        MockComponentLog logger = runner.getLogger();
        assertFalse(logger.getErrorMessages().isEmpty());
        String firstMessage = logger.getErrorMessages().get(0).getMsg();
        assertTrue(firstMessage.contains("xsl:message"));
    }

    @Test
    public void testMessageNonTerminate() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformXml());
        runner.setProperty("header", "Test message non terminate");
        runner.setProperty(TransformXml.XSLT_FILE_NAME, "src/test/resources/TestTransformXml/employeeMessageNonTerminate.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/employee.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/employee.html")));

        transformed.assertContentEquals(expectedContent);
        MockComponentLog logger = runner.getLogger();
        assertTrue(logger.getErrorMessages().isEmpty());
        assertTrue(logger.getWarnMessages().isEmpty());
    }
}
