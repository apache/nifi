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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.lookup.SimpleKeyValueLookupService;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTransformXml {
    private static final String XML_FOR_TESTING_PARAMETERS = """
                <?xml version="1.0" encoding="UTF-8"?>
                <data>
                    <item>Some data</item>
                </data>
                """;

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(TransformXml.class);
    }

    @Test
    public void testNonXmlContent() {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/math.xsl");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("not xml".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        final MockFlowFile original = runner.getFlowFilesForRelationship(TransformXml.REL_FAILURE).getFirst();
        original.assertContentEquals("not xml");
    }

    @Test
    public void testTransformMath() throws IOException {
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/math.xsl");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformCsv() throws IOException {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/tokens.xsl");
        runner.setProperty("uuid_0", "${uuid_0}");
        runner.setProperty("uuid_1", "${uuid_1}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid_0", "uuid_0");
        attributes.put("uuid_1", "uuid_1");

        StringBuilder builder = new StringBuilder();
        builder.append("<data>\n");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream("src/test/resources/TestTransformXml/tokens.csv")))) {

            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            builder.append("</data>");
            String data = builder.toString();
            runner.enqueue(data.getBytes(), attributes);
            runner.run();

            runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
            final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
            final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/tokens.xml")));

            transformed.assertContentEquals(expectedContent);
        }
    }

    @Test
    public void testTransformExpressionLanguage() throws IOException {
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "${xslt.path}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("xslt.path", "src/test/resources/TestTransformXml/math.xsl");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformNoCache() throws IOException {
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.CACHE_SIZE, "0");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/math.xsl");
        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformBothControllerFileNotValid() throws InitializationException {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/math.xsl");

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
        runner.setProperty(TransformXml.CACHE_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testTransformControllerNoKey() throws InitializationException {
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
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformWithXsltNotFoundInController() throws IOException, InitializationException {
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
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/math.html"))).trim();

        transformed.assertContentEquals(expectedContent);
    }

    @Test
    public void testTransformSecureProcessingEnabledXmlWithDocumentTypeDefinition() {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/doc-node.xsl");
        runner.setProperty(TransformXml.INDENT_OUTPUT, Boolean.FALSE.toString());
        runner.setProperty(TransformXml.SECURE_PROCESSING, Boolean.TRUE.toString());

        final String input = "<!DOCTYPE doc SYSTEM \"doc.dtd\"><doc></doc>";
        runner.enqueue(input);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();

        final String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><doc xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"/>";
        transformed.assertContentEquals(expected);
    }

    @Test
    public void testTransformSecureProcessingEnabledXmlWithEntity() {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/doc-node.xsl");
        runner.setProperty(TransformXml.INDENT_OUTPUT, Boolean.FALSE.toString());

        final String input = "<!DOCTYPE doc [<!ENTITY uri SYSTEM \"http://127.0.0.1\" >]><doc>&uri;</doc>";
        runner.enqueue(input);
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
    }

    @Test
    public void testTransformSecureProcessingEnabledXslWithEntity() throws IOException {
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/doctype-entity-file-uri.xsl");
        runner.setProperty(TransformXml.INDENT_OUTPUT, Boolean.FALSE.toString());

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/doctype-entity-file-uri.xsl"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();

        transformed.assertContentEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    }

    @Test
    public void testNonMatchingTemplateTag() throws IOException {
        runner.setProperty("header", "Test for mod");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/nonMatchingEndTag.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/math.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        MockComponentLog logger = runner.getLogger();
        assertFalse(logger.getErrorMessages().isEmpty());
        String firstMessage = logger.getErrorMessages().getFirst().getMsg();
        assertTrue(firstMessage.contains("xsl:template"));
    }

    @Test
    public void testMessageTerminate() throws IOException {
        runner.setProperty("header", "Test message terminate");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/employeeMessageTerminate.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/employee.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        MockComponentLog logger = runner.getLogger();
        assertFalse(logger.getErrorMessages().isEmpty());
        String firstMessage = logger.getErrorMessages().getFirst().getMsg();
        assertTrue(firstMessage.contains("xsl:message"));
    }

    @Test
    public void testMessageNonTerminate() throws IOException {
        runner.setProperty("header", "Test message non terminate");
        runner.setProperty(TransformXml.XSLT_DOCUMENT, "src/test/resources/TestTransformXml/employeeMessageNonTerminate.xsl");

        runner.enqueue(Paths.get("src/test/resources/TestTransformXml/employee.xml"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformXml/employee.html")));

        transformed.assertContentEquals(expectedContent);
        MockComponentLog logger = runner.getLogger();
        assertTrue(logger.getErrorMessages().isEmpty());
        assertTrue(logger.getWarnMessages().isEmpty());
    }

    @Test
    void testParameterDeclaredAndSet() {
        final String xslt = """
                <?xml version="1.0" encoding="UTF-8"?>
                <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:param name="customParam" select="'From XSLT'" />
                    <xsl:template match="/">
                        <root>
                            <message>
                                Value Selected: <xsl:value-of select="$customParam"/>
                            </message>
                        </root>
                    </xsl:template>
                </xsl:stylesheet>
                """;

        runner.setProperty(TransformXml.XSLT_DOCUMENT, xslt);
        runner.setProperty("customParam", "From NIFI");
        runner.enqueue(XML_FOR_TESTING_PARAMETERS);

        runner.run();
        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedTransform = """
                              <?xml version="1.0" encoding="UTF-8"?>
                              <root>
                                 <message>
                                              Value Selected: From NIFI</message>
                              </root>
                              """;
        transformed.assertContentEquals(expectedTransform);
    }

    @Test
    void testParameterSetButNotDeclared() {
        final String xslt = """
                <?xml version="1.0" encoding="UTF-8"?>
                <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:template match="/">
                        <root>
                            <message>
                                Value Selected:
                            </message>
                        </root>
                    </xsl:template>
                </xsl:stylesheet>
                """;

        runner.setProperty(TransformXml.XSLT_DOCUMENT, xslt);
        runner.setProperty("customParam", "From NIFI");
        runner.enqueue(XML_FOR_TESTING_PARAMETERS);

        runner.run();
        runner.assertAllFlowFilesTransferred(TransformXml.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformXml.REL_SUCCESS).getFirst();
        final String expectedTransform =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <root>
               <message>
                            Value Selected:
                        </message>
            </root>
            """;
        transformed.assertContentEquals(expectedTransform);
    }

    @Test
    void testParameterNotDeclaredButUsedInXslt() {
        final String xslt = """
                <?xml version="1.0" encoding="UTF-8"?>
                <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
                    <xsl:template match="/">
                        <root>
                            <message>
                                Value Selected: <xsl:value-of select="$customParam"/>
                            </message>
                        </root>
                    </xsl:template>
                </xsl:stylesheet>
                """;

        runner.setProperty(TransformXml.XSLT_DOCUMENT, xslt);
        runner.setProperty("customParam", "From NIFI");
        runner.enqueue(XML_FOR_TESTING_PARAMETERS);

        runner.run();

        runner.assertAllFlowFilesTransferred(TransformXml.REL_FAILURE);
        final MockComponentLog logger = runner.getLogger();
        final List<LogMessage> errorMessages = logger.getErrorMessages();
        assertTrue(errorMessages.getFirst().getMsg().contains("Variable $customParam has not been declared"));
    }

    @ParameterizedTest
    @MethodSource("parameterAsSpecificTypeArgs")
    void testParameterAsSpecificType(String paramType, String parameterValue, String defaultValue, Relationship expectedRelationship, String expectedTransform) {
        final String parameterName = "customParam";
        final String xslt = getXSLTWithParameterDefinedWithType(paramType, defaultValue);
        runner.setProperty(TransformXml.XSLT_DOCUMENT, xslt);
        runner.setProperty(parameterName, parameterValue);
        runner.enqueue(XML_FOR_TESTING_PARAMETERS);

        runner.run();
        runner.assertAllFlowFilesTransferred(expectedRelationship);

        if (expectedTransform != null) {
            final MockFlowFile transformed = runner.getFlowFilesForRelationship(expectedRelationship).getFirst();
            transformed.assertContentEquals(expectedTransform);
        }

        if (expectedRelationship == TransformXml.REL_FAILURE) {
            assertTrue(runner.getLogger().getErrorMessages().stream()
                       .map(LogMessage::getThrowable)
                       .map(ExceptionUtils::getStackTrace)
                       .anyMatch(stackTrace -> stackTrace.contains("ValidationException")));
        }
    }

    private static Stream<Arguments> parameterAsSpecificTypeArgs() {
        return Stream.of(
                Arguments.argumentSet("Valid number", "xs:integer", "100", "0", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:integer value is 100</message>
                        </report>
                        """),
                Arguments.argumentSet("Invalid number", "xs:integer", "NIFI", "0", TransformXml.REL_FAILURE, null),
                Arguments.argumentSet("Valid boolean lowercase", "xs:boolean", "true", "false", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:boolean value is true</message>
                        </report>
                        """),
                Arguments.argumentSet("Invalid boolean uppercase", "xs:boolean", "TRUE", "false", TransformXml.REL_FAILURE, null),
                Arguments.argumentSet("Valid ISO 8601 date", "xs:date", "2026-01-01", "xs:date('1970-01-01')", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:date value is 2026-01-01</message>
                        </report>
                        """, null), //29 April 2003
                Arguments.argumentSet("Invalid ISO 8601 date", "xs:date", "1 January 2026", "xs:date('1970-01-01')", TransformXml.REL_FAILURE, null),
                Arguments.argumentSet("Valid ISO 8601 date time", "xs:dateTime", "2026-01-01T00:00:00", "xs:dateTime('1970-01-01T00:00:00')", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:dateTime value is 2026-01-01T00:00:00</message>
                        </report>
                        """),
                Arguments.argumentSet("Invalid ISO 8601 date time", "xs:dateTime", "1970-01-01 00:00:00", "xs:dateTime('1970-01-01T00:00:00')", TransformXml.REL_FAILURE, null),
                Arguments.argumentSet("Valid time without timezone/offset", "xs:time", "12:34:56.789", "current-time()", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:time value is 12:34:56.789</message>
                        </report>
                        """),
                Arguments.argumentSet("Valid UTC time", "xs:time", "12:34:56Z", "current-time()", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:time value is 12:34:56Z</message>
                        </report>
                        """),
                Arguments.argumentSet("Valid Offset from UTC time", "xs:time", "12:34:56-05:00", "current-time()", TransformXml.REL_SUCCESS,
                        """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <report xmlns:xs="http://www.w3.org/2001/XMLSchema">
                           <message>Param of type xs:time value is 12:34:56-05:00</message>
                        </report>
                        """)

        );
    }

    private String getXSLTWithParameterDefinedWithType(String type, String defaultValue) {
        return """
                    <xsl:stylesheet version="3.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                                                                  xmlns:xs="http://www.w3.org/2001/XMLSchema">
                       <xsl:param name="customParam" as="%1$s" select="%2$s"/>
                       <xsl:template match="/">
                         <report>
                           <message>Param of type %1$s value is <xsl:value-of select="$customParam"/></message>
                         </report>
                       </xsl:template>
                     </xsl:stylesheet>
                    """.formatted(type, defaultValue);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("xslt-controller", TransformXml.XSLT_CONTROLLER.getName()),
                Map.entry("xslt-controller-key", TransformXml.XSLT_CONTROLLER_KEY.getName()),
                Map.entry("XSLT Lookup key", TransformXml.XSLT_CONTROLLER_KEY.getName()),
                Map.entry("indent-output", TransformXml.INDENT_OUTPUT.getName()),
                Map.entry("secure-processing", TransformXml.SECURE_PROCESSING.getName()),
                Map.entry("cache-size", TransformXml.CACHE_SIZE.getName()),
                Map.entry("cache-ttl-after-last-access", TransformXml.CACHE_TTL_AFTER_LAST_ACCESS.getName()),
                Map.entry("XSLT file name", TransformXml.XSLT_DOCUMENT.getName()),
                Map.entry("XSLT File Name", TransformXml.XSLT_DOCUMENT.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
