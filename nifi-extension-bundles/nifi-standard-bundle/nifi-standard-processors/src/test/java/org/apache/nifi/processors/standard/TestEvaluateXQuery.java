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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.Test;

public class TestEvaluateXQuery {

    private static final Path XML_SNIPPET = Paths.get("src/test/resources/TestXml/fruit.xml");
    private static final Path XML_SNIPPET_EMBEDDED_DOCTYPE = Paths.get("src/test/resources/TestXml/xml-snippet-embedded-doctype.xml");
    private static final Path XML_SNIPPET_NONEXISTENT_DOCTYPE = Paths.get("src/test/resources/TestXml/xml-snippet-external-doctype.xml");

    private static final String[] fruitNames = {"apple", "apple", "banana", "orange", "blueberry", "raspberry", "none"};

    private static final String[] methods = {EvaluateXQuery.OUTPUT_METHOD_XML, EvaluateXQuery.OUTPUT_METHOD_HTML, EvaluateXQuery.OUTPUT_METHOD_TEXT};
    private static final boolean[] booleans = {true, false};

    @Test
    public void testFormatting() throws Exception {

        List<String> formattedResults;
        final String atomicQuery = "count(//fruit)";
        final String singleElementNodeQuery = "//fruit[1]";
        final String singleTextNodeQuery = "//fruit[1]/name/text()";

        for (final String method : methods) {
            for (final boolean indent : booleans) {
                for (final boolean omitDeclaration : booleans) {
                    formattedResults = getFormattedResult(XML_SNIPPET, atomicQuery, method, indent, omitDeclaration);
                    assertEquals(1, formattedResults.size());
                    assertEquals("7", formattedResults.get(0));
                }
            }
        }

        for (final String method : methods) {
            for (final boolean indent : booleans) {
                for (final boolean omitDeclaration : booleans) {
                    formattedResults = getFormattedResult(XML_SNIPPET, singleTextNodeQuery, method, indent, omitDeclaration);
                    assertEquals(1, formattedResults.size());
                    assertEquals("apple", formattedResults.get(0));
                }
            }
        }
        {
            formattedResults = getFormattedResult(XML_SNIPPET, singleElementNodeQuery, "xml", false, false);
            assertEquals(1, formattedResults.size());
            String expectedXml
                    = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\">\n"
                    + "<!-- Apples are my favorite -->\n"
                    + "    <name>apple</name>\n"
                    + "    <color>red</color>\n"
                    + "  </fruit>";
            assertEquals(spaceTrimmed(expectedXml), spaceTrimmed(formattedResults.get(0)));
        }
        {
            formattedResults = getFormattedResult(XML_SNIPPET, singleElementNodeQuery, "html", false, false);
            assertEquals(1, formattedResults.size());
            String expectedXml
                    = "<fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\">\n"
                    + "    <!-- Apples are my favorite -->\n"
                    + "    <name>apple</name>\n"
                    + "    <color>red</color>\n"
                    + "  </fruit>";
            assertEquals(spaceTrimmed(expectedXml), spaceTrimmed(formattedResults.get(0)));
        }
        {
            formattedResults = getFormattedResult(XML_SNIPPET, singleElementNodeQuery, "text", false, false);
            assertEquals(1, formattedResults.size());
            String expectedXml
                    = "\n    \n"
                    + "    apple\n"
                    + "    red\n"
                    + "  ";
            assertEquals(spaceTrimmed(expectedXml), spaceTrimmed(formattedResults.get(0)));
        }
        {
            formattedResults = getFormattedResult(XML_SNIPPET, singleElementNodeQuery, "xml", true, false);
            assertEquals(1, formattedResults.size());
            String expectedXml
                    = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    + "<fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\">\n"
                    + "    <!-- Apples are my favorite -->\n"
                    + "    <name>apple</name>\n"
                    + "    <color>red</color>\n"
                    + "  </fruit>\n";
            assertEquals(spaceTrimmed(expectedXml), spaceTrimmed(formattedResults.get(0)));
        }
        {
            formattedResults = getFormattedResult(XML_SNIPPET, singleElementNodeQuery, "xml", true, true);
            assertEquals(1, formattedResults.size());
            String expectedXml
                    = "<fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\">\n"
                    + "    <!-- Apples are my favorite -->\n"
                    + "    <name>apple</name>\n"
                    + "    <color>red</color>\n"
                    + "  </fruit>\n";
            assertEquals(spaceTrimmed(expectedXml), spaceTrimmed(formattedResults.get(0)));
        }
    }

    private String spaceTrimmed(String str) {
        return Arrays.stream(str.split("\n")).map(String :: trim).reduce("", String :: concat);
    }

    private List<String> getFormattedResult(Path xml, final String xQuery, final String method, final boolean indent, final boolean omitDeclaration) throws Exception {

        Map<String, String> runnerProps = new HashMap<>();
        List<MockFlowFile> resultFlowFiles;
        List<String> resultStrings = new ArrayList<>();

        runnerProps.put(EvaluateXQuery.DESTINATION.getName(), EvaluateXQuery.DESTINATION_CONTENT);
        runnerProps.put(EvaluateXQuery.XML_OUTPUT_METHOD.getName(), method);
        runnerProps.put(EvaluateXQuery.XML_OUTPUT_INDENT.getName(), Boolean.toString(indent));
        runnerProps.put(EvaluateXQuery.XML_OUTPUT_OMIT_XML_DECLARATION.getName(), Boolean.toString(omitDeclaration));
        runnerProps.put("xquery", xQuery);
        resultFlowFiles = runXquery(xml, runnerProps);

        for (final MockFlowFile flowFile : resultFlowFiles) {
            final byte[] outData = flowFile.toByteArray();
            final String outXml = new String(outData, StandardCharsets.UTF_8);
            resultStrings.add(outXml);
        }

        return resultStrings;
    }

    @Test
    public void testBadXQuery() {
        assertThrows(AssertionError.class, () -> doXqueryTest(XML_SNIPPET, "counttttttt(*:fruitbasket/fruit)", Collections.singletonList("7")));
    }

    @Test
    public void testXQueries() throws Exception {

        /* count matches */
        doXqueryTest(XML_SNIPPET, "count(*:fruitbasket/fruit)", Collections.singletonList("7"));
        doXqueryTest(XML_SNIPPET, "count(//fruit)", Collections.singletonList("7"));

        /* Using a namespace */
        doXqueryTest(XML_SNIPPET, "declare namespace fb = \"http://namespace/1\"; count(fb:fruitbasket/fruit)", Collections.singletonList("7"));

        /* determine if node exists */
        doXqueryTest(XML_SNIPPET, "boolean(//fruit[1])", Collections.singletonList("true"));
        doXqueryTest(XML_SNIPPET, "boolean(//fruit[100])", Collections.singletonList("false"));

        /* XML first match */
        doXqueryTest(XML_SNIPPET, "//fruit[1]", Collections.singletonList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\"><!-- Apples are my favorite --><name>apple</name><color>red</color></fruit>"));

        /* XML last match */
        doXqueryTest(XML_SNIPPET, "//fruit[count(//fruit)]", Collections.singletonList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\"><name>none</name><color/></fruit>"));

        /* XML first match wrapped */
        doXqueryTest(XML_SNIPPET, "<wrap>{//fruit[1]}</wrap>", Collections.singletonList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><wrap><fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\"><!-- Apples are my favorite --><name>apple</name><color>red</color></fruit></wrap>"));

        /* XML all matches (multiple results) */
        doXqueryTest(XML_SNIPPET, "//fruit", Arrays.asList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\"><!-- Apples are my favorite --><name>apple</name><color>red</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\"><name>apple</name><color>green</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\"><name>banana</name><color>yellow</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\" taste=\"sweet\"><name>orange</name><color>orange</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\"><name>blueberry</name><color>blue</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\" taste=\"ÄÖÜäöüßéèóò\"><name>raspberry</name><color>red</color></fruit>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><fruit xmlns:ns=\"http://namespace/1\"><name>none</name><color/></fruit>"));

        /* XML all matches wrapped (one result)*/
        doXqueryTest(XML_SNIPPET, "<wrap>{//fruit}</wrap>", Collections.singletonList(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<wrap>"
                + "<fruit xmlns:ns=\"http://namespace/1\" taste=\"crisp\"><!-- Apples are my favorite --><name>apple</name><color>red</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\"><name>apple</name><color>green</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\"><name>banana</name><color>yellow</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\" taste=\"sweet\"><name>orange</name><color>orange</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\"><name>blueberry</name><color>blue</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\" taste=\"ÄÖÜäöüßéèóò\"><name>raspberry</name><color>red</color></fruit>"
                + "<fruit xmlns:ns=\"http://namespace/1\"><name>none</name><color/></fruit>"
                + "</wrap>"));

        /* String all matches fruit names*/
        doXqueryTest(XML_SNIPPET, "for $x in //fruit return $x/name/text()", Arrays.asList(fruitNames));

        /* String first match fruit name (XPath)*/
        doXqueryTest(XML_SNIPPET, "//fruit[1]/name/text()", Collections.singletonList("apple"));

        /* String first match fruit color (XPath)*/
        doXqueryTest(XML_SNIPPET, "//fruit[1]/color/text()", Collections.singletonList("red"));

        /* String first match fruit name (XQuery)*/
        doXqueryTest(XML_SNIPPET, "for $x in //fruit[1] return string-join(($x/name/text() , $x/color/text()), ' - ')",
                Collections.singletonList("apple - red"));

        /* String first match fruit & color (one result)*/
        doXqueryTest(XML_SNIPPET, "for $x in //fruit[1] return string-join(($x/name/text() , $x/color/text()), ' - ')",
                Collections.singletonList("apple - red"));

        /* String all matches fruit & color (multiple results)*/
        doXqueryTest(XML_SNIPPET, "for $x in //fruit return string-join(($x/name/text() , $x/color/text()), ' - ')",
                Arrays.asList(
                        "apple - red",
                        "apple - green",
                        "banana - yellow",
                        "orange - orange",
                        "blueberry - blue",
                        "raspberry - red",
                        "none"));

        /* String all matches fruit & color (single, newline delimited result)*/
        doXqueryTest(XML_SNIPPET, "string-join((for $y in (for $x in //fruit return string-join(($x/name/text() , $x/color/text()), ' - ')) return $y), '\n')",
                Collections.singletonList(
                        "apple - red\n"
                        + "apple - green\n"
                        + "banana - yellow\n"
                        + "orange - orange\n"
                        + "blueberry - blue\n"
                        + "raspberry - red\n"
                        + "none"));

        /* String all matches fruit & color using "let" (single, newline delimited result)*/
        doXqueryTest(XML_SNIPPET, "string-join((for $y in (for $x in //fruit let $d := string-join(($x/name/text() , $x/color/text()), ' - ')  return $d) return $y), '\n')",
                Collections.singletonList(
                        "apple - red\n"
                        + "apple - green\n"
                        + "banana - yellow\n"
                        + "orange - orange\n"
                        + "blueberry - blue\n"
                        + "raspberry - red\n"
                        + "none"));

        /* String all matches name only, comma delimited (one result)*/
        doXqueryTest(XML_SNIPPET, "string-join((for $x in //fruit return $x/name/text()), ', ')",
                Collections.singletonList("apple, apple, banana, orange, blueberry, raspberry, none"));

        /* String all matches color and name, comma delimited (one result)*/
        doXqueryTest(XML_SNIPPET, "string-join((for $y in (for $x in //fruit return string-join(($x/color/text() , $x/name/text()), ' ')) return $y), ', ')",
                Collections.singletonList("red apple, green apple, yellow banana, orange orange, blue blueberry, red raspberry, none"));

        /* String all matches color and name, comma delimited using let(one result)*/
        doXqueryTest(XML_SNIPPET, "string-join((for $y in (for $x in //fruit let $d := string-join(($x/color/text() , $x/name/text()), ' ')  return $d) return $y), ', ')",
                Collections.singletonList("red apple, green apple, yellow banana, orange orange, blue blueberry, red raspberry, none"));


        /* Query for attribute */
        doXqueryTest(XML_SNIPPET, "string(//fruit[1]/@taste)", Collections.singletonList("crisp"));

        /* Query for comment */
        doXqueryTest(XML_SNIPPET, "//fruit/comment()", Collections.singletonList(" Apples are my favorite "));

        /* Query for processing instruction */
        doXqueryTest(XML_SNIPPET, "//processing-instruction()[name()='xml-stylesheet']", Collections.singletonList("type=\"text/xsl\" href=\"foo.xsl\""));

    }

    private void doXqueryTest(Path xml, String xQuery, List<String> expectedResults) throws Exception {

        Map<String, String> runnerProps = new HashMap<>();
        List<MockFlowFile> resultFlowFiles;

        // test read from content, write to attribute
        runnerProps.put(EvaluateXQuery.DESTINATION.getName(), EvaluateXQuery.DESTINATION_ATTRIBUTE);
        runnerProps.put("xquery", xQuery);
        resultFlowFiles = runXquery(xml, runnerProps);

        assertEquals(1, resultFlowFiles.size());

        final MockFlowFile out = resultFlowFiles.get(0);

        for (int i = 0; i < expectedResults.size(); i++) {
            String key = "xquery";
            if (expectedResults.size() > 1) {
                key += "." + (i + 1); //NOPMD
            }
            final String actual = out.getAttribute(key).replaceAll(">\\s+<", "><");
            final String expected = expectedResults.get(i).replaceAll(">\\s+<", "><");
            assertEquals(expected, actual);
        }

        // test read from content, write to content
        runnerProps.clear();
        runnerProps.put(EvaluateXQuery.DESTINATION.getName(), EvaluateXQuery.DESTINATION_CONTENT);
        runnerProps.put("xquery", xQuery);
        resultFlowFiles = runXquery(xml, runnerProps);

        assertEquals(expectedResults.size(), resultFlowFiles.size());

        for (int i = 0; i < resultFlowFiles.size(); i++) {

            final MockFlowFile resultFlowFile = resultFlowFiles.get(i);
            final byte[] outData = resultFlowFile.toByteArray();
            final String outXml = new String(outData, StandardCharsets.UTF_8).replaceAll(">\\s+<", "><");
            final String expected = expectedResults.get(i).replaceAll(">\\s+<", "><");
            assertEquals(expected, outXml);
        }
    }

    private List<MockFlowFile> runXquery(Path xml, Map<String, String> runnerProps) throws Exception {
        return runXquery(xml, runnerProps, new HashMap<>());
    }

    private List<MockFlowFile> runXquery(Path xml, Map<String, String> runnerProps, Map<String, String> flowFileAttributes) throws Exception {

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());

        for (Entry<String, String> entry : runnerProps.entrySet()) {
            testRunner.setProperty(entry.getKey(), entry.getValue());
        }

        testRunner.enqueue(xml, flowFileAttributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH);

        return testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH);
    }

    @Test
    public void testRootPath() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xquery.result1", "/");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        final String attributeString = out.getAttribute("xquery.result1").replaceAll(">\\s+<", "><");
        final String xmlSnippetString = new String(Files.readAllBytes(XML_SNIPPET), StandardCharsets.UTF_8).replaceAll(">\\s+<", "><");

        assertEquals(xmlSnippetString, attributeString);
    }

    @Test
    public void testCheckIfElementExists() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xquery.result.exist.1", "boolean(/*:fruitbasket/fruit[1])");
        testRunner.setProperty("xquery.result.exist.2", "boolean(/*:fruitbasket/fruit[100])");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        out.assertAttributeEquals("xquery.result.exist.1", "true");
        out.assertAttributeEquals("xquery.result.exist.2", "false");
    }

    @Test
    public void testUnmatchedContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("xquery.result.exist.2", "/*:fruitbasket/node2");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_NO_MATCH, 1);
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testUnmatchedAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xquery.result.exist.2", "/*:fruitbasket/node2");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_NO_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0);
        out.assertAttributeEquals("xquery.result.exist.2", null);
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testNoXQueryAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_NO_MATCH, 1);
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testNoXQueryContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);

        testRunner.enqueue(XML_SNIPPET);
        assertThrows(AssertionError.class, testRunner::run);
    }

    @Test
    public void testOneMatchOneUnmatchAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("some.property", "//fruit/name/text()");
        testRunner.setProperty("xquery.result.exist.2", "/*:fruitbasket/node2");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);

        for (int i = 0; i < fruitNames.length; i++) {
            final String outXml = out.getAttribute("some.property." + (i + 1));
            assertEquals(fruitNames[i], outXml.trim());
        }

        out.assertAttributeEquals("xquery.result.exist.2", null);
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testMatchedEmptyStringAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xquery.result.exist.2", "/*:fruitbasket/*[name='none']/color/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_NO_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0);

        out.assertAttributeEquals("xquery.result.exist.2", null);
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_NO_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testMultipleXPathForContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property.1", "/*:fruitbasket/fruit[1]");
        testRunner.setProperty("some.property.2", "/*:fruitbasket/fruit[2]");

        testRunner.enqueue(XML_SNIPPET);
        assertThrows(AssertionError.class, testRunner::run);
    }

    @Test
    public void testWriteStringToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("xquery.result2", "/*:fruitbasket/fruit[1]/name/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        out.assertAttributeEquals("xquery.result2", "apple");
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testWriteStringToContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "/*:fruitbasket/fruit[1]/name/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        out.assertContentEquals("apple");
    }

    @Test
    public void testWriteXmlToAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("some.property", "/*:fruitbasket/fruit[1]/name");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        final String outXml = out.getAttribute("some.property");
        assertTrue(outXml.contains("<name xmlns:ns=\"http://namespace/1\">apple</name>"));
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testWriteXmlToContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "/*:fruitbasket/fruit[1]/name");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        final byte[] outData = testRunner.getContentAsByteArray(out);
        final String outXml = new String(outData, StandardCharsets.UTF_8);
        assertTrue(outXml.contains("<name xmlns:ns=\"http://namespace/1\">apple</name>"));
    }

    @Test
    public void testMatchesMultipleStringContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "//fruit/name/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 7);

        final List<MockFlowFile> flowFilesForRelMatch = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH);
        for (int i = 0; i < flowFilesForRelMatch.size(); i++) {

            final MockFlowFile out = flowFilesForRelMatch.get(i);
            out.assertContentEquals(fruitNames[i]);
        }
    }

    @Test
    public void testMatchesMultipleStringAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("some.property", "//fruit/name/text()");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);

        for (int i = 0; i < fruitNames.length; i++) {
            final String outXml = out.getAttribute("some.property." + (i + 1));
            assertEquals(fruitNames[i], outXml.trim());
        }
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testMatchesMultipleXmlContent() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "//fruit/name");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 7);

        final List<MockFlowFile> flowFilesForRelMatch = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH);
        for (int i = 0; i < flowFilesForRelMatch.size(); i++) {

            final MockFlowFile out = flowFilesForRelMatch.get(i);
            final byte[] outData = testRunner.getContentAsByteArray(out);
            final String outXml = new String(outData, StandardCharsets.UTF_8);
            String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><name xmlns:ns=\"http://namespace/1\">" + fruitNames[i] + "</name>";
            assertEquals(expectedXml, outXml.trim());
        }
    }

    @Test
    public void testMatchesMultipleXmlAttribute() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_ATTRIBUTE);
        testRunner.setProperty("some.property", "//fruit/name");

        testRunner.enqueue(XML_SNIPPET);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);

        for (int i = 0; i < fruitNames.length; i++) {
            final String outXml = out.getAttribute("some.property." + (i + 1));
            String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><name xmlns:ns=\"http://namespace/1\">" + fruitNames[i] + "</name>";
            assertEquals(expectedXml, outXml.trim());
        }
        testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0).assertContentEquals(XML_SNIPPET);
    }

    @Test
    public void testSuccessForEmbeddedDocTypeValidation() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXQuery.VALIDATE_DTD, "true");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_EMBEDDED_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_MATCH, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateXQuery.REL_MATCH).get(0);
        out.assertContentEquals("Hello");
    }

    @Test
    public void testFailureForEmbeddedDocTypeValidationDisabled() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXQuery.VALIDATE_DTD, "false");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_EMBEDDED_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_FAILURE, 1);
    }


    @Test
    public void testFailureForExternalDocTypeWithDocTypeValidationEnabled() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_NONEXISTENT_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_FAILURE, 1);
    }

    @Test
    public void testFailureForExternalDocTypeWithDocTypeValidationDisabled() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateXQuery());
        testRunner.setProperty(EvaluateXQuery.DESTINATION, EvaluateXQuery.DESTINATION_CONTENT);
        testRunner.setProperty(EvaluateXQuery.VALIDATE_DTD, "false");
        testRunner.setProperty("some.property", "/*:bundle/node/subNode[1]/value/text()");

        testRunner.enqueue(XML_SNIPPET_NONEXISTENT_DOCTYPE);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EvaluateXQuery.REL_FAILURE, 1);
    }
}
