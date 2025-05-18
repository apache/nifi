/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.basex;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestEvaluateBaseXQuery {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(EvaluateBaseXQuery.class);
    }

    @Test
    public void testsimple() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new EvaluateBaseXQuery());
        runner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);
        runner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "//books/book");

        runner.enqueue(Paths.get("src/test/resources/xml/test1.xml"));
        runner.run();
        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        assertEquals("<book>Java</book>" + "\n" + "<book>Python</book>", outOriginal.getContent());
    }

    @Test
    public void testBaseXQueryEvaluationReturnsFlowFileContent() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book return data($x/title)");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("id", "1");

        testRunner.enqueue(Paths.get("src/test/resources/xml/test2.xml"), attrs);

        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Everyday Italian"+ "\n" + "Harry Potter"+"\n" + "Java Soup");
    }


    @Test
    public void testBaseXQueryEvaluationIgnoresAttributes() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book let $title := data($x/title) where $title != id return $title");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_LIST);
        testRunner.setProperty(EvaluateBaseXQuery.MAPPING_LIST, "id");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("id", "Everyday Italian");

        testRunner.enqueue(Paths.get("src/test/resources/xml/test3.xml"), attrs);
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
    }

//    @Test
//    public void testFailureDueToInvalidXQueryScript() throws IOException {
//        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book let $title := data($x/title) where $title = $id return");
//        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_ALL);
//
//        final Map<String, String> attrs = new HashMap<>();
//        attrs.put("id", "1");
//
//        testRunner.enqueue(Paths.get("src/test/resources/xml/test3.xml"), attrs);
//        testRunner.run();
//
//        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_FAILURE, 1);
//
//    }

    @Test
    public void testBaseXQueryEvaluationWithPartialAttributeMapping() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book return $x/title || ' by ' || $x/author/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_LIST);
        testRunner.setProperty(EvaluateBaseXQuery.MAPPING_LIST, "title,author");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("title", "Effective Java");
        attrs.put("author", "Joshua Bloch");

        testRunner.enqueue(Paths.get("src/test/resources/xml/test4.xml"), attrs);

        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Effective Java by Joshua Bloch");
    }

    @Test
    public void testOutputBodyAttributeName() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book return $x/title/text() || ' by ' || $x/author");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.MAP_LIST);
        testRunner.setProperty(EvaluateBaseXQuery.MAPPING_LIST, "title,author");
        testRunner.setProperty(EvaluateBaseXQuery.OUTPUT_BODY_ATTRIBUTE_NAME, "OutputResult");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("title", "Effective Java");
        attrs.put("author", "Joshua Bloch");

        testRunner.enqueue(Paths.get("src/test/resources/xml/test4.xml"), attrs);
        testRunner.setValidateExpressionUsage(false);
        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Effective Java by Joshua Bloch");
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_ORIGINAL).get(0);
        assertEquals("Effective Java by Joshua Bloch", original.getAttribute("OutputResult"));
    }

    @Test
    public void testBaseXQueryEvaluationWithNoAttributeMapping() throws IOException {
        testRunner.setProperty(EvaluateBaseXQuery.XQUERY_SCRIPT, "for $x in //book return $x/author/text()");
        testRunner.setProperty(EvaluateBaseXQuery.ATTR_MAPPING_STRATEGY, EvaluateBaseXQuery.DO_NOT_MAP);

        testRunner.enqueue(Paths.get("src/test/resources/xml/test5.xml"));

        testRunner.run();

        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_SUCCESS, 1);
        testRunner.assertTransferCount(EvaluateBaseXQuery.REL_ORIGINAL, 1);
        final MockFlowFile out = testRunner.getFlowFilesForRelationship(EvaluateBaseXQuery.REL_SUCCESS).get(0);
        out.assertContentEquals("Giada De Laurentiis" + "\n" + "J.K. Rowling"+ "\n" +"Joshua Bloch");
    }


}