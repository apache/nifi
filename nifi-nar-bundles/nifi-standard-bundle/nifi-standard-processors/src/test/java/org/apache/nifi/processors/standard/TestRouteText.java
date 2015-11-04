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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestRouteText {

    @Test
    public void testRelationships() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "start");

        runner.run();

        Set<Relationship> relationshipSet = runner.getProcessor().getRelationships();
        Set<String> expectedRelationships = new HashSet<>(Arrays.asList("matched", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for (Relationship relationship : relationshipSet) {
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }


        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHING_PROPERTY_NAME);

        relationshipSet = runner.getProcessor().getRelationships();
        expectedRelationships = new HashSet<>(Arrays.asList("simple", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for (Relationship relationship : relationshipSet) {
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.run();
    }

    @Test
    public void testSeparationStrategyNotKnown() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);

        runner.assertNotValid();
    }

    @Test
    public void testNotText() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty("simple", "start");

        Set<Relationship> relationshipSet = runner.getProcessor().getRelationships();
        Set<String> expectedRelationships = new HashSet<>(Arrays.asList("simple", "unmatched"));

        assertEquals(expectedRelationships.size(), relationshipSet.size());
        for (Relationship relationship : relationshipSet) {
            assertTrue(expectedRelationships.contains(relationship.getName()));
        }

        runner.enqueue(Paths.get("src/test/resources/simple.jpg"));
        runner.run();

        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/simple.jpg"));
    }

    @Test
    public void testInvalidRegex() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty("simple", "[");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        try {
            runner.run();
            fail();
        } catch (AssertionError e) {
            // Expect to catch error asserting 'simple' as invalid
        }

    }

    @Test
    public void testSimpleDefaultStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultEnd() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.ENDS_WITH);
        runner.setProperty("simple", "end");


        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testRouteLineToMultipleRelationships() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS);
        runner.setProperty("t", "t");
        runner.setProperty("e", "e");
        runner.setProperty("z", "z");

        final String originalText = "start middle end\nnot match";
        runner.enqueue(originalText.getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("t", 1);
        runner.assertTransferCount("e", 1);
        runner.assertTransferCount("z", 0);
        runner.assertTransferCount("unmatched", 0);
        runner.assertTransferCount("original", 1);

        runner.getFlowFilesForRelationship("t").get(0).assertContentEquals(originalText);
        runner.getFlowFilesForRelationship("e").get(0).assertContentEquals("start middle end\n");
        runner.getFlowFilesForRelationship("z").isEmpty();
        runner.getFlowFilesForRelationship("original").get(0).assertContentEquals(originalText);
    }

    @Test
    public void testSimpleDefaultContains() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS);
        runner.setProperty("simple", "middle");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleContainsIgnoreCase() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS);
        runner.setProperty(RouteText.IGNORE_CASE, "true");
        runner.setProperty("simple", "miDDlE");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }


    @Test
    public void testSimpleDefaultEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.EQUALS);
        runner.setProperty("simple", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty("simple", ".*(mid).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleDefaultContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty("simple", "(m.d)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testSimpleAnyStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "start");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.ENDS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.EQUALS);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAnyContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ANY_PROPERTY_MATCHES);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("no", "no match");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    /* ------------------------------------------------------ */

    @Test
    public void testSimpleAllStarts() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "start middle");
        runner.setProperty("second", "star");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllEnds() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.ENDS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "middle end");
        runner.setProperty("second", "nd");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllEquals() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.EQUALS);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "start middle end");
        runner.setProperty("second", "start middle end");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllMatchRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.MATCHES_REGULAR_EXPRESSION);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", ".*(m.d).*");
        runner.setProperty("second", ".*(t.*m).*");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testSimpleAllContainRegularExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS_REGULAR_EXPRESSION);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHED_WHEN_ALL_PROPERTIES_MATCH);
        runner.setProperty("simple", "(m.d)");
        runner.setProperty("second", "(t.*m)");

        runner.enqueue("start middle end\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("matched", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("matched").get(0);
        outMatched.assertContentEquals("start middle end\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testRouteOnPropertiesStartsWindowsNewLine() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\r\nnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r\n".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testRouteOnPropertiesStartsJustCarriageReturn() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty("simple", "start");

        runner.enqueue("start middle end\rnot match".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount("simple", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship("simple").get(0);
        outMatched.assertContentEquals("start middle end\r".getBytes("UTF-8"));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        outUnmatched.assertContentEquals("not match".getBytes("UTF-8"));
    }

    @Test
    public void testJson() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.STARTS_WITH);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty("greeting", "\"greeting\"");
        runner.setProperty("address", "\"address\"");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-sample.json"));
        runner.run();

        runner.assertTransferCount("greeting", 1);
        runner.assertTransferCount("address", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);

        // Verify text is trimmed
        final MockFlowFile outGreeting = runner.getFlowFilesForRelationship("greeting").get(0);
        String outGreetingString = new String(runner.getContentAsByteArray(outGreeting));
        assertEquals(7, countLines(outGreetingString));
        final MockFlowFile outAddress = runner.getFlowFilesForRelationship("address").get(0);
        String outAddressString = new String(runner.getContentAsByteArray(outAddress));
        assertEquals(7, countLines(outAddressString));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        String outUnmatchedString = new String(runner.getContentAsByteArray(outUnmatched));
        assertEquals(400, countLines(outUnmatchedString));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestJson/json-sample.json"));

    }


    @Test
    public void testXml() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteText());
        runner.setProperty(RouteText.MATCH_STRATEGY, RouteText.CONTAINS);
        runner.setProperty(RouteText.ROUTE_STRATEGY, RouteText.ROUTE_TO_MATCHING_PROPERTY_NAME);
        runner.setProperty("NodeType", "name=\"NodeType\"");
        runner.setProperty("element", "<xs:element");
        runner.setProperty("name", "name=");

        runner.enqueue(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
        runner.run();

        runner.assertTransferCount("NodeType", 1);
        runner.assertTransferCount("element", 1);
        runner.assertTransferCount("name", 1);
        runner.assertTransferCount("unmatched", 1);
        runner.assertTransferCount("original", 1);


        // Verify text is trimmed
        final MockFlowFile outNode = runner.getFlowFilesForRelationship("NodeType").get(0);
        String outNodeString = new String(runner.getContentAsByteArray(outNode));
        assertEquals(1, countLines(outNodeString));
        final MockFlowFile outElement = runner.getFlowFilesForRelationship("element").get(0);
        String outElementString = new String(runner.getContentAsByteArray(outElement));
        assertEquals(4, countLines(outElementString));
        final MockFlowFile outName = runner.getFlowFilesForRelationship("name").get(0);
        String outNameString = new String(runner.getContentAsByteArray(outName));
        assertEquals(7, countLines(outNameString));
        final MockFlowFile outUnmatched = runner.getFlowFilesForRelationship("unmatched").get(0);
        String outUnmatchedString = new String(runner.getContentAsByteArray(outUnmatched));
        assertEquals(26, countLines(outUnmatchedString));

        final MockFlowFile outOriginal = runner.getFlowFilesForRelationship("original").get(0);
        outOriginal.assertContentEquals(Paths.get("src/test/resources/TestXml/XmlBundle.xsd"));
    }

    public static int countLines(String str) {
        if (str == null || str.isEmpty()) {
            return 0;
        }
        int lines = 0;
        int pos = 0;
        while ((pos = str.indexOf(System.lineSeparator(), pos) + 1) != 0) {
            lines++;
        }
        return lines;
    }
}
