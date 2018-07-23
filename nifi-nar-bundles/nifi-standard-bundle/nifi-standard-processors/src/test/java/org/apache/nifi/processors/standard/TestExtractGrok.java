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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class TestExtractGrok {

    private TestRunner testRunner;
    private final static Path GROK_LOG_INPUT = Paths.get("src/test/resources/TestExtractGrok/apache.log");
    private final static Path GROK_TEXT_INPUT = Paths.get("src/test/resources/TestExtractGrok/simple_text.log");

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractGrok.class);
    }

    @Test
    public void testExtractGrokWithMissingPattern() throws Exception {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{FOOLOG}");
        testRunner.enqueue(GROK_LOG_INPUT);
        testRunner.assertNotValid();
    }

    @Test
    public void testExtractGrokWithMatchedContent() throws IOException {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{COMMONAPACHELOG}");
        testRunner.setProperty(ExtractGrok.GROK_PATTERN_FILE, "src/test/resources/TestExtractGrok/patterns");
        testRunner.enqueue(GROK_LOG_INPUT);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractGrok.REL_MATCH);
        final MockFlowFile matched = testRunner.getFlowFilesForRelationship(ExtractGrok.REL_MATCH).get(0);

        matched.assertAttributeEquals("grok.verb","GET");
        matched.assertAttributeEquals("grok.response","401");
        matched.assertAttributeEquals("grok.bytes","12846");
        matched.assertAttributeEquals("grok.clientip","64.242.88.10");
        matched.assertAttributeEquals("grok.auth","-");
        matched.assertAttributeEquals("grok.timestamp","07/Mar/2004:16:05:49 -0800");
        matched.assertAttributeEquals("grok.request","/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables");
        matched.assertAttributeEquals("grok.httpversion","1.1");
    }

    @Test
    public void testExtractGrokKeepEmptyCaptures() throws Exception {
        String expression = "%{NUMBER}|%{NUMBER}";
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION,expression);
        testRunner.enqueue("-42");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractGrok.REL_MATCH);
        final MockFlowFile matched = testRunner.getFlowFilesForRelationship(ExtractGrok.REL_MATCH).get(0);
        matched.assertAttributeEquals("grok.NUMBER","[-42, null]");
    }

    @Test
    public void testExtractGrokDoNotKeepEmptyCaptures() throws Exception {
        String expression = "%{NUMBER}|%{NUMBER}";
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION,expression);
        testRunner.setProperty(ExtractGrok.KEEP_EMPTY_CAPTURES,"false");
        testRunner.enqueue("-42");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractGrok.REL_MATCH);
        final MockFlowFile matched = testRunner.getFlowFilesForRelationship(ExtractGrok.REL_MATCH).get(0);
        matched.assertAttributeEquals("grok.NUMBER","-42");
    }


    @Test
    public void testExtractGrokWithUnMatchedContent() throws IOException {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{URI}");
        testRunner.setProperty(ExtractGrok.GROK_PATTERN_FILE, "src/test/resources/TestExtractGrok/patterns");
        testRunner.enqueue(GROK_TEXT_INPUT);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractGrok.REL_NO_MATCH);
        final MockFlowFile notMatched = testRunner.getFlowFilesForRelationship(ExtractGrok.REL_NO_MATCH).get(0);
        notMatched.assertContentEquals(GROK_TEXT_INPUT);
    }

    @Test
    public void testExtractGrokWithNotFoundPatternFile() throws IOException {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{COMMONAPACHELOG}");
        testRunner.setProperty(ExtractGrok.GROK_PATTERN_FILE, "src/test/resources/TestExtractGrok/toto_file");
        testRunner.enqueue(GROK_LOG_INPUT);
        testRunner.assertNotValid();
    }

    @Test
    public void testExtractGrokWithBadGrokExpression() throws IOException {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{TOTO");
        testRunner.setProperty(ExtractGrok.GROK_PATTERN_FILE, "src/test/resources/TestExtractGrok/patterns");
        testRunner.enqueue(GROK_LOG_INPUT);
        testRunner.assertNotValid();
    }

    @Test
    public void testExtractGrokWithNamedCapturesOnly() throws IOException {
        testRunner.setProperty(ExtractGrok.GROK_EXPRESSION, "%{COMMONAPACHELOG}");
        testRunner.setProperty(ExtractGrok.GROK_PATTERN_FILE, "src/test/resources/TestExtractGrok/patterns");
        testRunner.setProperty(ExtractGrok.NAMED_CAPTURES_ONLY, "true");
        testRunner.enqueue(GROK_LOG_INPUT);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ExtractGrok.REL_MATCH);
        final MockFlowFile matched = testRunner.getFlowFilesForRelationship(ExtractGrok.REL_MATCH).get(0);

        matched.assertAttributeEquals("grok.verb","GET");
        matched.assertAttributeEquals("grok.response","401");
        matched.assertAttributeEquals("grok.bytes","12846");
        matched.assertAttributeEquals("grok.clientip","64.242.88.10");
        matched.assertAttributeEquals("grok.auth","-");
        matched.assertAttributeEquals("grok.timestamp","07/Mar/2004:16:05:49 -0800");
        matched.assertAttributeEquals("grok.request","/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables");
        matched.assertAttributeEquals("grok.httpversion","1.1");

        matched.assertAttributeNotExists("grok.INT");
        matched.assertAttributeNotExists("grok.BASE10NUM");
        matched.assertAttributeNotExists("grok.COMMONAPACHELOG");
    }
}
