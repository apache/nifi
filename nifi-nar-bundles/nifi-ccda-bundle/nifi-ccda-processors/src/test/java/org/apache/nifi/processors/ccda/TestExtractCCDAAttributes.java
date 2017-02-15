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
package org.apache.nifi.processors.ccda;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestExtractCCDAAttributes {

    private TestRunner runner;

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "INFO");
    }

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(ExtractCCDAAttributes.class);
    }

    @Test
    public void testProcessor() throws IOException {
        Map<String, String> expectedAttributes = new HashMap<String, String>();
        expectedAttributes.put("code.code", "34133-9");
        expectedAttributes.put("code.codeSystem", "2.16.840.1.113883.6.1");
        expectedAttributes.put("code.displayName", "Summarization of episode note");
        expectedAttributes.put("effectiveTime", "20130717114446.302-0500");

        runTests("CCDA-Example.xml", expectedAttributes, true, true);
    }

    private void runTests(final String fileName, Map<String, String> expectedAttributes, final boolean skipValidation, final boolean prettyPrinting) throws IOException{
        runner.setProperty(ExtractCCDAAttributes.SKIP_VALIDATION, String.valueOf(skipValidation));
        runner.setProperty(ExtractCCDAAttributes.PRETTY_PRINTING, String.valueOf(prettyPrinting));

        runner.enqueue(Paths.get("src/test/resources/" + fileName));

        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractCCDAAttributes.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractCCDAAttributes.REL_SUCCESS).get(0);
        for (final Map.Entry<String, String> entry : expectedAttributes.entrySet()) {
            flowFile.assertAttributeEquals(entry.getKey(), entry.getValue());
        }

    }
}
