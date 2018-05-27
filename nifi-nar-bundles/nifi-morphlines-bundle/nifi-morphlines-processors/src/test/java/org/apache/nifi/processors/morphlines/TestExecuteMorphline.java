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
package org.apache.nifi.processors.morphlines;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class TestExecuteMorphline {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExecuteMorphline.class);
        testRunner.setValidateExpressionUsage(false);
        URL file = TestExecuteMorphline.class.getClassLoader().getResource("morphlines.conf");
        testRunner.setProperty(ExecuteMorphline.MORPHLINES_FILE, file.getPath());
        testRunner.setProperty(ExecuteMorphline.MORPHLINES_ID, "test");
        // Try to get one of the fields which was parsed; syslog_timestamp in our case
        testRunner.setProperty(ExecuteMorphline.MORPHLINES_OUTPUT_FIELD, "syslog_timestamp");
    }

    @Test
    public void testProcessorSuccess() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = getClass().getResourceAsStream("/good_record.txt");
        } catch(Exception e) {
            System.out.println("ERROR: Good record file does not exist");
        }
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(CoreAttributes.FILENAME.key(), "good_record.txt");
        testRunner.enqueue(inputStream, attributes);
        testRunner.run();
        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ExecuteMorphline.REL_SUCCESS);

        assertEquals(1, result.size());
    }

    @Test
    public void testProcessorFail() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = getClass().getResourceAsStream("/bad_record.txt");
        } catch(Exception e) {
            System.out.println("ERROR: Bad record file does not exist");
        }
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(CoreAttributes.FILENAME.key(), "bad_record.txt");
        testRunner.enqueue(inputStream, attributes);
        testRunner.run();
        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(ExecuteMorphline.REL_FAILURE);

        assertEquals(1, result.size());
    }
}
