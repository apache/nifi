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
package org.apache.nifi.text;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestFreeFormTextRecordSetWriter {

    private TestRunner setup(FreeFormTextRecordSetWriter writer) throws InitializationException, IOException {
        TestRunner runner = TestRunners.newTestRunner(TestFreeFormTextRecordSetWriterProcessor.class);

        runner.addControllerService("writer", writer);
        runner.setProperty(TestFreeFormTextRecordSetWriterProcessor.WRITER, "writer");

        runner.setProperty(writer, FreeFormTextRecordSetWriter.TEXT, "ID: ${ID}, Name: ${NAME}, Age: ${AGE}, Country: ${COUNTRY}, Username: ${user.name}");

        return runner;
    }

    @Test
    public void testDefault() throws IOException, InitializationException {
        FreeFormTextRecordSetWriter writer = new FreeFormTextRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.enableControllerService(writer);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("user.name", "jdoe64");
        runner.enqueue("", attributes);
        runner.run();
        // In addition to making sure a flow file was output successfully, also check nothing got rolled back into the incoming queue. May be a moot point as there is a
        // fake processor, but in operation the flow file could be rolled back on error.
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestFreeFormTextRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "ID: ABC123, Name: John Doe, Age: 22, Country: USA, Username: jdoe64\nID: ABC123, Name: John Doe, Age: 22, Country: USA, Username: jdoe64\n";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestFreeFormTextRecordSetWriterProcessor.SUCCESS).get(0)));
        assertEquals(expected, actual);
    }

    @Test
    public void testDefaultSingleRecord() throws IOException, InitializationException {
        FreeFormTextRecordSetWriter writer = new FreeFormTextRecordSetWriter();
        TestRunner runner = setup(writer);

        runner.setProperty(TestFreeFormTextRecordSetWriterProcessor.MULTIPLE_RECORDS, "false");

        runner.enableControllerService(writer);
        Map<String, String> attributes = new HashMap<>();
        // Test ID field value does not get overridden
        attributes.put("ID", "jdoe64");
        runner.enqueue("", attributes);
        runner.run();
        // In addition to making sure a flow file was output successfully, also check nothing got rolled back into the incoming queue. May be a moot point as there is a
        // fake processor, but in operation the flow file could be rolled back on error.
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(TestFreeFormTextRecordSetWriterProcessor.SUCCESS, 1);

        String expected = "ID: ABC123, Name: John Doe, Age: 22, Country: USA, Username: \n";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(TestFreeFormTextRecordSetWriterProcessor.SUCCESS).get(0)));
        assertEquals(expected, actual);
    }

}