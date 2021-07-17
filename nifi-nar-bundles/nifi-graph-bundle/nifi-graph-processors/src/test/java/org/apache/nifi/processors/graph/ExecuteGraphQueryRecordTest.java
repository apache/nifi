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
package org.apache.nifi.processors.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import groovy.json.JsonOutput;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.processors.graph.util.InMemoryGraphClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecuteGraphQueryRecordTest {
    private TestRunner runner;
    Map<String, String> enqueProperties = new HashMap<>();

    @Before
    public void setup() throws InitializationException {
        MockRecordWriter writer = new MockRecordWriter();
        JsonTreeReader reader = new JsonTreeReader();
        runner = TestRunners.newTestRunner(ExecuteGraphQueryRecord.class);
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.setProperty(ExecuteGraphQueryRecord.READER_SERVICE, "reader");
        runner.setProperty(ExecuteGraphQueryRecord.WRITER_SERVICE, "writer");

        runner.enableControllerService(writer);
        runner.enableControllerService(reader);
    }

    @Test
    public void testFlowFileContent() throws Exception {
        setupGraphClient(false);
        List<Map<String,Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("M", 1);
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript;
        submissionScript = "[ 'M': M ]";

        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("M", "/M");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.GRAPH).get(0);

        assertTrue(contentEqualsWindowsSafe(relGraph, "/testFlowFileContent.json"));
    }

    @Test
    public void testFlowFileList() throws Exception {
        setupGraphClient(false);
        List<Map<String,Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("M", new ArrayList<Integer>(){
            {
                add(1);
                add(2);
                add(3);
            }
        });
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "[ " +
                "'M': M " +
                "]";

        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("M", "/M");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.GRAPH).get(0);

        assertTrue(contentEqualsWindowsSafe(relGraph, "/testFlowFileList.json"));
    }

    @Test
    public void testComplexFlowFile() throws Exception {
        setupGraphClient(false);
        List<Map<String,Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("tMap", "123");
        tempMap.put("L", new ArrayList<Integer>(){
            {
                add(1);
                add(2);
                add(3);
            }
        });
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "Map<String, Object> vertexHashes = new HashMap()\n" +
                "vertexHashes.put('1234', tMap)\n" +
                "[ 'L': L, 'result': vertexHashes ]";
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("tMap", "/tMap");
        runner.setProperty("L", "/L");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.GRAPH).get(0);

        assertTrue(contentEqualsWindowsSafe(relGraph, "/testComplexFlowFile.json"));
    }

    @Test
    public void testAttributes() throws Exception {
        setupGraphClient(false);
        List<Map<String, Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("tMap", "123");
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "[ " +
                "'testProperty': testProperty " +
                "] ";
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        Map<String, String> enqueProperties = new HashMap<>();
        enqueProperties.put("testProperty", "test");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.GRAPH).get(0);

        assertTrue(contentEqualsWindowsSafe(relGraph, "/testAttributes.json"));
    }

    private boolean contentEqualsWindowsSafe(MockFlowFile flowFile, String expectedPath) throws IOException {
        InputStream is = ExecuteGraphQueryRecordTest.class.getResourceAsStream(expectedPath);
        String expectedRaw = IOUtils.toString(is, StandardCharsets.UTF_8);
        String contentRaw = new String(runner.getContentAsByteArray(flowFile));
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> expected = mapper.readValue(expectedRaw, List.class);
        List<Map<String, Object>> content = mapper.readValue(contentRaw, List.class);

        assertEquals(expected.size(), content.size());
        for (int x = 0; x < content.size(); x++) {
            assertEquals(expected.get(x), content.get(x));
        }

        return expected.equals(content);
    }

    @Test
    public void testExceptionOnQuery() throws Exception {
        setupGraphClient(true);
        List<Map<String,Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("M", 1);
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript;
        submissionScript = "[ 'M': M[0] ]";

        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("M", "/M");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.GRAPH, 0);
        runner.assertTransferCount(ExecuteGraphQueryRecord.SUCCESS, 0);
        runner.assertTransferCount(ExecuteGraphQueryRecord.FAILURE, 1);
    }

    private void setupGraphClient(boolean failOnQuery) throws InitializationException {
        InMemoryGraphClient graphClient = new InMemoryGraphClient(failOnQuery);
        runner.addControllerService("graphClient", graphClient);

        runner.setProperty(ExecuteGraphQueryRecord.CLIENT_SERVICE, "graphClient");
        runner.enableControllerService(graphClient);
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, "[ 'testProperty': 'testResponse' ]");
        runner.assertValid();
        enqueProperties.put("graph.name", "graph");
    }
}
