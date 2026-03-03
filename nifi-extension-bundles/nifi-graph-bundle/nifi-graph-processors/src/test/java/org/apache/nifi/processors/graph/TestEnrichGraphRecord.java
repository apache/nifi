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

import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.graph.EnrichGraphRecord.CLIENT_SERVICE;
import static org.apache.nifi.processors.graph.EnrichGraphRecord.IDENTIFIER_FIELD;
import static org.apache.nifi.processors.graph.EnrichGraphRecord.NODE_TYPE;
import static org.apache.nifi.processors.graph.EnrichGraphRecord.READER_SERVICE;
import static org.apache.nifi.processors.graph.EnrichGraphRecord.WRITER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestEnrichGraphRecord {

    private TestRunner testRunner;
    private JsonTreeReader reader;
    private Processor processor;

    @BeforeEach
    public void setup() throws Exception {
        processor = new EnrichGraphRecord();
        testRunner = TestRunners.newTestRunner(processor);

        GraphClientService mockGraphClientService = new MockCypherClientService();
        MockRecordWriter writer = new MockRecordWriter();
        reader = new JsonTreeReader();

        testRunner.setProperty(CLIENT_SERVICE, "graphClient");
        testRunner.addControllerService("graphClient", mockGraphClientService);
        testRunner.addControllerService("reader", reader);
        testRunner.addControllerService("writer", writer);
        testRunner.setProperty(READER_SERVICE, "reader");
        testRunner.setProperty(WRITER_SERVICE, "writer");
        testRunner.enableControllerService(writer);
        testRunner.enableControllerService(reader);
        testRunner.enableControllerService(mockGraphClientService);
    }

    @Test
    public void testSuccessfulNodeProcessing() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("id", "123");

        String inputContent = "[{\"id\": \"123\", \"name\": \"Node1\"},{\"id\": \"789\", \"name\": \"Node2\"}]";
        testRunner.setProperty(IDENTIFIER_FIELD, "/id");
        testRunner.setProperty("name", "/name");
        testRunner.enqueue(inputContent.getBytes(), attributes);

        testRunner.run();

        testRunner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 1);
        testRunner.assertTransferCount(EnrichGraphRecord.FAILURE, 0);
        testRunner.assertTransferCount(EnrichGraphRecord.GRAPH, 2);

        MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(EnrichGraphRecord.ORIGINAL).get(0);
        assertEquals("123", originalFlowFile.getAttribute("id"));
        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(EnrichGraphRecord.GRAPH).get(0);

        try {
            RecordReader recordReader = reader.createRecordReader(successFlowFile, successFlowFile.getContentStream(), new MockComponentLog("1", processor));
            Record record = recordReader.nextRecord();
            assertEquals("John Smith", record.getValue("name"));
            assertEquals(40, record.getAsInt("age"));
        } catch (Exception e) {
            fail("Should not reach here");
        }
    }

    @Test
    public void testSuccessfulEdgeProcessing() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("id", "123");

        String inputContent = "[{\"id\": \"123\", \"name\": \"Node1\", \"relationship\": \"ASSOCIATED_WITH\"}," +
                "{\"id\": \"789\", \"name\": \"Node2\",\"relationship\": \"ASSOCIATED_WITH\"}]";
        testRunner.setProperty(IDENTIFIER_FIELD, "/relationship");
        testRunner.setProperty("name", "/name");
        testRunner.setProperty(NODE_TYPE, GraphClientService.EDGES_TYPE);
        testRunner.enqueue(inputContent.getBytes(), attributes);

        testRunner.run();

        testRunner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 1);
        testRunner.assertTransferCount(EnrichGraphRecord.FAILURE, 0);
        testRunner.assertTransferCount(EnrichGraphRecord.GRAPH, 2);

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(EnrichGraphRecord.GRAPH).get(0);

        try {
            RecordReader recordReader = reader.createRecordReader(successFlowFile, successFlowFile.getContentStream(), new MockComponentLog("1", processor));
            Record record = recordReader.nextRecord();
            assertEquals("John Smith", record.getValue("name"));
            assertEquals(40, record.getAsInt("age"));
            assertEquals("ASSOCIATED_WITH", record.getValue("relationship"));
        } catch (Exception e) {
            fail("Should not reach here");
        }
    }

    @Test
    public void testNullIdentifierValue() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("id", "123");

        // Two bad identifiers, one good
        String inputContent = "[{\"id\": null, \"name\": \"Node1\"},{\"id\": null, \"name\": \"Node2\"},{\"id\": \"123\", \"name\": \"Node3\"}]";
        testRunner.setProperty(IDENTIFIER_FIELD, "//id");
        testRunner.setProperty("name", "//name");
        testRunner.enqueue(inputContent.getBytes(), attributes);

        testRunner.run();

        testRunner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 0);
        testRunner.assertTransferCount(EnrichGraphRecord.FAILURE, 1);
        testRunner.assertTransferCount(EnrichGraphRecord.GRAPH, 1);

        // Verify 2 failed records
        MockFlowFile failedFlowFile = testRunner.getFlowFilesForRelationship(EnrichGraphRecord.FAILURE).get(0);
        assertEquals("2", failedFlowFile.getAttribute("record.count"));
    }

    @Test
    public void testFailedProcessing() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("id", "null");

        String inputContent = "[{\"id\": null, \"name\": \"Node1\"}]";
        testRunner.setProperty(IDENTIFIER_FIELD, "//id");
        testRunner.setProperty("name", "//name");

        testRunner.enqueue(inputContent.getBytes(), attributes);

        testRunner.run();

        testRunner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 0);
        testRunner.assertTransferCount(EnrichGraphRecord.FAILURE, 1);
        testRunner.assertTransferCount(EnrichGraphRecord.GRAPH, 0);
    }
}
