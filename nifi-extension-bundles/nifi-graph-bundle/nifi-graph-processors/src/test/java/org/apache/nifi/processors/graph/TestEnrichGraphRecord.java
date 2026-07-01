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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.graph.GraphElementType;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestEnrichGraphRecord {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private TestRunner runner;

    @BeforeEach
    void setup() throws Exception {
        runner = TestRunners.newTestRunner(EnrichGraphRecord.class);

        final MockEnrichGraphClientService graphClientService = new MockEnrichGraphClientService();
        final MockGraphQueryGeneratorService queryGeneratorService = new MockGraphQueryGeneratorService();
        final JsonTreeReader reader = new JsonTreeReader();
        final MockRecordWriter writer = new MockRecordWriter();

        runner.addControllerService("graphClient", graphClientService);
        runner.addControllerService("queryGenerator", queryGeneratorService);
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);

        runner.enableControllerService(graphClientService);
        runner.enableControllerService(queryGeneratorService);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);

        runner.setProperty(EnrichGraphRecord.CLIENT_SERVICE, "graphClient");
        runner.setProperty(EnrichGraphRecord.QUERY_GENERATOR_SERVICE, "queryGenerator");
        runner.setProperty(EnrichGraphRecord.READER_SERVICE, "reader");
        runner.setProperty(EnrichGraphRecord.WRITER_SERVICE, "writer");
        runner.setProperty(EnrichGraphRecord.IDENTIFIER_FIELD, "/id");
        runner.setProperty(EnrichGraphRecord.ELEMENT_LABEL, "Person");
    }

    @Test
    void testSuccessfulNodeProcessingProducesSingleResponseFlowFile() throws Exception {
        final String inputContent = "[{\"id\":\"1\",\"price\":100},{\"id\":\"2\",\"age\":\"10\"}]";
        runner.enqueue(inputContent.getBytes());

        runner.run();

        runner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 1);
        runner.assertTransferCount(EnrichGraphRecord.FAILURE, 0);
        runner.assertTransferCount(EnrichGraphRecord.RESPONSE, 1);

        final MockFlowFile responseFlowFile = runner.getFlowFilesForRelationship(EnrichGraphRecord.RESPONSE).getFirst();
        final List<Map<String, Object>> responses = objectMapper.readValue(runner.getContentAsByteArray(responseFlowFile), new TypeReference<>() {
        });

        assertEquals(2, responses.size());

        final Map<String, Object> firstProperties = (Map<String, Object>) responses.getFirst().get("properties");
        final Map<String, Object> secondProperties = (Map<String, Object>) responses.get(1).get("properties");
        assertEquals(100, firstProperties.get("price"));
        assertEquals("10", secondProperties.get("age"));
        assertFalse(firstProperties.containsKey("id"));
        assertFalse(secondProperties.containsKey("id"));
    }

    @Test
    void testPartialFailureWritesFailedRecords() throws Exception {
        final String inputContent = "[{\"id\":\"1\",\"price\":100},{\"id\":\"2\",\"forceFailure\":true}]";
        runner.enqueue(inputContent.getBytes());

        runner.run();

        runner.assertTransferCount(EnrichGraphRecord.RESPONSE, 1);
        runner.assertTransferCount(EnrichGraphRecord.FAILURE, 1);
        runner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 0);

        final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(EnrichGraphRecord.FAILURE).getFirst();
        assertEquals("1", failedFlowFile.getAttribute(EnrichGraphRecord.RECORD_COUNT));
    }

    @Test
    void testDynamicPropertiesAreUsedInsteadOfDefaultFields() throws Exception {
        runner.setProperty("cost", "/price");
        final String inputContent = "[{\"id\":\"1\",\"price\":100,\"name\":\"Widget\"}]";
        runner.enqueue(inputContent.getBytes());

        runner.run();

        runner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 1);
        runner.assertTransferCount(EnrichGraphRecord.FAILURE, 0);
        runner.assertTransferCount(EnrichGraphRecord.RESPONSE, 1);

        final MockFlowFile responseFlowFile = runner.getFlowFilesForRelationship(EnrichGraphRecord.RESPONSE).getFirst();
        final List<Map<String, Object>> responses = objectMapper.readValue(runner.getContentAsByteArray(responseFlowFile), new TypeReference<>() {
        });
        final Map<String, Object> properties = (Map<String, Object>) responses.getFirst().get("properties");
        assertEquals(100, properties.get("cost"));
        assertFalse(properties.containsKey("name"));
    }

    @Test
    void testEdgeSelectionIsPassedToQueryGenerator() throws Exception {
        runner.setProperty(EnrichGraphRecord.ELEMENT_TYPE, GraphElementType.EDGE.name());
        runner.setProperty(EnrichGraphRecord.ELEMENT_LABEL, "ASSOCIATED_WITH");
        final String inputContent = "[{\"id\":\"1\",\"weight\":7}]";
        runner.enqueue(inputContent.getBytes());

        runner.run();

        runner.assertTransferCount(EnrichGraphRecord.RESPONSE, 1);
        final MockFlowFile responseFlowFile = runner.getFlowFilesForRelationship(EnrichGraphRecord.RESPONSE).getFirst();
        final List<Map<String, Object>> responses = objectMapper.readValue(runner.getContentAsByteArray(responseFlowFile), new TypeReference<>() {
        });
        assertEquals("EDGE", responses.getFirst().get("elementType"));
        assertEquals("EDGE:ASSOCIATED_WITH", responses.getFirst().get("query"));
        assertEquals("ASSOCIATED_WITH", responses.getFirst().get("elementLabel"));
        assertTrue(((Map<String, Object>) responses.getFirst().get("properties")).containsKey("weight"));
    }

    @Test
    void testTransientGraphFailureRollsBackForRetry() {
        final String inputContent = "[{\"id\":\"1\",\"forceTransientFailure\":true}]";
        runner.enqueue(inputContent.getBytes());

        assertThrows(AssertionError.class, () -> runner.run(1, true, true));
        runner.assertTransferCount(EnrichGraphRecord.RESPONSE, 0);
        runner.assertTransferCount(EnrichGraphRecord.FAILURE, 0);
        runner.assertTransferCount(EnrichGraphRecord.ORIGINAL, 0);
        runner.assertQueueNotEmpty();
        assertTrue(runner.isYieldCalled());
    }
}
