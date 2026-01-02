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
package org.apache.nifi.kafka.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaInjectOffsetRecordIT extends AbstractConsumeKafkaIT {
    private static final int TEST_RECORD_COUNT = 2;

    private static final int FIRST_PARTITION = 0;

    private static final String FIRST_OFFSET = "0";

    private static final String MAX_OFFSET = "1";

    private static final String RECORD_1 = """
            { "id": 1, "name": "A" }
            """;

    private static final String OUTPUT_RECORD_1 = """
            {"id":1,"name":"A","kafkaOffset":0}
            """;

    private static final String RECORD_2 = """
            { "id": 2, "name": "B" }
            """;

    private static final String OUTPUT_RECORD_2 = """
            {"id":2,"name":"B","kafkaOffset":1}
            """;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addRecordReaderService(runner);
        addRecordWriterService(runner);
        addRecordKeyReaderService(runner);
    }

    @Test
    void testRun() throws InterruptedException, ExecutionException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = UUID.randomUUID().toString();

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST);
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.INJECT_OFFSET);

        runner.run(1, false, true);

        produceOne(topic, FIRST_PARTITION, null, RECORD_1, null);
        produceOne(topic, FIRST_PARTITION, null, RECORD_2, null);

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFilesForRelationship.size());
        final Iterator<MockFlowFile> flowFiles = flowFilesForRelationship.iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, MAX_OFFSET);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, FIRST_OFFSET);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Integer.toString(TEST_RECORD_COUNT));

        flowFile.assertAttributeEquals("record.count", Integer.toString(TEST_RECORD_COUNT));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());

        final JsonNode jsonNodeTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonNodeTree);
        assertEquals(2, jsonNodeTree.size());

        final JsonNode firstRecord = jsonNodeTree.get(0);
        final JsonNode expectedFirstRecord = objectMapper.readTree(OUTPUT_RECORD_1);
        assertEquals(expectedFirstRecord, firstRecord);

        final JsonNode secondRecord = jsonNodeTree.get(1);
        final JsonNode expectedSecondRecord = objectMapper.readTree(OUTPUT_RECORD_2);
        assertEquals(expectedSecondRecord, secondRecord);
    }
}
