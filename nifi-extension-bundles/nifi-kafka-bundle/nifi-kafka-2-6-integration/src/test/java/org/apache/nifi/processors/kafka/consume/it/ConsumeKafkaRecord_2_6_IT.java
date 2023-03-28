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
package org.apache.nifi.processors.kafka.consume.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumeKafkaRecord_2_6_IT extends ConsumeKafka_2_6_BaseIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/processors/kafka/publish/ff.json";
    private static final int TEST_RECORD_COUNT = 3;

    private static final String TOPIC = ConsumeKafkaRecord_2_6_IT.class.getName();
    private static final String GROUP_ID = ConsumeKafkaRecord_2_6_IT.class.getSimpleName();

    @Test
    public void testKafkaTestContainerProduceConsumeOne() throws ExecutionException, InterruptedException, IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafkaRecord_2_6.class);
        runner.setValidateExpressionUsage(false);
        final URI uri = URI.create(kafka.getBootstrapServers());
        addRecordReaderService(runner);
        addRecordWriterService(runner);
        runner.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", uri.getHost(), uri.getPort()));
        runner.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        runner.setProperty("topic", TOPIC);
        runner.setProperty("group.id", GROUP_ID);
        runner.run(1, false, true);

        final String message = new String(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE))), StandardCharsets.UTF_8);
        produceOne(TOPIC, null, null, message, Collections.emptyList());
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, TOPIC);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(0));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(0));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
        flowFile.assertAttributeEquals("record.count", Long.toString(3));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());

        // [{"id":1,"name":"A"},{"id":2,"name":"B"},{"id":3,"name":"C"}]
        final JsonNode jsonNodeTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonNodeTree);
        final ArrayNode arrayNode = (ArrayNode) jsonNodeTree;
        final Iterator<JsonNode> elements = arrayNode.elements();
        assertEquals(TEST_RECORD_COUNT, arrayNode.size());
        while (elements.hasNext()) {
            final JsonNode jsonNode = elements.next();
            assertTrue(Arrays.asList(1, 2, 3).contains(jsonNode.get("id").asInt()));
            assertTrue(Arrays.asList("A", "B", "C").contains(jsonNode.get("name").asText()));
        }
    }
}
