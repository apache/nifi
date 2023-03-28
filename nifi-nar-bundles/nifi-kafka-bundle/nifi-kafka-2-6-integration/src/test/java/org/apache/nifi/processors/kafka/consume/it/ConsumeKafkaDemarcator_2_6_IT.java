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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumeKafkaDemarcator_2_6_IT extends ConsumeKafka_2_6_BaseIT {
    private static final String TEST_RECORD_VALUE = "recordA,recordB,recordC,recordD,recordE,recordF";
    private static final int EXPECTED_TOKENS = 6;

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ConsumeKafka_2_6.class);
        final URI uri = URI.create(kafka.getBootstrapServers());
        runner.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", uri.getHost(), uri.getPort()));
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void testConsumeDemarcated() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, 4);
        runner.setProperty("group.id", groupId);
        runner.setProperty("topic", topic);
        runner.setProperty("message-demarcator", ",");
        runner.setProperty("separate-by-key", Boolean.FALSE.toString());
        runner.run(1, false, true);

        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        final String[] values = TEST_RECORD_VALUE.split(",");
        for (String value : values) {
            records.add(new ProducerRecord<>(topic, value));
        }
        produce(topic, records);

        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(TEST_RECORD_VALUE, flowFile.getContent());
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(0));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(0));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(EXPECTED_TOKENS - 1));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(EXPECTED_TOKENS));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
    }

    @Test
    public void testConsumeDemarcatedSeparateByKey() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, 4);
        runner.setProperty("group.id", groupId);
        runner.setProperty("topic", topic);
        runner.setProperty("message-demarcator", ",");
        runner.setProperty("separate-by-key", Boolean.TRUE.toString());
        runner.run(1, false, true);

        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        final String[] values = TEST_RECORD_VALUE.split(",");
        boolean key = false;
        for (String value : values) {
            records.add(new ProducerRecord<>(topic, Boolean.toString(key), value));
            key = !key;
        }
        produce(topic, records);

        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 2);  // key=false, key=true

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertEquals(2, flowFiles.size());
        for (MockFlowFile flowFile : flowFiles) {
            if (Boolean.TRUE.toString().equals(flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_KEY))) {
                assertEquals("recordB,recordD,recordF", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(1));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(EXPECTED_TOKENS - 1));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(EXPECTED_TOKENS / 2));
            } else if (Boolean.FALSE.toString().equals(flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_KEY))) {
                assertEquals("recordA,recordC,recordE", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(0));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(EXPECTED_TOKENS - 2));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(EXPECTED_TOKENS / 2));
            } else {
                Assertions.fail("expected KafkaFlowFileAttribute.KAFKA_KEY");
            }
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(0));
            flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
        }
    }
}
