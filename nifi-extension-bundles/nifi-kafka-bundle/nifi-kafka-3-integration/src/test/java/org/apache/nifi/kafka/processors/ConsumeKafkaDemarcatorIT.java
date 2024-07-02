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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaDemarcatorIT extends AbstractConsumeKafkaIT {

    private static final String RECORD_VALUE = "recordA,recordB,recordC,recordD,recordE,recordF";
    private static final int EXPECTED_TOKENS = 6;

    private static final int FIRST_PARTITION = 0;

    private static final long FIRST_OFFSET = 0;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    /**
     * This configuration amalgamates multiple kafka messages into a single FlowFile.
     */
    @Test
    void testConsumeDemarcated() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);

        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.setProperty(ConsumeKafka.SEPARATE_BY_KEY, Boolean.FALSE.toString());
        runner.run(1, false, true);

        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        final String[] values = RECORD_VALUE.split(",");
        for (String value : values) {
            records.add(new ProducerRecord<>(topic, value));
        }
        produce(topic, records);

        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while ((System.currentTimeMillis() < pollUntil) && (runner.getFlowFilesForRelationship("success").isEmpty())) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());
        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertContentEquals(RECORD_VALUE.replaceAll(",", "|"));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(FIRST_OFFSET));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(EXPECTED_TOKENS - 1));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(EXPECTED_TOKENS));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
    }

    /**
     * This configuration amalgamates multiple kafka messages into FlowFiles, keyed by the provided Kafka message key.
     */
    @Test
    void testConsumeDemarcatedSeparateByKey() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.setProperty(ConsumeKafka.SEPARATE_BY_KEY, Boolean.TRUE.toString());
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.run(1, false, true);

        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        final String[] values = RECORD_VALUE.split(",");
        boolean key = false;
        for (String value : values) {
            final List<Header> headers = new ArrayList<>();
            records.add(new ProducerRecord<>(topic, null, Boolean.toString(key), value, headers));
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
                assertEquals("recordB|recordD|recordF", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(1));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(EXPECTED_TOKENS - 1));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(EXPECTED_TOKENS / 2));
            } else if (Boolean.FALSE.toString().equals(flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_KEY))) {
                assertEquals("recordA|recordC|recordE", flowFile.getContent());
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

    /**
     * This configuration amalgamates multiple kafka messages into FlowFiles, keyed by a significant Kafka header.
     */
    @Test
    void testConsumeDemarcatedSeparateByHeader() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.setProperty(ConsumeKafka.HEADER_NAME_PATTERN, "A.*");

        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.run(1, false, true);

        final int expectedGroupCount = 3;
        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        final String[] values = RECORD_VALUE.split(",");
        int i = 0;
        for (String value : values) {
            final String string = String.format("A%d", (++i % expectedGroupCount));
            final Header header = new RecordHeader(string, string.getBytes(StandardCharsets.UTF_8));
            records.add(new ProducerRecord<>(topic, null, (String) null, value, Collections.singletonList(header)));
        }
        produce(topic, records);

        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", expectedGroupCount);  // header=A1, header=A2, header=A0

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertEquals(expectedGroupCount, flowFiles.size());
        for (MockFlowFile flowFile : flowFiles) {

            if ("A1".equals(flowFile.getAttribute("A1"))) {
                assertEquals("recordA|recordD", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(0));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(3));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(2));
            } else if ("A2".equals(flowFile.getAttribute("A2"))) {
                assertEquals("recordB|recordE", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(1));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(4));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(2));
            } else if ("A0".equals(flowFile.getAttribute("A0"))) {
                assertEquals("recordC|recordF", flowFile.getContent());
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(2));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(5));
                flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Long.toString(2));
            } else {
                Assertions.fail("expected KafkaFlowFileAttribute");
            }
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(0));
            flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
        }
    }
}
