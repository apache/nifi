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
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumeKafka_2_6_IT extends ConsumeKafka_2_6_BaseIT {
    private static final String TEST_RECORD_KEY = "key-" + System.currentTimeMillis();
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

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
    public void testProduceConsumeOne() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, 4);
        runner.setProperty("topic", topic);
        runner.setProperty("group.id", groupId);
        runner.run(1, false, true);

        produceOne(topic, null, TEST_RECORD_KEY, TEST_RECORD_VALUE, Collections.emptyList());
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(0));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(0));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());
    }

    /**
     * Test ability to specify a topic regular expression to query for messages.
     */
    @Test
    public void testTopicPattern() throws ExecutionException, InterruptedException {
        final String topicBase = UUID.randomUUID().toString();
        final String topicTestCase = topicBase + "-B";
        final String topicPattern = topicBase + ".*";

        // on use of "pattern" subscription, things seem to work better when topic exists prior to the subscribe event
        produceOne(topicTestCase, null, TEST_RECORD_KEY, TEST_RECORD_VALUE, Collections.emptyList());

        runner.setProperty("topic", topicPattern);
        runner.setProperty("topic_type", "pattern");
        runner.setProperty("group.id", "B");
        runner.run(1, false, true);

        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while ((System.currentTimeMillis() < pollUntil) && (runner.getFlowFilesForRelationship("success").isEmpty())) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);
    }

    /**
     * Test ability to specify multiple topics to query for messages.
     */
    @Test
    public void testTopicNames() throws ExecutionException, InterruptedException {
        final String topicBase = UUID.randomUUID().toString();
        final String topicTestCase = topicBase + "-C";
        final String topicNames = topicBase + "-D," + topicTestCase;

        runner.setProperty("topic", topicNames);
        runner.setProperty("topic_type", "names");
        runner.setProperty("group.id", "C");
        runner.run(1, false, true);

        produceOne(topicTestCase, null, TEST_RECORD_KEY, TEST_RECORD_VALUE, Collections.emptyList());
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while (System.currentTimeMillis() < pollUntil) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);
    }
}
