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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Custom max message size specified in [#getEnvironmentIntegration()]
@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaOneLargePayloadIT extends AbstractPublishKafkaIT {

    private static final int MESSAGE_SIZE = 1024 * 1024 * 3 / 2;

    @Test
    public void test_1_KafkaTestContainerProduceOneLargeFlowFile() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);

        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.MAX_REQUEST_SIZE, (MESSAGE_SIZE + 128) + "B");

        runner.enqueue(new byte[MESSAGE_SIZE]);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PublishKafka.REL_SUCCESS).getFirst();
        assertEquals(MESSAGE_SIZE, flowFile.getSize());
    }

    @Test
    public void test_2_KafkaTestContainerConsumeLargeFlowFileBatch() {
        final Properties kafkaConsumerProperties = getKafkaConsumerProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties)) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(1, records.count());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertEquals(MESSAGE_SIZE, record.value().length());
        }
    }
}
