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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.kafka.processors.producer.PartitionStrategy;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublishKafkaRoundRobinIT extends AbstractPublishKafkaIT {

    @Test
    public void testRoundRobin() throws InitializationException {
        try (final AdminClient admin = AdminClient.create(Map.of(Kafka3ConnectionService.BOOTSTRAP_SERVERS.getName(), kafkaContainer.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(getClass().getName(), 8, (short) 1)));
        }

        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.PARTITION_CLASS, PartitionStrategy.ROUND_ROBIN_PARTITIONING.getValue());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "\n");

        runner.enqueue("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);

        int totalRecords = 0;
        final Map<Integer, Integer> recordsPerPartition = new HashMap<>();
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(List.of(getClass().getName()));

            while (totalRecords < 10) {
                final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);

                for (final ConsumerRecord<String, String> record : records) {
                    totalRecords++;
                    recordsPerPartition.merge(record.partition(), 1, Integer::sum);
                }
            }
        }

        assertEquals(10, totalRecords);
        final int[] expectedCounts = {2, 2, 1, 1, 1, 1, 1, 1};
        for (int i = 0; i < 8; i++) {
            assertEquals(expectedCounts[i], recordsPerPartition.get(i));
        }
    }

}
