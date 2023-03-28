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
import org.apache.nifi.kafka.processors.producer.config.DeliveryGuarantee;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaAcksBestEffortIT extends AbstractPublishKafkaIT {
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    /**
     * Leaving this here as a placeholder.  Need a strategy to handle invalid config at initialization, which throws
     * on construction:
     * <ul>
     *     <li>declare exception in interface?</li>
     *     <li>unchecked exception?</li>
     *     <li>reorganize code to avoid Kafka object instantiation in ctor?</li>
     * </ul>
     */
    @Test
    @Disabled(value = "need a strategy to handle invalid config")
    public void test_1_KafkaTestContainerProduceOneFail() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.DELIVERY_GUARANTEE, DeliveryGuarantee.DELIVERY_BEST_EFFORT);  // invalid if transactionality=true (default)

        runner.enqueue(TEST_RECORD_VALUE);
        runner.run();
    }

    @Test
    public void test_1_KafkaTestContainerProduceOne() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, Boolean.FALSE.toString());
        runner.setProperty(PublishKafka.DELIVERY_GUARANTEE, DeliveryGuarantee.DELIVERY_BEST_EFFORT);

        runner.enqueue(TEST_RECORD_VALUE);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @Test
    public void test_2_KafkaTestContainerConsumeOne() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(1, records.count());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertNull(record.key());
            assertEquals(TEST_RECORD_VALUE, record.value());
        }
    }
}
