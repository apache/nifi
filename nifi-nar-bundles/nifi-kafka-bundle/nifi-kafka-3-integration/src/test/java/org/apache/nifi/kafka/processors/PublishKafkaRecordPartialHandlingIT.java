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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test handling of record-based FlowFile, given various NiFi and Kafka configurations.  Kafka transactionality allows
 * records in a FlowFile to succeed or fail as a unit.  Non-transactional sends might allow leakage of well-formatted
 * records at the beginning of a FlowFile with malformed content.
 */
public class PublishKafkaRecordPartialHandlingIT extends AbstractPublishKafkaIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/kafka/processors/publish/ff.partial.json";

    private static Stream<Arguments> publishArguments() {
        return Stream.of(
                Arguments.of("no-transactions-route-failure", Boolean.FALSE, FailureStrategy.ROUTE_TO_FAILURE, 0, 1, Boolean.FALSE, 1),
                Arguments.of("transactions-route-failure", Boolean.TRUE, FailureStrategy.ROUTE_TO_FAILURE, 0, 1, Boolean.FALSE, 0),
                Arguments.of("no-transactions-rollback", Boolean.FALSE, FailureStrategy.ROLLBACK, 0, 0, Boolean.TRUE, 1),  // [1]
                Arguments.of("transactions-rollback", Boolean.TRUE, FailureStrategy.ROLLBACK, 0, 0, Boolean.TRUE, 0)
        );
    }

    // [1] the Kafka client library requires "transactional" mode to be enabled in order to fail all records/KafkaMessages in a given FlowFile

    @ParameterizedTest
    @MethodSource("publishArguments")
    public void test1ProduceOneFlowFile(final String label,
                                        final Boolean useTransactions,
                                        final FailureStrategy failureStrategy,
                                        final Integer expectTransferSuccess,
                                        final Integer expectTransferFailure,
                                        final Boolean expectYield,
                                        final Integer expectedKafkaMessageCount) throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        addRecordReaderService(runner);
        addRecordWriterService(runner);

        final String topicName = getClass().getName() + "." + label;
        runner.setProperty(PublishKafka.TOPIC_NAME, topicName);
        runner.setProperty(PublishKafka.USE_TRANSACTIONS, useTransactions.toString());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, failureStrategy);

        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE)));
        runner.enqueue(bytesFlowFile);

        runner.run(1);
        runner.assertTransferCount(PublishKafka.REL_SUCCESS, expectTransferSuccess);
        runner.assertTransferCount(PublishKafka.REL_FAILURE, expectTransferFailure);
        assertEquals(expectYield, runner.isYieldCalled());

        checkKafkaState(topicName, expectedKafkaMessageCount);
    }

    public void checkKafkaState(final String topicName, final int expectedKafkaMessageCount) throws JsonProcessingException {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(topicName));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(expectedKafkaMessageCount, records.count());
            for (ConsumerRecord<String, String> record : records) {
                assertNotNull(objectMapper.readTree(record.value()));
            }
        }
    }
}
