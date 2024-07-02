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
package org.apache.nifi.kafka.processors.publish;

import org.apache.nifi.kafka.processors.PublishKafka;
import org.apache.nifi.kafka.processors.AbstractPublishKafkaIT;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class PublishKafkaMultipleFFIT extends AbstractPublishKafkaIT {
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    public static Stream<Arguments> argumentsTransactionality() {
        return Stream.of(
                arguments(Boolean.FALSE),
                arguments(Boolean.TRUE));
    }

    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    public void testKafkaMultipleFlowFilesSuccess(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, transactionality.toString());

        final String[] suffixes = {
                "-A", "-B", "-C"
        };
        for (String suffix : suffixes) {
            runner.enqueue(TEST_RECORD_VALUE + suffix);
        }
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, suffixes.length);
    }

    /**
     * Test NiFi processor failure on attempt to send over-sized Kafka record (NiFi limited).
     */
    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    public void testNiFiFailureTooBig(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, transactionality.toString());

        runner.enqueue(new byte[1024 * 1280]);  // by default, NiFi maximum is 1MB per record
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
    }

    /**
     * Test Kafka client library failure on attempt to send over-sized Kafka record (Kafka limited).
     */
    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    public void testKafkaFailureTooBig(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, transactionality.toString());
        runner.setProperty(PublishKafka.MAX_REQUEST_SIZE, "2 MB");

        runner.enqueue(new byte[1024 * 1280]);  // by default, Kafka maximum is 1MB per record
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
    }

    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    public void testKafkaMultipleFlowFilesPartialFailureRollback(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, transactionality.toString());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, FailureStrategy.ROLLBACK.getValue());

        runner.enqueue(TEST_RECORD_VALUE);
        runner.enqueue(new byte[1024 * 1280]);  // by default, max 1MB per record
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    public void testKafkaMultipleFlowFilesPartialFailureTransferFailure(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.TRANSACTIONS_ENABLED, transactionality.toString());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, FailureStrategy.ROUTE_TO_FAILURE);

        runner.enqueue(TEST_RECORD_VALUE);
        runner.enqueue(new byte[1024 * 1280]);  // by default, max 1MB per record
        runner.run(2);
        runner.assertTransferCount(PublishKafka.REL_SUCCESS, 1);
        runner.assertTransferCount(PublishKafka.REL_FAILURE, 1);
    }
}
