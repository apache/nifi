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
package org.apache.nifi.kafka.processors.publish.dependency;

import org.apache.nifi.kafka.processors.PublishKafka;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Quick integration test for testing against Kafka cluster defined via
 * <a href="https://github.com/confluentinc/kafka-images">confluentinc/kafka-images</a>.
 */
public class PublishKafkaMultipleFFIT {
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    private static final String BOOTSTRAP_SERVERS = "";
    private static final String SSL_CONTEXT_SERVICE_PATH = "";
    private static final String KEYSTORE_PATH = SSL_CONTEXT_SERVICE_PATH + "/kafka.producer.keystore.jks";
    private static final String TRUSTSTORE_PATH = SSL_CONTEXT_SERVICE_PATH + "/kafka.producer.truststore.jks";
    private static final String KEYSTORE_PASSWORD = "";
    private static final String TRUSTSTORE_PASSWORD = "";

    public static Stream<Arguments> argumentsTransactionality() {
        return Stream.of(
                arguments(Boolean.FALSE),
                arguments(Boolean.TRUE));
    }

    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    @Disabled("use this to test publish of multiple FlowFiles; requires running Kafka cluster")
    public void testKafkaMultipleFlowFilesSuccess(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.USE_TRANSACTIONS, transactionality.toString());

        final String[] suffixes = { "-A", "-B", "-C" };
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
    @Disabled("use this to test partial publish failure; requires running Kafka cluster")
    public void testNiFiFailureTooBig(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.USE_TRANSACTIONS, transactionality.toString());

        runner.enqueue(new byte[1024 * 1280]);  // by default, NiFi maximum is 1MB per record
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
    }

    /**
     * Test Kafka client library failure on attempt to send over-sized Kafka record (Kafka limited).
     */
    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    @Disabled("use this to test partial publish failure; requires running Kafka cluster")
    public void testKafkaFailureTooBig(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.USE_TRANSACTIONS, transactionality.toString());
        runner.setProperty(PublishKafka.MAX_REQUEST_SIZE, "2 MB");

        runner.enqueue(new byte[1024 * 1280]);  // by default, Kafka maximum is 1MB per record
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
    }

    @ParameterizedTest
    @MethodSource("argumentsTransactionality")
    @Disabled("use this to test partial publish failure; requires running Kafka cluster")
    public void testKafkaMultipleFlowFilesPartialFailure(final Boolean transactionality) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.USE_TRANSACTIONS, transactionality.toString());
        runner.setProperty(PublishKafka.FAILURE_STRATEGY, FailureStrategy.ROLLBACK.getValue());

        runner.enqueue(TEST_RECORD_VALUE);
        runner.enqueue(new byte[1024 * 1280]);  // by default, max 1MB per record
        runner.run();
        runner.assertTransferCount(PublishKafka.REL_SUCCESS, 0);
        runner.assertTransferCount(PublishKafka.REL_FAILURE, 0);
    }

    private String addKafkaConnectionService(final TestRunner runner) throws InitializationException {
        final Map<String, String> connectionServiceProps = new HashMap<>();
        connectionServiceProps.put(Kafka3ConnectionService.BOOTSTRAP_SERVERS.getName(), BOOTSTRAP_SERVERS);
        connectionServiceProps.put(Kafka3ConnectionService.SSL_CONTEXT_SERVICE.getName(), addSSLContextService(runner));
        connectionServiceProps.put(Kafka3ConnectionService.SECURITY_PROTOCOL.getName(), SecurityProtocol.SSL.name());

        final String identifier = Kafka3ConnectionService.class.getSimpleName();
        final KafkaConnectionService connectionService = new Kafka3ConnectionService();
        runner.addControllerService(identifier, connectionService, connectionServiceProps);

        runner.enableControllerService(connectionService);
        return identifier;
    }

    private String addSSLContextService(final TestRunner runner) throws InitializationException {
        final String identifier = SSLContextService.class.getSimpleName();
        final SSLContextService service = new StandardRestrictedSSLContextService();
        runner.addControllerService(identifier, service);

        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE, KEYSTORE_PATH);
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_TYPE, KeystoreType.JKS.name());
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE, TRUSTSTORE_PATH);
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_TYPE, KeystoreType.JKS.name());

        runner.enableControllerService(service);
        return identifier;
    }
}
