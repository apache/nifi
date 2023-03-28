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
package org.apache.nifi.kafka.processors.consume;

import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.stream.Stream;

class ConsumeKafkaConfigureTest {

    private static final String CONNECTION_SERVICE_ID = Kafka3ConnectionService.class.getSimpleName();
    private static final String CONFIG_BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:50000";
    private static final String CONFIG_MAX_POLL_RECORDS = "1234";

    @Test
    void testConfigureMaxPollRecords() throws InitializationException, NoSuchFieldException, IllegalAccessException {
        final Kafka3ConnectionService connectionService = new Kafka3ConnectionService();
        final Class<?> serviceClass = connectionService.getClass();

        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService(CONNECTION_SERVICE_ID, connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.BOOTSTRAP_SERVERS, CONFIG_BOOTSTRAP_SERVERS);
        runner.setProperty(connectionService, Kafka3ConnectionService.MAX_POLL_RECORDS, CONFIG_MAX_POLL_RECORDS);
        runner.enableControllerService(connectionService);

        final Field field = serviceClass.getDeclaredField("clientProperties");  // properties used to init ConsumerService
        field.setAccessible(true);
        final Properties clientProperties = (Properties) field.get(connectionService);
        Assertions.assertEquals(CONFIG_MAX_POLL_RECORDS, clientProperties.getProperty(Kafka3ConnectionService.MAX_POLL_RECORDS.getName()));
    }

    /**
     * Given a PropertyDescriptor value, verify the consumer configuration property.
     */
    public static Stream<Arguments> permutationsTransactionality() {
        return Stream.of(
                Arguments.arguments(Boolean.FALSE.toString(), "read_uncommitted"),
                Arguments.arguments(Boolean.TRUE.toString(), "read_committed"),
                Arguments.arguments(null, "read_committed")  // PropertyDescriptor default
        );
    }

    @ParameterizedTest
    @MethodSource("permutationsTransactionality")
    void testConfigureConsumeTransactions(final String honorTransactions, final String expectedProperty)
            throws InitializationException, NoSuchFieldException, IllegalAccessException {
        final Kafka3ConnectionService connectionService = new Kafka3ConnectionService();
        final Class<?> serviceClass = connectionService.getClass();

        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService(CONNECTION_SERVICE_ID, connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.BOOTSTRAP_SERVERS, CONFIG_BOOTSTRAP_SERVERS);
        runner.setProperty(connectionService, Kafka3ConnectionService.HONOR_TRANSACTIONS, honorTransactions);
        runner.enableControllerService(connectionService);

        final Field field = serviceClass.getDeclaredField("consumerProperties");
        field.setAccessible(true);
        final Properties consumerProperties = (Properties) field.get(connectionService);
        Assertions.assertEquals(expectedProperty, consumerProperties.getProperty("isolation.level"));
    }
}
