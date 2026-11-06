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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Kafka3ConnectionServiceTest {

    private static final String SERVICE_ID = Kafka3ConnectionService.class.getSimpleName();

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String MAX_PARTITION_FETCH_SIZE = "256 KB";

    private static final int MAX_PARTITION_FETCH_SIZE_BYTES = 262144;

    private static final String MAX_FETCH_SIZE = "10 MB";

    private static final int MAX_FETCH_SIZE_BYTES = 10485760;

    private TestRunner runner;

    private Kafka3ConnectionService service;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new Kafka3ConnectionService();
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, Kafka3ConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
    }

    @Test
    void testFetchSizeInConsumerProperties() {
        runner.assertValid(service);

        final Properties defaultConsumerProperties = getConsumerProperties();

        assertEquals(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES, defaultConsumerProperties.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals(ConsumerConfig.DEFAULT_FETCH_MAX_BYTES, defaultConsumerProperties.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));

        runner.setProperty(service, Kafka3ConnectionService.MAX_PARTITION_FETCH_SIZE, MAX_PARTITION_FETCH_SIZE);
        runner.setProperty(service, Kafka3ConnectionService.MAX_FETCH_SIZE, MAX_FETCH_SIZE);
        runner.assertValid(service);

        final Properties consumerProperties = getConsumerProperties();

        assertEquals(MAX_PARTITION_FETCH_SIZE_BYTES, consumerProperties.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals(MAX_FETCH_SIZE_BYTES, consumerProperties.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> propertyValues = Map.of(
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(MAX_PARTITION_FETCH_SIZE_BYTES),
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.toString(MAX_FETCH_SIZE_BYTES)
        );
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);

        service.migrateProperties(configuration);

        final Map<String, String> expectedProperties = Map.of(
                Kafka3ConnectionService.MAX_PARTITION_FETCH_SIZE.getName(), MAX_PARTITION_FETCH_SIZE_BYTES + " B",
                Kafka3ConnectionService.MAX_FETCH_SIZE.getName(), MAX_FETCH_SIZE_BYTES + " B"
        );
        assertEquals(expectedProperties, configuration.getRawProperties());
    }

    @Test
    void testMigratePropertiesExpressionLanguageNotConverted() {
        final String expression = "${MAX_PARTITION_FETCH_BYTES}";
        final Map<String, String> propertyValues = Map.of(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, expression);
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);

        service.migrateProperties(configuration);

        final Map<String, String> expectedProperties = Map.of(Kafka3ConnectionService.MAX_PARTITION_FETCH_SIZE.getName(), expression);
        assertEquals(expectedProperties, configuration.getRawProperties());
    }

    private Properties getConsumerProperties() {
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        final Map<PropertyDescriptor, String> properties = processContext.getConfiguration(SERVICE_ID).getProperties();
        final MockConfigurationContext configurationContext = new MockConfigurationContext(service, properties, processContext, Collections.emptyMap());

        final Properties clientProperties = service.getClientProperties(configurationContext);
        return service.getConsumerProperties(configurationContext, clientProperties);
    }
}
