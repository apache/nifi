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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class Kafka3ConnectionServiceTest {

    @ParameterizedTest
    @ValueSource(strings = {
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG,
            ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG
    })
    void testStripShareUnsupportedConsumerConfig(final String propertyName) {
        final Properties properties = new Properties();
        properties.setProperty(propertyName, "configured");

        Kafka3ConnectionService.stripShareUnsupportedConsumerConfigs(properties);

        assertFalse(properties.containsKey(propertyName));
    }

    @Test
    void testStripShareUnsupportedConsumerConfigsPreservesSupportedConfig() {
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        Kafka3ConnectionService.stripShareUnsupportedConsumerConfigs(properties);

        assertEquals("localhost:9092", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("100", properties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }
}
