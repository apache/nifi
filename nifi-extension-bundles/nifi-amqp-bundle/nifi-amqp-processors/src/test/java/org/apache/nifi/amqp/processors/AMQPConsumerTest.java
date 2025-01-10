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
package org.apache.nifi.amqp.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.nifi.logging.ComponentLog;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AMQPConsumerTest {

    private static final int DEFAULT_PREFETCH_COUNT = 0;
    private ComponentLog processorLog;

    @BeforeEach
    public void setUp() {
        processorLog = mock(ComponentLog.class);
    }

    @Test
    public void testResponseQueueDrained() throws TimeoutException, IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final TestConnection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);
        final AMQPConsumer consumer = new AMQPConsumer(connection, "queue1", true, DEFAULT_PREFETCH_COUNT, processorLog);
        consumer.getChannel().basicPublish("myExchange", "key1", new BasicProperties(), new byte[0]);

        consumer.close();

        assertEquals(0, consumer.getResponseQueueSize());
    }

    @Test
    public void testConsumerHandlesCancelling() throws IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final TestConnection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);
        final AMQPConsumer consumer = new AMQPConsumer(connection, "queue1", true, DEFAULT_PREFETCH_COUNT, processorLog);

        assertFalse(consumer.closed);

        consumer.getChannel().basicCancel("queue1");

        assertTrue(consumer.closed);
    }

    @Test
    public void failOnNullConnection() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPConsumer(null, null, true, DEFAULT_PREFETCH_COUNT, processorLog));
    }

    @Test
    public void failOnNullQueueName() {
        assertThrows(IllegalArgumentException.class, () -> {
            Connection conn = new TestConnection(null, null);
            new AMQPConsumer(conn, null, true, DEFAULT_PREFETCH_COUNT, processorLog);
        });
    }

    @Test
    public void failOnEmptyQueueName() {
        assertThrows(IllegalArgumentException.class, () -> {
            Connection conn = new TestConnection(null, null);
            new AMQPConsumer(conn, " ", true, DEFAULT_PREFETCH_COUNT, processorLog);
        });
    }

    @Test
    public void failOnNonExistingQueue() {
        assertThrows(IOException.class, () -> {
            Connection conn = new TestConnection(null, null);
            try (AMQPConsumer consumer = new AMQPConsumer(conn, "hello", true, DEFAULT_PREFETCH_COUNT, processorLog)) {
                consumer.consume();
            }
        });
    }

    @Test
    public void validateSuccessfulConsumeWithEmptyQueueDefaultExchange() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("queue1", Arrays.asList("queue1"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("", "queue1");

        Connection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1", true, DEFAULT_PREFETCH_COUNT, processorLog)) {
            GetResponse response = consumer.consume();
            assertNull(response);
        }
    }

    @Test
    public void validateSuccessfulConsumeWithEmptyQueue() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        conn.createChannel().basicPublish("myExchange", "key1", null, "hello Joe".getBytes());
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1", true, DEFAULT_PREFETCH_COUNT, processorLog)) {
            GetResponse response = consumer.consume();
            assertNotNull(response);
        }
    }

    @Test
    public void validatePrefetchSet() throws Exception {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");
        Connection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1", true, 100, processorLog)) {
            TestChannel channel = (TestChannel) consumer.getChannel();
            assertEquals(100, channel.getPrefetchCount());
        }
    }
}
