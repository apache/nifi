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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

public class AMQPConsumerTest {


    @Test
    public void testUnconsumedMessagesNacked() throws TimeoutException, IOException {
        final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
        final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

        final TestConnection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);
        final AMQPConsumer consumer = new AMQPConsumer(connection, "queue1", true);
        consumer.getChannel().basicPublish("myExchange", "key1", new BasicProperties(), new byte[0]);

        consumer.close();
        assertTrue(((TestChannel) consumer.getChannel()).isNack(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void failOnNullConnection() throws IOException {
        new AMQPConsumer(null, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failOnNullQueueName() throws Exception {
        Connection conn = new TestConnection(null, null);
        new AMQPConsumer(conn, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failOnEmptyQueueName() throws Exception {
        Connection conn = new TestConnection(null, null);
        new AMQPConsumer(conn, " ", true);
    }

    @Test(expected = IOException.class)
    public void failOnNonExistingQueue() throws Exception {
        Connection conn = new TestConnection(null, null);
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "hello", true)) {
            consumer.consume();
        }
    }

    @Test
    public void validateSuccessfullConsumeWithEmptyQueueDefaultExchange() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("queue1", Arrays.asList("queue1"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("", "queue1");

        Connection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1", true)) {
            GetResponse response = consumer.consume();
            assertNull(response);
        }
    }

    @Test
    public void validateSuccessfullConsumeWithEmptyQueue() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        conn.createChannel().basicPublish("myExchange", "key1", null, "hello Joe".getBytes());
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1", true)) {
            GetResponse response = consumer.consume();
            assertNotNull(response);
        }
    }
}
