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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

public class AMQPConsumerTest {

    @SuppressWarnings("resource")
    @Test(expected = IllegalArgumentException.class)
    public void failOnNullConnection() {
        new AMQPConsumer(null, null);
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalArgumentException.class)
    public void failOnNullQueueName() throws Exception {
        Connection conn = new TestConnection(null, null);
        new AMQPConsumer(conn, null);
    }

    @SuppressWarnings("resource")
    @Test(expected = IllegalArgumentException.class)
    public void failOnEmptyQueueName() throws Exception {
        Connection conn = new TestConnection(null, null);
        new AMQPConsumer(conn, " ");
    }

    @Test(expected = ProcessException.class)
    public void failOnNonExistingQueue() throws Exception {
        Connection conn = new TestConnection(null, null);
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "hello")) {
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
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1")) {
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
        try (AMQPConsumer consumer = new AMQPConsumer(conn, "queue1")) {
            GetResponse response = consumer.consume();
            assertNotNull(response);
        }
    }
}
