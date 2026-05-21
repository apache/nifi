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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AMQPPublisherTest {

    @Test
    public void failOnNullConnection() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPPublisher(null, null, false));
    }

    @Test
    public void failPublishIfChannelClosed() {
        assertThrows(AMQPRollbackException.class, () -> {
            Connection conn = new TestConnection(null, null);
            try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class), false)) {
                conn.close();
                sender.publish("oleg".getBytes(), null, "foo", "");
            }
        });
    }

    @Test
    public void failPublishIfChannelFails() {
        assertThrows(AMQPException.class, () -> {
            TestConnection conn = new TestConnection(null, null);
            try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class), false)) {
                ((TestChannel) conn.createChannel()).corruptChannel();
                sender.publish("oleg".getBytes(), null, "foo", "");
            }
        });
    }

    @Test
    public void validateSuccessfulPublishingAndRouting() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1", "queue2"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class), false)) {
            sender.publish("hello".getBytes(), null, "key1", "myExchange");
        }

        assertNotNull(connection.createChannel().basicGet("queue1", true));
        assertNotNull(connection.createChannel().basicGet("queue2", true));

        connection.close();
    }

    @Test
    public void validateSuccessfulPublishingAndUndeliverableRoutingKey() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1", "queue2"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        ReturnListener retListener = mock(ReturnListener.class);
        connection.createChannel().addReturnListener(retListener);

        try (AMQPPublisher sender = new AMQPPublisher(connection, new MockComponentLog("foo", ""), false)) {
            sender.publish("hello".getBytes(), null, "key1", "myExchange");
        }

        verify(retListener, atMost(1)).handleReturn(Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(BasicProperties.class), (byte[]) Mockito.any());
        connection.close();
    }

    /**
     * Verifies that a {@link com.rabbitmq.client.ShutdownSignalException} thrown by
     * {@code waitForConfirms()} (e.g., broker closes channel with 404 NOT_FOUND because the
     * exchange does not exist) is converted to {@link AMQPException} so the FlowFile routes
     * to REL_FAILURE instead of surfacing as an unhandled processor error.
     */
    @Test
    public void failPublishWhenBrokerClosesChannelDuringConfirmInAtLeastOnce() {
        assertThrows(AMQPException.class, () -> {
            TestConnection conn = new TestConnection(null, null);
            conn.getTestChannel().setSimulateShutdownOnConfirm(true);
            try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class), true)) {
                sender.publish("hello".getBytes(), null, "foo", "");
            }
        });
    }

    @Test
    public void failPublishWhenBrokerNacksMessageInAtLeastOnce() {
        assertThrows(AMQPException.class, () -> {
            TestConnection conn = new TestConnection(null, null);
            conn.getTestChannel().setSimulateNackOnConfirm(true);
            try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class), true)) {
                sender.publish("hello".getBytes(), null, "foo", "");
            }
        });
    }

    @Test
    public void failPublishWhenMessageReturnedAsUndeliverableInAtLeastOnce() {
        assertThrows(AMQPException.class, () -> {
            Map<String, List<String>> routingMap = new HashMap<>();
            routingMap.put("key1", Arrays.asList("queue1"));
            Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
            exchangeToRoutingKeymap.put("myExchange", "key1");

            TestConnection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
            conn.getTestChannel().setSimulateSynchronousReturn(true);

            try (AMQPPublisher sender = new AMQPPublisher(conn, new MockComponentLog("id", ""), true)) {
                sender.publish("hello".getBytes(), null, "wrongKey", "myExchange");
            }
        });
    }

    @Test
    public void succeedsPublishWhenMessageUndeliverableInAtMostOnceMode() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        TestConnection conn = new TestConnection(exchangeToRoutingKeymap, routingMap);
        conn.getTestChannel().setSimulateSynchronousReturn(true);

        try (AMQPPublisher sender = new AMQPPublisher(conn, new MockComponentLog("id", ""), false)) {
            // In AT_MOST_ONCE mode, undeliverable messages only produce a warning — no exception
            sender.publish("hello".getBytes(), null, "wrongKey", "myExchange");
        }
        conn.close();
    }

}
