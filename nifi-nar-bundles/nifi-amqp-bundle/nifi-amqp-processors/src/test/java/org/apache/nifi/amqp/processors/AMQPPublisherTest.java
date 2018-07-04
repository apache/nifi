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
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;
import org.mockito.Mockito;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;

public class AMQPPublisherTest {

    @SuppressWarnings("resource")
    @Test(expected = IllegalArgumentException.class)
    public void failOnNullConnection() {
        new AMQPPublisher(null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void failPublishIfChannelClosed() throws Exception {
        Connection conn = new TestConnection(null, null);
        try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class))) {
            conn.close();
            sender.publish("oleg".getBytes(), null, "foo", "");
        }
    }

    @Test(expected = IllegalStateException.class)
    public void failPublishIfChannelFails() throws Exception {
        TestConnection conn = new TestConnection(null, null);
        try (AMQPPublisher sender = new AMQPPublisher(conn, mock(ComponentLog.class))) {
            ((TestChannel) conn.createChannel()).corruptChannel();
            sender.publish("oleg".getBytes(), null, "foo", "");
        }
    }

    @Test
    public void validateSuccessfullPublishingAndRouting() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1", "queue2"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        try (AMQPPublisher sender = new AMQPPublisher(connection, mock(ComponentLog.class))) {
            sender.publish("hello".getBytes(), null, "key1", "myExchange");
        }

        assertNotNull(connection.createChannel().basicGet("queue1", true));
        assertNotNull(connection.createChannel().basicGet("queue2", true));

        connection.close();
    }

    @Test
    public void validateSuccessfullPublishingAndUndeliverableRoutingKey() throws Exception {
        Map<String, List<String>> routingMap = new HashMap<>();
        routingMap.put("key1", Arrays.asList("queue1", "queue2"));
        Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
        exchangeToRoutingKeymap.put("myExchange", "key1");

        Connection connection = new TestConnection(exchangeToRoutingKeymap, routingMap);

        ReturnListener retListener = mock(ReturnListener.class);
        connection.createChannel().addReturnListener(retListener);

        try (AMQPPublisher sender = new AMQPPublisher(connection, new MockComponentLog("foo", ""))) {
            sender.publish("hello".getBytes(), null, "key1", "myExchange");
        }

        verify(retListener, atMost(1)).handleReturn(Mockito.anyInt(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(BasicProperties.class), (byte[]) Mockito.any());
        connection.close();
    }

}
