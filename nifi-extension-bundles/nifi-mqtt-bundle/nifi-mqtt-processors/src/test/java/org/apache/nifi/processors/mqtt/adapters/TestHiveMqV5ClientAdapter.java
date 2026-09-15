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
package org.apache.nifi.processors.mqtt.adapters;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttTopicSubscription;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHiveMqV5ClientAdapter {

    @Test
    public void testSubscribeMultipleTopicFiltersSendsSingleSubscribeRequest() {
        final Mqtt5BlockingClient blockingClient = mock(Mqtt5BlockingClient.class);
        final Mqtt5AsyncClient asyncClient = mock(Mqtt5AsyncClient.class);
        when(blockingClient.toAsync()).thenReturn(asyncClient);

        final Mqtt5SubAck subAck = mock(Mqtt5SubAck.class);
        when(subAck.getReasonCodes()).thenReturn(List.of(Mqtt5SubAckReasonCode.GRANTED_QOS_1, Mqtt5SubAckReasonCode.GRANTED_QOS_1));
        when(asyncClient.subscribe(any(Mqtt5Subscribe.class), any())).thenReturn(CompletableFuture.completedFuture(subAck));

        final MqttClientProperties clientProperties = new MqttClientProperties();
        clientProperties.setConnectionTimeout(5);
        final HiveMqV5ClientAdapter adapter = new HiveMqV5ClientAdapter(blockingClient, clientProperties, mock(ComponentLog.class));

        final List<MqttTopicSubscription> subscriptions = List.of(
                new MqttTopicSubscription("topic/a", 1),
                new MqttTopicSubscription("topic/b", 1));

        adapter.subscribe(subscriptions, message -> { });

        // Both topic filters have to be carried by a single SUBSCRIBE request.
        final ArgumentCaptor<Mqtt5Subscribe> captor = ArgumentCaptor.forClass(Mqtt5Subscribe.class);
        verify(asyncClient, times(1)).subscribe(captor.capture(), any());
        assertEquals(List.of("topic/a", "topic/b"), captor.getValue().getSubscriptions().stream()
                .map(subscription -> subscription.getTopicFilter().toString())
                .collect(Collectors.toList()));
    }

    @Test
    public void testSubscribeThrowsWhenBrokerRejectsATopicFilter() {
        final Mqtt5BlockingClient blockingClient = mock(Mqtt5BlockingClient.class);
        final Mqtt5AsyncClient asyncClient = mock(Mqtt5AsyncClient.class);
        when(blockingClient.toAsync()).thenReturn(asyncClient);

        final Mqtt5SubAck subAck = mock(Mqtt5SubAck.class);
        // Broker grants the first filter but rejects the second, e.g. due to an ACL denial.
        when(subAck.getReasonCodes()).thenReturn(List.of(Mqtt5SubAckReasonCode.GRANTED_QOS_1, Mqtt5SubAckReasonCode.NOT_AUTHORIZED));
        when(asyncClient.subscribe(any(Mqtt5Subscribe.class), any())).thenReturn(CompletableFuture.completedFuture(subAck));

        final MqttClientProperties clientProperties = new MqttClientProperties();
        clientProperties.setConnectionTimeout(5);
        final HiveMqV5ClientAdapter adapter = new HiveMqV5ClientAdapter(blockingClient, clientProperties, mock(ComponentLog.class));

        final List<MqttTopicSubscription> subscriptions = List.of(
                new MqttTopicSubscription("topic/a", 1),
                new MqttTopicSubscription("topic/denied", 1));

        final MqttException e = assertThrows(MqttException.class, () -> adapter.subscribe(subscriptions, message -> { }));
        assertTrue(e.getMessage().contains("topic/denied"));
    }
}
