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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttTopicSubscription;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPahoMqttClientAdapter {

    private static final int SUBACK_FAILURE_CODE = 0x80;

    @Test
    public void testSubscribeMultipleTopicFiltersSendsSingleSubscribeRequest() throws Exception {
        final IMqttClient client = mock(IMqttClient.class);
        final IMqttToken token = mock(IMqttToken.class);
        when(client.subscribeWithResponse(any(String[].class), any(int[].class))).thenReturn(token);
        when(token.getGrantedQos()).thenReturn(new int[]{1, 1});

        final PahoMqttClientAdapter adapter = new PahoMqttClientAdapter(client, new MqttClientProperties(), mock(ComponentLog.class));

        final List<MqttTopicSubscription> subscriptions = List.of(
                new MqttTopicSubscription("topic/a", 1),
                new MqttTopicSubscription("topic/b", 1));

        adapter.subscribe(subscriptions, message -> { });

        verify(client, times(1)).subscribeWithResponse(new String[]{"topic/a", "topic/b"}, new int[]{1, 1});
    }

    @Test
    public void testSubscribeThrowsWhenBrokerRejectsATopicFilter() throws Exception {
        final IMqttClient client = mock(IMqttClient.class);
        final IMqttToken token = mock(IMqttToken.class);
        when(client.subscribeWithResponse(any(String[].class), any(int[].class))).thenReturn(token);
        // Broker grants the first filter but rejects the second, e.g. due to an ACL denial.
        when(token.getGrantedQos()).thenReturn(new int[]{1, SUBACK_FAILURE_CODE});

        final PahoMqttClientAdapter adapter = new PahoMqttClientAdapter(client, new MqttClientProperties(), mock(ComponentLog.class));

        final List<MqttTopicSubscription> subscriptions = List.of(
                new MqttTopicSubscription("topic/a", 1),
                new MqttTopicSubscription("topic/denied", 1));

        final MqttException e = assertThrows(MqttException.class, () -> adapter.subscribe(subscriptions, message -> { }));
        assertTrue(e.getMessage().contains("topic/denied"));
    }
}
