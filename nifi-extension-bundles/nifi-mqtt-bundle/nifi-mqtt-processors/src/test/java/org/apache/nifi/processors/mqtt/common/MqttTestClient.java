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

package org.apache.nifi.processors.mqtt.common;

import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MqttTestClient implements MqttClient {

    private final Queue<Pair<String, StandardMqttMessage>> publishedMessages = new LinkedList<>();

    public AtomicBoolean connected = new AtomicBoolean(false);

    public ConnectType type;

    public enum ConnectType { Publisher, Subscriber }

    public String subscribedTopic;
    public int subscribedQos;
    public ReceivedMqttMessageHandler receivedMqttMessageHandler;
    public MqttTestClient(ConnectType type) {
        this.type = type;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void connect() {
        connected.set(true);
    }

    @Override
    public void disconnect() {
        connected.set(false);
    }

    @Override
    public void close() {

    }

    @Override
    public void publish(String topic, StandardMqttMessage message) {
        switch (type) {
            case Publisher:
                publishedMessages.add(Pair.of(topic, message));
                break;
            case Subscriber:
                receivedMqttMessageHandler.handleReceivedMessage(new ReceivedMqttMessage(message.getPayload(), message.getQos(), message.isRetained(), topic));
                break;
        }
    }

    @Override
    public void subscribe(String topicFilter, int qos, ReceivedMqttMessageHandler handler) {
        subscribedTopic = topicFilter;
        subscribedQos = qos;
        receivedMqttMessageHandler = handler;
    }

    public Pair<String, StandardMqttMessage> getLastPublished() {
        return publishedMessages.poll();
    }
}
