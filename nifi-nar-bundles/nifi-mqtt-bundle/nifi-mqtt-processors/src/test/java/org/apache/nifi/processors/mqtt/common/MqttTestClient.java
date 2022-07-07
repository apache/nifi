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

import java.util.concurrent.atomic.AtomicBoolean;

public class MqttTestClient implements NifiMqttClient {

    public AtomicBoolean connected = new AtomicBoolean(false);

    public NifiMqttCallback nifiMqttCallback;
    public ConnectType type;
    public enum ConnectType {Publisher, Subscriber}

    public MQTTQueueMessage publishedMessage;

    public String subscribedTopic;
    public int subscribedQos;


    public MqttTestClient(ConnectType type) {
        this.type = type;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void connect(MqttConnectionProperties connectionProperties) throws NifiMqttException {
        connected.set(true);
    }

    @Override
    public void disconnect(long disconnectTimeout) throws NifiMqttException {
        connected.set(false);
    }

    @Override
    public void close() throws NifiMqttException {

    }

    @Override
    public void publish(String topic, NifiMqttMessage message) throws NifiMqttException {
        switch (type) {
            case Publisher:
                publishedMessage = new MQTTQueueMessage(topic, message);
                break;
            case Subscriber:
                try {
                    nifiMqttCallback.messageArrived(topic, message);
                } catch (Exception e) {
                    throw new NifiMqttException(e);
                }
                break;
        }
    }

    @Override
    public void subscribe(String topicFilter, int qos) {
        subscribedTopic = topicFilter;
        subscribedQos = qos;
    }

    @Override
    public void setCallback(NifiMqttCallback callback) {
        this.nifiMqttCallback = callback;
    }
}
