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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttCallback;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttConnectionProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class HiveMqV5ClientAdapter implements MqttClient {

    private final Mqtt5BlockingClient mqtt5BlockingClient;
    private final ComponentLog logger;

    private MqttCallback callback;

    public HiveMqV5ClientAdapter(Mqtt5BlockingClient mqtt5BlockingClient, ComponentLog logger) {
        this.mqtt5BlockingClient = mqtt5BlockingClient;
        this.logger = logger;
    }

    @Override
    public boolean isConnected() {
        return mqtt5BlockingClient.getState().isConnected();
    }

    @Override
    public void connect(MqttConnectionProperties connectionProperties) {
        logger.debug("Connecting to broker");

        final Mqtt5ConnectBuilder connectBuilder = Mqtt5Connect.builder()
                .keepAlive(connectionProperties.getKeepAliveInterval());

        final boolean cleanSession = connectionProperties.isCleanSession();
        connectBuilder.cleanStart(cleanSession);
        if (!cleanSession) {
            connectBuilder.sessionExpiryInterval(connectionProperties.getSessionExpiryInterval());
        }

        final String lastWillTopic = connectionProperties.getLastWillTopic();
        if (lastWillTopic != null) {
            connectBuilder.willPublish()
                    .topic(lastWillTopic)
                    .payload(connectionProperties.getLastWillMessage().getBytes())
                    .retain(connectionProperties.getLastWillRetain())
                    .qos(MqttQos.fromCode(connectionProperties.getLastWillQOS()))
                    .applyWillPublish();
        }

        final String username = connectionProperties.getUsername();
        final String password = connectionProperties.getPassword();
        if (username != null && password != null) {
            connectBuilder.simpleAuth()
                    .username(connectionProperties.getUsername())
                    .password(password.getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth();
        }

        final Mqtt5Connect mqtt5Connect = connectBuilder.build();
        mqtt5BlockingClient.connect(mqtt5Connect);
    }

    @Override
    public void disconnect(long disconnectTimeout) {
        logger.debug("Disconnecting client");
        // Currently it is not possible to set timeout for disconnect with HiveMQ Client. (Only connect timeout exists.)
        mqtt5BlockingClient.disconnect();
    }

    @Override
    public void close() {
        // there is no paho's close equivalent in hivemq client
    }

    @Override
    public void publish(String topic, StandardMqttMessage message) {
        logger.debug("Publishing message to {} with QoS: {}", topic, message.getQos());

        mqtt5BlockingClient.publishWith()
                .topic(topic)
                .payload(message.getPayload())
                .retain(message.isRetained())
                .qos(Objects.requireNonNull(MqttQos.fromCode(message.getQos())))
                .send();
    }

    @Override
    public void subscribe(String topicFilter, int qos) {
        Objects.requireNonNull(callback, "callback should be set");

        logger.debug("Subscribing to {} with QoS: {}", topicFilter, qos);

        CompletableFuture<Mqtt5SubAck> ack = mqtt5BlockingClient.toAsync().subscribeWith()
                .topicFilter(topicFilter)
                .qos(Objects.requireNonNull(MqttQos.fromCode(qos)))
                .callback(mqtt5Publish -> {
                    final ReceivedMqttMessage receivedMessage = new ReceivedMqttMessage(
                            mqtt5Publish.getPayloadAsBytes(),
                            mqtt5Publish.getQos().getCode(),
                            mqtt5Publish.isRetain(),
                            mqtt5Publish.getTopic().toString());
                    callback.messageArrived(receivedMessage);
                })
                .send();

        // Setting "listener" callback is only possible with async client, though sending subscribe message
        // should happen in a blocking way to make sure the processor is blocked until ack is not arrived.
        ack.whenComplete((mqtt5SubAck, throwable) -> {
            logger.debug("Received mqtt5 subscribe ack: {}", mqtt5SubAck.toString());

            if (throwable != null) {
                throw new MqttException("An error has occurred during sending subscribe message to broker", throwable);
            }
        });
    }

    @Override
    public void setCallback(MqttCallback callback) {
        this.callback = callback;
    }

}
