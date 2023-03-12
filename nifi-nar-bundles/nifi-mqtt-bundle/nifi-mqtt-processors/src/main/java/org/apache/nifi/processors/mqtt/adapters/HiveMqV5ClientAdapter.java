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
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttProtocolScheme;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessageHandler;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.SSL;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WS;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WSS;

public class HiveMqV5ClientAdapter implements MqttClient {

    private final Mqtt5BlockingClient mqtt5BlockingClient;
    private final MqttClientProperties clientProperties;
    private final ComponentLog logger;

    public HiveMqV5ClientAdapter(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) throws TlsException {
        this.mqtt5BlockingClient = createClient(brokerUri, clientProperties, logger);
        this.clientProperties = clientProperties;
        this.logger = logger;
    }

    @Override
    public boolean isConnected() {
        return mqtt5BlockingClient.getState().isConnected();
    }

    @Override
    public void connect() {
        logger.debug("Connecting to broker");

        final Mqtt5ConnectBuilder connectBuilder = Mqtt5Connect.builder()
                .keepAlive(clientProperties.getKeepAliveInterval());

        final boolean cleanSession = clientProperties.isCleanSession();
        connectBuilder.cleanStart(cleanSession);
        if (!cleanSession) {
            connectBuilder.sessionExpiryInterval(clientProperties.getSessionExpiryInterval());
        }

        final String lastWillTopic = clientProperties.getLastWillTopic();
        if (lastWillTopic != null) {
            connectBuilder.willPublish()
                    .topic(lastWillTopic)
                    .payload(clientProperties.getLastWillMessage().getBytes())
                    .retain(clientProperties.getLastWillRetain())
                    .qos(MqttQos.fromCode(clientProperties.getLastWillQos()))
                    .applyWillPublish();
        }

        final String username = clientProperties.getUsername();
        final String password = clientProperties.getPassword();
        if (username != null && password != null) {
            connectBuilder.simpleAuth()
                    .username(clientProperties.getUsername())
                    .password(password.getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth();
        }

        final Mqtt5Connect mqtt5Connect = connectBuilder.build();
        mqtt5BlockingClient.connect(mqtt5Connect);
    }

    @Override
    public void disconnect() {
        logger.debug("Disconnecting client");
        // Currently it is not possible to set timeout for disconnect with HiveMQ Client.
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
    public void subscribe(String topicFilter, int qos, ReceivedMqttMessageHandler handler) {
        logger.debug("Subscribing to {} with QoS: {}", topicFilter, qos);

        CompletableFuture<Mqtt5SubAck> futureAck = mqtt5BlockingClient.toAsync().subscribeWith()
                .topicFilter(topicFilter)
                .qos(Objects.requireNonNull(MqttQos.fromCode(qos)))
                .callback(mqtt5Publish -> {
                    final ReceivedMqttMessage receivedMessage = new ReceivedMqttMessage(
                            mqtt5Publish.getPayloadAsBytes(),
                            mqtt5Publish.getQos().getCode(),
                            mqtt5Publish.isRetain(),
                            mqtt5Publish.getTopic().toString());
                    handler.handleReceivedMessage(receivedMessage);
                })
                .send();

        // Setting "listener" callback is only possible with async client, though sending subscribe message
        // should happen in a blocking way to make sure the processor is blocked until ack is not arrived.
        try {
            final Mqtt5SubAck ack = futureAck.get(clientProperties.getConnectionTimeout(), TimeUnit.SECONDS);
            logger.debug("Received mqtt5 subscribe ack: {}", ack);
        } catch (Exception e) {
            throw new MqttException("An error has occurred during sending subscribe message to broker", e);
        }
    }

    private static Mqtt5BlockingClient createClient(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) throws TlsException {
        logger.debug("Creating Mqtt v5 client");

        final Mqtt5ClientBuilder mqtt5ClientBuilder = Mqtt5Client.builder()
                .identifier(clientProperties.getClientId())
                .serverHost(brokerUri.getHost());

        final int port = brokerUri.getPort();
        if (port != -1) {
            mqtt5ClientBuilder.serverPort(port);
        }

        final MqttProtocolScheme scheme = MqttProtocolScheme.valueOf(brokerUri.getScheme().toUpperCase());
        // default is tcp
        if (WS.equals(scheme) || WSS.equals(scheme)) {
            mqtt5ClientBuilder.webSocketConfig().applyWebSocketConfig();
        }

        if (SSL.equals(scheme) || WSS.equals(scheme)) {
            if (clientProperties.getTlsConfiguration().getTruststorePath() != null) {
                mqtt5ClientBuilder
                        .sslConfig()
                        .trustManagerFactory(KeyStoreUtils.loadTrustManagerFactory(
                                clientProperties.getTlsConfiguration().getTruststorePath(),
                                clientProperties.getTlsConfiguration().getTruststorePassword(),
                                clientProperties.getTlsConfiguration().getTruststoreType().getType()))
                        .applySslConfig();
            }

            if (clientProperties.getTlsConfiguration().getKeystorePath() != null) {
                mqtt5ClientBuilder
                        .sslConfig()
                        .keyManagerFactory(KeyStoreUtils.loadKeyManagerFactory(
                                clientProperties.getTlsConfiguration().getKeystorePath(),
                                clientProperties.getTlsConfiguration().getKeystorePassword(),
                                null,
                                clientProperties.getTlsConfiguration().getKeystoreType().getType()))
                        .applySslConfig();
            }
        }

        return mqtt5ClientBuilder.buildBlocking();
    }
}
