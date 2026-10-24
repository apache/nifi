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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttProtocolScheme;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessageHandler;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextProvider;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;

import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.SSL;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WS;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WSS;

@SuppressWarnings("PMD.UnusedReturnValue")
public class HiveMqV5ClientAdapter implements MqttClient {

    private final Mqtt5AsyncClient mqtt5AsyncClient;
    private final MqttClientProperties clientProperties;
    private final ComponentLog logger;

    public HiveMqV5ClientAdapter(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) throws TlsException {
        this.mqtt5AsyncClient = createClient(brokerUri, clientProperties, logger);
        this.clientProperties = clientProperties;
        this.logger = logger;
    }

    @Override
    public boolean isConnected() {
        return mqtt5AsyncClient.getState().isConnected();
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
        awaitWithTimeout(mqtt5AsyncClient.connect(mqtt5Connect));
    }

    @Override
    public void disconnect() {
        logger.debug("Disconnecting client");
        awaitWithTimeout(mqtt5AsyncClient.disconnect());
    }

    @Override
    public void close() {
        // there is no paho's close equivalent in hivemq client
    }

    @Override
    public void publish(String topic, StandardMqttMessage message) {
        logger.debug("Publishing message to {} with QoS: {}", topic, message.getQos());

        awaitWithTimeout(mqtt5AsyncClient.publishWith()
                .topic(topic)
                .payload(message.getPayload())
                .retain(message.isRetained())
                .qos(Objects.requireNonNull(MqttQos.fromCode(message.getQos())))
                .send());
    }

    @Override
    public void subscribe(String topicFilter, int qos) {
        logger.debug("Subscribing to {} with QoS: {}", topicFilter, qos);

        awaitWithTimeout(mqtt5AsyncClient.subscribeWith()
                .topicFilter(topicFilter)
                .qos(Objects.requireNonNull(MqttQos.fromCode(qos)))
                .send());
    }

    @Override
    public void setReceivedMessageHandler(ReceivedMqttMessageHandler handler) {
        mqtt5AsyncClient.publishes(MqttGlobalPublishFilter.ALL, mqtt5Publish -> {
            final ReceivedMqttMessage receivedMessage = new ReceivedMqttMessage(
                    mqtt5Publish.getPayloadAsBytes(),
                    mqtt5Publish.getQos().getCode(),
                    mqtt5Publish.isRetain(),
                    mqtt5Publish.getTopic().toString());
            handler.handleReceivedMessage(receivedMessage);
        });
    }

    private <T> T awaitWithTimeout(Future<T> future) {
        try {
            final T response = future.get(clientProperties.getConnectionTimeout(), TimeUnit.SECONDS);
            logger.debug("Received response: {}", response);
            return response;
        } catch (Exception e) {
            future.cancel(true);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new MqttException("An error has occurred while communicating with the broker", e);
        }
    }

    private static Mqtt5AsyncClient createClient(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) throws TlsException {
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
            final SSLContextProvider sslContextProvider = clientProperties.getSslContextProvider();

            if (sslContextProvider == null) {
                throw new TlsException("SSL Context Provider not configured for Broker URI scheme requiring TLS communication: " + scheme);
            }

            final X509TrustManager trustManager = sslContextProvider.createTrustManager();
            final TrustManagerFactory trustManagerFactory = new PredefinedTrustManagerFactory(trustManager);
            mqtt5ClientBuilder
                    .sslConfig()
                    .trustManagerFactory(trustManagerFactory)
                    .applySslConfig();

            final Optional<X509ExtendedKeyManager> keyManagerFound = sslContextProvider.createKeyManager();
            if (keyManagerFound.isPresent()) {
                final KeyManagerFactory keyManagerFactory = new PredefinedKeyManagerFactory(keyManagerFound.get());
                mqtt5ClientBuilder
                        .sslConfig()
                        .keyManagerFactory(keyManagerFactory)
                        .applySslConfig();
            }
        }

        return mqtt5ClientBuilder.buildAsync();
    }
}
