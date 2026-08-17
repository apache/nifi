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
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.MqttProtocolScheme;
import org.apache.nifi.processors.mqtt.common.MqttTopicSubscription;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessageHandler;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextProvider;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;

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

    // Package-private constructor for injecting a test double for the underlying HiveMQ client.
    HiveMqV5ClientAdapter(Mqtt5BlockingClient mqtt5BlockingClient, MqttClientProperties clientProperties, ComponentLog logger) {
        this.mqtt5BlockingClient = mqtt5BlockingClient;
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
    public void subscribe(List<MqttTopicSubscription> subscriptions, ReceivedMqttMessageHandler handler) {
        logger.debug("Subscribing to {}", subscriptions);

        final List<Mqtt5Subscription> mqtt5Subscriptions = subscriptions.stream()
                .map(subscription -> Mqtt5Subscription.builder()
                        .topicFilter(subscription.topicFilter())
                        .qos(Objects.requireNonNull(MqttQos.fromCode(subscription.qos())))
                        .build())
                .collect(Collectors.toList());

        final Mqtt5Subscribe mqtt5Subscribe = Mqtt5Subscribe.builder()
                .addSubscriptions(mqtt5Subscriptions)
                .build();

        // Setting the "listener" callback is only possible with the async client, though sending the subscribe
        // message should happen in a blocking way to make sure the processor is blocked until the ack arrives.
        final CompletableFuture<Mqtt5SubAck> futureAck = mqtt5BlockingClient.toAsync().subscribe(mqtt5Subscribe, mqtt5Publish -> {
            final ReceivedMqttMessage receivedMessage = new ReceivedMqttMessage(
                    mqtt5Publish.getPayloadAsBytes(),
                    mqtt5Publish.getQos().getCode(),
                    mqtt5Publish.isRetain(),
                    mqtt5Publish.getTopic().toString());
            handler.handleReceivedMessage(receivedMessage);
        });

        final Mqtt5SubAck ack;
        try {
            ack = futureAck.get(clientProperties.getConnectionTimeout(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new MqttException("An error has occurred during sending subscribe message to broker", e);
        }
        logger.debug("Received mqtt5 subscribe ack: {}", ack);

        // A SUBACK carries one reason code per requested Topic Filter, in the order they were sent, so a subscription
        // can be rejected individually, for example due to an ACL denial, while the others are granted.
        final List<Mqtt5SubAckReasonCode> reasonCodes = ack.getReasonCodes();
        final List<String> failedTopicFilters = new ArrayList<>();
        for (int i = 0; i < reasonCodes.size() && i < subscriptions.size(); i++) {
            if (reasonCodes.get(i).isError()) {
                failedTopicFilters.add(subscriptions.get(i).topicFilter() + " (" + reasonCodes.get(i) + ")");
            }
        }
        if (!failedTopicFilters.isEmpty()) {
            throw new MqttException("Broker rejected subscription for the following topic filter(s): " + failedTopicFilters);
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

        return mqtt5ClientBuilder.buildBlocking();
    }
}
