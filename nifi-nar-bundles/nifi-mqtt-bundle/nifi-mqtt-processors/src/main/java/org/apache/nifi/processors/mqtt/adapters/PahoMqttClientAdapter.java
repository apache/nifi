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
import org.apache.nifi.processors.mqtt.common.MqttCallback;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttClientProperties;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.security.util.TlsConfiguration;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URI;
import java.util.Properties;

public class PahoMqttClientAdapter implements MqttClient {

    public static final int DISCONNECT_TIMEOUT = 5000;

    private final IMqttClient client;
    private final MqttClientProperties clientProperties;
    private final ComponentLog logger;

    public PahoMqttClientAdapter(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) {
        this.client = createClient(brokerUri, clientProperties, logger);
        this.clientProperties = clientProperties;
        this.logger = logger;
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void connect() {
        logger.debug("Connecting to broker");

        try {
            final MqttConnectOptions connectOptions = new MqttConnectOptions();

            connectOptions.setCleanSession(clientProperties.isCleanSession());
            connectOptions.setKeepAliveInterval(clientProperties.getKeepAliveInterval());
            connectOptions.setMqttVersion(clientProperties.getMqttVersion().getVersionCode());
            connectOptions.setConnectionTimeout(clientProperties.getConnectionTimeout());

            final TlsConfiguration tlsConfiguration = clientProperties.getTlsConfiguration();
            if (tlsConfiguration != null) {
                connectOptions.setSSLProperties(transformSSLContextService(tlsConfiguration));
            }

            final String lastWillTopic = clientProperties.getLastWillTopic();
            if (lastWillTopic != null) {
                boolean lastWillRetain = clientProperties.getLastWillRetain() != null && clientProperties.getLastWillRetain();
                connectOptions.setWill(lastWillTopic, clientProperties.getLastWillMessage().getBytes(), clientProperties.getLastWillQos(), lastWillRetain);
            }

            final String username = clientProperties.getUsername();
            if (username != null) {
                connectOptions.setUserName(username);
                connectOptions.setPassword(clientProperties.getPassword().toCharArray());
            }

            client.connect(connectOptions);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during connecting to broker", e);
        }
    }

    @Override
    public void disconnect() {
        logger.debug("Disconnecting client with timeout: {}", DISCONNECT_TIMEOUT);

        try {
            client.disconnect(DISCONNECT_TIMEOUT);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during disconnecting client with timeout: " + DISCONNECT_TIMEOUT, e);
        }
    }

    @Override
    public void close() {
        logger.debug("Closing client");

        try {
            client.close();
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during closing client", e);
        }
    }

    @Override
    public void publish(String topic, StandardMqttMessage message) {
        logger.debug("Publishing message to {} with QoS: {}", topic, message.getQos());

        try {
            client.publish(topic, message.getPayload(), message.getQos(), message.isRetained());
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during publishing message to " + topic + " with QoS: " + message.getQos(), e);
        }
    }

    @Override
    public void subscribe(String topicFilter, int qos) {
        logger.debug("Subscribing to {} with QoS: {}", topicFilter, qos);

        try {
            client.subscribe(topicFilter, qos);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during subscribing to " + topicFilter + " with QoS: " + qos, e);
        }
    }

    @Override
    public void setCallback(MqttCallback callback) {
        client.setCallback(new org.eclipse.paho.client.mqttv3.MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                callback.connectionLost(cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                logger.debug("Message arrived with id: {}", message.getId());
                final ReceivedMqttMessage receivedMessage = new ReceivedMqttMessage(message.getPayload(), message.getQos(), message.isRetained(), topic);
                callback.messageArrived(receivedMessage);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                callback.deliveryComplete(token.toString());
            }
        });
    }

    public static Properties transformSSLContextService(TlsConfiguration tlsConfiguration) {
        final Properties properties = new Properties();
        if (tlsConfiguration.getProtocol() != null) {
            properties.setProperty("com.ibm.ssl.protocol", tlsConfiguration.getProtocol());
        }
        if (tlsConfiguration.getKeystorePath() != null) {
            properties.setProperty("com.ibm.ssl.keyStore", tlsConfiguration.getKeystorePath());
        }
        if (tlsConfiguration.getKeystorePassword() != null) {
            properties.setProperty("com.ibm.ssl.keyStorePassword", tlsConfiguration.getKeystorePassword());
        }
        if (tlsConfiguration.getKeystoreType() != null) {
            properties.setProperty("com.ibm.ssl.keyStoreType", tlsConfiguration.getKeystoreType().getType());
        }
        if (tlsConfiguration.getTruststorePath() != null) {
            properties.setProperty("com.ibm.ssl.trustStore", tlsConfiguration.getTruststorePath());
        }
        if (tlsConfiguration.getTruststorePassword() != null) {
            properties.setProperty("com.ibm.ssl.trustStorePassword", tlsConfiguration.getTruststorePassword());
        }
        if (tlsConfiguration.getTruststoreType() != null) {
            properties.setProperty("com.ibm.ssl.trustStoreType", tlsConfiguration.getTruststoreType().getType());
        }
        return  properties;
    }

    private static org.eclipse.paho.client.mqttv3.MqttClient createClient(URI brokerUri, MqttClientProperties clientProperties, ComponentLog logger) {
        logger.debug("Creating Mqtt v3 client");

        try {
            return new org.eclipse.paho.client.mqttv3.MqttClient(brokerUri.toString(), clientProperties.getClientId(), new MemoryPersistence());
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during creating adapter for MQTT v3 client", e);
        }
    }

}
