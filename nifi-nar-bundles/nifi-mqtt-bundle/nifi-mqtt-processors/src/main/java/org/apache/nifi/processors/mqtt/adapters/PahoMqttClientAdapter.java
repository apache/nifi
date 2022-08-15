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
import org.apache.nifi.processors.mqtt.common.MqttConnectionProperties;
import org.apache.nifi.processors.mqtt.common.MqttCallback;
import org.apache.nifi.processors.mqtt.common.MqttClient;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.processors.mqtt.common.StandardMqttMessage;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Properties;

public class PahoMqttClientAdapter implements MqttClient {

    private final IMqttClient client;
    private final ComponentLog logger;

    public PahoMqttClientAdapter(org.eclipse.paho.client.mqttv3.MqttClient client, ComponentLog logger) {
        this.client = client;
        this.logger = logger;
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void connect(MqttConnectionProperties connectionProperties) {
        logger.debug("Connecting to broker");

        try {
            final MqttConnectOptions connectOptions = new MqttConnectOptions();

            connectOptions.setCleanSession(connectionProperties.isCleanSession());
            connectOptions.setKeepAliveInterval(connectionProperties.getKeepAliveInterval());
            connectOptions.setMqttVersion(connectionProperties.getMqttVersion().getVersionCode());
            connectOptions.setConnectionTimeout(connectionProperties.getConnectionTimeout());

            final SSLContextService sslContextService = connectionProperties.getSslContextService();
            if (sslContextService != null) {
                connectOptions.setSSLProperties(transformSSLContextService(sslContextService));
            }

            final String lastWillTopic = connectionProperties.getLastWillTopic();
            if (lastWillTopic != null) {
                boolean lastWillRetain = connectionProperties.getLastWillRetain() != null && connectionProperties.getLastWillRetain();
                connectOptions.setWill(lastWillTopic, connectionProperties.getLastWillMessage().getBytes(), connectionProperties.getLastWillQOS(), lastWillRetain);
            }

            final String username = connectionProperties.getUsername();
            if (username != null) {
                connectOptions.setUserName(username);
                connectOptions.setPassword(connectionProperties.getPassword().toCharArray());
            }

            client.connect(connectOptions);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during connecting to broker", e);
        }
    }

    @Override
    public void disconnect(long disconnectTimeout) {
        logger.debug("Disconnecting client with timeout: {}", disconnectTimeout);

        try {
            client.disconnect(disconnectTimeout);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during disconnecting client with timeout: " + disconnectTimeout, e);
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

    public static Properties transformSSLContextService(SSLContextService sslContextService) {
        final Properties properties = new Properties();
        if (sslContextService.getSslAlgorithm() != null) {
            properties.setProperty("com.ibm.ssl.protocol", sslContextService.getSslAlgorithm());
        }
        if (sslContextService.getKeyStoreFile() != null) {
            properties.setProperty("com.ibm.ssl.keyStore", sslContextService.getKeyStoreFile());
        }
        if (sslContextService.getKeyStorePassword() != null) {
            properties.setProperty("com.ibm.ssl.keyStorePassword", sslContextService.getKeyStorePassword());
        }
        if (sslContextService.getKeyStoreType() != null) {
            properties.setProperty("com.ibm.ssl.keyStoreType", sslContextService.getKeyStoreType());
        }
        if (sslContextService.getTrustStoreFile() != null) {
            properties.setProperty("com.ibm.ssl.trustStore", sslContextService.getTrustStoreFile());
        }
        if (sslContextService.getTrustStorePassword() != null) {
            properties.setProperty("com.ibm.ssl.trustStorePassword", sslContextService.getTrustStorePassword());
        }
        if (sslContextService.getTrustStoreType() != null) {
            properties.setProperty("com.ibm.ssl.trustStoreType", sslContextService.getTrustStoreType());
        }
        return  properties;
    }

}
