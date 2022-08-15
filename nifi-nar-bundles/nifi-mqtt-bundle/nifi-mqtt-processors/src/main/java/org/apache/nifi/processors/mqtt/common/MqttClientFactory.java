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

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.adapters.HiveMqV5ClientAdapter;
import org.apache.nifi.processors.mqtt.adapters.PahoMqttClientAdapter;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.SSL;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WS;
import static org.apache.nifi.processors.mqtt.common.MqttProtocolScheme.WSS;

public class MqttClientFactory {
    public MqttClient create(MqttClientProperties clientProperties, MqttConnectionProperties connectionProperties, ComponentLog logger) throws TlsException {
        switch (clientProperties.getMqttVersion()) {
            case MQTT_VERSION_3_AUTO:
            case MQTT_VERSION_3_1:
            case MQTT_VERSION_3_1_1:
                return createPahoMqttV3ClientAdapter(clientProperties, logger);
            case MQTT_VERSION_5_0:
                return createHiveMqV5ClientAdapter(clientProperties, connectionProperties, logger);
            default:
                throw new MqttException("Unsupported Mqtt version: " + clientProperties.getMqttVersion());
        }
    }

    private PahoMqttClientAdapter createPahoMqttV3ClientAdapter(MqttClientProperties clientProperties, ComponentLog logger) {
        logger.debug("Creating Mqtt v3 client");

        try {
            return new PahoMqttClientAdapter(new org.eclipse.paho.client.mqttv3.MqttClient(clientProperties.getBroker(), clientProperties.getClientId(), new MemoryPersistence()), logger);
        } catch (org.eclipse.paho.client.mqttv3.MqttException e) {
            throw new MqttException("An error has occurred during creating adapter for MQTT v3 client", e);
        }
    }

    private HiveMqV5ClientAdapter createHiveMqV5ClientAdapter(MqttClientProperties clientProperties, MqttConnectionProperties connectionProperties, ComponentLog logger) throws TlsException {
        logger.debug("Creating Mqtt v5 client");

        Mqtt5ClientBuilder mqtt5ClientBuilder = Mqtt5Client.builder()
                .identifier(clientProperties.getClientId())
                .serverHost(clientProperties.getBrokerUri().getHost());

        int port = clientProperties.getBrokerUri().getPort();
        if (port != -1) {
            mqtt5ClientBuilder.serverPort(port);
        }

        // default is tcp
        if (WS.equals(clientProperties.getScheme()) || WSS.equals(clientProperties.getScheme())) {
            mqtt5ClientBuilder.webSocketConfig().applyWebSocketConfig();
        }

        if (SSL.equals(clientProperties.getScheme())) {
            if (connectionProperties.getSslContextService().getTrustStoreFile() != null) {
                mqtt5ClientBuilder
                        .sslConfig()
                        .trustManagerFactory(KeyStoreUtils.loadTrustManagerFactory(
                                connectionProperties.getSslContextService().getTrustStoreFile(),
                                connectionProperties.getSslContextService().getTrustStorePassword(),
                                connectionProperties.getSslContextService().getTrustStoreType()))
                        .applySslConfig();
            }

            if (connectionProperties.getSslContextService().getKeyStoreFile() != null) {
                mqtt5ClientBuilder
                        .sslConfig()
                        .keyManagerFactory(KeyStoreUtils.loadKeyManagerFactory(
                                connectionProperties.getSslContextService().getKeyStoreFile(),
                                connectionProperties.getSslContextService().getKeyStorePassword(),
                                null,
                                connectionProperties.getSslContextService().getKeyStoreType()))
                        .applySslConfig();
            }
        }

        return new HiveMqV5ClientAdapter(mqtt5ClientBuilder.buildBlocking(), logger);
    }
}
