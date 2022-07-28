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
import org.apache.nifi.processors.mqtt.adapters.HiveMqV5ClientAdapter;
import org.apache.nifi.processors.mqtt.adapters.PahoMqttClientAdapter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import static org.apache.nifi.processors.mqtt.common.MqttConstants.SupportedSchemes.SSL;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.SupportedSchemes.WS;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.SupportedSchemes.WSS;

public class MqttClientFactory {
    public NifiMqttClient create(MqttClientProperties clientProperties, MqttConnectionProperties connectionProperties) {
        switch (clientProperties.getMqttVersion()) {
            case 0:
            case 3:
            case 4:
                return createPahoMqttV3ClientAdapter(clientProperties);
            case 5:
                return createHiveMqV5ClientAdapter(clientProperties, connectionProperties);
            default:
                throw new NifiMqttException("Unsupported Mqtt version: " + clientProperties.getMqttVersion());
        }
    }

    private PahoMqttClientAdapter createPahoMqttV3ClientAdapter(MqttClientProperties clientProperties) {
        try {
            return new PahoMqttClientAdapter(new MqttClient(clientProperties.getBroker(), clientProperties.getClientID(), new MemoryPersistence()));
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    private HiveMqV5ClientAdapter createHiveMqV5ClientAdapter(MqttClientProperties clientProperties, MqttConnectionProperties connectionProperties) {
        Mqtt5ClientBuilder mqtt5ClientBuilder = Mqtt5Client.builder()
                .identifier(clientProperties.getClientID())
                .serverHost(clientProperties.getBrokerURI().getHost());

        int port = clientProperties.getBrokerURI().getPort();
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
                        .trustManagerFactory(HiveMqV5ClientAdapter.getTrustManagerFactory(
                                connectionProperties.getSslContextService().getTrustStoreType(),
                                connectionProperties.getSslContextService().getTrustStoreFile(),
                                connectionProperties.getSslContextService().getTrustStorePassword().toCharArray()))
                        .applySslConfig();
            }

            if (connectionProperties.getSslContextService().getKeyStoreFile() != null) {
                mqtt5ClientBuilder
                        .sslConfig()
                        .keyManagerFactory(HiveMqV5ClientAdapter.getKeyManagerFactory(
                                connectionProperties.getSslContextService().getKeyStoreType(),
                                connectionProperties.getSslContextService().getKeyStoreFile(),
                                connectionProperties.getSslContextService().getKeyStorePassword().toCharArray()))
                        .applySslConfig();
            }
        }

        return new HiveMqV5ClientAdapter(mqtt5ClientBuilder.buildBlocking());
    }
}
