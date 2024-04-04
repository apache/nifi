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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.mqtt.adapters.HiveMqV5ClientAdapter;
import org.apache.nifi.processors.mqtt.adapters.PahoMqttClientAdapter;
import org.apache.nifi.security.util.TlsException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MqttClientFactory {

    private final MqttClientProperties clientProperties;
    private final ComponentLog logger;

    private final RoundRobinList<URI> brokerUris;

    public MqttClientFactory(MqttClientProperties clientProperties, ComponentLog logger) {
        this.clientProperties = clientProperties;
        this.logger = logger;
        this.brokerUris = new RoundRobinList<>(clientProperties.getBrokerUris());
    }

    public MqttClient create() throws TlsException {
        switch (clientProperties.getMqttVersion()) {
            case MQTT_VERSION_3_AUTO:
            case MQTT_VERSION_3_1:
            case MQTT_VERSION_3_1_1:
                return new PahoMqttClientAdapter(brokerUris.next(), clientProperties, logger);
            case MQTT_VERSION_5_0:
                return new HiveMqV5ClientAdapter(brokerUris.next(), clientProperties, logger);
            default:
                throw new MqttException("Unsupported Mqtt version: " + clientProperties.getMqttVersion());
        }
    }

    static class RoundRobinList<T> {

        private final List<T> list;
        private int index = 0;

        public RoundRobinList(List<T> list) {
            this.list = new ArrayList<>(list);
        }

        public synchronized T next() {
            final T next = list.get(index);

            if (index < list.size() - 1) {
                index++;
            } else {
                index = 0;
            }

            return next;
        }
    }
}
