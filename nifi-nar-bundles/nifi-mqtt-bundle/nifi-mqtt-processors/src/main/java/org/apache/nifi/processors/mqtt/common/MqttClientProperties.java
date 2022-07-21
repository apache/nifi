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

import java.net.URI;

public class MqttClientProperties {
    private URI brokerURI;
    private String clientID;

    private int mqttVersion;

    public String getBroker() {
        return brokerURI.toString();
    }

    public MqttConstants.SupportedSchemes getScheme() {
        return MqttConstants.SupportedSchemes.valueOf(brokerURI.getScheme().toUpperCase());
    }

    public URI getBrokerURI() {
        return brokerURI;
    }

    public void setBrokerURI(URI brokerURI) {
        this.brokerURI = brokerURI;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public int getMqttVersion() {
        return mqttVersion;
    }

    public void setMqttVersion(int mqttVersion) {
        this.mqttVersion = mqttVersion;
    }
}
