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

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public enum MqttVersion {
    MQTT_VERSION_3_AUTO(MqttConnectOptions.MQTT_VERSION_DEFAULT, "v3 AUTO"),
    MQTT_VERSION_3_1(MqttConnectOptions.MQTT_VERSION_3_1, "v3.1.0"),
    MQTT_VERSION_3_1_1(MqttConnectOptions.MQTT_VERSION_3_1_1, "v3.1.1"),
    MQTT_VERSION_5_0(5, "v5.0");

    private final int versionCode;
    private final String displayName;

    MqttVersion(int versionCode, String displayName) {
        this.versionCode = versionCode;
        this.displayName = displayName;
    }

    public int getVersionCode() {
        return versionCode;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static MqttVersion fromVersionCode(int versionCode) {
        for (MqttVersion version : values()) {
            if (version.getVersionCode() == versionCode) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unable to map MqttVersionCode from version code: " + versionCode);
    }
}
