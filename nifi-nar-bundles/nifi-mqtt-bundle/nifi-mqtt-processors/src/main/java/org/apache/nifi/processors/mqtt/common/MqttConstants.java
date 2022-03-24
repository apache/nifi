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

import org.apache.nifi.components.AllowableValue;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttConstants {

    /*
      ------------------------------------------
        Clean Session Values
      ------------------------------------------
     */

    public static final AllowableValue ALLOWABLE_VALUE_CLEAN_SESSION_TRUE =
            new AllowableValue("true", "Clean Session", "Client and Server discard any previous session and start a new " +
                    "one. This session lasts as long as the network connection. " +
                    "State data associated with this session is not reused in any subsequent session");

    public static final AllowableValue ALLOWABLE_VALUE_CLEAN_SESSION_FALSE =
            new AllowableValue("false", "Resume Session", "Server resumes communications with the client based on state from " +
                    "the current session (as identified by the ClientID). The client and server store the session after " +
                    "the client and server are disconnected. After the disconnection of a session that was not a clean session, " +
                    "the server stores further QoS 1 and QoS 2 messages that match any subscriptions that the client had at " +
                    "the time of disconnection as part of the session state");

    /*
      ------------------------------------------
        QoS Values
      ------------------------------------------
     */


    public static final AllowableValue ALLOWABLE_VALUE_QOS_0 =
            new AllowableValue("0", "0 - At most once", "Best effort delivery. A message won’t be acknowledged by the receiver or stored and redelivered by the sender. " +
                    "This is often called “fire and forget” and provides the same guarantee as the underlying TCP protocol.");

    public static final AllowableValue ALLOWABLE_VALUE_QOS_1 =
            new AllowableValue("1", "1 - At least once", "Guarantees that a message will be delivered at least once to the receiver. " +
                    "The message can also be delivered more than once");

    public static final AllowableValue ALLOWABLE_VALUE_QOS_2 =
            new AllowableValue("2", "2 - Exactly once", "Guarantees that each message is received only once by the counterpart. It is the safest and also " +
                    "the slowest quality of service level. The guarantee is provided by two round-trip flows between sender and receiver.");


    /*
      ------------------------------------------
        MQTT Version Values
      ------------------------------------------
     */
    public static final AllowableValue ALLOWABLE_VALUE_MQTT_VERSION_AUTO =
            new AllowableValue(String.valueOf(MqttConnectOptions.MQTT_VERSION_DEFAULT),
                    "AUTO",
                    "Start with v3.1.1 and fallback to v3.1.0 if not supported by a broker");

    public static final AllowableValue ALLOWABLE_VALUE_MQTT_VERSION_311 =
            new AllowableValue(String.valueOf(MqttConnectOptions.MQTT_VERSION_3_1_1),
                    "v3.1.1");

    public static final AllowableValue ALLOWABLE_VALUE_MQTT_VERSION_310 =
            new AllowableValue(String.valueOf(MqttConnectOptions.MQTT_VERSION_3_1),
                    "v3.1.0");
}
