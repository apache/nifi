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

public interface MqttClient {

    /**
     * Determines if this client is currently connected to an MQTT broker.
     *
     * @return whether the client is connected.
     */
    boolean isConnected();

    /**
     * Connects the client to an MQTT broker.
     */
    void connect();

    /**
     * Disconnects client from an MQTT broker.
     */
    void disconnect();

    /**
     * Releases all resource associated with the client. After the client has
     * been closed it cannot be reused. For instance attempts to connect will fail.
     */
    void close();

    /**
     * Publishes a message to a topic on the MQTT broker.
     *
     * @param topic to deliver the message to, for example "pipe-1/flow-rate"
     * @param message to deliver to the MQTT broker
     */
    void publish(String topic, StandardMqttMessage message);

    /**
     * Subscribe to a topic.
     *
     * @param topicFilter the topic to subscribe to, which can include wildcards.
     * @param qos the maximum quality of service at which to subscribe. Messages
     *            published at a lower quality of service will be received at the published
     *            QoS. Messages published at a higher quality of service will be received using
     *            the QoS specified on the subscribe.
     * @param handler that further processes the message received by the client
     */
    void subscribe(String topicFilter, int qos, ReceivedMqttMessageHandler handler);
}
