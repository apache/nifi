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

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import java.util.concurrent.atomic.AtomicBoolean;

public class MqttTestClient implements IMqttClient {

    public String serverURI;
    public String clientId;

    public AtomicBoolean connected = new AtomicBoolean(false);

    public MqttCallback mqttCallback;
    public ConnectType type;
    public enum ConnectType {Publisher, Subscriber}

    public MQTTQueueMessage publishedMessage;

    public String subscribedTopic;
    public int subscribedQos;


    public MqttTestClient(String serverURI, String clientId, ConnectType type) throws MqttException {
        this.serverURI = serverURI;
        this.clientId = clientId;
        this.type = type;
    }

    @Override
    public void connect() throws MqttSecurityException, MqttException {
        connected.set(true);
    }

    @Override
    public void connect(MqttConnectOptions options) throws MqttSecurityException, MqttException {
        connected.set(true);
    }

    @Override
    public IMqttToken connectWithResult(MqttConnectOptions options) throws MqttSecurityException, MqttException {
        return null;
    }

    @Override
    public void disconnect() throws MqttException {
        connected.set(false);
    }

    @Override
    public void disconnect(long quiesceTimeout) throws MqttException {
        connected.set(false);
    }

    @Override
    public void disconnectForcibly() throws MqttException {
        connected.set(false);
    }

    @Override
    public void disconnectForcibly(long disconnectTimeout) throws MqttException {
        connected.set(false);
    }

    @Override
    public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout) throws MqttException {
        connected.set(false);
    }

    @Override
    public void subscribe(String topicFilter) throws MqttException, MqttSecurityException {
        subscribedTopic = topicFilter;
        subscribedQos = -1;
    }

    @Override
    public void subscribe(String[] topicFilters) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }

    @Override
    public void subscribe(String topicFilter, int qos) throws MqttException {
        subscribedTopic = topicFilter;
        subscribedQos = qos;
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }

    @Override
    public void subscribe(String s, IMqttMessageListener iMqttMessageListener) throws MqttException, MqttSecurityException {

    }

    @Override
    public void subscribe(String[] strings, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {

    }

    @Override
    public void subscribe(String s, int i, IMqttMessageListener iMqttMessageListener) throws MqttException {

    }

    @Override
    public void subscribe(String[] strings, int[] ints, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {

    }

    @Override
    public IMqttToken subscribeWithResponse(String s) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String s, IMqttMessageListener iMqttMessageListener) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String s, int i) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String s, int i, IMqttMessageListener iMqttMessageListener) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] strings) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] strings, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] strings, int[] ints) throws MqttException {
        return null;
    }

    @Override
    public IMqttToken subscribeWithResponse(String[] strings, int[] ints, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {
        return null;
    }

    @Override
    public void unsubscribe(String topicFilter) throws MqttException {
        subscribedTopic = "";
        subscribedQos = -2;
    }

    @Override
    public void unsubscribe(String[] topicFilters) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws MqttException, MqttPersistenceException {
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        message.setRetained(retained);
        switch (type) {
            case Publisher:
                publishedMessage = new MQTTQueueMessage(topic, message);
                break;
            case Subscriber:
                try {
                    mqttCallback.messageArrived(topic, message);
                } catch (Exception e) {
                    throw new MqttException(e);
                }
                break;
        }
    }

    @Override
    public void publish(String topic, MqttMessage message) throws MqttException, MqttPersistenceException {
        switch (type) {
            case Publisher:
                publishedMessage = new MQTTQueueMessage(topic, message);
                break;
            case Subscriber:
                try {
                    mqttCallback.messageArrived(topic, message);
                } catch (Exception e) {
                    throw new MqttException(e);
                }
                break;
        }
    }

    @Override
    public void setCallback(MqttCallback callback) {
        this.mqttCallback = callback;
    }

    @Override
    public MqttTopic getTopic(String topic) {
        return null;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public String getServerURI() {
        return serverURI;
    }

    @Override
    public IMqttDeliveryToken[] getPendingDeliveryTokens() {
        return new IMqttDeliveryToken[0];
    }

    @Override
    public void setManualAcks(boolean b) {

    }

    @Override
    public void messageArrivedComplete(int i, int i1) throws MqttException {

    }

    @Override
    public void close() throws MqttException {

    }
}
