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
package org.apache.nifi.processors.aws.iot.util;

import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.nifi.logging.ProcessorLog;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.TimerPingSender;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.internal.NetworkModule;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttWebSocketAsyncClient extends MqttAsyncClient implements MqttCallback {

    protected volatile LinkedBlockingQueue<IoTMessage> awsQueuedMqttMessages = new LinkedBlockingQueue<IoTMessage>();
    protected final ProcessorLog logger;
    protected final String serverURI;

    protected static String createDummyURI(String original) {
        if (!original.startsWith("ws:") && !original.startsWith("wss:")) {
            return original;
        }
        final URI uri = URI.create(original);
        return "tcp://DUMMY-" + uri.getHost() + ":"
                + (uri.getPort() > 0 ? uri.getPort() : 80);
    }

    protected static boolean isDummyURI(String uri) {
        return uri.startsWith("tcp://DUMMY-");
    }

    public MqttWebSocketAsyncClient(String serverURI, String clientId,
                                    ProcessorLog logger) throws MqttException {
        super(createDummyURI(serverURI), clientId, new MemoryPersistence(), new TimerPingSender());
        this.serverURI = serverURI;
        this.logger = logger;
        this.setCallback(this);
    }

    @Override
    protected NetworkModule[] createNetworkModules(String address,
                                                   MqttConnectOptions options) throws MqttException{
        String[] serverURIs = options.getServerURIs();
        String[] array = serverURIs == null ? new String[] { address } :
            serverURIs.length == 0 ? new String[] { address }: serverURIs;

        NetworkModule[] networkModules = new NetworkModule[array.length];
        for (int i = 0; i < array.length; i++) {
            networkModules[i] = createNetworkModule(array[i], options);
        }
        return networkModules;
    }

    protected NetworkModule createNetworkModule(String input,
                                                MqttConnectOptions options) throws MqttException,
            MqttSecurityException {
        final String address = isDummyURI(input) ? this.serverURI : input;
        if (!address.startsWith("ws:") && !address.startsWith("wss:")) {
            return super.createNetworkModules(address, options)[0];
        }

        final String subProtocol = (options.getMqttVersion() == MqttConnectOptions.MQTT_VERSION_3_1) ? "mqttv3.1" : "mqtt";
        return newWebSocketNetworkModule(URI.create(address), subProtocol, options);
    }

    protected NetworkModule newWebSocketNetworkModule(URI uri,
                                                      String subProtocol, MqttConnectOptions options) {
        final WebSocketNetworkModule netModule = new WebSocketNetworkModule(
                uri, subProtocol, getClientId());
        netModule.setConnectTimeout(options.getConnectionTimeout());
        return netModule;
    }

    public LinkedBlockingQueue<IoTMessage> getAwsQueuedMqttMessages() {
        return awsQueuedMqttMessages;
    }

    public void setAwsQueuedMqttMessages(LinkedBlockingQueue<IoTMessage> queue) {
        awsQueuedMqttMessages = queue;
    }

    @Override
    public void connectionLost(Throwable t) {
        logger.error("Connection to " + this.getServerURI() + " lost with cause: " + t.getMessage());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        logger.debug("Message arrived from topic: " + topic);
        awsQueuedMqttMessages.add(new IoTMessage(message, topic));
    }
}
