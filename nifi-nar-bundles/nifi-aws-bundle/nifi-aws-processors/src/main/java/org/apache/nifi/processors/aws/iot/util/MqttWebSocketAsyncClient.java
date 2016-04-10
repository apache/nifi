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
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.NetworkModule;
import org.eclipse.paho.client.mqttv3.logging.Logger;
import org.eclipse.paho.client.mqttv3.logging.LoggerFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class MqttWebSocketAsyncClient extends MqttAsyncClient implements MqttCallback {

    private volatile LinkedBlockingQueue<IoTMessage> awsQueuedMqttMessages = new LinkedBlockingQueue<IoTMessage>();
    private final ProcessorLog logger;
    private final String serverURI;

    private static String createDummyURI(String original) {
        if (!original.startsWith("ws:") && !original.startsWith("wss:")) {
            return original;
        }
        final URI uri = URI.create(original);
        return "tcp://DUMMY-" + uri.getHost() + ":"
                + (uri.getPort() > 0 ? uri.getPort() : 80);
    }

    private static boolean isDummyURI(String uri) {
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


    /**
     * Factory method to create the correct network module, based on the
     * supplied address URI.
     *
     * @param input
     *            the URI for the server.
     * @param options
     *            MQTT connect options
     * @return a network module appropriate to the specified address.
     */
    private NetworkModule createNetworkModule(String input,
                                                MqttConnectOptions options) throws MqttException,
            MqttSecurityException {
        final String address = isDummyURI(input) ? this.serverURI : input;
        if (!address.startsWith("ws:") && !address.startsWith("wss:")) {
            return super.createNetworkModules(address, options)[0];
        }

        final String subProtocol;
        if (options.getMqttVersion() == MqttConnectOptions.MQTT_VERSION_3_1) {
            // http://wiki.eclipse.org/Paho/Paho_Websockets#Ensuring_implementations_can_inter-operate
            subProtocol = "mqttv3.1";
        } else {
            // http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/cs01/mqtt-v3.1.1-cs01.html#_Toc388534418
            subProtocol = "mqtt";
        }
        return newWebSocketNetworkModule(URI.create(address), subProtocol,
                options);
    }

    /**
     * A factory method for instantiating a {@link NetworkModule} with websocket
     * support. Subclasses is able to extend this method in order to create an
     * arbitrary {@link NetworkModule} class instance.
     *
     * @param uri
     * @param subProtocol
     *            Either `mqtt` for MQTT v3 or `mqttv3.1` for MQTT v3.1
     * @param options
     * @return
     */
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

    @Override
    public void connectionLost(Throwable t) {
        logger.error("Connection to " + this.getServerURI() + " lost with cause: " + t.getMessage());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        logger.info("Message arrived from topic: " + topic);
        awsQueuedMqttMessages.add(new IoTMessage(message, topic));
    }
}
