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
package org.apache.nifi.processors.mqtt.adapters;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import org.apache.nifi.processors.mqtt.common.MqttConnectionProperties;
import org.apache.nifi.processors.mqtt.common.NifiMqttCallback;
import org.apache.nifi.processors.mqtt.common.NifiMqttClient;
import org.apache.nifi.processors.mqtt.common.NifiMqttException;
import org.apache.nifi.processors.mqtt.common.NifiMqttMessage;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Objects;

public class HiveMqV5ClientAdapter implements NifiMqttClient {

    private final Mqtt5Client mqtt5Client;

    private NifiMqttCallback callback;

    public HiveMqV5ClientAdapter(Mqtt5BlockingClient mqtt5BlockingClient) {
        this.mqtt5Client = mqtt5BlockingClient;
    }

    @Override
    public boolean isConnected() {
        return mqtt5Client.getState().isConnected();
    }

    @Override
    public void connect(MqttConnectionProperties connectionProperties) throws NifiMqttException {
        final Mqtt5ConnectBuilder connectBuilder = Mqtt5Connect.builder()
                .keepAlive(connectionProperties.getKeepAliveInterval())
                .cleanStart(connectionProperties.isCleanSession());

        final String lastWillTopic = connectionProperties.getLastWillTopic();
        if (lastWillTopic != null) {
            connectBuilder.willPublish()
                    .topic(lastWillTopic)
                    .payload(connectionProperties.getLastWillMessage().getBytes())
                    .retain(connectionProperties.getLastWillRetain())
                    .qos(MqttQos.fromCode(connectionProperties.getLastWillQOS()))
                    .applyWillPublish();
        }

        // checking for presence of password because username can be null
        final char[] password = connectionProperties.getPassword();
        if (password != null) {
            connectBuilder.simpleAuth()
                    .username(connectionProperties.getUsername())
                    .password(toBytes(password))
                    .applySimpleAuth();

            clearSensitive(connectionProperties.getPassword());
            clearSensitive(password);
        }

        final Mqtt5Connect mqtt5Connect = connectBuilder.build();
        mqtt5Client.toBlocking().connect(mqtt5Connect);
    }

    @Override
    public void disconnect(long disconnectTimeout) throws NifiMqttException {
        // Currently it is not possible to set timeout for disconnect with HiveMQ Client. (Only connect timeout exists.)
        mqtt5Client.toBlocking().disconnect();
    }

    @Override
    public void close() throws NifiMqttException {
        // there is no paho's close equivalent in hivemq client
    }

    @Override
    public void publish(String topic, NifiMqttMessage message) throws NifiMqttException {
        mqtt5Client.toAsync().publishWith()
                .topic(topic)
                .payload(message.getPayload())
                .retain(message.isRetained())
                .qos(Objects.requireNonNull(MqttQos.fromCode(message.getQos())))
                .send();
    }

    @Override
    public void subscribe(String topicFilter, int qos) {
        Objects.requireNonNull(callback, "callback should be set");

        mqtt5Client.toAsync().subscribeWith()
                .topicFilter(topicFilter)
                .qos(Objects.requireNonNull(MqttQos.fromCode(qos)))
                .callback(mqtt5Publish -> {
                    final NifiMqttMessage nifiMqttMessage = new NifiMqttMessage();
                    nifiMqttMessage.setPayload(mqtt5Publish.getPayloadAsBytes());
                    nifiMqttMessage.setQos(mqtt5Publish.getQos().getCode());
                    nifiMqttMessage.setRetained(mqtt5Publish.isRetain());
                    try {
                        callback.messageArrived(mqtt5Publish.getTopic().toString(), nifiMqttMessage);
                    } catch (Exception e) {
                        throw new NifiMqttException(e);
                    }
                })
                .send();
    }

    @Override
    public void setCallback(NifiMqttCallback callback) {
        this.callback = callback;
    }

    public static KeyManagerFactory getKeyManagerFactory(String keyStoreType, String path, char[] keyStorePassword) {
        try {
            final KeyStore keyStore = loadIntoKeyStore(keyStoreType, path, keyStorePassword);

            final KeyManagerFactory kmf = KeyManagerFactory
                    .getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, new char[0]); // https://stackoverflow.com/questions/1814048/sun-java-keymanagerfactory-and-null-passwords

            return kmf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TrustManagerFactory getTrustManagerFactory(String trustStoreType, String path, char[] keyStorePassword) {
        try {
            final KeyStore trustStore = loadIntoKeyStore(trustStoreType, path, keyStorePassword);

            final TrustManagerFactory tmf = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            return tmf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyStore loadIntoKeyStore(String type, String path, char[] keyStorePassword) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        final KeyStore keyStore = KeyStore.getInstance(type);
        final InputStream in = new FileInputStream(path);
        keyStore.load(in, keyStorePassword);
        return keyStore;
    }

    private byte[] toBytes(char[] chars) {
        final ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars));
        final byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        clearSensitive(byteBuffer.array());
        return bytes;
    }

    private void clearSensitive(char[] chars) {
        Arrays.fill(chars, '\u0000');
    }

    private void clearSensitive(byte[] bytes) {
        Arrays.fill(bytes, (byte) 0);
    }

}
