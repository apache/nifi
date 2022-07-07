package org.apache.nifi.processors.mqtt.paho;

import org.apache.nifi.processors.mqtt.common.MqttConnectionProperties;
import org.apache.nifi.processors.mqtt.common.NifiMqttCallback;
import org.apache.nifi.processors.mqtt.common.NifiMqttClient;
import org.apache.nifi.processors.mqtt.common.NifiMqttException;
import org.apache.nifi.processors.mqtt.common.NifiMqttMessage;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Properties;

public class PahoMqttClientAdapter implements NifiMqttClient {

    private final IMqttClient client;

    public PahoMqttClientAdapter(MqttClient client) {
        this.client = client;
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void connect(MqttConnectionProperties connectionProperties) throws NifiMqttException {
        try {
            final MqttConnectOptions connectOptions = new MqttConnectOptions();

            connectOptions.setCleanSession(connectionProperties.isCleanSession());
            connectOptions.setKeepAliveInterval(connectionProperties.getKeepAliveInterval());
            connectOptions.setMqttVersion(connectionProperties.getMqttVersion());
            connectOptions.setConnectionTimeout(connectionProperties.getConnectionTimeout());

            final SSLContextService sslContextService = connectionProperties.getSslContextService();
            if (sslContextService != null) {
                connectOptions.setSSLProperties(transformSSLContextService(sslContextService));
            }

            final String lastWillTopic = connectionProperties.getLastWillTopic();
            if (lastWillTopic != null) {
                boolean lastWillRetain = connectionProperties.getLastWillRetain() != null && connectionProperties.getLastWillRetain();
                connectOptions.setWill(lastWillTopic, connectionProperties.getLastWillMessage().getBytes(), connectionProperties.getLastWillQOS(), lastWillRetain);
            }

            final String username = connectionProperties.getUsername();
            if (username != null) {
                connectOptions.setUserName(username);
                connectOptions.setPassword(connectionProperties.getPassword());
            }

            client.connect(connectOptions);
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    @Override
    public void disconnect(long disconnectTimeout) throws NifiMqttException {
        try {
            client.disconnect(disconnectTimeout);
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    @Override
    public void close() throws NifiMqttException {
        try {
            client.close();
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    @Override
    public void publish(String topic, NifiMqttMessage message) throws NifiMqttException {
        try {
            client.publish(topic, message.getPayload(), message.getQos(), message.isRetained());
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    @Override
    public void subscribe(String topicFilter, int qos) throws NifiMqttException {
        try {
            client.subscribe(topicFilter, qos);
        } catch (MqttException e) {
            throw new NifiMqttException(e);
        }
    }

    @Override
    public void setCallback(NifiMqttCallback callback) {
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                callback.connectionLost(cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                final NifiMqttMessage nifiMqttMessage = new NifiMqttMessage();
                nifiMqttMessage.setMessageId(message.getId());
                nifiMqttMessage.setPayload(message.getPayload());
                nifiMqttMessage.setQos(message.getQos());
                nifiMqttMessage.setRetained(message.isRetained());
                callback.messageArrived(topic, nifiMqttMessage);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                callback.deliveryComplete(token.toString());
            }
        });
    }

    public static Properties transformSSLContextService(SSLContextService sslContextService){
        final Properties properties = new Properties();
        if (sslContextService.getSslAlgorithm() != null) {
            properties.setProperty("com.ibm.ssl.protocol", sslContextService.getSslAlgorithm());
        }
        if (sslContextService.getKeyStoreFile() != null) {
            properties.setProperty("com.ibm.ssl.keyStore", sslContextService.getKeyStoreFile());
        }
        if (sslContextService.getKeyStorePassword() != null) {
            properties.setProperty("com.ibm.ssl.keyStorePassword", sslContextService.getKeyStorePassword());
        }
        if (sslContextService.getKeyStoreType() != null) {
            properties.setProperty("com.ibm.ssl.keyStoreType", sslContextService.getKeyStoreType());
        }
        if (sslContextService.getTrustStoreFile() != null) {
            properties.setProperty("com.ibm.ssl.trustStore", sslContextService.getTrustStoreFile());
        }
        if (sslContextService.getTrustStorePassword() != null) {
            properties.setProperty("com.ibm.ssl.trustStorePassword", sslContextService.getTrustStorePassword());
        }
        if (sslContextService.getTrustStoreType() != null) {
            properties.setProperty("com.ibm.ssl.trustStoreType", sslContextService.getTrustStoreType());
        }
        return  properties;
    }
}
