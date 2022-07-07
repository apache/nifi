package org.apache.nifi.processors.mqtt.common;

public interface NifiMqttClient {
    boolean isConnected();
    void connect(MqttConnectionProperties connectionProperties) throws NifiMqttException;
    void disconnect(long disconnectTimeout) throws NifiMqttException;
    void close() throws NifiMqttException;
    void publish(String topic, NifiMqttMessage message) throws NifiMqttException;
    void subscribe(String topicFilter, int qos);
    void setCallback(NifiMqttCallback callback);
}
