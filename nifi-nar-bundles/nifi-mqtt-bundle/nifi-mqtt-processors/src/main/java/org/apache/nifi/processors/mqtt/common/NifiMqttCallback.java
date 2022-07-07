package org.apache.nifi.processors.mqtt.common;

public interface NifiMqttCallback {
    void connectionLost(Throwable cause);
    void messageArrived(String topic, NifiMqttMessage message) throws Exception;
    void deliveryComplete(String token);
}
