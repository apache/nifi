package org.apache.nifi.processors.mqtt.common;

public class NifiMqttException extends RuntimeException {

    public NifiMqttException(String message) {
        super(message);
    }

    public NifiMqttException(Throwable cause) {
        super(cause);
    }
}
