package org.apache.nifi.processors.mqtt.common;

import java.net.URI;

public class MqttClientProperties {
    private URI brokerURI;
    private String clientID;

    private int mqttVersion;

    public String getBroker() {
        return brokerURI.toString();
    }

    public MqttConstants.SupportedSchemes getScheme() {
        return MqttConstants.SupportedSchemes.valueOf(brokerURI.getScheme().toUpperCase());
    }

    public URI getBrokerURI() {
        return brokerURI;
    }

    public void setBrokerURI(URI brokerURI) {
        this.brokerURI = brokerURI;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public int getMqttVersion() {
        return mqttVersion;
    }

    public void setMqttVersion(int mqttVersion) {
        this.mqttVersion = mqttVersion;
    }
}
