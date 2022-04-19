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
package org.apache.nifi.c2.client.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class C2Properties {

    private static final Logger logger = LoggerFactory.getLogger(C2Properties.class);

    public static final String NIFI_PREFIX = "nifi.";

    public static final String C2_PROPERTY_BASE = NIFI_PREFIX + "c2.";
    public static final String C2_ENABLE_KEY = C2_PROPERTY_BASE + "enable";
    public static final String C2_AGENT_PROTOCOL_KEY = C2_PROPERTY_BASE + "agent.protocol.class";
    public static final String C2_REST_URL_KEY = C2_PROPERTY_BASE + "rest.url";
    public static final String C2_REST_URL_ACK_KEY = C2_PROPERTY_BASE + "rest.url.ack";
    public static final String C2_AGENT_HEARTBEAT_PERIOD_KEY = C2_PROPERTY_BASE + "agent.heartbeat.period";
    public static final String C2_AGENT_CLASS_KEY = C2_PROPERTY_BASE + "agent.class";
    public static final String C2_AGENT_IDENTIFIER_KEY = C2_PROPERTY_BASE + "agent.identifier";

    public static final String C2_ROOT_CLASS_DEFINITIONS_KEY = C2_PROPERTY_BASE + "root.class.definitions";
    public static final String C2_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.name";
    public static final String C2_METRICS_METRICS_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics";
    public static final String C2_METRICS_METRICS_TYPED_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.typedmetrics.name";
    public static final String C2_METRICS_METRICS_QUEUED_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.queuemetrics.name";
    public static final String C2_METRICS_METRICS_QUEUE_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.queuemetrics.classes";
    public static final String C2_METRICS_METRICS_TYPED_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.typedmetrics.classes";
    public static final String C2_METRICS_METRICS_PROCESSOR_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.processorMetrics.name";
    public static final String C2_METRICS_METRICS_PROCESSOR_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.processorMetrics.classes";

    /* REST Heartbeat Reporter Security Properties */
    private static final String C2_REST_SECURITY_BASE_KEY = NIFI_PREFIX + "c2.security";
    public static final String TRUSTSTORE_LOCATION_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.location";
    public static final String TRUSTSTORE_PASSWORD_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.password";
    public static final String TRUSTSTORE_TYPE_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.type";
    public static final String KEYSTORE_LOCATION_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.location";
    public static final String KEYSTORE_PASSWORD_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.password";
    public static final String KEYSTORE_TYPE_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.type";
    public static final String NEED_CLIENT_AUTH_KEY = C2_REST_SECURITY_BASE_KEY + ".need.client.auth";

    // Defaults
    // Heartbeat period of 1 second
    public static final long C2_AGENT_DEFAULT_HEARTBEAT_PERIOD = TimeUnit.SECONDS.toMillis(1);

    private final Properties properties;

    public C2Properties() {
        this(null);
    }

    public C2Properties(Properties properties) {
        this.properties = properties == null ? new Properties() : properties;
    }

    String getProperty(String key) {
        String value = null;
        if (properties.containsKey(key)) {
            value = properties.getProperty(key);
        }
        return value;
    }


    public boolean isEnabled() {
        return Boolean.parseBoolean(getProperty(C2_ENABLE_KEY));
    }

    // Agent configuration

    public String getAgentClass() {
        return getProperty(C2_AGENT_CLASS_KEY);
    }

    public String getAgentIdentifier() {
        return getProperty(C2_AGENT_IDENTIFIER_KEY);
    }

    public long getAgentHeartbeatPeriod() {
        long heartbeatPeriod = C2_AGENT_DEFAULT_HEARTBEAT_PERIOD;
        try {
            heartbeatPeriod = Long.parseLong(getProperty(C2_AGENT_HEARTBEAT_PERIOD_KEY));
        } catch (NumberFormatException nfe) {
            logger.warn("Heartbeat period is required to be set and could not be parsed into a long representing milliseconds.  Defaulting to {}ms", C2_AGENT_DEFAULT_HEARTBEAT_PERIOD);
        }
        return heartbeatPeriod;
    }

    // REST based protocol

    public String getRestUrl() {
        return getProperty(C2_REST_URL_KEY);
    }

    public String getRestAckUrl() {
        return getProperty(C2_REST_URL_ACK_KEY);
    }

    // C2 TLS security properties
    public String getKeystore() {
        return getProperty(KEYSTORE_LOCATION_KEY);
    }

    public String getKeystorePassword() {
        return getProperty(KEYSTORE_PASSWORD_KEY);
    }

    public String getKeystoreType() {
        return getProperty(KEYSTORE_TYPE_KEY);
    }

    public String getTruststore() {
        return getProperty(TRUSTSTORE_LOCATION_KEY);
    }

    public String getTruststorePassword() {
        return getProperty(TRUSTSTORE_PASSWORD_KEY);
    }

    public String getTruststoreType() {
        return getProperty(TRUSTSTORE_TYPE_KEY);
    }
}
