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
package org.apache.nifi.kafka.shared.property;

/**
 * Enumeration of Kafka Client property names without reference to Kafka libraries
 */
public enum KafkaClientProperty {
    SASL_JAAS_CONFIG("sasl.jaas.config"),
    SASL_LOGIN_CLASS("sasl.login.class"),
    SASL_CLIENT_CALLBACK_HANDLER_CLASS("sasl.client.callback.handler.class"),

    SSL_KEYSTORE_LOCATION("ssl.keystore.location"),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password"),
    SSL_KEYSTORE_TYPE("ssl.keystore.type"),
    SSL_KEY_PASSWORD("ssl.key.password"),
    SSL_TRUSTSTORE_LOCATION("ssl.truststore.location"),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password"),
    SSL_TRUSTSTORE_TYPE("ssl.truststore.type");

    private final String property;

    KafkaClientProperty(final String property) {
        this.property = property;
    }

    public String getProperty() {
        return property;
    }
}
