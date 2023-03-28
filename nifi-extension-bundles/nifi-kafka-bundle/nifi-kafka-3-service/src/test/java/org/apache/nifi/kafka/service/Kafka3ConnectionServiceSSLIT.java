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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.nifi.kafka.shared.property.KafkaClientProperty;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.reporting.InitializationException;

import java.util.LinkedHashMap;
import java.util.Map;

public class Kafka3ConnectionServiceSSLIT extends Kafka3ConnectionServiceBaseIT {

    protected Map<String, String> getKafkaContainerConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaContainerConfigProperties());
        properties.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:SSL,PLAINTEXT:SSL");
        properties.put("KAFKA_SSL_KEYSTORE_LOCATION", tlsConfiguration.getKeystorePath());
        properties.put("KAFKA_SSL_KEYSTORE_TYPE", tlsConfiguration.getKeystoreType().getType());
        properties.put("KAFKA_SSL_KEYSTORE_PASSWORD", tlsConfiguration.getKeystorePassword());
        properties.put("KAFKA_SSL_KEY_PASSWORD", tlsConfiguration.getKeyPassword());
        properties.put("KAFKA_SSL_TRUSTSTORE_LOCATION", tlsConfiguration.getTruststorePath());
        properties.put("KAFKA_SSL_TRUSTSTORE_TYPE", tlsConfiguration.getTruststoreType().getType());
        properties.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", tlsConfiguration.getTruststorePassword());
        properties.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
        properties.put("KAFKA_SSL_CLIENT_AUTH", "required");
        properties.put("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", " ");
        return properties;
    }

    @Override
    protected Map<String, String> getKafkaServiceConfigProperties() throws InitializationException {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaServiceConfigProperties());
        properties.put(Kafka3ConnectionService.SECURITY_PROTOCOL.getName(), SecurityProtocol.SSL.name());
        properties.put(Kafka3ConnectionService.SSL_CONTEXT_SERVICE.getName(), addSSLContextService(runner));
        return properties;
    }

    @Override
    protected Map<String, String> getAdminClientConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getAdminClientConfigProperties());
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
        properties.put(KafkaClientProperty.SSL_KEY_PASSWORD.getProperty(), tlsConfiguration.getKeystorePassword());
        properties.put(KafkaClientProperty.SSL_KEYSTORE_LOCATION.getProperty(), tlsConfiguration.getKeystorePath());
        properties.put(KafkaClientProperty.SSL_KEYSTORE_TYPE.getProperty(), tlsConfiguration.getKeystoreType().getType());
        properties.put(KafkaClientProperty.SSL_KEYSTORE_PASSWORD.getProperty(), tlsConfiguration.getKeystorePassword());
        properties.put(KafkaClientProperty.SSL_TRUSTSTORE_LOCATION.getProperty(), tlsConfiguration.getTruststorePath());
        properties.put(KafkaClientProperty.SSL_TRUSTSTORE_TYPE.getProperty(), tlsConfiguration.getTruststoreType().getType());
        properties.put(KafkaClientProperty.SSL_TRUSTSTORE_PASSWORD.getProperty(), tlsConfiguration.getTruststorePassword());
        return properties;
    }
}
