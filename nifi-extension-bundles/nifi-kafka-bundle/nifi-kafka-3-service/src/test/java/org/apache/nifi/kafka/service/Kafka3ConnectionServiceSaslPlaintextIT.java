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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.reporting.InitializationException;

import java.util.LinkedHashMap;
import java.util.Map;

public class Kafka3ConnectionServiceSaslPlaintextIT extends Kafka3ConnectionServiceBaseIT {

    protected Map<String, String> getKafkaContainerConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaContainerConfigProperties());
        properties.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
        properties.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        properties.put("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                getJaasConfigKafkaContainer(TEST_USERNAME, TEST_PASSWORD));
        return properties;
    }

    @Override
    protected Map<String, String> getKafkaServiceConfigProperties() throws InitializationException {
        final Map<String, String> properties = new LinkedHashMap<>(super.getKafkaServiceConfigProperties());
        properties.put(Kafka3ConnectionService.SECURITY_PROTOCOL.getName(), SecurityProtocol.SASL_PLAINTEXT.name());
        properties.put(Kafka3ConnectionService.SASL_MECHANISM.getName(), SaslMechanism.PLAIN.getValue());
        properties.put(Kafka3ConnectionService.SASL_USERNAME.getName(), TEST_USERNAME);
        properties.put(Kafka3ConnectionService.SASL_PASSWORD.getName(), TEST_PASSWORD);
        return properties;
    }

    @Override
    protected Map<String, String> getAdminClientConfigProperties() {
        final Map<String, String> properties = new LinkedHashMap<>(super.getAdminClientConfigProperties());
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        properties.put(SaslConfigs.SASL_MECHANISM, SaslMechanism.PLAIN.getValue());
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfigKafkaClient(TEST_USERNAME, TEST_PASSWORD));
        return properties;
    }
}
