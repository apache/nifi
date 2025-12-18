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

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class KafkaConnectionStep {
    public static final String STEP_NAME = "Kafka Connection";

    public static final ConnectorPropertyDescriptor KAFKA_BROKERS = new ConnectorPropertyDescriptor.Builder()
        .name("Kafka Brokers")
        .description("A comma-separated list of Kafka brokers to connect to.")
        .required(true)
        .validators(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
        .type(PropertyType.STRING_LIST)
        .build();

    public static final ConnectorPropertyDescriptor SECURITY_PROTOCOL = new ConnectorPropertyDescriptor.Builder()
        .name("Security Protocol")
        .description("The security protocol to use when connecting to Kafka brokers.")
        .required(true)
        .type(PropertyType.STRING)
        .defaultValue("SASL_PLAINTEXT")
        .allowableValues("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
        .build();

    public static final ConnectorPropertyDescriptor SASL_MECHANISM = new ConnectorPropertyDescriptor.Builder()
        .name("SASL Mechanism")
        .description("The SASL mechanism to use for authentication.")
        .required(true)
        .type(PropertyType.STRING)
        .allowableValues("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER")
        .dependsOn(SECURITY_PROTOCOL, "SASL_PLAINTEXT", "SASL_SSL")
        .defaultValue("SCRAM-SHA-512")
        .build();

    public static final ConnectorPropertyDescriptor USERNAME = new ConnectorPropertyDescriptor.Builder()
        .name("Username")
        .description("The username for SASL authentication.")
        .required(true)
        .type(PropertyType.STRING)
        .validators(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(SECURITY_PROTOCOL, "SASL_PLAINTEXT", "SASL_SSL")
        .build();

    public static final ConnectorPropertyDescriptor PASSWORD = new ConnectorPropertyDescriptor.Builder()
        .name("Password")
        .description("The password for SASL authentication.")
        .required(true)
        .type(PropertyType.SECRET)
        .validators(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(SECURITY_PROTOCOL, "SASL_PLAINTEXT", "SASL_SSL")
        .build();

    public static final ConnectorPropertyGroup KAFKA_SERVER_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Server Settings")
        .description("Settings for connecting to the Kafka server")
        .properties(List.of(
            KAFKA_BROKERS,
            SECURITY_PROTOCOL,
            SASL_MECHANISM,
            USERNAME,
            PASSWORD
        ))
        .build();

    public static final ConnectorPropertyDescriptor SCHEMA_REGISTRY_URL = new ConnectorPropertyDescriptor.Builder()
        .name("Schema Registry URL")
        .description("The URL of the Schema Registry.")
        .required(false)
        .type(PropertyType.STRING)
        .validators(StandardValidators.URL_VALIDATOR)
        .validators(Validator.VALID)
        .build();

    public static final ConnectorPropertyDescriptor SCHEMA_REGISTRY_USERNAME = new ConnectorPropertyDescriptor.Builder()
        .name("Schema Registry Username")
        .description("The username for Schema Registry authentication.")
        .required(false)
        .type(PropertyType.STRING)
        .validators(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(SCHEMA_REGISTRY_URL)
        .build();

    public static final ConnectorPropertyDescriptor SCHEMA_REGISTRY_PASSWORD = new ConnectorPropertyDescriptor.Builder()
        .name("Schema Registry Password")
        .description("The password for Schema Registry authentication.")
        .required(false)
        .type(PropertyType.SECRET)
        .validators(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(SCHEMA_REGISTRY_URL)
        .build();


    public static final ConnectorPropertyGroup SCHEMA_REGISTRY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Schema Registry Settings")
        .description("Settings for connecting to the Schema Registry")
        .properties(List.of(
            SCHEMA_REGISTRY_URL,
            SCHEMA_REGISTRY_USERNAME,
            SCHEMA_REGISTRY_PASSWORD
        ))
        .build();

    public static final ConfigurationStep KAFKA_CONNECTION_STEP = new ConfigurationStep.Builder()
        .name(STEP_NAME)
        .description("Configure Kafka connection settings")
        .propertyGroups(List.of(
            KAFKA_SERVER_GROUP,
            SCHEMA_REGISTRY_GROUP
        ))
        .build();
}
