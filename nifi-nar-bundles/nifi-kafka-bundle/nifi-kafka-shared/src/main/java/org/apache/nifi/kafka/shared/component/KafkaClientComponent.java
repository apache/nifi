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
package org.apache.nifi.kafka.shared.component;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Kafka Client Component interface with common Property Descriptors
 */
public interface KafkaClientComponent {

    PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .displayName("Kafka Brokers")
            .description("Comma-separated list of Kafka Brokers in the format host:port")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("localhost:9092")
            .build();

    PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SecurityProtocol.values())
            .defaultValue(SecurityProtocol.PLAINTEXT.name())
            .build();

    PropertyDescriptor SASL_MECHANISM = new PropertyDescriptor.Builder()
            .name("sasl.mechanism")
            .displayName("SASL Mechanism")
            .description("SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SaslMechanism.getAvailableSaslMechanisms())
            .defaultValue(SaslMechanism.GSSAPI)
            .build();

    PropertyDescriptor SASL_USERNAME = new PropertyDescriptor.Builder()
            .name("sasl.username")
            .displayName("Username")
            .description("Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN,
                    SaslMechanism.SCRAM_SHA_256,
                    SaslMechanism.SCRAM_SHA_512
            )
            .build();

    PropertyDescriptor SASL_PASSWORD = new PropertyDescriptor.Builder()
            .name("sasl.password")
            .displayName("Password")
            .description("Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN,
                    SaslMechanism.SCRAM_SHA_256,
                    SaslMechanism.SCRAM_SHA_512
            )
            .build();

    PropertyDescriptor TOKEN_AUTHENTICATION = new PropertyDescriptor.Builder()
            .name("sasl.token.auth")
            .displayName("Token Authentication")
            .description("Enables or disables Token authentication when using SCRAM SASL Mechanisms")
            .required(false)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(Boolean.FALSE.toString())
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.SCRAM_SHA_256,
                    SaslMechanism.SCRAM_SHA_512
            )
            .build();

    PropertyDescriptor AWS_PROFILE_NAME = new PropertyDescriptor.Builder()
            .name("aws.profile.name")
            .displayName("AWS Profile Name")
            .description("The Amazon Web Services Profile to select when multiple profiles are available.")
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.AWS_MSK_IAM
            )
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Service supporting SSL communication with Kafka brokers")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    PropertyDescriptor KERBEROS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    PropertyDescriptor SELF_CONTAINED_KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Service supporting user authentication with Kerberos")
            .identifiesControllerService(SelfContainedKerberosUserService.class)
            .required(false)
            .build();
}
