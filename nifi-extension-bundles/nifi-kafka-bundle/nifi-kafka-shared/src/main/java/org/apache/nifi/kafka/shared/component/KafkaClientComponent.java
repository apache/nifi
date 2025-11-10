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
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Kafka Client Component interface with common Property Descriptors
 */
public interface KafkaClientComponent {

    PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .displayName("Bootstrap Servers")
            .description("Comma-separated list of Kafka Bootstrap Servers in the format host:port. Corresponds to Kafka bootstrap.servers property")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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
            .dependsOn(SECURITY_PROTOCOL,
                    SecurityProtocol.SASL_PLAINTEXT.name(),
                    SecurityProtocol.SASL_SSL.name())
            .build();

    PropertyDescriptor SASL_USERNAME = new PropertyDescriptor.Builder()
            .name("sasl.username")
            .displayName("SASL Username")
            .description("Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms")
            .required(true)
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
            .displayName("SASL Password")
            .description("Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms")
            .required(true)
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

    PropertyDescriptor AWS_ROLE_SOURCE = new PropertyDescriptor.Builder()
            .name("AWS Role Source")
            .description("Select how AWS credentials are sourced for AWS MSK IAM: Default Profile searches standard locations," +
                    " Specified Profile selects a named profile, or Specified Role configures a Role ARN and Session Name.")
            .required(true)
            .allowableValues(AwsRoleSource.class)
            .defaultValue(AwsRoleSource.DEFAULT_PROFILE)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.AWS_MSK_IAM
            )
            .build();

    PropertyDescriptor AWS_PROFILE_NAME = new PropertyDescriptor.Builder()
            .name("aws.profile.name")
            .displayName("AWS Profile Name")
            .description("The Amazon Web Services Profile to select when multiple profiles are available.")
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.SPECIFIED_PROFILE
            )
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    PropertyDescriptor AWS_ASSUME_ROLE_ARN = new PropertyDescriptor.Builder()
            .name("AWS Assume Role ARN")
            .description("The AWS Role ARN for cross-account access when using AWS MSK IAM. Used with Assume Role Session Name.")
            .required(true)
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.SPECIFIED_ROLE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    PropertyDescriptor AWS_ASSUME_ROLE_SESSION_NAME = new PropertyDescriptor.Builder()
            .name("AWS Assume Role Session Name")
            .description("The AWS Role Session Name for cross-account access. Used in conjunction with Assume Role ARN.")
            .required(true)
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.SPECIFIED_ROLE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    PropertyDescriptor AWS_WEB_IDENTITY_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("AWS Web Identity Token Provider")
            .description("Controller Service providing tokens with OAuth2 OpenID Connect for AWS Web Identity federation.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .dependsOn(
                    KafkaClientComponent.AWS_ROLE_SOURCE,
                    AwsRoleSource.WEB_IDENTITY_TOKEN
            )
            .build();

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Service supporting SSL communication with Kafka brokers")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .dependsOn(
                    SECURITY_PROTOCOL,
                    SecurityProtocol.SSL.name(),
                    SecurityProtocol.SASL_SSL.name())
            .build();

    PropertyDescriptor KERBEROS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.GSSAPI)
            .build();

    PropertyDescriptor SELF_CONTAINED_KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Service supporting user authentication with Kerberos")
            .identifiesControllerService(SelfContainedKerberosUserService.class)
            .required(true)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.GSSAPI)
            .build();

    PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-provider-service")
            .displayName("OAuth2 Access Token Provider Service")
            .description("Service providing OAuth2 Access Tokens for authentication")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .dependsOn(SASL_MECHANISM, SaslMechanism.OAUTHBEARER)
            .build();
}
