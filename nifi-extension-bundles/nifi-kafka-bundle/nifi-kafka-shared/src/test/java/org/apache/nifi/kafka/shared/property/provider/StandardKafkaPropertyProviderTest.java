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
package org.apache.nifi.kafka.shared.property.provider;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.KafkaClientProperty;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.kafka.shared.aws.AwsMskKafkaProperties;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.reporting.InitializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardKafkaPropertyProviderTest {

    private static final String SCRAM_LOGIN_MODULE = "ScramLoginModule";

    StandardKafkaPropertyProvider provider;

    TestRunner runner;

    @BeforeEach
    void setProvider() {
        provider = new StandardKafkaPropertyProvider(String.class);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.setValidateExpressionUsage(false);
    }

    @Test
    void testGetProperties() {
        final SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;

        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, securityProtocol.name());
        final PropertyContext propertyContext = runner.getProcessContext();

        final Map<String, Object> properties = provider.getProperties(propertyContext);

        assertEquals(securityProtocol.name(), properties.get(KafkaClientComponent.SECURITY_PROTOCOL.getName()));
    }

    @Test
    void testGetPropertiesSaslMechanismScram() {
        final SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;

        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, securityProtocol.name());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.SCRAM_SHA_256);
        final PropertyContext propertyContext = runner.getProcessContext();

        final Map<String, Object> properties = provider.getProperties(propertyContext);

        final Object securityProtocolProperty = properties.get(KafkaClientComponent.SECURITY_PROTOCOL.getName());
        assertEquals(securityProtocol.name(), securityProtocolProperty);

        final Object saslConfigProperty = properties.get(KafkaClientProperty.SASL_JAAS_CONFIG.getProperty());
        assertNotNull(saslConfigProperty, "SASL configuration not found");
        assertTrue(saslConfigProperty.toString().contains(SCRAM_LOGIN_MODULE), "SCRAM configuration not found");
    }

    @Test
    void testGetPropertiesAwsMskIamWithCredentialsService() throws InitializationException {
        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.AWS_MSK_IAM);
        runner.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());
        runner.setProperty(KafkaClientComponent.BOOTSTRAP_SERVERS, "localhost:9092");

        final MockAwsCredentialsProviderService credentialsService = new MockAwsCredentialsProviderService();
        runner.addControllerService("awsCredentials", credentialsService);
        runner.enableControllerService(credentialsService);
        runner.setProperty(KafkaClientComponent.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentials");

        final PropertyContext propertyContext = runner.getProcessContext();
        final Map<String, Object> properties = provider.getProperties(propertyContext);

        final Object callbackHandler = properties.get(KafkaClientProperty.SASL_CLIENT_CALLBACK_HANDLER_CLASS.getProperty());
        assertEquals(AwsMskKafkaProperties.NIFI_AWS_MSK_CALLBACK_HANDLER_CLASS, callbackHandler);
    }

    private static class MockAwsCredentialsProviderService extends AbstractControllerService implements AwsCredentialsProviderService {
        private final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));

        @Override
        public AwsCredentialsProvider getAwsCredentialsProvider() {
            return credentialsProvider;
        }
    }
}
