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
package org.apache.nifi.kafka.service.aws;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.kafka.shared.aws.AwsMskKafkaProperties;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class AmazonMSKConnectionServiceTest {

    private static final String SERVICE_ID = AmazonMSKConnectionService.class.getName();

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @Test
    void testValidProperties() throws InitializationException {
        final AmazonMSKConnectionService service = new AmazonMSKConnectionService();
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);

        runner.assertValid(service);

        runner.enableControllerService(service);
    }

    @Test
    void testWebIdentityWithoutCredentialsServiceNotValid() throws InitializationException {
        final AmazonMSKConnectionService service = new AmazonMSKConnectionService();
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        runner.setProperty(service, KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());

        runner.assertNotValid(service);
    }

    @Test
    void testWebIdentityWithCredentialsServiceAddsProperty() throws InitializationException {
        final TestAmazonMSKConnectionService service = new TestAmazonMSKConnectionService();
        runner.addControllerService(SERVICE_ID, service);

        final MockAwsCredentialsProviderService credentialsService = new MockAwsCredentialsProviderService();
        runner.addControllerService("awsCredentials", credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(service, AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        runner.setProperty(service, KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());
        runner.setProperty(service, KafkaClientComponent.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentials");

        runner.assertValid(service);

        runner.enableControllerService(service);

        final java.util.Properties properties = new java.util.Properties();
        final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
        configuredProperties.put(AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        configuredProperties.put(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());
        configuredProperties.put(KafkaClientComponent.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentials");

        final MockConfigurationContext configurationContext = new MockConfigurationContext(service, configuredProperties,
                runner.getProcessContext().getControllerServiceLookup(), Collections.emptyMap());
        configurationContext.setValidateExpressions(false);
        service.customize(properties, configurationContext);

        final Object provider = properties.get(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE);
        final Object identifier = properties.get(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID);

        org.junit.jupiter.api.Assertions.assertSame(credentialsService, provider);
        org.junit.jupiter.api.Assertions.assertEquals("awsCredentials", identifier);
    }

    private static class MockAwsCredentialsProviderService extends AbstractControllerService implements AwsCredentialsProviderService {
        private final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));

        @Override
        public AwsCredentialsProvider getAwsCredentialsProvider() {
            return credentialsProvider;
        }
    }

    private static class TestAmazonMSKConnectionService extends AmazonMSKConnectionService {
        void customize(final java.util.Properties properties, final org.apache.nifi.context.PropertyContext context) {
            super.customizeKafkaProperties(properties, context);
        }
    }
}
