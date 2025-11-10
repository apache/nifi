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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kafka.shared.aws.AmazonMSKProperty;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        runner.setProperty(service, KafkaClientComponent.AWS_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/TestRole");
        runner.setProperty(service, KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME, "nifi-session");

        runner.assertNotValid(service);
    }

    @Test
    void testWebIdentityWithTokenProviderAddsProperty() throws InitializationException {
        final TestAmazonMSKConnectionService service = new TestAmazonMSKConnectionService();
        runner.addControllerService(SERVICE_ID, service);

        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("webIdentityToken", tokenProvider);
        runner.enableControllerService(tokenProvider);

        runner.setProperty(service, AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        runner.setProperty(service, KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());
        runner.setProperty(service, KafkaClientComponent.AWS_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/TestRole");
        runner.setProperty(service, KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME, "nifi-session");
        runner.setProperty(service, AmazonMSKConnectionService.AWS_WEB_IDENTITY_TOKEN_PROVIDER, "webIdentityToken");
        runner.setProperty(service, AmazonMSKConnectionService.AWS_WEB_IDENTITY_STS_REGION, "us-east-1");

        runner.assertValid(service);

        runner.enableControllerService(service);

        final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
        configuredProperties.put(AmazonMSKConnectionService.BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
        configuredProperties.put(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());
        configuredProperties.put(KafkaClientComponent.AWS_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/TestRole");
        configuredProperties.put(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME, "nifi-session");
        configuredProperties.put(AmazonMSKConnectionService.AWS_WEB_IDENTITY_TOKEN_PROVIDER, "webIdentityToken");
        configuredProperties.put(AmazonMSKConnectionService.AWS_WEB_IDENTITY_STS_REGION, "us-east-1");

        final MockConfigurationContext configurationContext = new MockConfigurationContext(service, configuredProperties,
                runner.getProcessContext().getControllerServiceLookup(), Collections.emptyMap());
        configurationContext.setValidateExpressions(false);
        final Properties properties = service.buildConsumerProperties(configurationContext);

        final Object provider = properties.get(AmazonMSKProperty.NIFI_AWS_MSK_CREDENTIALS_PROVIDER.getProperty());

        assertNotNull(provider);
        assertTrue(provider instanceof AwsCredentialsProvider);
    }

    private static class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("mock-access-token");
            accessToken.setAdditionalParameter("id_token", "mock-id-token");
            return accessToken;
        }
    }

    private static class TestAmazonMSKConnectionService extends AmazonMSKConnectionService {
        Properties buildConsumerProperties(final MockConfigurationContext context) {
            final Properties clientProperties = super.getClientProperties(context);
            return super.getConsumerProperties(context, clientProperties);
        }
    }
}
