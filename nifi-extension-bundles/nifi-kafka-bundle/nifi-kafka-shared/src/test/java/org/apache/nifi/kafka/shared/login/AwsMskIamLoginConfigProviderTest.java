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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsMskIamLoginConfigProviderTest {

    private static final String IAM_LOGIN_MODULE = "software.amazon.msk.auth.iam.IAMLoginModule";

    private TestRunner runner;
    private AwsMskIamLoginConfigProvider provider;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        provider = new AwsMskIamLoginConfigProvider();
    }

    @Test
    void testConfigurationWithSpecifiedProfile() {
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.AWS_MSK_IAM);
        runner.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, "SPECIFIED_PROFILE");
        runner.setProperty(KafkaClientComponent.AWS_PROFILE_NAME, "myProfile");

        final PropertyContext context = runner.getProcessContext();
        final String configuration = provider.getConfiguration(context);

        assertNotNull(configuration);
        assertTrue(configuration.contains(IAM_LOGIN_MODULE), "IAM Login Module not present");
        assertTrue(configuration.contains("awsProfileName=\"myProfile\""), "awsProfileName JAAS option not present");
    }

    @Test
    void testConfigurationWithSpecifiedRole() {
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.AWS_MSK_IAM);
        runner.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, "SPECIFIED_ROLE");
        runner.setProperty(KafkaClientComponent.AWS_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/MyRole");
        runner.setProperty(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME, "MySession");

        final PropertyContext context = runner.getProcessContext();
        final String configuration = provider.getConfiguration(context);

        assertNotNull(configuration);
        assertTrue(configuration.contains(IAM_LOGIN_MODULE), "IAM Login Module not present");
        assertTrue(configuration.contains("awsRoleArn=\"arn:aws:iam::123456789012:role/MyRole\""), "awsRoleArn JAAS option not present");
        assertTrue(configuration.contains("awsRoleSessionName=\"MySession\""), "awsRoleSessionName JAAS option not present");
    }

    @Test
    void testConfigurationWithWebIdentityProvider() throws InitializationException {
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.AWS_MSK_IAM);
        runner.setProperty(KafkaClientComponent.AWS_ROLE_SOURCE, AwsRoleSource.WEB_IDENTITY_TOKEN.name());

        runner.setProperty(KafkaClientComponent.AWS_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/WebIdentityRole");
        runner.setProperty(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME, "WebIdentitySession");

        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("webIdentityToken", tokenProvider);
        runner.enableControllerService(tokenProvider);
        runner.setProperty(KafkaClientComponent.AWS_WEB_IDENTITY_TOKEN_PROVIDER, "webIdentityToken");

        final PropertyContext context = runner.getProcessContext();
        final String configuration = provider.getConfiguration(context);

        assertNotNull(configuration);
        assertTrue(configuration.contains("awsRoleArn=\"arn:aws:iam::123456789012:role/WebIdentityRole\""),
                "awsRoleArn JAAS option not present");
        assertTrue(configuration.contains("awsRoleSessionName=\"WebIdentitySession\""),
                "awsRoleSessionName JAAS option not present");
    }

    private static class MockOAuth2AccessTokenProvider extends org.apache.nifi.controller.AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("mock-access-token");
            accessToken.setAdditionalParameter("id_token", "mock-id-token");
            return accessToken;
        }
    }
}
