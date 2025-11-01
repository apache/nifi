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

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kafka.shared.aws.AwsMskKafkaProperties;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NiFiAwsMskCredentialsCallbackHandlerTest {

    @Test
    void testHandleCredentialsCallback() throws IOException, UnsupportedCallbackException {
        final MockAwsCredentialsProviderService credentialsProviderService = new MockAwsCredentialsProviderService();
        final String serviceId = "awsCredentials";

        final AuthenticateCallbackHandler handler = new NiFiAwsMskCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of(
                AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE, credentialsProviderService,
                AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID, serviceId
        );

        final Map<String, ?> options = Map.of(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID, serviceId);
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options);

        handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry));

        final AWSCredentialsCallback callback = new AWSCredentialsCallback();
        handler.handle(new Callback[]{callback});

        assertNotNull(callback.getAwsCredentials());
        assertEquals("accessKey", callback.getAwsCredentials().accessKeyId());
    }

    @Test
    void testConfigureWithoutCredentialsServiceThrows() {
        final AuthenticateCallbackHandler handler = new NiFiAwsMskCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of();
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, Map.of());

        assertThrows(IllegalArgumentException.class, () -> handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry)));
    }

    @Test
    void testConfigureWithMismatchedServiceIdentifierThrows() {
        final MockAwsCredentialsProviderService credentialsProviderService = new MockAwsCredentialsProviderService();
        final AuthenticateCallbackHandler handler = new NiFiAwsMskCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of(
                AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE, credentialsProviderService,
                AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID, "differentService"
        );

        final Map<String, ?> options = Map.of(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID, "anotherService");
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options);

        assertThrows(IllegalStateException.class, () -> handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry)));
    }

    private static class MockAwsCredentialsProviderService extends AbstractControllerService implements AwsCredentialsProviderService {
        private final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));

        @Override
        public AwsCredentialsProvider getAwsCredentialsProvider() {
            return credentialsProvider;
        }

        @Override
        public String getIdentifier() {
            return "awsCredentials";
        }
    }
}
