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
import org.apache.nifi.kafka.shared.aws.AmazonMSKProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AmazonMSKCredentialsCallbackHandlerTest {

    @Test
    void testHandleCredentialsCallback() throws IOException, UnsupportedCallbackException {
        final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));

        final AuthenticateCallbackHandler handler = new AmazonMSKCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of(
                AmazonMSKProperty.NIFI_AWS_MSK_CREDENTIALS_PROVIDER.getProperty(), credentialsProvider
        );

        final Map<String, ?> options = Map.of(
                "awsRoleArn", "arn:aws:iam::123456789012:role/TestRole",
                "awsRoleSessionName", "nifi-session"
        );
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options);

        handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry));

        final AWSCredentialsCallback callback = new AWSCredentialsCallback();
        handler.handle(new Callback[]{callback});

        assertNotNull(callback.getAwsCredentials());
        assertEquals("accessKey", callback.getAwsCredentials().accessKeyId());
    }

    @Test
    void testConfigureWithoutCredentialsServiceThrows() {
        final AuthenticateCallbackHandler handler = new AmazonMSKCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of();
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, Map.of());

        assertThrows(IllegalArgumentException.class, () -> handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry)));
    }

    @Test
    void testConfigureWithoutRequiredJaasOptionsThrows() {
        final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));
        final AuthenticateCallbackHandler handler = new AmazonMSKCredentialsCallbackHandler();
        final Map<String, Object> configs = Map.of(
                AmazonMSKProperty.NIFI_AWS_MSK_CREDENTIALS_PROVIDER.getProperty(), credentialsProvider
        );

        final Map<String, ?> options = Map.of();
        final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(IAMLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options);

        assertThrows(IllegalStateException.class, () -> handler.configure(configs, IAMLoginModule.MECHANISM, List.of(configurationEntry)));
    }
}
