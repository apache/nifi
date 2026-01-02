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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * Callback handler that supplies AWS credentials sourced from NiFi configuration for AWS MSK IAM authentication.
 */
public class AmazonMSKCredentialsCallbackHandler implements AuthenticateCallbackHandler {

    private AwsCredentialsProvider credentialsProvider;

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism, final List<AppConfigurationEntry> jaasConfigEntries) {
        if (!IAMLoginModule.MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }

        final Object provider = configs.get(AmazonMSKProperty.NIFI_AWS_MSK_CREDENTIALS_PROVIDER.getProperty());
        if (!(provider instanceof AwsCredentialsProvider)) {
            throw new IllegalArgumentException("Kafka configuration missing AWS Web Identity credentials provider");
        }

        credentialsProvider = (AwsCredentialsProvider) provider;

        final AppConfigurationEntry loginModuleEntry = jaasConfigEntries.stream()
                .filter(entry -> IAMLoginModule.class.getName().equals(entry.getLoginModuleName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("JAAS configuration missing IAMLoginModule entry"));

        final Map<String, ?> jaasOptions = loginModuleEntry.getOptions();
        final Object roleArn = jaasOptions.get("awsRoleArn");
        final Object roleSessionName = jaasOptions.get("awsRoleSessionName");
        if (roleArn == null || roleSessionName == null) {
            throw new IllegalStateException("JAAS configuration missing required awsRoleArn or awsRoleSessionName options for Web Identity authentication");
        }
    }

    @Override
    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof AWSCredentialsCallback awsCredentialsCallback) {
                handleCredentialsCallback(awsCredentialsCallback);
            } else {
                throw new UnsupportedCallbackException(callback,
                        "Unsupported callback type [%s]".formatted(callback.getClass().getName()));
            }
        }
    }

    private void handleCredentialsCallback(final AWSCredentialsCallback callback) {
        try {
            final AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
            callback.setAwsCredentials(awsCredentials);
        } catch (final Exception e) {
            callback.setLoadingException(e);
        }
    }

    /**
     * No resources to close because the credentials provider lifecycle is managed by the NiFi service.
     */
    @Override
    public void close() {
    }
}
