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
import org.apache.nifi.kafka.shared.aws.AwsMskKafkaProperties;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.msk.auth.iam.IAMLoginModule;
import software.amazon.msk.auth.iam.internals.AWSCredentialsCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Callback handler that supplies AWS credentials sourced from a NiFi {@link AwsCredentialsProviderService}
 * for AWS MSK IAM authentication.
 */
public class NiFiAwsMskCredentialsCallbackHandler implements AuthenticateCallbackHandler {

    private AwsCredentialsProvider credentialsProvider;

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism, final List<AppConfigurationEntry> jaasConfigEntries) {
        if (!IAMLoginModule.MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }

        final Object service = configs.get(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE);
        if (!(service instanceof AwsCredentialsProviderService)) {
            throw new IllegalArgumentException("Kafka configuration missing AWS Credentials Provider Service");
        }

        final AwsCredentialsProviderService credentialsProviderService = (AwsCredentialsProviderService) service;
        credentialsProvider = credentialsProviderService.getAwsCredentialsProvider();

        final String configuredServiceIdentifier = credentialsProviderService.getIdentifier();

        final AppConfigurationEntry loginModuleEntry = jaasConfigEntries.stream()
                .filter(entry -> IAMLoginModule.class.getName().equals(entry.getLoginModuleName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("JAAS configuration missing IAMLoginModule entry"));

        final Map<String, ?> jaasOptions = loginModuleEntry.getOptions();
        final Object jaasServiceIdentifier = jaasOptions.get(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID);
        if (jaasServiceIdentifier instanceof String expected && !Objects.equals(expected, configuredServiceIdentifier)) {
            throw new IllegalStateException(String.format("AWS Credentials Provider Service identifier [%s] does not match JAAS configuration identifier [%s]",
                    configuredServiceIdentifier, expected));
        }

        final Object configServiceIdentifier = configs.get(AwsMskKafkaProperties.NIFI_AWS_CREDENTIALS_PROVIDER_SERVICE_ID);
        if (configServiceIdentifier instanceof String configIdentifier && !Objects.equals(configIdentifier, configuredServiceIdentifier)) {
            throw new IllegalStateException(String.format("Kafka configuration service identifier [%s] does not match configured service identifier [%s]",
                    configIdentifier, configuredServiceIdentifier));
        }
    }

    @Override
    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof AWSCredentialsCallback awsCredentialsCallback) {
                handleCredentialsCallback(awsCredentialsCallback);
            } else {
                throw new UnsupportedCallbackException(callback, String.format("Unsupported callback type [%s]", callback.getClass().getName()));
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

    @Override
    public void close() {
        // No resources to clean up
    }
}
