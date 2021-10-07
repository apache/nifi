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
package org.apache.nifi.properties.configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkClient;

import java.util.Properties;

/**
 * Amazon Web Services Service Client Provider base class
 */
public abstract class AbstractAwsClientProvider<T extends SdkClient> extends BootstrapPropertiesClientProvider<T> {
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";

    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";

    private static final String REGION_KEY_PROPS_NAME = "aws.region";

    public AbstractAwsClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.AWS_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

    /**
     * Get Configured Client using either Client Properties or AWS Default Credentials Provider
     *
     * @param clientProperties Client Properties
     * @return KMS Client
     */
    @Override
    protected T getConfiguredClient(final Properties clientProperties) {
        final String accessKey = clientProperties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKey = clientProperties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = clientProperties.getProperty(REGION_KEY_PROPS_NAME);

        if (StringUtils.isNoneBlank(accessKey, secretKey, region)) {
            logger.debug("AWS Credentials Location: Client Properties");
            try {
                final AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                return createClient(credentials, region);
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException("AWS Client Builder Failed using Client Properties", e);
            }
        } else {
            logger.debug("AWS Credentials Location: Default Credentials Provider");
            try {
                final DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();
                return createDefaultClient(credentialsProvider);
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException("AWS Client Builder Failed using Default Credentials Provider", e);
            }
        }
    }

    /**
     * Create a client with the given credentials and region.
     * @param credentials AWS credentials
     * @param region AWS region
     * @return The created client
     */
    protected abstract T createClient(AwsCredentials credentials, String region);

    /**
     * Create a default client with the given credentials provider.
     * @param credentialsProvider AWS credentials provider
     * @return The created client
     */
    protected abstract T createDefaultClient(AwsCredentialsProvider credentialsProvider);
}
