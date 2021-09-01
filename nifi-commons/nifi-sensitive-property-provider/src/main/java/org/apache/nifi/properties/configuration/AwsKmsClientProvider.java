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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

import java.util.Properties;

/**
 * Amazon Web Services Key Management Service Client Provider
 */
public class AwsKmsClientProvider extends BootstrapPropertiesClientProvider<KmsClient> {
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";

    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";

    private static final String REGION_KEY_PROPS_NAME = "aws.region";

    public AwsKmsClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.AWS_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

    /**
     * Get Configured Client using either Client Properties or AWS Default Credentials Provider
     *
     * @param clientProperties Client Properties
     * @return KMS Client
     */
    @Override
    protected KmsClient getConfiguredClient(final Properties clientProperties) {
        final String accessKey = clientProperties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKey = clientProperties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = clientProperties.getProperty(REGION_KEY_PROPS_NAME);

        final KmsClientBuilder kmsClientBuilder = KmsClient.builder();
        if (StringUtils.isNoneBlank(accessKey, secretKey, region)) {
            logger.debug("AWS Credentials Location: Client Properties");
            try {
                final AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                return kmsClientBuilder
                        .region(Region.of(region))
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .build();
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException("AWS KMS Client Builder Failed using Client Properties", e);
            }
        } else {
            logger.debug("AWS Credentials Location: Default Credentials Provider");
            try {
                final DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();
                return kmsClientBuilder.credentialsProvider(credentialsProvider).build();
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException("AWS KMS Client Builder Failed using Default Credentials Provider", e);
            }
        }
    }
}
