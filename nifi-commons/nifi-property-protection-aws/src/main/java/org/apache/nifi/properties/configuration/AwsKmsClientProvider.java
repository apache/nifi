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

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

import java.util.Collections;
import java.util.Set;

/**
 * Amazon Web Services Key Management Service Client Provider
 */
public class AwsKmsClientProvider extends AbstractAwsClientProvider<KmsClient> {

    protected static final String KEY_ID_PROPERTY = "aws.kms.key.id";

    @Override
    protected KmsClient createClient(final AwsCredentials credentials, final String region) {
        return KmsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of(region))
                .build();
    }

    @Override
    protected KmsClient createDefaultClient(final AwsCredentialsProvider credentialsProvider) {
        return KmsClient.builder().credentialsProvider(credentialsProvider).build();
    }

    @Override
    protected Set<String> getRequiredPropertyNames() {
        return Collections.singleton(KEY_ID_PROPERTY);
    }
}
