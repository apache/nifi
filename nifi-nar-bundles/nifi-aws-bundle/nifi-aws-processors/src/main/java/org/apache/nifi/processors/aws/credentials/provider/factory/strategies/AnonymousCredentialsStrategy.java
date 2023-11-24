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
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


/**
 * Supports Anonymous AWS credentials.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AnonymousAWSCredentials.html">
 *     AnonymousAWSCredentials</a>
 */
public class AnonymousCredentialsStrategy extends AbstractBooleanCredentialsStrategy {

    public AnonymousCredentialsStrategy() {
        super("Anonymous Credentials", AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS);
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider(final PropertyContext propertyContext) {
        AnonymousAWSCredentials credentials = new AnonymousAWSCredentials();
        return new AWSStaticCredentialsProvider(credentials);
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        return AnonymousCredentialsProvider.create();
    }

}
