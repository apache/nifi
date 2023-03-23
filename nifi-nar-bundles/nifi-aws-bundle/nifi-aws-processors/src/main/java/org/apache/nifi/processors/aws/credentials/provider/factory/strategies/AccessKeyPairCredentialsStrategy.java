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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


/**
 * Supports AWS credentials defined by an Access Key and Secret Key pair.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/BasicAWSCredentials.html">
 *     BasicAWSCredentials</a>
 */
public class AccessKeyPairCredentialsStrategy extends AbstractCredentialsStrategy {

    public AccessKeyPairCredentialsStrategy() {
        super("Access Key Pair", new PropertyDescriptor[] {
                CredentialPropertyDescriptors.ACCESS_KEY,
                CredentialPropertyDescriptors.SECRET_KEY
        });
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider(final PropertyContext propertyContext) {
        final String accessKey = propertyContext.getProperty(CredentialPropertyDescriptors.ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = propertyContext.getProperty(CredentialPropertyDescriptors.SECRET_KEY).evaluateAttributeExpressions().getValue();
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        return new StaticCredentialsProvider(credentials);
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        final String accessKey = propertyContext.getProperty(CredentialPropertyDescriptors.ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = propertyContext.getProperty(CredentialPropertyDescriptors.SECRET_KEY).evaluateAttributeExpressions().getValue();
        return software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }

}
