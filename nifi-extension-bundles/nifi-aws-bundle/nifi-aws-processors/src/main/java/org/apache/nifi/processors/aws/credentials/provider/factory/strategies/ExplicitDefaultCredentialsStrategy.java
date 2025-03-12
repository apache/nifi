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
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;


/**
 * Supports AWS Default Credentials.  Compared to ImplicitDefaultCredentialsStrategy, this strategy is designed to be
 * visible to the user, and depends on an affirmative selection from the user.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">
 *     DefaultAWSCredentialsProviderChain</a>
 */
public class ExplicitDefaultCredentialsStrategy extends AbstractBooleanCredentialsStrategy {

    public ExplicitDefaultCredentialsStrategy() {
        super("Default Credentials", AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS);
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider(final PropertyContext propertyContext) {
      return new DefaultAWSCredentialsProviderChain();
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        // always create a new Connection Pool to avoid STS auth issues after an existing Credentials Provider has been closed
        // see https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/troubleshooting.html#faq-connection-pool-shutdown-exception
        return DefaultCredentialsProvider.builder().build();
    }

}
