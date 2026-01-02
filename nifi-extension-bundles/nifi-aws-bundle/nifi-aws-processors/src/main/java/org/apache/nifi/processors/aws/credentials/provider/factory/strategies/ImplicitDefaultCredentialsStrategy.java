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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Supports AWS Default Credentials.  Compared to ExplicitDefaultCredentialsStrategy, this strategy is always
 * willing to provide primary credentials, regardless of user input.  It is intended to be used as an invisible
 * fallback or default strategy.
 */
public class ImplicitDefaultCredentialsStrategy extends AbstractCredentialsStrategy {

    public ImplicitDefaultCredentialsStrategy() {
        super("Default Credentials", new PropertyDescriptor[]{});
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        // always create a new Connection Pool to avoid STS auth issues after an existing Credentials Provider has been closed
        // see https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/troubleshooting.html#faq-connection-pool-shutdown-exception
        return DefaultCredentialsProvider.builder().build();
    }
}
