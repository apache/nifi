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
package org.apache.nifi.processors.aws.credentials.provider.service;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * AWSCredentialsProviderService interface to support getting AWSCredentialsProvider used for instantiating
 * aws clients
 *
 * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
 */
@Tags({"aws", "security", "credentials", "provider", "session"})
@CapabilityDescription("Provides AWSCredentialsProvider.")
public interface AWSCredentialsProviderService extends AwsCredentialsProviderService {

    /**
     * Get credentials provider for Java SDK v1
     * @return credentials provider
     * @throws ProcessException process exception in case there is problem in getting credentials provider
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    AWSCredentialsProvider getCredentialsProvider() throws ProcessException;

    /**
     * Default implementation of {@link AwsCredentialsProviderService#getAwsCredentialsProvider()} throwing UnsupportedOperationException.
     * @return always throws UnsupportedOperationException
     */
    @Override
    default AwsCredentialsProvider getAwsCredentialsProvider() {
        throw new UnsupportedOperationException("AWS Java SDK v2 credentials are not supported by this service");
    }
}
