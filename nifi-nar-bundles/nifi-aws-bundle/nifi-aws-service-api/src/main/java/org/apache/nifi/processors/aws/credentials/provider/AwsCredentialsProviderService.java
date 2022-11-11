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
package org.apache.nifi.processors.aws.credentials.provider;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * AwsCredentialsProviderService interface to support getting AwsCredentialsProvider used for instantiating
 * aws clients using the v2 SDK.
 *
 * @see <a href="https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
 */
@Tags({"aws", "v2", "security", "credentials", "provider", "session"})
@CapabilityDescription("Provides AwsCredentialsProvider.")
public interface AwsCredentialsProviderService extends ControllerService {

    /**
     * Get credentials provider for Java SDK v2
     * @return credentials provider
     * @throws ProcessException process exception in case there is problem in getting credentials provider
     *
     * @see <a href="https://sdk.amazonaws.com/java/api/2.0.0/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html">AwsCredentialsProvider</a>
     */
    AwsCredentialsProvider getAwsCredentialsProvider();
}
