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
package org.apache.nifi.processors.aws.credentials.provider.factory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.PROFILE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors.ASSUME_ROLE_EXTERNAL_ID;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;


/**
 * Mock Processor implementation used to test CredentialsProviderFactory.
 */
public class MockAWSProcessor extends AbstractAWSCredentialsProviderProcessor<AmazonS3Client> {

    public final List<PropertyDescriptor> properties = Arrays.asList(
            USE_DEFAULT_CREDENTIALS,
            ACCESS_KEY,
            SECRET_KEY,
            CREDENTIALS_FILE,
            PROFILE_NAME,
            USE_ANONYMOUS_CREDENTIALS,
            ASSUME_ROLE_ARN,
            ASSUME_ROLE_NAME,
            MAX_SESSION_TIME,
            ASSUME_ROLE_EXTERNAL_ID
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        CredentialsProviderFactory credsFactory = new CredentialsProviderFactory();
        final Collection<ValidationResult> validationFailureResults = credsFactory.validate(validationContext);
        return validationFailureResults;
    }

    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().info("Creating client with credentials provider");
        final AmazonS3Client s3 = new AmazonS3Client(credentialsProvider, config);
        return s3;
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().info("Creating client with awd credentials");

        final AmazonS3Client s3 = new AmazonS3Client(credentials, config);

        return s3;
    }

}
