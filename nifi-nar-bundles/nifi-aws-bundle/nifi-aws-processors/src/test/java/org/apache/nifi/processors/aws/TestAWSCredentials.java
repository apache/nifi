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
package org.apache.nifi.processors.aws;

import java.util.Arrays;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Unit tests for AWS Credential specification based on {@link AbstractAWSProcessor} and
 * [@link AbstractAWSCredentialsProviderProcessor},  without interaction with S3.
 */
public class TestAWSCredentials {

    private TestRunner runner = null;
    private AbstractAWSProcessor mockAwsProcessor = null;
    private AWSCredentials awsCredentials = null;
    private AWSCredentialsProvider awsCredentialsProvider = null;
    private ClientConfiguration clientConfiguration = null;

    @Before
    public void setUp() {
        mockAwsProcessor = new AbstractAWSCredentialsProviderProcessor<AmazonS3Client>() {

            protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return  Arrays.asList(
                        AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE,
                        AbstractAWSProcessor.CREDENTIALS_FILE,
                        AbstractAWSProcessor.ACCESS_KEY,
                        AbstractAWSProcessor.SECRET_KEY,
                        AbstractAWSProcessor.TIMEOUT
                );
            }

            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
                awsCredentials = credentials;
                clientConfiguration = config;
                final AmazonS3Client s3 = new AmazonS3Client(credentials, config);
                return s3;
            }

            @Override
            protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
                awsCredentialsProvider = credentialsProvider;
                clientConfiguration = config;
                final AmazonS3Client s3 = new AmazonS3Client(credentialsProvider, config);
                return s3;
            }

            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) {
            }
        };
        runner = TestRunners.newTestRunner(mockAwsProcessor);
    }

    @Test
    public void testAnonymousByDefault() {
        runner.assertValid();
        runner.run(1);

        assertEquals(AnonymousAWSCredentials.class, awsCredentials.getClass());
        assertNull(awsCredentialsProvider);
    }

    @Test
    public void testAccessKeySecretKey() {
        runner.setProperty(AbstractAWSProcessor.ACCESS_KEY, "testAccessKey");
        runner.setProperty(AbstractAWSProcessor.SECRET_KEY, "testSecretKey");

        runner.assertValid();
        runner.run(1);

        assertEquals(BasicAWSCredentials.class, awsCredentials.getClass());
        assertNull(awsCredentialsProvider);
    }

    @Test
    public void testCredentialsFile() {
        runner.setProperty(AbstractAWSProcessor.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");

        runner.assertValid();
        runner.run(1);

        assertEquals(PropertiesCredentials.class, awsCredentials.getClass());
        assertNull(awsCredentialsProvider);
    }


    @Test
    public void testCredentialsProviderControllerService() throws InitializationException {
        final AWSCredentialsProviderControllerService credsService = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", credsService);
        runner.setProperty(credsService, AbstractAWSProcessor.ACCESS_KEY, "awsAccessKey");
        runner.setProperty(credsService, AbstractAWSProcessor.SECRET_KEY, "awsSecretKey");
        runner.enableControllerService(credsService);

        runner.setProperty(AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");

        runner.assertValid();
        runner.run(1);

        assertEquals(StaticCredentialsProvider.class, awsCredentialsProvider.getClass());
        assertNull(awsCredentials);
    }
}
