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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestAWSCredentialsProviderControllerServiceStrategies {

    private TestRunner runner;
    private AWSCredentialsProviderControllerService service;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        service = new AWSCredentialsProviderControllerService();
        runner.addControllerService("auth", service);
    }

    @Test
    public void testImpliedDefaultCredentials() {
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(DefaultAWSCredentialsProviderChain.class, credentialsProvider.getClass());

        final AwsCredentialsProvider credentialsProviderV2 = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProviderV2);
        assertEquals(DefaultCredentialsProvider.class, credentialsProviderV2.getClass());
    }

    @Test
    public void testExplicitDefaultCredentials() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS, "true");
        runner.assertValid(service);
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(DefaultAWSCredentialsProviderChain.class, credentialsProvider.getClass());

        final AwsCredentialsProvider credentialsProviderV2 = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProviderV2);
        assertEquals(DefaultCredentialsProvider.class, credentialsProviderV2.getClass());
    }

    @Test
    public void testExplicitDefaultCredentialsExclusive() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(service, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "BogusAccessKey");
        runner.assertNotValid(service);
    }

    @Test
    public void testAssumeRoleCredentials() throws Throwable {
        runner.setProperty(service, AWSCredentialsProviderControllerService.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(service, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(service, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "BogusSession");
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class, credentialsProvider.getClass());
    }


    @Test
    public void testFileCredentials() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(PropertiesFileCredentialsProvider.class, credentialsProvider.getClass());

        final AwsCredentialsProvider credentialsProviderV2 = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProviderV2);
        assertEquals(PropertiesCredentialsProvider.class, credentialsProviderV2.getClass());
    }

    @Test
    public void testAccessKeyPairIncomplete() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "BogusAccessKey");
        runner.assertNotValid(service);
    }


    @Test
    public void testAssumeRoleCredentialsInvalidSessionTime() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(service, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(service, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(service, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "10");
        runner.assertNotValid(service);
    }

    @Test
    public void testAnonymousCredentials() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertValid(service);
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        final AWSCredentials creds = credentialsProvider.getCredentials();
        assertEquals(AnonymousAWSCredentials.class, creds.getClass());

        final AwsCredentialsProvider credentialsProviderV2 = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProviderV2);
        assertEquals(AnonymousCredentialsProvider.class, credentialsProviderV2.getClass());
    }

    @Test
    public void testAnonymousAndDefaultCredentials() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertNotValid(service);
    }

    @Test
    public void testNamedProfileCredentials() {
        runner.setProperty(service, AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS, "false");
        runner.setProperty(service, AWSCredentialsProviderControllerService.PROFILE_NAME, "BogusProfile");
        runner.enableControllerService(service);

        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(ProfileCredentialsProvider.class, credentialsProvider.getClass());

        final AwsCredentialsProvider credentialsProviderV2 = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProviderV2);
        assertEquals(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.class, credentialsProviderV2.getClass());
    }
}
