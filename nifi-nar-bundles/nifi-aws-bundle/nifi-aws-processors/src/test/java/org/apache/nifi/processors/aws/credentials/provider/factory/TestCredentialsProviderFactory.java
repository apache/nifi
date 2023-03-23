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

import com.amazonaws.SignableRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processors.aws.signer.AwsSignerType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests of the validation and credentials provider capabilities of CredentialsProviderFactory.
 */
public class TestCredentialsProviderFactory {

    @Test
    public void testImpliedDefaultCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(DefaultAWSCredentialsProviderChain.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(DefaultCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testExplicitDefaultCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(DefaultAWSCredentialsProviderChain.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(DefaultCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testExplicitDefaultCredentialsExclusive() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.assertNotValid();
    }

    @Test
    public void testAccessKeyPairCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "false");
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.setProperty(CredentialPropertyDescriptors.SECRET_KEY, "BogusSecretKey");
        runner.assertValid();


        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(StaticCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAccessKeyPairIncomplete() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.assertNotValid();
    }

    @Test
    public void testAccessKeyPairIncompleteS3() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        runner.setProperty(CredentialPropertyDescriptors.ACCESS_KEY, "BogusAccessKey");
        runner.assertNotValid();
    }

    @Test
    public void testFileCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(PropertiesFileCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(PropertiesCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAssumeRoleCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAssumeRoleCredentialsInvalidSessionTime() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.MAX_SESSION_TIME, "10");
        runner.assertNotValid();
    }

    @Test
    public void testAnonymousCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        final AWSCredentials creds = credentialsProvider.getCredentials();
        assertEquals(AnonymousAWSCredentials.class, creds.getClass(), "credentials should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(AnonymousCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAnonymousAndDefaultCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "true");
        runner.setProperty(CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertNotValid();
    }

    @Test
    public void testNamedProfileCredentials() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.USE_DEFAULT_CREDENTIALS, "false");
        runner.setProperty(CredentialPropertyDescriptors.PROFILE_NAME, "BogusProfile");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(ProfileCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAssumeRoleCredentialsWithProxy() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_STS_REGION, Region.US_WEST_2.id());
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "8080");
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");

        final AwsCredentialsProvider credentialsProviderV2 = factory.getAwsCredentialsProvider(runner.getProcessContext());
        assertNotNull(credentialsProviderV2);
        assertEquals(StsAssumeRoleCredentialsProvider.class,
                credentialsProviderV2.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testAssumeRoleMissingProxyHost() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "8080");
        runner.assertNotValid();
    }

    @Test
    public void testAssumeRoleMissingProxyPort() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.assertNotValid();
    }

    @Test
    public void testAssumeRoleInvalidProxyPort() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_HOST, "proxy.company.com");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_PROXY_PORT, "notIntPort");
        runner.assertNotValid();
    }

    @Test
    public void testAssumeRoleCredentialsWithCustomSigner() {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        runner.setProperty(CredentialPropertyDescriptors.CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_STS_SIGNER_OVERRIDE, AwsSignerType.CUSTOM_SIGNER.getValue());
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME, CustomSTSSigner.class.getName());
        runner.assertValid();

        final CredentialsProviderFactory factory = new CredentialsProviderFactory();

        final Signer signerChecker = mock(Signer.class);
        CustomSTSSigner.setSignerChecker(signerChecker);

        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(runner.getProcessContext());

        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            // Expected to fail, we are only interested in the Signer
        }

        verify(signerChecker).sign(any(), any());
    }

    public static class CustomSTSSigner extends AWS4Signer {

        private static final ThreadLocal<Signer> SIGNER_CHECKER = new ThreadLocal<>();

        public static void setSignerChecker(Signer signerChecker) {
            SIGNER_CHECKER.set(signerChecker);
        }

        @Override
        public void sign(SignableRequest<?> request, AWSCredentials credentials) {
            SIGNER_CHECKER.get().sign(request, credentials);
        }
    }
}
