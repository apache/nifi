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
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ACCESS_KEY_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AWSCredentialsProviderControllerServiceTest {

    @Test
    public void testDefaultAWSCredentialsProviderChain() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(DefaultAWSCredentialsProviderChain.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testKeysCredentialsProvider() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndName() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_1.id());
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndNameAndSessionTimeoutInRange() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_1.id());
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "1000");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndNameAndSessionTimeout900() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_1.id());
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "900");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndNameAndSessionTimeout3600() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_1.id());
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "900");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndNameAndSessionTimeoutLessThan900() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "899");
        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testKeysCredentialsProviderWithRoleAndNameAndSessionTimeoutGreaterThan3600() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.MAX_SESSION_TIME, "899");
        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testKeysCredentialsProviderWithRoleOnlyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testFileCredentialsProviderWithRole() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_1.id());
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "Role");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "RoleName");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(STSAssumeRoleSessionCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testFileCredentialsProvider() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE,
                "src/test/resources/mock-aws-credentials.properties");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AWSCredentialsProvider credentialsProvider = service.getCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(PropertiesFileCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testFileCredentialsProviderBadFile() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE,
                "src/test/resources/bad-mock-aws-credentials.properties");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testFileAndAccessSecretKeyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE,
                "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testFileAndAccessKeyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE,
                "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testFileAndSecretKeyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE,
                "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testAccessKeyOnlyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "awsAccessKey");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testSecretKeyOnlyInvalid() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");

        runner.assertNotValid(serviceImpl);
    }

    @Test
    public void testExpressionLanguageSupport() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY_ID, "${literal(\"awsAccessKey\")}");
        runner.setProperty(serviceImpl, SECRET_KEY, "${literal(\"awsSecretKey\")}");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");

        assertEquals(
                "awsAccessKey", service.getCredentialsProvider().getCredentials().getAWSAccessKeyId(),
            "Expression language should be supported for " + ACCESS_KEY_ID.getName());
        assertEquals(
                "awsSecretKey", service.getCredentialsProvider().getCredentials().getAWSSecretKey(),
            "Expression language should be supported for " + SECRET_KEY.getName());
    }

    @Test
    public void testDefaultAWSCredentialsProviderChainV2() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(DefaultCredentialsProvider.class,
                credentialsProvider.getClass(), "credentials provider should be equal");
    }

    @Test
    public void testWebIdentityPropertiesAreValidAndServiceEnables() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        // Add and enable a mock OAuth2 Access Token Provider
        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2", tokenProvider);
        runner.enableControllerService(tokenProvider);

        // Configure AWS Credentials Provider for Web Identity
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "nifi-test");

        // Enable should succeed even though v1 is not supported by Web Identity
        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final AWSCredentialsProviderService service = (AWSCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);

        // v2 provider should be available
        final AwsCredentialsProvider v2 = service.getAwsCredentialsProvider();
        assertNotNull(v2);

        // v1 provider should throw since Web Identity is v2-only
        assertThrows(UnsupportedOperationException.class, service::getCredentialsProvider);
    }

    @Test
    public void testWebIdentityDoesNotChainAssumeRoleDerivedProvider() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        // Mock OAuth2 provider
        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2", tokenProvider);
        runner.enableControllerService(tokenProvider);

        // Configure Web Identity properties along with Assume Role settings
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "nifi-test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_2.id());

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final AWSCredentialsProviderService service = (AWSCredentialsProviderControllerService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);

        final AwsCredentialsProvider v2 = service.getAwsCredentialsProvider();
        assertNotNull(v2);

        // Ensure derived STS AssumeRole provider was not chained when OAuth2 is set
        // i.e., provider is NOT StsAssumeRoleCredentialsProvider
        final Class<?> providerClass = v2.getClass();
        final boolean isDerivedAssumeRole = StsAssumeRoleCredentialsProvider.class.isAssignableFrom(providerClass);
        assertEquals(false, isDerivedAssumeRole, "Derived AssumeRole should not be chained when OAuth2 (Web Identity) is configured");
    }

    /**
     * Minimal mock OAuth2 provider returning a static AccessToken.
     * Web Identity credentials resolution lazily reads the token, so
     * the exact contents are not needed for validation/enabling.
     */
    private static final class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            // Return a token with an access token placeholder
            final AccessToken token = new AccessToken();
            token.setAccessToken("dummy-access-token");
            return token;
        }

        @Override
        public void refreshAccessDetails() { /* no-op */ }
    }
}
