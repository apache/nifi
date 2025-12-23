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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ACCESS_KEY_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_ENDPOINT;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.PROFILE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.SECRET_KEY;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AWSCredentialsProviderControllerServiceTest {

    @Test
    public void testDefaultAWSCredentialsProviderChain() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(DefaultCredentialsProvider.class,
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
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
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
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(StsAssumeRoleCredentialsProvider.class,
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
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(StsAssumeRoleCredentialsProvider.class,
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
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(StsAssumeRoleCredentialsProvider.class,
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
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);
        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals(PropertiesCredentialsProvider.class,
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

        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");

        assertEquals(
                "awsAccessKey", service.getAwsCredentialsProvider().resolveCredentials().accessKeyId(),
            "Expression language should be supported for " + ACCESS_KEY_ID.getName());
        assertEquals(
                "awsSecretKey", service.getAwsCredentialsProvider().resolveCredentials().secretAccessKey(),
            "Expression language should be supported for " + SECRET_KEY.getName());
    }

    @Test
    public void testDefaultAWSCredentialsProviderChainV2() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
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

        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2", tokenProvider);
        runner.enableControllerService(tokenProvider);

        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "nifi-test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_2.id());

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);

        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertEquals("WebIdentityRefreshingCredentialsProvider", credentialsProvider.getClass().getSimpleName());
    }

    @Test
    public void testWebIdentityDoesNotChainAssumeRoleDerivedProvider() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(FetchS3Object.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        final MockOAuth2AccessTokenProvider tokenProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("oauth2", tokenProvider);
        runner.enableControllerService(tokenProvider);

        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth2");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME, "nifi-test");
        runner.setProperty(serviceImpl, AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION, Region.US_WEST_2.id());

        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);

        final AwsCredentialsProviderService service = (AwsCredentialsProviderService) runner.getProcessContext()
                .getControllerServiceLookup().getControllerService("awsCredentialsProvider");
        assertNotNull(service);

        final AwsCredentialsProvider credentialsProvider = service.getAwsCredentialsProvider();
        assertNotNull(credentialsProvider);
        assertFalse(StsAssumeRoleCredentialsProvider.class.isAssignableFrom(credentialsProvider.getClass()),
                "Derived AssumeRole should not be chained when OAuth2 (Web Identity) is configured");
    }

    @Test
    void testMigration() {
        final Map<String, String> propertyValues = Map.of(
                AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME.getName(), "nifi-test"
        );

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        final AWSCredentialsProviderControllerService awsCredentialsProviderControllerService = new AWSCredentialsProviderControllerService();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("default-credentials", USE_DEFAULT_CREDENTIALS.getName()),
                Map.entry("profile-name", PROFILE_NAME.getName()),
                Map.entry("Access Key", ACCESS_KEY_ID.getName()),
                Map.entry("Secret Key", SECRET_KEY.getName()),
                Map.entry("anonymous-credentials", USE_ANONYMOUS_CREDENTIALS.getName()),
                Map.entry("assume-role-sts-region", ASSUME_ROLE_STS_REGION.getName()),
                Map.entry("assume-role-external-id", ASSUME_ROLE_EXTERNAL_ID.getName()),
                Map.entry("assume-role-ssl-context-service", ASSUME_ROLE_SSL_CONTEXT_SERVICE.getName()),
                Map.entry("assume-role-proxy-configuration-service", ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE.getName()),
                Map.entry("assume-role-sts-endpoint", ASSUME_ROLE_STS_ENDPOINT.getName()),
                Map.entry("Session Time", MAX_SESSION_TIME.getName()),
                Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE)
        );

        awsCredentialsProviderControllerService.migrateProperties(configuration);
        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> expectedRemoved = new HashSet<>(Arrays.asList(null, "assume-role-proxy-host", "assume-role-proxy-port", "assume-role-sts-signer-override",
                "Assume Role STS Signer Override", "custom-signer-class-name", "Custom Signer Class Name",
                "custom-signer-module-location", "Custom Signer Module Location"));

        assertEquals(expectedRemoved, result.getPropertiesRemoved());
    }

    private static final class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken token = new AccessToken();
            token.setAccessToken("dummy-access-token");
            return token;
        }

        @Override
        public void refreshAccessDetails() {
            // no-op
        }
    }
}
