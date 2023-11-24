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
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ACCESS_KEY_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.CREDENTIALS_FILE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
