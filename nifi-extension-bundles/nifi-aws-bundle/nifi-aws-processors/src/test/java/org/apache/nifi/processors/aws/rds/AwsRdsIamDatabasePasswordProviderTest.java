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
package org.apache.nifi.processors.aws.rds;

import org.apache.nifi.dbcp.api.DatabasePasswordProvider;
import org.apache.nifi.dbcp.api.DatabasePasswordRequestContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ACCESS_KEY_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.SECRET_KEY;
import static org.apache.nifi.processors.aws.rds.AwsRdsIamDatabasePasswordProvider.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwsRdsIamDatabasePasswordProviderTest {

    private TestRunner runner;
    private AWSCredentialsProviderControllerService credentialsService;
    private AwsRdsIamDatabasePasswordProvider passwordProvider;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(FetchS3Object.class);

        credentialsService = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentials", credentialsService);
        runner.setProperty(credentialsService, ACCESS_KEY_ID, "accessKey");
        runner.setProperty(credentialsService, SECRET_KEY, "secretKey");
        runner.enableControllerService(credentialsService);

        passwordProvider = new AwsRdsIamDatabasePasswordProvider();
        runner.addControllerService("iamProvider", passwordProvider);
        runner.setProperty(passwordProvider, AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentials");
        runner.setProperty(passwordProvider, REGION, Region.US_WEST_2.id());
        runner.enableControllerService(passwordProvider);
    }

    @Test
    void testGeneratesTokenUsingRequestContext() {
        final DatabasePasswordProvider service = getService();
        final DatabasePasswordRequestContext context = DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:postgresql://example.us-west-2.rds.amazonaws.com:5432/dev")
                .databaseUser("dbuser")
                .driverClassName("org.postgresql.Driver")
                .build();

        final String token = new String(service.getPassword(context));
        assertTrue(token.startsWith("example.us-west-2.rds.amazonaws.com:5432/"));
        assertTrue(token.contains("DBUser=dbuser"));
    }

    @Test
    void testGeneratesTokenWithDefaultPort() {
        final DatabasePasswordProvider service = getService();
        final DatabasePasswordRequestContext context = DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:postgresql://example.us-west-2.rds.amazonaws.com/db")
                .databaseUser("dbuser")
                .driverClassName("org.postgresql.Driver")
                .build();

        final String token = new String(service.getPassword(context));
        assertTrue(token.startsWith("example.us-west-2.rds.amazonaws.com:5432/"));
        assertTrue(token.contains("DBUser=dbuser"));
    }

    @Test
    void testMissingHostnameThrowsProcessException() {
        final DatabasePasswordProvider service = getService();
        final DatabasePasswordRequestContext context = DatabasePasswordRequestContext.builder()
                .jdbcUrl("jdbc:postgresql:///dbname")
                .databaseUser("dbuser")
                .driverClassName("org.postgresql.Driver")
                .build();

        assertThrows(ProcessException.class, () -> service.getPassword(context));
    }

    private DatabasePasswordProvider getService() {
        return (DatabasePasswordProvider) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("iamProvider");
    }
}
