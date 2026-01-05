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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.NoOpProcessor;
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

    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String DB_USER = "dbuser";
    private static final String HOSTNAME = "example.us-west-2.rds.amazonaws.com";
    private static final String JDBC_PREFIX = "jdbc:postgresql://";
    private static final String DATABASE = "dev";
    private static final int PORT = 5432;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

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
                .jdbcUrl("%s%s:%d/%s".formatted(JDBC_PREFIX, HOSTNAME, PORT, DATABASE))
                .databaseUser(DB_USER)
                .driverClassName(POSTGRES_DRIVER_CLASS)
                .build();

        final String token = new String(service.getPassword(context));
        assertTrue(token.startsWith("%s:%d/".formatted(HOSTNAME, PORT)));
        assertTrue(token.contains("DBUser=" + DB_USER));
    }

    @Test
    void testGeneratesTokenWithDefaultPort() {
        final DatabasePasswordProvider service = getService();
        final DatabasePasswordRequestContext context = DatabasePasswordRequestContext.builder()
                .jdbcUrl("%s%s:%d/%s".formatted(JDBC_PREFIX, HOSTNAME, PORT, DATABASE))
                .databaseUser(DB_USER)
                .driverClassName(POSTGRES_DRIVER_CLASS)
                .build();

        final String token = new String(service.getPassword(context));
        assertTrue(token.startsWith("%s:%d/".formatted(HOSTNAME, PORT)));
        assertTrue(token.contains("DBUser=" + DB_USER));
    }

    @Test
    void testMissingHostnameThrowsProcessException() {
        final DatabasePasswordProvider service = getService();
        final DatabasePasswordRequestContext context = DatabasePasswordRequestContext.builder()
                .jdbcUrl("%s/%s".formatted(JDBC_PREFIX, DATABASE))
                .databaseUser(DB_USER)
                .driverClassName(POSTGRES_DRIVER_CLASS)
                .build();

        assertThrows(ProcessException.class, () -> service.getPassword(context));
    }

    private DatabasePasswordProvider getService() {
        return (DatabasePasswordProvider) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("iamProvider");
    }
}
