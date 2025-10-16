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
package org.apache.nifi.services.iceberg.aws;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.iceberg.ProviderContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class S3IcebergFileIOProviderTest {
    private static final String SERVICE_ID = S3IcebergFileIOProvider.class.getSimpleName();

    private static final String CLIENT_REGION = "us-east-1";

    private static final String ACCESS_KEY_ID = "AccessKeyID";

    private static final String SECRET_ACCESS_KEY = "SecretAccessKey";

    private static final String SESSION_TOKEN = "SessionToken";

    private TestRunner runner;

    private S3IcebergFileIOProvider provider;

    @BeforeEach
    void setProvider() throws InitializationException {
        provider = new S3IcebergFileIOProvider();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, provider);
    }

    @AfterEach
    void disableProvider() {
        runner.disableControllerService(provider);
    }

    @Test
    void testGetFileIO() {
        runner.enableControllerService(provider);

        final Map<String, String> properties = Map.of();
        final ProviderContext providerContext = () -> properties;

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            assertFileIOConfigured(fileIO);
        }
    }

    @Test
    void testGetFileIOSessionCredentials() {
        runner.setProperty(provider, S3IcebergFileIOProvider.AUTHENTICATION_STRATEGY, AuthenticationStrategy.SESSION_CREDENTIALS.getValue());
        runner.setProperty(provider, S3IcebergFileIOProvider.ACCESS_KEY_ID, ACCESS_KEY_ID);
        runner.setProperty(provider, S3IcebergFileIOProvider.SECRET_ACCESS_KEY, SECRET_ACCESS_KEY);
        runner.setProperty(provider, S3IcebergFileIOProvider.SESSION_TOKEN, SESSION_TOKEN);
        runner.setProperty(provider, S3IcebergFileIOProvider.CLIENT_REGION, CLIENT_REGION);

        runner.enableControllerService(provider);

        final Map<String, String> properties = Map.of();
        final ProviderContext providerContext = () -> properties;

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            assertFileIOConfigured(fileIO);

            final Map<String, String> configuredProperties = fileIO.properties();
            assertEquals(ACCESS_KEY_ID, configuredProperties.get(S3FileIOProperties.ACCESS_KEY_ID));
            assertEquals(SECRET_ACCESS_KEY, configuredProperties.get(S3FileIOProperties.SECRET_ACCESS_KEY));
            assertEquals(SESSION_TOKEN, configuredProperties.get(S3FileIOProperties.SESSION_TOKEN));
            assertEquals(CLIENT_REGION, configuredProperties.get(AwsClientProperties.CLIENT_REGION));
        }
    }

    private void assertFileIOConfigured(final FileIO fileIO) {
        assertNotNull(fileIO);
        assertInstanceOf(S3FileIO.class, fileIO);
        final Map<String, String> configuredProperties = fileIO.properties();
        assertFalse(configuredProperties.isEmpty());
    }
}
