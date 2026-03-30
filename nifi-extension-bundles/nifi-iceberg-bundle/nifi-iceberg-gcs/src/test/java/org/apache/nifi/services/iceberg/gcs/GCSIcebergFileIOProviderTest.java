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
package org.apache.nifi.services.iceberg.gcs;

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

import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.OAUTH2_TOKEN;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageProperty.SERVICE_HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GCSIcebergFileIOProviderTest {
    private static final String SERVICE_ID = GCSIcebergFileIOProvider.class.getSimpleName();
    private static final String ACCESS_TOKEN = "access-token";
    private static final String LOCALHOST_URL = "http://localhost:9000";

    private TestRunner runner;

    private GCSIcebergFileIOProvider provider;

    @BeforeEach
    void setProvider() throws InitializationException {
        provider = new GCSIcebergFileIOProvider();
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
            assertNotNull(fileIO);
            assertInstanceOf(GoogleCloudStorageFileIO.class, fileIO);
        }
    }

    @Test
    void testGetFileIOWithVendedToken() {
        runner.enableControllerService(provider);

        final Map<String, String> properties = Map.of(
                OAUTH2_TOKEN.getProperty(), ACCESS_TOKEN,
                SERVICE_HOST.getProperty(), LOCALHOST_URL
        );
        final ProviderContext providerContext = () -> properties;

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            assertNotNull(fileIO);
            assertInstanceOf(GoogleCloudStorageFileIO.class, fileIO);
            final Map<String, String> configuredProperties = fileIO.properties();
            assertEquals(ACCESS_TOKEN, configuredProperties.get(OAUTH2_TOKEN.getProperty()));
            assertEquals(LOCALHOST_URL, configuredProperties.get(SERVICE_HOST.getProperty()));
        }
    }
}
