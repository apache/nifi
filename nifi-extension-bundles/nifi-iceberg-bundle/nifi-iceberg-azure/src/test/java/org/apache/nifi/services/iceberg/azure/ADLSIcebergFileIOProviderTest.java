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
package org.apache.nifi.services.iceberg.azure;

import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ADLSIcebergFileIOProviderTest {
    private static final String SERVICE_ID = ADLSIcebergFileIOProvider.class.getSimpleName();

    private static final String STORAGE_ACCOUNT = "storage-account";

    private static final String SHARED_ACCESS_SIGNATURE_TOKEN = "sas-token";

    private TestRunner runner;

    private ADLSIcebergFileIOProvider provider;

    @BeforeEach
    void setProvider() throws InitializationException {
        provider = new ADLSIcebergFileIOProvider();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, provider);
    }

    @AfterEach
    void disableProvider() {
        if (runner.isControllerServiceEnabled(provider)) {
            runner.disableControllerService(provider);
        }
    }

    @Test
    void testGetFileIO() {
        runner.enableControllerService(provider);

        final Map<String, String> properties = Map.of();
        final ProviderContext providerContext = () -> properties;

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            assertInstanceOf(ADLSFileIO.class, fileIO);
        }
    }

    @Test
    void testGetFileIOSharedAccessSignatureToken() {
        runner.setProperty(provider, ADLSIcebergFileIOProvider.AUTHENTICATION_STRATEGY, AuthenticationStrategy.SHARED_ACCESS_SIGNATURE_TOKEN.getValue());
        runner.setProperty(provider, ADLSIcebergFileIOProvider.STORAGE_ACCOUNT, STORAGE_ACCOUNT);
        runner.setProperty(provider, ADLSIcebergFileIOProvider.SHARED_ACCESS_SIGNATURE_TOKEN, SHARED_ACCESS_SIGNATURE_TOKEN);

        runner.enableControllerService(provider);

        final Map<String, String> properties = Map.of();
        final ProviderContext providerContext = () -> properties;

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            assertInstanceOf(ADLSFileIO.class, fileIO);
            final Map<String, String> configuredProperties = fileIO.properties();
            assertFalse(configuredProperties.isEmpty());
            assertTrue(configuredProperties.containsValue(SHARED_ACCESS_SIGNATURE_TOKEN));
        }

        runner.disableControllerService(provider);

        try (FileIO fileIO = provider.getFileIO(providerContext)) {
            final Map<String, String> configuredProperties = fileIO.properties();
            assertTrue(configuredProperties.isEmpty());
        }
    }
}
