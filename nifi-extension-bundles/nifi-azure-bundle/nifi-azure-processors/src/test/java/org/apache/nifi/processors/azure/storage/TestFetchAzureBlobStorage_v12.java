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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFetchAzureBlobStorage_v12 {
    @Test
    void testMigration() {
        TestRunner runner = TestRunners.newTestRunner(FetchAzureBlobStorage_v12.class);
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed =
                Map.of(AbstractAzureBlobProcessor_v12.OLD_BLOB_NAME_PROPERTY_DESCRIPTOR_NAME, FetchAzureBlobStorage_v12.BLOB_NAME.getName(),
                        AzureStorageUtils.OLD_CONTAINER_DESCRIPTOR_NAME, FetchAzureBlobStorage_v12.CONTAINER.getName(),
                        AzureStorageUtils.OLD_BLOB_STORAGE_CREDENTIALS_SERVICE_DESCRIPTOR_NAME, AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE.getName(),
                        "range-start", FetchAzureBlobStorage_v12.RANGE_START.getName(),
                        "range-length", FetchAzureBlobStorage_v12.RANGE_LENGTH.getName(),
                        ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE);
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
