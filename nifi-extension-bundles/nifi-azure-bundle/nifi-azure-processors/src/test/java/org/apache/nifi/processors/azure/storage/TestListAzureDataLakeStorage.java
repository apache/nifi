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

import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestListAzureDataLakeStorage {
    @Test
    void testMigration() {
        TestRunner runner = TestRunners.newTestRunner(ListAzureDataLakeStorage.class);
        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        final Map<String, String> expectedRenamed = Map.of(
                AzureStorageUtils.OLD_ADLS_CREDENTIALS_SERVICE_DESCRIPTOR_NAME, AzureStorageUtils.ADLS_CREDENTIALS_SERVICE.getName(),
                AzureStorageUtils.OLD_FILESYSTEM_DESCRIPTOR_NAME, AzureStorageUtils.FILESYSTEM.getName(),
                AzureStorageUtils.OLD_DIRECTORY_DESCRIPTOR_NAME, AzureStorageUtils.DIRECTORY.getName(),
                "recurse-subdirectories", ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES.getName(),
                "file-filter", ListAzureDataLakeStorage.FILE_FILTER.getName(),
                "path-filter", ListAzureDataLakeStorage.PATH_FILTER.getName(),
                "include-temporary-files", ListAzureDataLakeStorage.INCLUDE_TEMPORARY_FILES.getName()
        );

        expectedRenamed.forEach((key, value) -> assertEquals(value, propertyMigrationResult.getPropertiesRenamed().get(key)));
    }
}
