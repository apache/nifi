/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.dropbox;

import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDropboxProcessorMigration {

    @Test
    void testFetchDropboxMigration() {
        TestRunner runner = TestRunners.newTestRunner(FetchDropbox.class);
        final Map<String, String> expected = Map.ofEntries(
                Map.entry(DropboxTrait.OLD_CREDENTIAL_SERVICE_PROPERTY_NAME, DropboxTrait.CREDENTIAL_SERVICE.getName()),
                Map.entry("file", FetchDropbox.FILE.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    @Test
    void testPutDropboxMigration() {
        TestRunner runner = TestRunners.newTestRunner(PutDropbox.class);
        final Map<String, String> expected = Map.ofEntries(
                Map.entry(DropboxTrait.OLD_CREDENTIAL_SERVICE_PROPERTY_NAME, DropboxTrait.CREDENTIAL_SERVICE.getName()),
                Map.entry("folder", PutDropbox.FOLDER.getName()),
                Map.entry("file-name", PutDropbox.FILE_NAME.getName()),
                Map.entry("conflict-resolution-strategy", PutDropbox.CONFLICT_RESOLUTION.getName()),
                Map.entry("chunked-upload-size", PutDropbox.CHUNKED_UPLOAD_SIZE.getName()),
                Map.entry("chunked-upload-threshold", PutDropbox.CHUNKED_UPLOAD_THRESHOLD.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    @Test
    void testListDropboxMigration() {
        TestRunner runner = TestRunners.newTestRunner(ListDropbox.class);
        final Map<String, String> expected = Map.ofEntries(
                Map.entry("target-system-timestamp-precision", AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION.getName()),
                Map.entry("listing-strategy", AbstractListProcessor.LISTING_STRATEGY.getName()),
                Map.entry("record-writer", AbstractListProcessor.RECORD_WRITER.getName()),
                Map.entry(DropboxTrait.OLD_CREDENTIAL_SERVICE_PROPERTY_NAME, DropboxTrait.CREDENTIAL_SERVICE.getName()),
                Map.entry("folder", ListDropbox.FOLDER.getName()),
                Map.entry("recursive-search", ListDropbox.RECURSIVE_SEARCH.getName()),
                Map.entry("min-age", ListDropbox.MIN_AGE.getName()),
                Map.entry(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListedEntityTracker.TRACKING_STATE_CACHE.getName()),
                Map.entry(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListedEntityTracker.TRACKING_TIME_WINDOW.getName()),
                Map.entry(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListedEntityTracker.INITIAL_LISTING_TARGET.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of("Distributed Cache Service");
        assertEquals(expectedRemoved, propertyMigrationResult.getPropertiesRemoved());
    }
}
