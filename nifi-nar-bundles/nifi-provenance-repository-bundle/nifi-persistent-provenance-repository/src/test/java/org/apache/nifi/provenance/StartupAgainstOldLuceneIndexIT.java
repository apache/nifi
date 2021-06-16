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
package org.apache.nifi.provenance;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.events.EventReporter;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * With NiFi 1.10.0 (?) we changed from Lucene 4.x to Lucene 8.x
 * This test is intended to ensure that we can properly startup even when pointing to a Provenance
 * Repository that was created against the old Lucene.
 */
public class StartupAgainstOldLuceneIndexIT {

    @Test(timeout = 30000)
    public void testStartup() throws IOException, InterruptedException {
        // Test startup with old lucene 4 index directory and no temp or migrated directory.
        testStartup(false, false);

        // Test startup with old lucene 4 index directory and temp directory.
        testStartup(true, false);

        // Test startup with old lucene 4 index directory, temp directory, and final migrated directory.
        testStartup(true, true);
    }

    private void testStartup(final boolean createTempDirectory, final boolean createMigratedDirectory) throws IOException, InterruptedException {
        final File existingRepo = new File("src/test/resources/lucene-4-prov-repo");
        final File tempDir = new File("target/" + UUID.randomUUID().toString());

        copy(existingRepo, tempDir);
        final File oldIndexDir = new File(tempDir, "index-1554304717707");
        assertTrue(oldIndexDir.exists());

        if (createTempDirectory) {
            final File tempIndexDir = new File(tempDir, "temp-lucene-8-index-1554304717707");
            assertTrue(tempIndexDir.mkdirs());

            final File dummyFile = new File(tempIndexDir, "_0.fdt");
            try (final OutputStream fos = new FileOutputStream(dummyFile)) {
                fos.write("hello world".getBytes());
            }
        }

        if (createMigratedDirectory) {
            final File migratedDirectory = new File(tempDir, "lucene-8-index-1554304717707");
            assertTrue(migratedDirectory.mkdirs());

            final File dummyFile = new File(migratedDirectory, "_0.fdt");
            try (final OutputStream fos = new FileOutputStream(dummyFile)) {
                fos.write("hello world".getBytes());
            }
        }

        final RepositoryConfiguration repoConfig = new RepositoryConfiguration();
        repoConfig.addStorageDirectory("1", tempDir);
        repoConfig.setSearchableFields(Arrays.asList(SearchableFields.FlowFileUUID, SearchableFields.Filename, SearchableFields.EventTime, SearchableFields.EventType));

        final WriteAheadProvenanceRepository writeAheadRepo = new WriteAheadProvenanceRepository(repoConfig);
        final Authorizer authorizer = Mockito.mock(Authorizer.class);
        writeAheadRepo.initialize(EventReporter.NO_OP, authorizer, Mockito.mock(ProvenanceAuthorizableFactory.class), Mockito.mock(IdentifierLookup.class));

        final ProvenanceEventRecord event = TestUtil.createEvent();
        writeAheadRepo.registerEvents(Collections.singleton(event));

        while (oldIndexDir.exists()) {
            Thread.sleep(5L);
        }

        assertFalse(oldIndexDir.exists());

        final File newIndexDir = new File(tempDir, "lucene-8-index-1554304717707");
        while (!newIndexDir.exists()) {
            Thread.sleep(5L);
        }

        assertTrue(newIndexDir.exists());
    }


    private void copy(final File from, final File to) throws IOException {
        if (from.isFile()) {
            Files.copy(from.toPath(), to.toPath());
            return;
        }

        to.mkdirs();

        final File[] children = from.listFiles();
        for (final File child : children) {
            final File destination = new File(to, child.getName());
            copy(child, destination);
        }
    }
}
