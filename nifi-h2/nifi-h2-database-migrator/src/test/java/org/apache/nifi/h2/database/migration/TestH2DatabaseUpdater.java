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
package org.apache.nifi.h2.database.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestH2DatabaseUpdater {

    public static final String DB_NAME = "nifi-flow-audit";
    public static final String MVDB_EXTENSION = ".mv.db";
    public static final String ORIGINAL_AUDIT_DB_PATH = "./src/test/resources/" + DB_NAME;
    public static final String ORIGINAL_AUDIT_DB_PATH_FILE = ORIGINAL_AUDIT_DB_PATH + MVDB_EXTENSION;

    @TempDir
    File tmpDir;

    @BeforeEach
    public void copyDatabaseFile() throws IOException {
        // Copy the legacy database file to a temporary directory
        final Path origAuditDbPath = Paths.get(ORIGINAL_AUDIT_DB_PATH_FILE);
        final Path destAuditDbPath = Paths.get(tmpDir.getAbsolutePath(), DB_NAME + MVDB_EXTENSION);
        Files.copy(origAuditDbPath, destAuditDbPath, REPLACE_EXISTING);
    }

    @Test
    public void testMigration() throws Exception {
        final Path testAuditDbPath = Paths.get(tmpDir.getAbsolutePath(), DB_NAME);
        final File dbFileNoExtension = testAuditDbPath.toFile();
        final String migrationDbUrl = H2DatabaseUpdater.H2_URL_PREFIX + dbFileNoExtension + ";LOCK_MODE=3";

        H2DatabaseUpdater.checkAndPerformMigration(dbFileNoExtension.getAbsolutePath(), migrationDbUrl, "nf", "nf");

        // Verify the export, backup, and new database files were created
        final File exportFile = Paths.get(dbFileNoExtension.getParent(), H2DatabaseUpdater.EXPORT_FILE_PREFIX + dbFileNoExtension.getName() + H2DatabaseUpdater.EXPORT_FILE_POSTFIX).toFile();
        assertTrue(exportFile.exists());

        File dbDir = dbFileNoExtension.getParentFile();
        File[] backupFiles = dbDir.listFiles((dir, name) -> name.endsWith(H2DatabaseMigrator.BACKUP_FILE_POSTFIX) && name.startsWith(dbFileNoExtension.getName()));
        try {
            assertNotNull(backupFiles);

            // The database and its trace file should exist after the initial connection is made, so they both should be backed up
            assertEquals(2, backupFiles.length);
            final File newDbFile = Paths.get(tmpDir.getAbsolutePath(), DB_NAME + MVDB_EXTENSION).toFile();
            assertTrue(newDbFile.exists());
        } finally {
            // Remove the export and backup files
            exportFile.delete();
            for (File f : backupFiles) {
                f.delete();
            }
        }
    }
}