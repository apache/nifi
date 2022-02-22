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
package org.apache.nifi.dbmigration.h2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestH2DatabaseUpdater {

    public static final String AUDIT_DB_PATH = "./src/test/resources/nifi-flow-audit";
    public static final String AUDIT_DB_PATH_FILE = AUDIT_DB_PATH + ".mv.db";
    public static final String AUDIT_DB_PATH_FILE_ORIGINAL = "./src/test/resources/orig.nifi-flow-audit";

    @Test
    public void testMigration() throws Exception {
        final File dbFile = new File(AUDIT_DB_PATH);
        final String migrationDbUrl = H2DatabaseUpdater.H2_URL_PREFIX + dbFile + ";LOCK_MODE=3";

        final File newDbFile = new File(AUDIT_DB_PATH_FILE);
        H2DatabaseUpdater.checkAndPerformMigration(dbFile, migrationDbUrl, "nf", "nf");

        // Verify the export, backup, and new database files were created
        final File exportFile = Paths.get(dbFile.getParent(), H2DatabaseUpdater.EXPORT_FILE_PREFIX + dbFile.getName() + H2DatabaseUpdater.EXPORT_FILE_POSTFIX).toFile();
        assertTrue(exportFile.exists());

        File dbDir = dbFile.getParentFile();
        File[] backupFiles = dbDir.listFiles((dir, name) -> name.endsWith(H2DatabaseMigrator.BACKUP_FILE_POSTFIX) && name.startsWith(dbFile.getName()));
        try {
            assertNotNull(backupFiles);

            // The database and its trace file should exist after the initial connection is made, so they both should be backed up
            assertEquals(2, backupFiles.length);
            assertTrue(newDbFile.exists());
        } finally {
            // Remove the export and backup files
            exportFile.delete();
            for (File f : backupFiles) {
                f.delete();
            }
        }
    }

    @AfterEach
    public void restoreOriginalDatabases() throws Exception {
        Files.copy(Paths.get(AUDIT_DB_PATH_FILE_ORIGINAL), Paths.get(AUDIT_DB_PATH_FILE), REPLACE_EXISTING);
    }

}