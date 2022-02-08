package org.apache.nifi.dbmigration.h2;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class TestH2DatabaseUpdater {

    @Test
    public void testMigration() throws Exception {
        final String dbPathNoExtension = "./src/test/resources/nifi-flow-audit";
        final String dbFilePath = dbPathNoExtension + ".mv.db";
        final File dbFile = new File(dbPathNoExtension);
        final String migrationDbUrl = H2DatabaseUpdater.H2_URL_PREFIX + dbFile + ";LOCK_MODE=3";

        final File newDbFile = new File(dbFilePath);
        H2DatabaseUpdater.checkAndPerformMigration(dbFile, migrationDbUrl, "nf", "nf");

        // Verify the export, backup, and new database files were created
        final File exportFile = Paths.get(dbFile.getParent(), H2DatabaseUpdater.EXPORT_FILE_PREFIX + dbFile.getName() + H2DatabaseUpdater.EXPORT_FILE_POSTFIX).toFile();
        assertTrue(exportFile.exists());

        File dbDir = dbFile.getParentFile();
        File[] backupFiles = dbDir.listFiles((dir, name) -> name.endsWith(H2DatabaseMigrator.BACKUP_FILE_POSTFIX) && name.startsWith(dbFile.getName()));
        assertNotNull(backupFiles);
        // The database and its trace file should exist after the initial connection is made, so they both should be backed up
        assertEquals(2, backupFiles.length);
        assertTrue(newDbFile.exists());

        // Remove the export and backup files
        exportFile.delete();
        for (File f : backupFiles) {
            f.delete();
        }
    }

}