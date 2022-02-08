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

import org.apache.nifi.org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.nifi.dbmigration.h2.H2DatabaseUpdater.EXPORT_FILE_POSTFIX;
import static org.apache.nifi.dbmigration.h2.H2DatabaseUpdater.EXPORT_FILE_PREFIX;

public class H2DatabaseMigrator {

    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseMigrator.class);

    public static final String BACKUP_FILE_POSTFIX = ".migration_backup";

    public static void exportAndBackup(final String dbUrl, final String dbPath, final String user, final String pass) {

        // Attempt to connect with the latest driver
        JdbcDataSource migrationDataSource = new JdbcDataSource();
        migrationDataSource.setURL(dbUrl);
        migrationDataSource.setUser(user);
        migrationDataSource.setPassword(pass);

        final File dbPathFile = Paths.get(dbPath).toFile();

        String exportFile = Paths.get(dbPathFile.getParent(), EXPORT_FILE_PREFIX + dbPathFile.getName() + EXPORT_FILE_POSTFIX).toString();

        Connection conn = null;
        Statement s = null;
        try {
            conn = migrationDataSource.getConnection();
            s = conn.createStatement();
        } catch (SQLException sqle) {
            logger.error("Could not connect to the database with the legacy driver, cause: " + sqle.getMessage() + ", SQL State = " + sqle.getSQLState());
            throw new RuntimeException(sqle);
        }

        try {
            // Perform an export of the database to SQL statements
            s.execute("SCRIPT TO '" + exportFile + "'");
        } catch (SQLException sqle) {
            try {
                s.close();
            } catch (SQLException se2) {
                // Ignore, the error will be handled
            }
            logger.error("Export of the database with the legacy driver failed, cause: " + sqle.getMessage() + ", SQL State = " + sqle.getSQLState());
            throw new RuntimeException(sqle);
        }

        try {
            s.close();
            conn.close();
        } catch (SQLException se2) {
            // Ignore, all connections should be severed when the process exits
        }

        // Verify the export file exists
        if (!Files.exists(Paths.get(exportFile))) {
            logger.error("Export of the database with the legacy driver failed, no export file was created");
            throw new RuntimeException("Export of the database with the legacy driver failed, no export file was created");
        }

        // Now that the export file exists, backup (rename) the DB files so the main process with the newer H2 driver can create and import the previous database
        File dbDir = new File(dbPath).getParentFile();
        File[] dbFiles = dbDir.listFiles((dir, name) -> !name.endsWith(EXPORT_FILE_POSTFIX) && name.startsWith(dbPathFile.getName()));
        if (dbFiles == null || dbFiles.length == 0) {
            logger.error("Backing up the legacy database failed, no database files were found.");
            throw new RuntimeException("Backing up the legacy database failed, no database files were found.");
        }

        for (File dbFile : dbFiles) {
            File dbBackupFile = new File(dbFile.getAbsolutePath() + BACKUP_FILE_POSTFIX);
            if (!dbFile.renameTo(dbBackupFile)) {
                logger.error("Backing up the legacy database failed, " + dbFile.getName() + " could not be renamed.");
                throw new RuntimeException("Backing up the legacy database failed, " + dbFile.getName() + " could not be renamed.");
            }
        }

        // exit gracefully so the H2 migrator can proceed with rebuilding the database
    }
}
