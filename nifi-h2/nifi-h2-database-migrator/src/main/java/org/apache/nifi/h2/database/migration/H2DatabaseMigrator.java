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

import org.apache.nifi.org.h2.jdbcx.JdbcDataSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.nifi.h2.database.migration.H2DatabaseUpdater.EXPORT_FILE_POSTFIX;
import static org.apache.nifi.h2.database.migration.H2DatabaseUpdater.EXPORT_FILE_PREFIX;

public class H2DatabaseMigrator {
    public static final String BACKUP_FILE_POSTFIX = ".migration_backup";

    public static void exportAndBackup(final String dbUrl, final String dbPath, final String user, final String pass) {

        // Attempt to connect with the latest driver
        JdbcDataSource migrationDataSource = new JdbcDataSource();
        migrationDataSource.setURL(dbUrl);
        migrationDataSource.setUser(user);
        migrationDataSource.setPassword(pass);

        final File dbPathFile = Paths.get(dbPath).toFile();

        String exportFile = Paths.get(dbPathFile.getParent(), EXPORT_FILE_PREFIX + dbPathFile.getName() + EXPORT_FILE_POSTFIX).toString();

        Connection conn;
        Statement s;
        try {
            conn = migrationDataSource.getConnection();
            s = conn.createStatement();
        } catch (SQLException sqle) {
            final String message = String.format("H2 1.4 connection failed URL [%s] Path [%s] SQL State [%s]", dbUrl, dbPath, sqle.getSQLState());
            throw new RuntimeException(message, sqle);
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
            final String message = String.format("H2 1.4 export failed URL [%s] Path [%s] SQL State [%s]", dbUrl, dbPath, sqle.getSQLState());
            throw new RuntimeException(message, sqle);
        }

        closeQuietly(s);
        closeQuietly(conn);

        // Verify the export file exists
        if (!Files.exists(Paths.get(exportFile))) {
            throw new RuntimeException(String.format("H2 1.4 export failed URL [%s] Path [%s] Export File not found [%s]", dbUrl, dbPath, exportFile));
        }

        // Now that the export file exists, backup (rename) the DB files so the main process with the newer H2 driver can create and import the previous database
        File dbDir = new File(dbPath).getParentFile();
        File[] dbFiles = dbDir.listFiles((dir, name) -> !name.endsWith(EXPORT_FILE_POSTFIX) && name.startsWith(dbPathFile.getName()));
        if (dbFiles == null || dbFiles.length == 0) {
            throw new RuntimeException(String.format("H2 1.4 backup failed URL [%s] Path [%s] no database files found", dbUrl, dbPath));
        }

        for (File dbFile : dbFiles) {
            File dbBackupFile = new File(dbFile.getAbsolutePath() + BACKUP_FILE_POSTFIX);
            if (!dbFile.renameTo(dbBackupFile)) {
                throw new RuntimeException(String.format("H2 1.4 backup failed URL [%s] Path [%s] rename failed [%s]", dbUrl, dbPath, dbFile));
            }
        }
    }

    private static void closeQuietly(final Statement statement) {
        try {
            statement.close();
        } catch (final SQLException e) {
            // Ignore, nothing to be done
        }
    }

    private static void closeQuietly(final Connection connection) {
        try {
            connection.close();
        } catch (final SQLException e) {
            // Ignore, nothing to be done
        }
    }
}
