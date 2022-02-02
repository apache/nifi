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

import org.h2.jdbcx.JdbcDataSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class H2DatabaseMigratorProcess {

    public static final String EXPORT_FILE_PREFIX = "export_";
    public static final String EXPORT_FILE_POSTFIX = ".sql";
    public static final String BACKUP_FILE_POSTFIX = ".migration_backup";

    public static void main(String[] args) {
        if (args.length < 4) {
            // Write error to file
            StringBuilder errorMsg = new StringBuilder("Migration tool requires 4 arguments, the JDBC URL to the H2 database, the path to the database file, "
                    + "and the username and password for the database.\n");
            if (args.length == 0) {
                errorMsg.append("No arguments given");
            } else {
                for (int i = 1; i < args.length; i++) {
                    errorMsg.append("arg[");
                    errorMsg.append(i);
                    errorMsg.append("] = ");
                    errorMsg.append(args[i]);
                    errorMsg.append("\n");
                }
            }
            handleError(errorMsg.toString(), 10);
        }

        final String dbUrl = args[0];
        final String dbPath = args[1];
        final String user = args[2];
        final String pass = args[3];

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
            handleError("Could not connect to the database with the legacy driver, cause: " + sqle.getMessage() + ", SQL State = " + sqle.getSQLState(), 20);
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
            handleError("Export of the database with the legacy driver failed, cause: " + sqle.getMessage() + ", SQL State = " + sqle.getSQLState(), 30);
        }

        try {
            s.close();
            conn.close();
        } catch (SQLException se2) {
            // Ignore, all connections should be severed when the process exits
        }

        // Verify the export file exists
        if (!Files.exists(Paths.get(exportFile))) {
            handleError("Export of the database with the legacy driver failed, no export file was created", 40);
        }

        // Now that the export file exists, backup (rename) the DB files so the main process with the newer H2 driver can create and import the previous database
        File dbDir = new File(dbPath).getParentFile();
        File[] dbFiles = dbDir.listFiles((dir, name) -> !name.endsWith(EXPORT_FILE_POSTFIX) && name.startsWith(dbPathFile.getName()));
        if (dbFiles == null || dbFiles.length == 0) {
            handleError("Backing up the legacy database failed, no database files were found.", 50);
        }

        for (File dbFile : dbFiles) {
            File dbBackupFile = new File(dbFile.getAbsolutePath() + BACKUP_FILE_POSTFIX);
            if (!dbFile.renameTo(dbBackupFile)) {
                handleError("Backing up the legacy database failed, " + dbFile.getName() + " could not be renamed.", 60);
            }
        }

        // exit gracefully so the H2 migrator can proceed with rebuilding the database
    }

    private static void handleError(final String errorMsg, final int exitCode) {
        // Write the message to a file and exit with the given code
        try {
            Path path = Paths.get("./h2_migration_error.txt");
            byte[] errorMsgBytes = errorMsg.getBytes();
            Files.write(path, errorMsgBytes);
        } catch (IOException ioe) {
            // Not much we can do here, so just exit
        }
        System.exit(exitCode);
    }
}
