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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * H2 Database Exporter responsible for writing database contents and creating a backup
 */
class DatabaseExporter {

    private static final String URL_FORMAT = "jdbc:h2:%s;LOCK_MODE=3";

    private static final String DATABASE_FILE_FORMAT = "%s.mv.db";

    private static final String BACKUP_DATABASE_FILE_FORMAT = "%s.backup";

    private static final String REPACKAGED_DATABASE_FORMAT = "repackaged-%s";

    private static final String EXPORT_SCRIPT_NAME_FORMAT = "export-%s.sql";

    private static final String SCRIPT_TO_FORMAT = "SCRIPT TO '%s'";

    private static final Logger logger = LoggerFactory.getLogger(DatabaseExporter.class);

    /**
     * Run database export to script SQL and move database file to backup location
     *
     * @param databaseVersion H2 Source Database Version
     * @param user Username
     * @param pass Password
     * @param databaseLocation Absolute Path to Database Location without file extension
     * @return Exported Database SQL Script Path
     */
    static Path runExportBackup(final DatabaseVersion databaseVersion, final String user, final String pass, final String databaseLocation) {
        final String databaseFileLocation = String.format(DATABASE_FILE_FORMAT, databaseLocation);
        final Path databaseFilePath = Paths.get(databaseFileLocation);
        final Path databasePath = Paths.get(databaseLocation);

        final Path exportDatabaseScriptPath;
        if (databaseVersion.isRepackagingRequired()) {
            final Path repackagedDatabasePath = getRepackagedDatabasePath(databaseVersion, databaseFilePath, databasePath);
            exportDatabaseScriptPath = exportDatabaseScript(databaseVersion, repackagedDatabasePath, user, pass);

            final Path repackagedDatabaseFilePath = Paths.get(String.format(DATABASE_FILE_FORMAT, repackagedDatabasePath));
            try {
                Files.delete(repackagedDatabaseFilePath);
            } catch (final IOException e) {
                logger.warn("H2 DB {} delete repackaged database [{}] failed", databaseVersion.getVersion(), repackagedDatabaseFilePath, e);
            }
        } else {
            exportDatabaseScriptPath = exportDatabaseScript(databaseVersion, databasePath, user, pass);
        }

        if (Files.isReadable(exportDatabaseScriptPath)) {
            createDatabaseBackup(databaseFilePath);
        } else {
            final String message = String.format("H2 DB [%s] export failed: script path [%s] not found", databaseLocation, exportDatabaseScriptPath);
            throw new IllegalStateException(message);
        }

        return exportDatabaseScriptPath;
    }

    private static Path getRepackagedDatabasePath(final DatabaseVersion databaseVersion, final Path databaseFilePath, final Path databasePath) {
        final String repackagedDatabaseFileName = String.format(REPACKAGED_DATABASE_FORMAT, databaseFilePath.getFileName());
        final Path repackagedDatabaseFilePath = databaseFilePath.resolveSibling(repackagedDatabaseFileName);

        DatabaseRepackager.repackageDatabaseFile(databaseVersion, databaseFilePath, repackagedDatabaseFilePath);

        final String repackagedDatabaseName = String.format(REPACKAGED_DATABASE_FORMAT, databasePath.getFileName());
        return databasePath.resolveSibling(repackagedDatabaseName);
    }

    private static Path exportDatabaseScript(final DatabaseVersion databaseVersion, final Path databasePath, final String user, final String password) {
        final Path databaseName = databasePath.getFileName();

        final String exportScriptName = String.format(EXPORT_SCRIPT_NAME_FORMAT, databaseName);
        final Path exportScriptPath = databasePath.resolveSibling(exportScriptName);

        final String scriptToCommand = String.format(SCRIPT_TO_FORMAT, exportScriptPath);
        final String url = String.format(URL_FORMAT, databasePath);

        try {
            DatabaseStatementRunner.run(databaseVersion, url, user, password, scriptToCommand);
        } catch (final SQLException e) {
            throw new IllegalStateException(String.format("H2 DB %s export script failed [%s]", databaseVersion.getVersion(), scriptToCommand), e);
        }

        return exportScriptPath;
    }

    private static void createDatabaseBackup(final Path databaseFilePath) {
        final String backupDatabaseFileLocation = String.format(BACKUP_DATABASE_FILE_FORMAT, databaseFilePath);
        final Path backupDatabaseFilePath = Paths.get(backupDatabaseFileLocation);

        try {
            Files.move(databaseFilePath, backupDatabaseFilePath, REPLACE_EXISTING);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("H2 DB [%s] migration backup failed", databaseFilePath), e);
        }
    }
}
