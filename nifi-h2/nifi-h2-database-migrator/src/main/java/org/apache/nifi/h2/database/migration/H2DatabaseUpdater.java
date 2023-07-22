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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * H2 Database Updater responsible for evaluating current version and migrating when required
 */
public class H2DatabaseUpdater {
    public static final String H2_URL_PREFIX = "jdbc:h2:";

    private static final DatabaseVersion LATEST_VERSION = DatabaseVersion.VERSION_2_2;

    private static final String FILE_PATH_FORMAT = "%s.mv.db";

    private static final String RUNSCRIPT_FORMAT = "RUNSCRIPT FROM '%s'";

    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseUpdater.class);

    /**
     * Check H2 Database File status and perform migration when required
     *
     * @param dbPathNoExtension H2 Database File Path without mv.db extension
     * @param url H2 Database URL
     * @param user H2 Database User
     * @param pass H2 Database Password
     * @throws Exception Thrown on failures performing migration
     */
    public static void checkAndPerformMigration(final String dbPathNoExtension, final String url, final String user, final String pass) throws Exception {
        final Path databaseFilePath = Paths.get(String.format(FILE_PATH_FORMAT, dbPathNoExtension));
        final DatabaseVersion databaseVersion = DatabaseVersionReader.readDatabaseVersion(databaseFilePath);

        if (LATEST_VERSION == databaseVersion) {
            logger.debug("H2 DB migration not required from Driver {} for [{}]", databaseVersion.getVersion(), databaseFilePath);
        } else {
            logger.info("H2 DB migration required to Driver {} from Driver {} for [{}]", LATEST_VERSION.getVersion(), databaseVersion.getVersion(), databaseFilePath);

            final Path exportScriptPath = DatabaseExporter.runExportBackup(databaseVersion, user, pass, dbPathNoExtension);

            final String command = String.format(RUNSCRIPT_FORMAT, exportScriptPath);
            DatabaseStatementRunner.run(LATEST_VERSION, url, user, pass, command);

            logger.info("H2 DB migration completed to Driver {} from Driver {} for [{}]", LATEST_VERSION.getVersion(), databaseVersion.getVersion(), databaseFilePath);

            try {
                Files.delete(exportScriptPath);
            } catch (final IOException e) {
                logger.warn("H2 DB {} for [{}] delete export script failed [{}]", databaseVersion.getVersion(), databaseFilePath, exportScriptPath, e);
            }
        }
    }
}
