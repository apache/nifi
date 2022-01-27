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

import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class H2DatabaseUpdater {

    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseUpdater.class);

    public static final String EXPORT_FILE_PREFIX = "export_";
    public static final String EXPORT_FILE_POSTFIX = ".sql";
    public static final String H2_URL_PREFIX = "jdbc:h2:";

    public static void checkAndPerformMigration(final String dbPathNoExtension, final String dbUrl, final String user, final String pass) throws Exception {

        final JdbcDataSource migrationDataSource = new JdbcDataSource();

        // Attempt to connect with the latest driver
        migrationDataSource.setURL(dbUrl);
        migrationDataSource.setUser(user);
        migrationDataSource.setPassword(pass);
        try (Connection connection = migrationDataSource.getConnection()) {
            return;
        } catch (JdbcSQLNonTransientException jsqlnte) {
            // Migration/version issues will be caused by an MVStoreException
            final Throwable exceptionCause = jsqlnte.getCause();
            if (exceptionCause instanceof MVStoreException) {
                // Check for specific error message
                final String errorMessage = exceptionCause.getMessage();
                if (!errorMessage.contains("The write format")
                        && !errorMessage.contains("is smaller than the supported format")) {
                    throw jsqlnte;
                }
            }
        } catch (SQLException sqle) {
            throw new RuntimeException(String.format("H2 connection failed URL [%s] File [%s]", dbUrl, dbPathNoExtension), sqle);
        }
        // At this point it is known that migration should be attempted
        logger.info("H2 1.4 database detected [{}]: starting migration to H2 2.1", dbPathNoExtension);
        H2DatabaseMigrator.exportAndBackup(dbUrl, dbPathNoExtension, user, pass);

        // The export file has been created and the DB has been backed up, now create a new one with the same name and run the SQL script to import the database
        try (Connection migrationConnection = migrationDataSource.getConnection();
             Statement s = migrationConnection.createStatement()) {
            final Path dbFilePath = Paths.get(dbPathNoExtension);
            final String dbDirectory = dbFilePath.getParent().toFile().getAbsolutePath();
            // use RUNSCRIPT to recreate the database
            final String exportSqlLocation = dbDirectory + File.separator
                    + H2DatabaseUpdater.EXPORT_FILE_PREFIX + dbFilePath.toFile().getName() + H2DatabaseUpdater.EXPORT_FILE_POSTFIX;
            s.execute("RUNSCRIPT FROM '" + exportSqlLocation + "'");

        } catch (SQLException sqle) {
            throw new IOException(String.format("H2 import database creation failed URL [%s]", dbUrl), sqle);
        }

        logger.info("H2 1.4 to 2.1 migration completed [{}]", dbPathNoExtension);
    }
}
