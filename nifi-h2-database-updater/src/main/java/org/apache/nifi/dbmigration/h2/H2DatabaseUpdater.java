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

import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class H2DatabaseUpdater {

    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseUpdater.class);

    private static final String H2_MIGRATION_DIRECTORY = "./h2_migration";
    private static final String DEFAULT_JAVA_CMD = "java";

    public static final String EXPORT_FILE_PREFIX = "export_";
    public static final String EXPORT_FILE_POSTFIX = ".sql";
    public static final String H2_URL_PREFIX = "jdbc:h2:";

    private static final JdbcDataSource migrationDataSource = new JdbcDataSource();

    public static void checkAndPerformMigration(final File dbFile, final String dbUrl, final String user, final String pass, final String libraryClasspath, final String javaPath) throws Exception {
        try {
            // Attempt to connect with the latest driver
            migrationDataSource.setURL(dbUrl);
            migrationDataSource.setUser(user);
            migrationDataSource.setPassword(pass);
            migrationDataSource.getConnection();
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
            throw new RuntimeException("Error getting connection to H2 database unrelated to migration issues", sqle);
        }
        // At this point it is known that migration should be attempted
        executeMigratorProcess(new File(libraryClasspath + File.separatorChar + H2_MIGRATION_DIRECTORY), javaPath, dbUrl, dbFile, user, pass);

        // The export file has been created and the DB has been backed up, now create a new one with the same name and run the SQL script to import the database
        try (Connection migrationConnection = migrationDataSource.getConnection();
             Statement s = migrationConnection.createStatement()) {
            // use RUNSCRIPT to recreate the database
            final String exportSqlLocation = dbFile.getParentFile().getAbsolutePath() + File.separator
                    + H2DatabaseUpdater.EXPORT_FILE_PREFIX + dbFile.getName() + H2DatabaseUpdater.EXPORT_FILE_POSTFIX;
            s.execute("RUNSCRIPT FROM '" + exportSqlLocation + "'");

        } catch (SQLException sqle) {
            throw new IOException("Error creating database for import", sqle);
        }

        logger.info("H2 Database Migration process for " + dbFile.getName() + " completed successfully");
    }

    private static void executeMigratorProcess(final File libDir, final String javaPath, final String dbUrl, final File dbFile, final String user, final String pass) throws IOException {

        final File[] libFiles = libDir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(".jar"));

        if (libFiles == null || libFiles.length == 0) {
            throw new RuntimeException("Could not find H2 migration library directory at " + libDir.getAbsolutePath());
        }

        final StringBuilder classPathBuilder = new StringBuilder();
        for (int i = 0; i < libFiles.length; i++) {
            final String filename = libFiles[i].getAbsolutePath();
            classPathBuilder.append(filename);
            if (i < libFiles.length - 1) {
                classPathBuilder.append(File.pathSeparatorChar);
            }
        }

        final String classPath = classPathBuilder.toString();
        String javaCmd = javaPath;
        if (javaCmd == null) {
            javaCmd = DEFAULT_JAVA_CMD;
        }
        if (javaCmd.equals(DEFAULT_JAVA_CMD)) {
            String javaHome = System.getenv("JAVA_HOME");
            if (javaHome != null) {
                String fileExtension = isWindows() ? ".exe" : "";
                File javaFile = new File(javaHome + File.separatorChar + "bin"
                        + File.separatorChar + "java" + fileExtension);
                if (javaFile.exists() && javaFile.canExecute()) {
                    javaCmd = javaFile.getAbsolutePath();
                }
            }
        }

        final List<String> cmd = new ArrayList<>();

        cmd.add(javaCmd);
        //cmd.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000");  // For debugging
        cmd.add("-classpath");
        cmd.add(classPath);
        cmd.add("org.apache.nifi.dbmigration.h2.H2DatabaseMigratorProcess");
        cmd.add(dbUrl);
        cmd.add(dbFile.getAbsolutePath());
        cmd.add(user);
        cmd.add(pass);

        final ProcessBuilder builder = new ProcessBuilder();
        builder.command(cmd);

        logger.info("Starting H2 Database Migration process for " + dbFile.getName());
        //final String cmdBuilder = String.join(" ", cmd);
        //logger.info("Command: {}", cmdBuilder);  // exposes the password, should only be used for debugging

        Process process = builder.start();
        try {
            final int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("H2 Migrator process failed with exit code " + exitCode);
            }
        } catch (InterruptedException ie) {
            throw new IOException("Migration process interrupted", ie);
        }
    }

    private static boolean isWindows() {
        final String osName = System.getProperty("os.name");
        return osName != null && osName.toLowerCase().contains("win");
    }
}
