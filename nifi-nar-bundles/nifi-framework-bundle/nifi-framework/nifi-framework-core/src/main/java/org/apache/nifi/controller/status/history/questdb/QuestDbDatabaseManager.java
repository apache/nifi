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
package org.apache.nifi.controller.status.history.questdb;

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.apache.nifi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The database manager is responsible for checking and maintaining the health of the database during startup.
 */
public final class QuestDbDatabaseManager {
    private enum DatabaseStatus {
        HEALTHY, NON_EXISTING, CORRUPTED;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QuestDbDatabaseManager.class);
    private static final Set<String> COMPONENT_TABLES = new HashSet<>();
    private static final Set<String> NODE_TABLES = new HashSet<>();

    static {
        COMPONENT_TABLES.add("componentCounter");
        COMPONENT_TABLES.add("connectionStatus");
        COMPONENT_TABLES.add("processGroupStatus");
        COMPONENT_TABLES.add("remoteProcessGroupStatus");
        COMPONENT_TABLES.add("processorStatus");

        NODE_TABLES.add("nodeStatus");
        NODE_TABLES.add("garbageCollectionStatus");
        NODE_TABLES.add("storageStatus");
    }

    private QuestDbDatabaseManager() {
        // Should not be instantiated.
    }

    public static void checkDatabaseStatus(final Path persistLocation) {
        final QuestDbDatabaseManager.DatabaseStatus databaseStatus = getDatabaseStatus(persistLocation);
        LOGGER.debug("Starting status repository. It's estimated status is {}", databaseStatus);

        if (databaseStatus == QuestDbDatabaseManager.DatabaseStatus.NON_EXISTING) {
            createDatabase(persistLocation);
        } else if (databaseStatus == QuestDbDatabaseManager.DatabaseStatus.CORRUPTED) {
            throw new RuntimeException("The database is corrupted. The expected set of tables is not matching with the reachable tables.");
        }
    }

    private static DatabaseStatus getDatabaseStatus(final Path persistLocation) {

        if (!checkPersistentLocationExists(persistLocation)) {
            return DatabaseStatus.NON_EXISTING;
        }

        if (checkPersistentLocationExists(persistLocation) && checkPersistentLocationIsEmpty(persistLocation)) {
            return DatabaseStatus.NON_EXISTING;
        }

        if (!checkTablesAreInPlace(persistLocation) || !checkConnection(persistLocation)) {
            return DatabaseStatus.CORRUPTED;
        }

        return DatabaseStatus.HEALTHY;
    }

    private static boolean checkPersistentLocationExists(final Path persistLocation) {
        final File persistLocationDirectory = persistLocation.toFile();
        return persistLocationDirectory.exists() && persistLocationDirectory.isDirectory();
    }

    private static boolean checkPersistentLocationIsEmpty(final Path persistLocation) {
        final File persistLocationDirectory = persistLocation.toFile();
        return persistLocationDirectory.list().length == 0;
    }

    private static boolean checkTablesAreInPlace(final Path persistLocation) {
        final File persistLocationDirectory = persistLocation.toFile();
        final Map<String, File> databaseFiles = Arrays.stream(persistLocationDirectory.listFiles())
                .collect(Collectors.toMap(f -> f.getAbsolutePath().substring(persistLocationDirectory.getAbsolutePath().length() + 1), f -> f));

        final Set<String> expectedTables = new HashSet<>();
        expectedTables.addAll(NODE_TABLES);
        expectedTables.addAll(COMPONENT_TABLES);

        for (final String expectedTable : expectedTables) {
            if (!databaseFiles.containsKey(expectedTable) || !databaseFiles.get(expectedTable).isDirectory()) {
                LOGGER.error("Missing table during database status check: ", expectedTable);
                return false;
            }
        }

        return true;
    }

    private static boolean checkConnection(final Path persistLocation) {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(persistLocation.toFile().getAbsolutePath());

        try (
            final CairoEngine engine = new CairoEngine(configuration);
        ) {
            LOGGER.info("Connection to database was successful");
            return true;
        } catch (Exception e) {
            LOGGER.error("Error during connection to database", e);
            return false;
        }
    }

    private static void createDatabase(final Path persistLocation) {
        LOGGER.info("Creating database");
        final CairoConfiguration configuration;

        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(persistLocation.toFile());
        } catch (final Exception e) {
            throw new RuntimeException("Could not create database folder " + persistLocation.toAbsolutePath().toString(), e);
        }

        configuration = new DefaultCairoConfiguration(persistLocation.toFile().getAbsolutePath());

        try (
            final CairoEngine engine = new CairoEngine(configuration);
            final SqlCompiler compiler = new SqlCompiler(engine);
        ) {
            final SqlExecutionContext context = new SqlExecutionContextImpl(engine.getConfiguration(), new MessageBusImpl(), 1);

            // Node status tables
            compiler.compile(QuestDbQueries.CREATE_GARBAGE_COLLECTION_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_NODE_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_STORAGE_STATUS, context);

            // Component status tables
            compiler.compile(QuestDbQueries.CREATE_CONNECTION_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_PROCESS_GROUP_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_REMOTE_PROCESS_GROUP_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_PROCESSOR_STATUS, context);
            compiler.compile(QuestDbQueries.CREATE_COMPONENT_COUNTER, context);

            LOGGER.info("Database is created");
        } catch (final Exception e) {
            throw new RuntimeException("Could not create database!", e);
        }
    }

    public static Set<String> getNodeTableNames() {
        return NODE_TABLES;
    }

    public static Set<String> getComponentTableNames() {
        return COMPONENT_TABLES;
    }
}
