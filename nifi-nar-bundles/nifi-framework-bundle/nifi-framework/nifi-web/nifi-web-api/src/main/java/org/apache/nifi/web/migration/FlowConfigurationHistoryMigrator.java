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
package org.apache.nifi.web.migration;

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.impl.StandardAuditService;
import org.apache.nifi.admin.service.transaction.impl.StandardTransactionBuilder;
import org.apache.nifi.h2.database.migration.H2DatabaseUpdater;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

/**
 * Migrator for Flow Configuration History from H2 Database Engine to JetBrains Xodus
 */
public class FlowConfigurationHistoryMigrator {
    private static final String H2_DATABASE_URL_FORMAT = "%s%s;AUTOCOMMIT=OFF;DB_CLOSE_ON_EXIT=FALSE;LOCK_MODE=3";

    private static final String H2_DATABASE_NAME = "nifi-flow-audit";

    private static final String H2_DATABASE_FILE_NAME = String.format("%s.mv.db", H2_DATABASE_NAME);

    private static final String BACKUP_FILE_NAME_FORMAT = "%s.backup.%d";

    private static final String H2_CREDENTIALS = "nf";

    private static final int PAGE_COUNT = 1000;

    private static final Logger logger = LoggerFactory.getLogger(FlowConfigurationHistoryMigrator.class);

    /**
     * Reconcile Database Repository checks for an H2 database and migrates records to Xodus Persistent Entity Store
     *
     * @param databaseRepositoryPath Database Repository directory
     * @param auditService Audit Service destination for migrated records
     * @throws Exception Thrown on migration failures
     */
    public static void reconcileDatabaseRepository(final Path databaseRepositoryPath, final AuditService auditService) throws Exception {
        Objects.requireNonNull(databaseRepositoryPath, "Repository Path required");

        final Path databaseFilePath = databaseRepositoryPath.resolve(H2_DATABASE_FILE_NAME);
        if (Files.exists(databaseFilePath)) {
            logger.info("H2 DB migration for Flow Configuration History required for [{}]", databaseFilePath);
            final AuditService sourceAuditService = getSourceAuditSource(databaseFilePath);
            final int migrated = migrateActions(sourceAuditService, auditService);
            logger.info("H2 DB migration for Flow Configuration History completed for [{}] containing [{}] records", databaseFilePath, migrated);

            final String backupFileName = String.format(BACKUP_FILE_NAME_FORMAT, databaseFilePath.getFileName(), System.currentTimeMillis());
            final Path backupFilePath = databaseFilePath.resolveSibling(backupFileName);
            Files.move(databaseFilePath, backupFilePath);
            logger.info("H2 DB migration for Flow Configuration History moved [{}] to [{}]", databaseFilePath, backupFilePath);
        } else {
            logger.debug("H2 DB migration for Flow Configuration History not required");
        }
    }

    private static AuditService getSourceAuditSource(final Path databaseFilePath) throws Exception {
        final String databaseNamePath = databaseFilePath.resolveSibling(H2_DATABASE_NAME).toString();
        final String databaseUrl = String.format(H2_DATABASE_URL_FORMAT, H2DatabaseUpdater.H2_URL_PREFIX, databaseNamePath);

        H2DatabaseUpdater.checkAndPerformMigration(databaseNamePath, databaseUrl, H2_CREDENTIALS, H2_CREDENTIALS);
        final DataSource dataSource = JdbcConnectionPool.create(databaseUrl, H2_CREDENTIALS, H2_CREDENTIALS);

        final StandardTransactionBuilder transactionBuilder = new StandardTransactionBuilder();
        transactionBuilder.setDataSource(dataSource);

        final StandardAuditService auditService = new StandardAuditService();
        auditService.setTransactionBuilder(transactionBuilder);
        return auditService;
    }

    private static int migrateActions(final AuditService sourceAuditService, final AuditService auditService) {
        final HistoryQuery query = new HistoryQuery();
        query.setCount(PAGE_COUNT);
        query.setOffset(0);

        final History initialHistory = sourceAuditService.getActions(query);
        final int total = initialHistory.getTotal();
        int migrated = 0;

        Collection<Action> actions = initialHistory.getActions();
        while (migrated < total) {
            auditService.addActions(actions);
            migrated += actions.size();

            query.setOffset(query.getOffset() + PAGE_COUNT);
            final History history = sourceAuditService.getActions(query);
            actions = history.getActions();
        }

        return migrated;
    }
}
