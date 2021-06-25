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
package org.apache.nifi.registry.db;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.db.migration.BucketEntityV1;
import org.apache.nifi.registry.db.migration.FlowEntityV1;
import org.apache.nifi.registry.db.migration.FlowSnapshotEntityV1;
import org.apache.nifi.registry.db.migration.LegacyDataSourceFactory;
import org.apache.nifi.registry.db.migration.LegacyDatabaseService;
import org.apache.nifi.registry.db.migration.LegacyEntityMapper;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.service.MetadataService;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Custom Flyway migration strategy that lets us perform data migration from the original database used in the
 * 0.1.0 release, to the new database. The data migration will be triggered when it is determined that new database
 * is brand new AND the legacy DB properties are specified. If the primary database already contains the 'BUCKET' table,
 * or if the legacy database properties are not specified, then no data migration is performed.
 */
@Component
public class CustomFlywayMigrationStrategy implements FlywayMigrationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomFlywayMigrationStrategy.class);

    private NiFiRegistryProperties properties;

    @Autowired
    public CustomFlywayMigrationStrategy(final NiFiRegistryProperties properties) {
        this.properties = properties;
    }

    @Override
    public void migrate(Flyway flyway) {
        final boolean newDatabase = isNewDatabase(flyway.getConfiguration().getDataSource());
        if (newDatabase) {
            LOGGER.info("First time initializing database...");
        } else {
            LOGGER.info("Found existing database...");
        }

        boolean existingLegacyDatabase = false;
        if (!StringUtils.isBlank(properties.getLegacyDatabaseDirectory())) {
            LOGGER.info("Found legacy database properties...");

            final File legacyDatabaseFile = new File(properties.getLegacyDatabaseDirectory(), "nifi-registry.mv.db");
            if (legacyDatabaseFile.exists()) {
                LOGGER.info("Found legacy database file...");
                existingLegacyDatabase = true;
            } else {
                LOGGER.info("Did not find legacy database file...");
                existingLegacyDatabase = false;
            }
        }

        // If newDatabase is true, then we need to run the Flyway migration first to create all the tables, then the data migration
        // If newDatabase is false, then we need to run the Flyway migration to run any schema updates, but no data migration

        flyway.migrate();

        if (newDatabase && existingLegacyDatabase) {
            final LegacyDataSourceFactory legacyDataSourceFactory = new LegacyDataSourceFactory(properties);
            final DataSource legacyDataSource = legacyDataSourceFactory.getDataSource();
            final DataSource primaryDataSource = flyway.getConfiguration().getDataSource();
            migrateData(legacyDataSource, primaryDataSource);
        }
    }

    /**
     * Determines if the database represented by this data source is being initialized for the first time based on
     * whether or not the table named 'BUCKET' or 'bucket' already exists.
     *
     * @param dataSource the data source
     * @return true if the database has never been initialized before, false otherwise
     */
    private boolean isNewDatabase(final DataSource dataSource) {
        try (final Connection connection = dataSource.getConnection();
             final ResultSet rsUpper = connection.getMetaData().getTables(null, null, "BUCKET", null);
             final ResultSet rsLower = connection.getMetaData().getTables(null, null, "bucket", null)) {
            return !rsUpper.next() && !rsLower.next();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new FlywayException("Unable to obtain connection from Flyway DataSource", e);
        }
    }

    /**
     * Transfers all data from the source to the destination.
     *
     * @param source the legacy H2 DataSource
     * @param dest the new destination DataSource
     */
    private void migrateData(final DataSource source, final DataSource dest) {
        final LegacyDatabaseService legacyDatabaseService = new LegacyDatabaseService(source);

        final JdbcTemplate destJdbcTemplate = new JdbcTemplate(dest);
        final MetadataService destMetadataService = new DatabaseMetadataService(destJdbcTemplate);

        LOGGER.info("Migrating data from legacy database to new new database...");

        // Migrate buckets
        final List<BucketEntityV1> sourceBuckets = legacyDatabaseService.getAllBuckets();
        LOGGER.info("Migrating {} buckets..", new Object[]{sourceBuckets.size()});

        sourceBuckets.stream()
                .map(b -> LegacyEntityMapper.createBucketEntity(b))
                .forEach(b -> destMetadataService.createBucket(b));

        // Migrate flows
        final List<FlowEntityV1> sourceFlows = legacyDatabaseService.getAllFlows();
        LOGGER.info("Migrating {} flows..", new Object[]{sourceFlows.size()});

        sourceFlows.stream()
                .map(f -> LegacyEntityMapper.createFlowEntity(f))
                .forEach(f -> destMetadataService.createFlow(f));

        // Migrate flow snapshots
        final List<FlowSnapshotEntityV1> sourceSnapshots = legacyDatabaseService.getAllFlowSnapshots();
        LOGGER.info("Migrating {} flow snapshots..", new Object[]{sourceSnapshots.size()});

        sourceSnapshots.stream()
                .map(fs -> LegacyEntityMapper.createFlowSnapshotEntity(fs))
                .forEach(fs -> destMetadataService.createFlowSnapshot(fs));

        LOGGER.info("Data migration complete!");
    }

}
