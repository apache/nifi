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

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.h2.database.migration.H2DatabaseUpdater;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.io.File;

/**
 * Overriding Spring Boot's normal automatic creation of a DataSource in order to use the properties
 * from NiFiRegistryProperties rather than the standard application.properties/yaml.
 */
@Configuration
public class DataSourceFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceFactory.class);

    private final NiFiRegistryProperties properties;

    private DataSource dataSource;

    @Autowired
    public DataSourceFactory(final NiFiRegistryProperties properties) {
        this.properties = properties;
    }

    @Bean
    @Primary
    public DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = createDataSource();
        }

        return dataSource;
    }

    private DataSource createDataSource() {
        final String databaseUrl = properties.getDatabaseUrl();
        if (StringUtils.isBlank(databaseUrl)) {
            throw new IllegalStateException(NiFiRegistryProperties.DATABASE_URL + " is required");
        }

        final String databaseDriver = properties.getDatabaseDriverClassName();
        if (StringUtils.isBlank(databaseDriver)) {
            throw new IllegalStateException(NiFiRegistryProperties.DATABASE_DRIVER_CLASS_NAME + " is required");
        }

        final String databaseUsername = properties.getDatabaseUsername();
        if (StringUtils.isBlank(databaseUsername)) {
            throw new IllegalStateException(NiFiRegistryProperties.DATABASE_USERNAME + " is required");
        }

        String databasePassword = properties.getDatabasePassword();
        if (StringUtils.isBlank(databasePassword)) {
            throw new IllegalStateException(NiFiRegistryProperties.DATABASE_PASSWORD + " is required");
        }

        final DataSource dataSource = DataSourceBuilder
                .create()
                .url(databaseUrl)
                .driverClassName(databaseDriver)
                .username(databaseUsername)
                .password(databasePassword)
                .build();

        if (dataSource instanceof HikariDataSource) {
            LOGGER.info("Setting maximum pool size on HikariDataSource to {}", properties.getDatabaseMaxConnections());
            ((HikariDataSource) dataSource).setMaximumPoolSize(properties.getDatabaseMaxConnections());
        }

        // Migrate an H2 existing database if required
        if (databaseUrl.startsWith(H2DatabaseUpdater.H2_URL_PREFIX)) {
            final File databaseFile = getFileFromH2URL(databaseUrl);
            final String migrationDbUrl = H2DatabaseUpdater.H2_URL_PREFIX + databaseFile + ";LOCK_MODE=3";

            try {
                H2DatabaseUpdater.checkAndPerformMigration(databaseFile.getAbsolutePath(), migrationDbUrl, databaseUsername, databasePassword);
            } catch (Exception e) {
                throw new RuntimeException("H2 database migration failed", e);
            }
        }

        return dataSource;
    }

    private File getFileFromH2URL(final String databaseUrl) {
        final String dbPath = databaseUrl.substring(H2DatabaseUpdater.H2_URL_PREFIX.length(), databaseUrl.indexOf(";"));
        return new File(dbPath);
    }
}
