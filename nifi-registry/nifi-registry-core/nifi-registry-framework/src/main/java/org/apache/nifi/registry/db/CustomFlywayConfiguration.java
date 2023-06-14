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

import static org.apache.nifi.registry.db.DataSourceUtils.getDatabaseType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.DataSource;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.database.DatabaseType;
import org.flywaydb.core.internal.database.oracle.OracleDatabaseType;
import org.flywaydb.core.internal.database.postgresql.PostgreSQLDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcUtils;
import org.flywaydb.database.mysql.MySQLDatabaseType;
import org.flywaydb.database.mysql.mariadb.MariaDBDatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomFlywayConfiguration implements FlywayConfigurationCustomizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomFlywayConfiguration.class);

    private static final String LOCATION_COMMON = "classpath:db/migration/common";

    private static final String LOCATION_DEFAULT = "classpath:db/migration/default";
    private static final String[] LOCATIONS_DEFAULT = {LOCATION_COMMON, LOCATION_DEFAULT};

    private static final String LOCATION_MYSQL = "classpath:db/migration/mysql";
    private static final String[] LOCATIONS_MYSQL = {LOCATION_COMMON, LOCATION_MYSQL};

    private static final String LOCATION_POSTGRES = "classpath:db/migration/postgres";
    private static final String[] LOCATIONS_POSTGRES = {LOCATION_COMMON, LOCATION_POSTGRES};

    private static final String LOCATION_ORACLE = "classpath:db/migration/oracle";
    private static final String[] LOCATIONS_ORACLE = {LOCATION_COMMON, LOCATION_ORACLE};

    private static final String LEGACY_FLYWAY_SCHEMA_TABLE = "schema_version";

    @Override
    public void customize(final FluentConfiguration configuration) {
        final DatabaseType databaseType;
        try {
            databaseType = getDatabaseType(configuration.getDataSource());
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new FlywayException("Unable to obtain connection from Flyway DataSource", e);
        }
        LOGGER.info("Determined database type is {}", databaseType.getName());

        final String[] locations = getDatabaseTypeLocations(databaseType);
        LOGGER.info("Setting migration locations to {}", Arrays.asList(locations));
        configuration.locations(locations);

        // At some point Flyway changed their default table name: https://github.com/flyway/flyway/issues/1848
        // So we need to determine if we are upgrading from an existing nifi registry that is using the older
        // name, and if so then continue using that name, otherwise use the new default name
        if (isLegacyFlywaySchemaTable(configuration.getDataSource())) {
            LOGGER.info("Using legacy Flyway configuration table - {}", LEGACY_FLYWAY_SCHEMA_TABLE);
            configuration.table(LEGACY_FLYWAY_SCHEMA_TABLE);
        } else {
            LOGGER.info("Using default Flyway configuration table");
        }
    }

    /**
     * Determines if the legacy flyway schema table exists.
     *
     * @param dataSource the data source
     * @return true if the legacy schema tables exists, false otherwise
     */
    private boolean isLegacyFlywaySchemaTable(final DataSource dataSource) {
        try (final Connection connection = dataSource.getConnection()) {
            final DatabaseMetaData databaseMetaData = JdbcUtils.getDatabaseMetaData(connection);

            try (final ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
                while (resultSet.next()) {
                    final String table = resultSet.getString(3);
                    LOGGER.trace("Found table {}", table);
                    if (LEGACY_FLYWAY_SCHEMA_TABLE.equals(table)) {
                        return true;
                    }
                }
            }

            return false;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new FlywayException("Unable to obtain connection from Flyway DataSource", e);
        }
    }

    private String[] getDatabaseTypeLocations(final DatabaseType databaseType) {
        if (databaseType instanceof MySQLDatabaseType || databaseType instanceof MariaDBDatabaseType) {
            return LOCATIONS_MYSQL;
        }
        if (databaseType instanceof PostgreSQLDatabaseType) {
            return LOCATIONS_POSTGRES;
        }
        if (databaseType instanceof OracleDatabaseType) {
            return LOCATIONS_ORACLE;
        }
        return LOCATIONS_DEFAULT;
    }
}
