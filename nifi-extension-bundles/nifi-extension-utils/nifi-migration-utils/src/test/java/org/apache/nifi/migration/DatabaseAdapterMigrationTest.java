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
package org.apache.nifi.migration;

import static java.util.Collections.emptyMap;
import static org.apache.nifi.migration.DatabaseAdapterMigration.GENERIC_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.LEGACY_ORACLE_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.MSSQL_2008_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.MSSQL_2012_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.MYSQL_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.ORACLE_12_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.PHOENIX_DATABASE_ADAPTER_CLASSNAME;
import static org.apache.nifi.migration.DatabaseAdapterMigration.POSTGRESQL_DATABASE_ADAPTER_CLASSNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyConfiguration.CreatedControllerService;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DatabaseAdapterMigrationTest {

    private static final PropertyDescriptor DATABASE_ADAPTER = new PropertyDescriptor.Builder()
            .name("Database Adapter")
            .build();

    private static final String DB_TYPE = "db-type";

    @ParameterizedTest
    @MethodSource("provideArgs")
    void testMigrateDatabaseTypeProperty(String serviceClassname, String dbType) {
        final Map<String, String> properties = Map.of(
                DB_TYPE, dbType
        );
        final MockPropertyConfiguration config = new MockPropertyConfiguration(properties);

        DatabaseAdapterMigration.migrateProperties(config, DATABASE_ADAPTER, DB_TYPE);

        assertFalse(config.hasProperty(DB_TYPE));
        assertTrue(config.isPropertySet(DATABASE_ADAPTER));

        PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertEquals(1, result.getCreatedControllerServices().size());

        final CreatedControllerService createdService = result.getCreatedControllerServices().iterator().next();

        assertEquals(config.getRawPropertyValue(DATABASE_ADAPTER).orElseThrow(), createdService.id());
        assertEquals(serviceClassname, createdService.implementationClassName());

        assertEquals(emptyMap(), createdService.serviceProperties());
    }

    private static Stream<Arguments> provideArgs() {
        return Stream.of(
                Arguments.of(GENERIC_DATABASE_ADAPTER_CLASSNAME, "Generic"),
                Arguments.of(MSSQL_2012_DATABASE_ADAPTER_CLASSNAME, "MS SQL 2012+"),
                Arguments.of(MSSQL_2008_DATABASE_ADAPTER_CLASSNAME, "MS SQL 2008"),
                Arguments.of(MYSQL_DATABASE_ADAPTER_CLASSNAME, "MySQL"),
                Arguments.of(LEGACY_ORACLE_DATABASE_ADAPTER_CLASSNAME, "Oracle"),
                Arguments.of(ORACLE_12_DATABASE_ADAPTER_CLASSNAME, "Oracle 12+"),
                Arguments.of(PHOENIX_DATABASE_ADAPTER_CLASSNAME, "Phoenix"),
                Arguments.of(POSTGRESQL_DATABASE_ADAPTER_CLASSNAME, "PostgreSQL")
        );
    }
}
