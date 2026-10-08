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
package org.apache.nifi.processors.standard.db;

import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardQueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DatabaseAdapterDatabaseDialectServiceTest {
    private static final String TABLE_NAME = "test_table";

    private static final String DERIVED_TABLE = "SELECT id, name FROM test_table";

    @ParameterizedTest
    @ValueSource(strings = {"Generic", "MySQL"})
    void testSelectDerivedTableWithZeroResultWhereClause(final String databaseType) {
        final DatabaseAdapterDatabaseDialectService service = new DatabaseAdapterDatabaseDialectService(databaseType);

        final String sql = service.getStatement(newRequest(Optional.of("1 = 0"))).sql();

        assertEquals("SELECT * FROM (%s) AS %s WHERE 1 = 0".formatted(DERIVED_TABLE, TABLE_NAME), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Generic", "MySQL"})
    void testSelectDerivedTableWithCustomWhereClause(final String databaseType) {
        final DatabaseAdapterDatabaseDialectService service = new DatabaseAdapterDatabaseDialectService(databaseType);

        final String sql = service.getStatement(newRequest(Optional.of("id > 5 AND (gender = 'male')"))).sql();

        assertEquals("SELECT * FROM (%s) AS %s WHERE id > 5 AND (gender = 'male')".formatted(DERIVED_TABLE, TABLE_NAME), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Generic", "MySQL"})
    void testSelectDerivedTableWithoutWhereClause(final String databaseType) {
        final DatabaseAdapterDatabaseDialectService service = new DatabaseAdapterDatabaseDialectService(databaseType);

        final String sql = service.getStatement(newRequest(Optional.empty())).sql();

        assertFalse(sql.contains("WHERE"));
        assertEquals("SELECT * FROM (%s) AS %s".formatted(DERIVED_TABLE, TABLE_NAME), sql);
    }

    private QueryStatementRequest newRequest(final Optional<String> whereClause) {
        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), TABLE_NAME, Collections.emptyList());
        return new StandardQueryStatementRequest(
                StatementType.SELECT, tableDefinition, Optional.of(DERIVED_TABLE), whereClause, Optional.empty(), Optional.empty()
        );
    }
}
