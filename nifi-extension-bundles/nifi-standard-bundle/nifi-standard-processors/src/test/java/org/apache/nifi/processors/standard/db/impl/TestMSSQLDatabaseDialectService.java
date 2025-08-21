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
package org.apache.nifi.processors.standard.db.impl;

import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMSSQLDatabaseDialectService {
    private final MSSQLDatabaseDialectService service = new MSSQLDatabaseDialectService();

    @Test
    public void testSelectGeneration() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), null, null, null, null, null);
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename", sql);
    }

    @Test
    public void testSelectWhereAndOrder() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "that='some\"' value'", "might DESC", null, null, null);
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE that='some\"' value' ORDER BY might DESC", sql);
    }

    @Test
    public void testTopQuery() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "", 100L, null, null);
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT TOP 100 some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename", sql);
    }

    @Test
    public void testPagingNoOrderBy() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "", 10L, 1L, null);
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getStatement(req));
        assertTrue(e.getMessage().contains("Order by clause cannot be null or empty when using row paging"));
    }

    @Test
    public void testPagingQuery() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "contain", 100L, 0L, null);
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename ORDER BY contain OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY", sql);
    }

    @Test
    public void testPartitionedPaging() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req1 = selectReq(table, sampleColumns(), "1=1", "contain", 100L, 0L, "contain");
        final String sql1 = service.getStatement(req1).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE 1=1 AND contain >= 0 AND contain < 100", sql1);

        final StatementRequest req2 = selectReq(table, sampleColumns(), "1=1", "contain", 10000L, 123456L, "contain");
        final String sql2 = service.getStatement(req2).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE 1=1 AND contain >= 123456 AND contain < 133456", sql2);
    }

    @Test
    public void testSupportsUpsert() {
        assertTrue(service.getSupportedStatementTypes().containsAll(EnumSet.of(StatementType.UPSERT, StatementType.SELECT)));
    }

    @Test
    public void testUpsertMerge() {
        final List<String> cols = List.of("column1", "column2", "column3", "column4");
        final boolean[] pk = new boolean[]{
            false, true, false, true};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = upsertReq(table);
        final String expected = "MERGE INTO table AS target USING (VALUES (?, ?, ?, ?)) AS source (column1, column2, column3, column4) " +
                "ON target.column2 = source.column2 AND target.column4 = source.column4 " +
                "WHEN MATCHED THEN UPDATE SET column1 = source.column1, column3 = source.column3 " +
                "WHEN NOT MATCHED THEN INSERT (column1, column2, column3, column4) VALUES (source.column1, source.column2, source.column3, source.column4);";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testUpsertValidation() {
        final List<String> cols = List.of("column1");
        final boolean[] pk = new boolean[]{
            false};
        final TableDefinition t1 = table("   ", columns(cols, pk));
        final IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> service.getStatement(upsertReq(t1)));
        assertTrue(e1.getMessage().contains("Table name cannot be null or blank"));
    }
    @Test
    public void testCreateStatement() {
        final List<String> cols = List.of("id", "name", "email", "active");
        final boolean[] pk = new boolean[]{true, false, false, false};
        final TableDefinition table = table("users", columns(cols, pk));
        final StatementRequest req = createReq(table);
        final String expected = "CREATE TABLE IF NOT EXISTS users (id VARCHAR PRIMARY KEY, name VARCHAR, email VARCHAR, active VARCHAR)";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testCreateStatementWithNullableColumns() {
        final List<String> cols = List.of("id", "name");
        final boolean[] pk = new boolean[]{true, false};
        final boolean[] nullable = new boolean[]{false, true};
        final TableDefinition table = table("test_table", columns(cols, pk, nullable));
        final StatementRequest req = createReq(table);
        final String expected = "CREATE TABLE IF NOT EXISTS test_table (id VARCHAR NOT NULL PRIMARY KEY, name VARCHAR)";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testCreateStatementWithQualifiedTableName() {
        final List<String> cols = List.of("id", "data");
        final boolean[] pk = new boolean[]{true, false};
        final TableDefinition table = qualifiedTable("catalog", "schema", "table", columns(cols, pk));
        final StatementRequest req = createReq(table);
        final String expected = "CREATE TABLE IF NOT EXISTS catalog.schema.table (id VARCHAR PRIMARY KEY, data VARCHAR)";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testAlterStatement() {
        final List<String> cols = List.of("new_column1", "new_column2");
        final boolean[] pk = new boolean[]{false, false};
        final TableDefinition table = table("existing_table", columns(cols, pk));
        final StatementRequest req = alterReq(table);
        final String expected = "ALTER TABLE existing_table ADD new_column1 VARCHAR, ADD new_column2 VARCHAR";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testAlterStatementWithQualifiedTableName() {
        final List<String> cols = List.of("new_col");
        final boolean[] pk = new boolean[]{false};
        final TableDefinition table = qualifiedTable("db", "dbo", "table1", columns(cols, pk));
        final StatementRequest req = alterReq(table);
        final String expected = "ALTER TABLE db.dbo.table1 ADD new_col VARCHAR";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testUnsupportedStatementType() {
        final List<String> cols = List.of("col1");
        final boolean[] pk = new boolean[]{false};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = insertIgnoreReq(table);
        final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, () -> service.getStatement(req));
        assertTrue(e.getMessage().contains("Statement Type [INSERT_IGNORE] not supported"));
    }

    @Test
    public void testSelectWithDerivedTable() {
        final TableDefinition table = table("derived_alias", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReqWithDerivedTable(table, "SELECT * FROM base_table WHERE condition = 'value'");
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT * FROM (SELECT * FROM base_table WHERE condition = 'value') AS derived_alias", sql);
    }

    @Test
    public void testSelectWithEmptyColumns() {
        final TableDefinition table = table("database.tablename", List.of());
        final StatementRequest req = selectReq(table, List.of(), null, null, null, null, null);
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT * FROM database.tablename", sql);
    }

    @Test
    public void testPartitionedPagingWithoutLimit() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "status='active'", "id", null, 1000L, "id");
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE status='active' AND id >= 1000", sql);
    }

    @Test
    public void testPartitionedPagingWithBlankIndexColumn() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), null, "id", 100L, 0L, "   ");
        final String sql = service.getStatement(req).sql();
        assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename ORDER BY id OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY", sql);
    }

    @Test
    public void testUpsertWithAllPrimaryKeyColumns() {
        final List<String> cols = List.of("key1", "key2");
        final boolean[] pk = new boolean[]{true, true};
        final TableDefinition table = table("lookup_table", columns(cols, pk));
        final StatementRequest req = upsertReq(table);
        final String expected = "MERGE INTO lookup_table AS target USING (VALUES (?, ?)) AS source (key1, key2) " +
                "ON target.key1 = source.key1 AND target.key2 = source.key2 " +
                "WHEN NOT MATCHED THEN INSERT (key1, key2) VALUES (source.key1, source.key2);";
        assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testUpsertValidationEmptyColumns() {
        final TableDefinition table = table("table", List.of());
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getStatement(upsertReq(table)));
        assertTrue(e.getMessage().contains("Column names cannot be null or empty"));
    }

    @Test
    public void testUpsertValidationNoPrimaryKeys() {
        final List<String> cols = List.of("col1", "col2");
        final boolean[] pk = new boolean[]{false, false};
        final TableDefinition table = table("table", columns(cols, pk));
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getStatement(upsertReq(table)));
        assertTrue(e.getMessage().contains("Key column names cannot be null or empty"));
    }

    @Test
    public void testSelectWithNonQueryStatementRequest() {
        final List<String> cols = List.of("col1");
        final boolean[] pk = new boolean[]{false};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = new StatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.SELECT;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
        };
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getStatement(req));
        assertTrue(e.getMessage().contains("Query Statement Request not found"));
    }

    private static List<String> sampleColumns() {
        return List.of("some(set)", "of(columns)", "that", "might", "contain", "methods", "a.*");
    }

    private static TableDefinition table(final String tableName, final List<ColumnDefinition> cols) {
        return new TableDefinition(Optional.empty(), Optional.empty(), tableName, cols);
    }

    private static List<ColumnDefinition> columns(final List<String> names, final boolean[] primaryKeys) {
        final List<ColumnDefinition> defs = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            final int idx = i;
            defs.add(new ColumnDefinition() {
                @Override
                public String columnName() {
                    return names.get(idx);
                }
                @Override
                public int dataType() {
                    return java.sql.Types.VARCHAR;
                }
                @Override
                public Nullable nullable() {
                    return org.apache.nifi.database.dialect.service.api.ColumnDefinition.Nullable.YES;
                }
                @Override
                public boolean primaryKey() {
                    return primaryKeys.length > idx && primaryKeys[idx];
                }
            });
        }
        return defs;
    }

    private static StatementRequest selectReq(final TableDefinition table,
                                              final List<String> columnNames,
                                              final String where,
                                              final String orderBy,
                                              final Long limit,
                                              final Long offset,
                                              final String indexColumn) {
        return new QueryStatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.SELECT;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
            @Override
            public Optional<String> derivedTable() {
                return Optional.empty();
            }
            @Override
            public Optional<String> whereClause() {
                return Optional.ofNullable(where).filter(s -> !s.isEmpty());
            }
            @Override
            public Optional<String> orderByClause() {
                return Optional.ofNullable(orderBy).filter(s -> !s.isEmpty());
            }
            @Override
            public Optional<PageRequest> pageRequest() {
                if (limit == null && offset == null && indexColumn == null) {
                    return Optional.empty();
                }
                return Optional.of(new PageRequest() {
                    @Override
                    public OptionalLong limit() {
                        return limit == null ? OptionalLong.empty() : OptionalLong.of(limit);
                    }
                    @Override
                    public long offset() {
                        return offset == null ? 0L : offset;
                    }
                    @Override
                    public Optional<String> indexColumnName() {
                        return Optional.ofNullable(indexColumn);
                    }
                });
            }
        };
    }

    private static StatementRequest upsertReq(final TableDefinition table) {
        return new StatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.UPSERT;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
        };
    }
    private static TableDefinition qualifiedTable(final String catalog, final String schema, final String tableName, final List<ColumnDefinition> cols) {
        return new TableDefinition(Optional.ofNullable(catalog), Optional.ofNullable(schema), tableName, cols);
    }

    private static List<ColumnDefinition> columns(final List<String> names, final boolean[] primaryKeys, final boolean[] nullable) {
        final List<ColumnDefinition> defs = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            final int idx = i;
            defs.add(new ColumnDefinition() {
                @Override
                public String columnName() {
                    return names.get(idx);
                }
                @Override
                public int dataType() {
                    return java.sql.Types.VARCHAR;
                }
                @Override
                public Nullable nullable() {
                    return (nullable.length > idx && !nullable[idx])
                        ? org.apache.nifi.database.dialect.service.api.ColumnDefinition.Nullable.NO
                        : org.apache.nifi.database.dialect.service.api.ColumnDefinition.Nullable.YES;
                }
                @Override
                public boolean primaryKey() {
                    return primaryKeys.length > idx && primaryKeys[idx];
                }
            });
        }
        return defs;
    }

    private static StatementRequest createReq(final TableDefinition table) {
        return new StatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.CREATE;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
        };
    }

    private static StatementRequest alterReq(final TableDefinition table) {
        return new StatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.ALTER;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
        };
    }

    private static StatementRequest insertIgnoreReq(final TableDefinition table) {
        return new StatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.INSERT_IGNORE;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
        };
    }

    private static StatementRequest selectReqWithDerivedTable(final TableDefinition table, final String derivedTableSql) {
        return new QueryStatementRequest() {
            @Override
            public StatementType statementType() {
                return StatementType.SELECT;
            }
            @Override
            public TableDefinition tableDefinition() {
                return table;
            }
            @Override
            public Optional<String> derivedTable() {
                return Optional.of(derivedTableSql);
            }
            @Override
            public Optional<String> whereClause() {
                return Optional.empty();
            }
            @Override
            public Optional<String> orderByClause() {
                return Optional.empty();
            }
            @Override
            public Optional<PageRequest> pageRequest() {
                return Optional.empty();
            }
        };
    }
}


