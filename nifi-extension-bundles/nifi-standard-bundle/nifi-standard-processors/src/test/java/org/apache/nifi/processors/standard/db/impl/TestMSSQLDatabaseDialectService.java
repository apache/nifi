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
}


