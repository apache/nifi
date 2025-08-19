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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class TestMSSQL2008DatabaseDialectService {
    private final MSSQL2008DatabaseDialectService service = new MSSQL2008DatabaseDialectService();

    @Test
    public void testPagingQuery2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "contain", 100L, 0L, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals(
            "SELECT * FROM (SELECT TOP 100 some(set), of(columns), that, might, contain, methods, a.*, ROW_NUMBER() OVER(ORDER BY contain asc) rnum " +
            "FROM database.tablename ORDER BY contain) A WHERE rnum > 0 AND rnum <= 100",
            sql
        );
    }

    @Test
    public void testTopOnly2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "", 50L, null, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT TOP 50 some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename", sql);
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
                    return Nullable.YES;
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
}


