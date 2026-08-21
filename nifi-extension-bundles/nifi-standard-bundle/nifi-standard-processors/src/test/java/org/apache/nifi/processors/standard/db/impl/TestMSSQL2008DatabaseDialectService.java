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
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardPageRequest;
import org.apache.nifi.database.dialect.service.api.StandardQueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardStatementRequest;
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
        Assertions.assertEquals("SELECT TOP 100 some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename ORDER BY contain", sql);
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
            defs.add(new StandardColumnDefinition(names.get(i), java.sql.Types.VARCHAR, ColumnDefinition.Nullable.YES, primaryKeys.length > i && primaryKeys[i]));
        }
        return defs;
    }
    @Test
    public void testSelectGeneration2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), null, null, null, null, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename", sql);
    }

    @Test
    public void testSelectWhereAndOrder2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "that='some\"' value'", "might DESC", null, null, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE that='some\"' value' ORDER BY might DESC", sql);
    }

    @Test
    public void testSelectWithDerivedTable2008() {
        final TableDefinition table = table("derived_alias", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReqWithDerivedTable(table, "SELECT * FROM base_table WHERE condition = 'value'");
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT * FROM (SELECT * FROM base_table WHERE condition = 'value') AS derived_alias", sql);
    }

    @Test
    public void testSelectWithEmptyColumns2008() {
        final TableDefinition table = table("database.tablename", List.of());
        final StatementRequest req = selectReq(table, List.of(), null, null, null, null, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT * FROM database.tablename", sql);
    }

    @Test
    public void testPagingWithOffsetAndOrder2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "status='active'", "id", 10L, 20L, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals(
            "SELECT * FROM (SELECT TOP 30 some(set), of(columns), that, might, contain, methods, a.*, ROW_NUMBER() OVER(ORDER BY id asc) rnum " +
            "FROM database.tablename WHERE status='active') A WHERE rnum > 20 AND rnum <= 30",
            sql
        );
    }

    @Test
    public void testPagingNoOrderByWithOffset2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "", 10L, 5L, null);
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> service.getStatement(req));
        Assertions.assertTrue(e.getMessage().contains("Order by clause required for pagination when offset > 0"));
    }

    @Test
    public void testPartitionedPaging2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req1 = selectReq(table, sampleColumns(), "1=1", "contain", 100L, 0L, "contain");
        final String sql1 = service.getStatement(req1).sql();
        Assertions.assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE 1=1 AND contain >= 0 AND contain < 100", sql1);

        final StatementRequest req2 = selectReq(table, sampleColumns(), "1=1", "contain", 10000L, 123456L, "contain");
        final String sql2 = service.getStatement(req2).sql();
        Assertions.assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE 1=1 AND contain >= 123456 AND contain < 133456", sql2);
    }

    @Test
    public void testPartitionedPagingWithoutLimit2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "status='active'", "id", null, 1000L, "id");
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename WHERE status='active' AND id >= 1000", sql);
    }

    @Test
    public void testPartitionedPagingWithBlankIndexColumn2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), null, "id", 100L, 50L, "   ");
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals(
            "SELECT * FROM (SELECT TOP 150 some(set), of(columns), that, might, contain, methods, a.*, ROW_NUMBER() OVER(ORDER BY id asc) rnum " +
            "FROM database.tablename) A WHERE rnum > 50 AND rnum <= 150",
            sql
        );
    }

    @Test
    public void testLimitZeroWithOffset2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "id", 0L, 10L, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals(
            "SELECT * FROM (SELECT TOP 10 some(set), of(columns), that, might, contain, methods, a.*, ROW_NUMBER() OVER(ORDER BY id asc) rnum " +
            "FROM database.tablename) A WHERE rnum > 10 AND rnum <= 10",
            sql
        );
    }

    @Test
    public void testLimitZeroWithoutOffset2008() {
        final TableDefinition table = table("database.tablename", columns(sampleColumns(), new boolean[sampleColumns().size()]));
        final StatementRequest req = selectReq(table, sampleColumns(), "", "", 0L, null, null);
        final String sql = service.getStatement(req).sql();
        Assertions.assertEquals("SELECT TOP 0 some(set), of(columns), that, might, contain, methods, a.* FROM database.tablename", sql);
    }

    @Test
    public void testInheritedUpsertFunctionality2008() {
        final List<String> cols = List.of("column1", "column2", "column3", "column4");
        final boolean[] pk = new boolean[]{false, true, false, true};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = upsertReq(table);
        final String expected = "MERGE INTO table AS target USING (VALUES (?, ?, ?, ?)) AS source (column1, column2, column3, column4) " +
                "ON target.column2 = source.column2 AND target.column4 = source.column4 " +
                "WHEN MATCHED THEN UPDATE SET column1 = source.column1, column3 = source.column3 " +
                "WHEN NOT MATCHED THEN INSERT (column1, column2, column3, column4) VALUES (source.column1, source.column2, source.column3, source.column4);";
        Assertions.assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testInheritedCreateFunctionality2008() {
        final List<String> cols = List.of("id", "name", "active");
        final boolean[] pk = new boolean[]{true, false, false};
        final TableDefinition table = table("users", columns(cols, pk));
        final StatementRequest req = createReq(table);
        final String expected = "IF OBJECT_ID('users', 'U') IS NULL CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR, active VARCHAR)";
        Assertions.assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testInheritedAlterFunctionality2008() {
        final List<String> cols = List.of("new_column1", "new_column2");
        final boolean[] pk = new boolean[]{false, false};
        final TableDefinition table = table("existing_table", columns(cols, pk));
        final StatementRequest req = alterReq(table);
        final String expected = "ALTER TABLE existing_table ADD new_column1 VARCHAR, new_column2 VARCHAR";
        Assertions.assertEquals(expected, service.getStatement(req).sql());
    }

    @Test
    public void testInheritedUnsupportedStatementType2008() {
        final List<String> cols = List.of("col1");
        final boolean[] pk = new boolean[]{false};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = insertIgnoreReq(table);
        final UnsupportedOperationException e = Assertions.assertThrows(UnsupportedOperationException.class, () -> service.getStatement(req));
        Assertions.assertTrue(e.getMessage().contains("Statement Type [INSERT_IGNORE] not supported"));
    }

    @Test
    public void testSelectWithNonQueryStatementRequest2008() {
        final List<String> cols = List.of("col1");
        final boolean[] pk = new boolean[]{false};
        final TableDefinition table = table("table", columns(cols, pk));
        final StatementRequest req = new StandardStatementRequest(StatementType.SELECT, table);
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> service.getStatement(req));
        Assertions.assertTrue(e.getMessage().contains("Query Statement Request not found"));
    }

    private static StatementRequest selectReq(final TableDefinition table,
                                              final List<String> columnNames,
                                              final String where,
                                              final String orderBy,
                                              final Long limit,
                                              final Long offset,
                                              final String indexColumn) {
        return new StandardQueryStatementRequest(
                StatementType.SELECT,
                table,
                Optional.empty(),
                Optional.ofNullable(where).filter(s -> !s.isEmpty()),
                Optional.ofNullable(orderBy).filter(s -> !s.isEmpty()),
                pageRequest(limit, offset, indexColumn)
        );
    }

    private static Optional<PageRequest> pageRequest(final Long limit, final Long offset, final String indexColumn) {
        if (limit == null && offset == null && indexColumn == null) {
            return Optional.empty();
        }
        return Optional.of(new StandardPageRequest(
                offset == null ? 0L : offset,
                limit == null ? OptionalLong.empty() : OptionalLong.of(limit),
                Optional.ofNullable(indexColumn)
        ));
    }

    private static StatementRequest selectReqWithDerivedTable(final TableDefinition table, final String derivedTableSql) {
        return new StandardQueryStatementRequest(
                StatementType.SELECT,
                table,
                Optional.of(derivedTableSql),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );
    }

    private static StatementRequest upsertReq(final TableDefinition table) {
        return new StandardStatementRequest(StatementType.UPSERT, table);
    }

    private static StatementRequest createReq(final TableDefinition table) {
        return new StandardStatementRequest(StatementType.CREATE, table);
    }

    private static StatementRequest alterReq(final TableDefinition table) {
        return new StandardStatementRequest(StatementType.ALTER, table);
    }

    private static StatementRequest insertIgnoreReq(final TableDefinition table) {
        return new StandardStatementRequest(StatementType.INSERT_IGNORE, table);
    }
}
