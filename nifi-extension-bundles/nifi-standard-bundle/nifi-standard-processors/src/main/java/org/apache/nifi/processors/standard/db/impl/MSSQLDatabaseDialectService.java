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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardStatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

@Tags({"mssql", "sqlserver", "database", "dialect"})
@CapabilityDescription("Microsoft SQL Server 2012+ Database Dialect Service providing SELECT with paging, UPSERT using MERGE, and basic ALTER/CREATE DDL generation.")
public class MSSQLDatabaseDialectService extends AbstractControllerService implements DatabaseDialectService {
    @Override
    public StatementResponse getStatement(final StatementRequest statementRequest) {
        Objects.requireNonNull(statementRequest, "Statement Request required");

        final StatementType statementType = statementRequest.statementType();
        final TableDefinition tableDefinition = statementRequest.tableDefinition();

        final String sql;
        switch (statementType) { // NOPMD: ExhaustiveSwitchHasDefault - All cases are handled, default is for unsupported types
            case SELECT:
                sql = buildSelect(statementRequest);
                break;
            case UPSERT:
                sql = buildMerge(tableDefinition);
                break;
            case ALTER:
                sql = buildAlter(tableDefinition);
                break;
            case CREATE:
                sql = buildCreate(tableDefinition);
                break;
            case INSERT_IGNORE:
            default:
                throw new UnsupportedOperationException("Statement Type [" + statementType + "] not supported");
        }

        return new StandardStatementResponse(sql);
    }

    @Override
    public Set<StatementType> getSupportedStatementTypes() {
        return EnumSet.of(StatementType.ALTER, StatementType.CREATE, StatementType.SELECT, StatementType.UPSERT);
    }

    private String buildSelect(final StatementRequest statementRequest) {
        if (!(statementRequest instanceof QueryStatementRequest query)) {
            throw new IllegalArgumentException("Query Statement Request not found [" + statementRequest.getClass() + "]");
        }

        final TableDefinition table = statementRequest.tableDefinition();
        final String qualifiedTableName = qualifyTableName(table);

        final Optional<String> derivedTable = query.derivedTable();
        if (derivedTable.isPresent()) {
            final String tableAlias = "AS " + table.tableName();
            return "SELECT * FROM (" + derivedTable.get() + ") " + tableAlias;
        }

        final String selectColumns = buildSelectColumns(table.columns());

        final Optional<PageRequest> page = query.pageRequest();
        final Long limit;
        final Long offset;
        final String indexColumnName;
        if (page.isPresent()) {
            final PageRequest p = page.get();
            limit = p.limit().isPresent() ? p.limit().getAsLong() : null;
            offset = p.offset();
            indexColumnName = p.indexColumnName().orElse(null);
        } else {
            limit = null;
            offset = null;
            indexColumnName = null;
        }

        final String whereClause = query.whereClause().orElse(null);
        final String orderByClause = query.orderByClause().orElse(null);

        final StringBuilder sql = new StringBuilder("SELECT ");

        final boolean partitioned = indexColumnName != null && !indexColumnName.isBlank();
        final boolean orderByBlank = (orderByClause == null || orderByClause.isBlank());
        if (limit != null && !partitioned && (offset == null || (offset == 0 && orderByBlank))) {
            sql.append("TOP ").append(limit).append(' ');
        }

        sql.append(selectColumns)
            .append(" FROM ")
            .append(qualifiedTableName);

        if (whereClause != null && !whereClause.isBlank()) {
            sql.append(" WHERE ").append(whereClause);
            if (partitioned) {
                sql.append(" AND ")
                    .append(indexColumnName)
                    .append(" >= ")
                    .append(offset != null ? offset : 0);
                if (limit != null) {
                    sql.append(" AND ")
                        .append(indexColumnName)
                        .append(" < ")
                        .append((offset == null ? 0 : offset) + limit);
                }
            }
        }

        if (!partitioned && orderByClause != null && !orderByClause.isBlank()) {
            sql.append(" ORDER BY ").append(orderByClause);
        }

        if (!partitioned && limit != null && offset != null) {
            if (orderByBlank) {
                if (offset > 0) {
                    throw new IllegalArgumentException("Order by clause cannot be null or empty when using row paging");
                }
            } else {
                sql.append(" OFFSET ").append(offset).append(" ROWS FETCH NEXT ").append(limit).append(" ROWS ONLY");
            }
        }

        return sql.toString();
    }

    private String buildMerge(final TableDefinition table) {
        final String tableName = qualifyTableName(table);

        final List<String> columnNames = table.columns().stream().map(ColumnDefinition::columnName).toList();
        final List<String> keyColumnNames = table.columns().stream().filter(ColumnDefinition::primaryKey).map(ColumnDefinition::columnName).toList();

        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (keyColumnNames == null || keyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        final String sourceColumns = String.join(", ", columnNames);
        final StringJoiner valuesJoiner = new StringJoiner(", ");
        columnNames.forEach(col -> valuesJoiner.add("?"));
        final String sourceValues = valuesJoiner.toString();

        final String onClause = String.join(" AND ", keyColumnNames.stream()
                .map(k -> "target." + k + " = source." + k)
                .toList());

        final List<String> nonKeyColumns = new ArrayList<>(columnNames);
        nonKeyColumns.removeAll(keyColumnNames);
        final String updateSetClause = String.join(", ", nonKeyColumns.stream()
                .map(c -> c + " = source." + c)
                .toList());

        final String insertValues = String.join(", ", columnNames.stream().map(c -> "source." + c).toList());

        final StringBuilder sql = new StringBuilder();
        sql.append("MERGE INTO ").append(tableName).append(" AS target ")
                .append("USING (VALUES (").append(sourceValues).append(")) AS source (").append(sourceColumns).append(") ")
                .append("ON ").append(onClause).append(' ');

        if (!nonKeyColumns.isEmpty()) {
            sql.append("WHEN MATCHED THEN UPDATE SET ").append(updateSetClause).append(' ');
        }
        sql.append("WHEN NOT MATCHED THEN INSERT (").append(sourceColumns).append(") VALUES (").append(insertValues).append(");");

        return sql.toString();
    }

    private String buildAlter(final TableDefinition table) {
        final String tableName = qualifyTableName(table);
        final List<String> columnAdds = new ArrayList<>();
        for (ColumnDefinition c : table.columns()) {
            final String dataType = JDBCType.valueOf(c.dataType()).getName();
            columnAdds.add("ADD " + c.columnName() + ' ' + dataType);
        }
        return "ALTER TABLE " + tableName + ' ' + String.join(", ", columnAdds);
    }

    private String buildCreate(final TableDefinition table) {
        final String tableName = qualifyTableName(table);
        final List<String> defs = new ArrayList<>();
        for (ColumnDefinition c : table.columns()) {
            final String dataType = JDBCType.valueOf(c.dataType()).getName();
            final StringBuilder d = new StringBuilder()
                    .append(c.columnName())
                    .append(' ')
                    .append(dataType);
            if (c.nullable() == ColumnDefinition.Nullable.NO) {
                d.append(" NOT NULL");
            }
            if (c.primaryKey()) {
                d.append(" PRIMARY KEY");
            }
            defs.add(d.toString());
        }
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + String.join(", ", defs) + ")";
    }

    private String buildSelectColumns(final List<ColumnDefinition> columns) {
        if (columns == null || columns.isEmpty()) {
            return "*";
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i).columnName());
        }
        return sb.toString();
    }

    private String qualifyTableName(final TableDefinition table) {
        final StringBuilder name = new StringBuilder();
        table.catalog().ifPresent(c -> name.append(c).append('.'));
        table.schemaName().ifPresent(s -> name.append(s).append('.'));
        name.append(table.tableName());
        return name.toString();
    }
}


