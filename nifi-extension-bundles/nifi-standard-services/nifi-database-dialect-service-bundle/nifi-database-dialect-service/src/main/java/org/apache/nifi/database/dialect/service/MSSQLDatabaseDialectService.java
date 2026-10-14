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
package org.apache.nifi.database.dialect.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

@CapabilityDescription("""
        Database Dialect Service supporting Microsoft SQL Server 2012 and higher.
        Supported Statement Types: ALTER, CREATE, SELECT, UPSERT
        UPSERT statements use MERGE with the HOLDLOCK serializable hint applied to the target table.
        """
)
@Tags({ "Microsoft", "SQL Server", "MSSQL", "Relational", "Database", "JDBC", "SQL" })
public class MSSQLDatabaseDialectService extends AbstractControllerService implements DatabaseDialectService {
    private static final String NOT_NULL_QUALIFIER = "NOT NULL";

    private static final String PRIMARY_KEY_QUALIFIER = "PRIMARY KEY";

    private static final Set<StatementType> supportedStatementTypes = Set.of(
            StatementType.ALTER,
            StatementType.CREATE,
            StatementType.SELECT,
            StatementType.UPSERT
    );

    @Override
    public StatementResponse getStatement(final StatementRequest statementRequest) {
        Objects.requireNonNull(statementRequest, "Statement Request required");

        final StatementType statementType = statementRequest.statementType();
        final TableDefinition tableDefinition = statementRequest.tableDefinition();

        final String sql = switch (statementType) {
            case ALTER -> buildAlter(tableDefinition);
            case CREATE -> buildCreate(tableDefinition);
            case SELECT -> buildSelect(statementRequest);
            case UPSERT -> buildMerge(tableDefinition);
            default -> throw new UnsupportedOperationException("Statement Type [%s] not supported".formatted(statementType));
        };

        return new StandardStatementResponse(sql);
    }

    @Override
    public Set<StatementType> getSupportedStatementTypes() {
        return supportedStatementTypes;
    }

    private String buildSelect(final StatementRequest statementRequest) {
        if (!(statementRequest instanceof QueryStatementRequest query)) {
            throw new IllegalArgumentException("Query Statement Request not found [%s]".formatted(statementRequest.getClass()));
        }

        final TableDefinition table = statementRequest.tableDefinition();

        final Optional<PageRequest> page = query.pageRequest();
        final Long limit;
        final Long offset;
        final String indexColumnName;
        if (page.isPresent()) {
            final PageRequest pageRequest = page.get();
            limit = pageRequest.limit().isPresent() ? pageRequest.limit().getAsLong() : null;
            offset = pageRequest.offset();
            indexColumnName = pageRequest.indexColumnName().orElse(null);
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

        // A derived table is selected through an alias, otherwise the qualified table name is the source. Both
        // paths share the clause handling below so that filtering and paging apply to a custom query as well.
        final Optional<String> derivedTable = query.derivedTable();
        if (derivedTable.isPresent()) {
            sql.append("* FROM (")
                .append(derivedTable.get())
                .append(") AS ")
                .append(table.tableName());
        } else {
            sql.append(buildSelectColumns(table.columns()))
                .append(" FROM ")
                .append(qualifyTableName(table));
        }

        boolean whereAdded = false;
        if (whereClause != null && !whereClause.isBlank()) {
            sql.append(" WHERE ").append(whereClause);
            whereAdded = true;
        }

        if (partitioned) {
            sql.append(whereAdded ? " AND " : " WHERE ");
            sql.append(indexColumnName)
                .append(" >= ")
                .append(offset != null ? offset : 0);
            if (limit != null) {
                sql.append(" AND ")
                    .append(indexColumnName)
                    .append(" < ")
                    .append((offset == null ? 0 : offset) + limit);
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
        final String tableName = table.tableName();
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }

        final List<ColumnDefinition> columnDefinitions = table.columns();
        if (columnDefinitions == null || columnDefinitions.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }

        final List<String> columnNames = columnDefinitions.stream().map(ColumnDefinition::columnName).toList();
        final List<String> keyColumnNames = columnDefinitions.stream().filter(ColumnDefinition::primaryKey).map(ColumnDefinition::columnName).toList();
        if (keyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        final String qualifiedTableName = qualifyTableName(table);

        final String sourceColumns = String.join(", ", columnNames);
        final StringJoiner valuesJoiner = new StringJoiner(", ");
        columnNames.forEach(columnName -> valuesJoiner.add("?"));
        final String sourceValues = valuesJoiner.toString();

        final String onClause = String.join(" AND ", keyColumnNames.stream()
                .map(keyColumnName -> "target." + keyColumnName + " = source." + keyColumnName)
                .toList());

        final List<String> nonKeyColumns = new ArrayList<>(columnNames);
        nonKeyColumns.removeAll(keyColumnNames);
        final String updateSetClause = String.join(", ", nonKeyColumns.stream()
                .map(columnName -> columnName + " = source." + columnName)
                .toList());

        final String insertValues = String.join(", ", columnNames.stream().map(columnName -> "source." + columnName).toList());

        // HOLDLOCK applies a serializable range lock to the target, preventing a concurrent session from
        // inserting a matching key between evaluation of the ON clause and WHEN NOT MATCHED THEN INSERT
        final StringBuilder sql = new StringBuilder();
        sql.append("MERGE INTO ").append(qualifiedTableName).append(" WITH (HOLDLOCK) AS target ")
                .append("USING (VALUES (").append(sourceValues).append(")) AS source (").append(sourceColumns).append(") ")
                .append("ON ").append(onClause).append(' ');

        if (!nonKeyColumns.isEmpty()) {
            sql.append("WHEN MATCHED THEN UPDATE SET ").append(updateSetClause).append(' ');
        }
        sql.append("WHEN NOT MATCHED THEN INSERT (").append(sourceColumns).append(") VALUES (").append(insertValues).append(");");

        return sql.toString();
    }

    private String buildAlter(final TableDefinition table) {
        final List<String> columnAdds = new ArrayList<>();
        for (final ColumnDefinition column : table.columns()) {
            columnAdds.add("%s %s".formatted(column.columnName(), getJdbcTypeName(column)));
        }
        return "ALTER TABLE %s ADD %s".formatted(qualifyTableName(table), String.join(", ", columnAdds));
    }

    private String buildCreate(final TableDefinition table) {
        final List<String> columnDefinitions = new ArrayList<>();
        for (final ColumnDefinition column : table.columns()) {
            final StringBuilder columnDefinition = new StringBuilder("%s %s".formatted(column.columnName(), getJdbcTypeName(column)));
            if (ColumnDefinition.Nullable.NO == column.nullable()) {
                columnDefinition.append(' ').append(NOT_NULL_QUALIFIER);
            }
            if (column.primaryKey()) {
                columnDefinition.append(' ').append(PRIMARY_KEY_QUALIFIER);
            }
            columnDefinitions.add(columnDefinition.toString());
        }

        return "CREATE TABLE %s (%s)".formatted(qualifyTableName(table), String.join(", ", columnDefinitions));
    }

    private String buildSelectColumns(final List<ColumnDefinition> columns) {
        if (columns == null || columns.isEmpty()) {
            return "*";
        }
        return String.join(", ", columns.stream().map(ColumnDefinition::columnName).toList());
    }

    private String getJdbcTypeName(final ColumnDefinition columnDefinition) {
        return JDBCType.valueOf(columnDefinition.dataType()).getName();
    }

    private String qualifyTableName(final TableDefinition table) {
        final StringBuilder name = new StringBuilder();
        table.catalog().ifPresent(catalog -> name.append(catalog).append('.'));
        table.schemaName().ifPresent(schemaName -> name.append(schemaName).append('.'));
        name.append(table.tableName());
        return name.toString();
    }
}
