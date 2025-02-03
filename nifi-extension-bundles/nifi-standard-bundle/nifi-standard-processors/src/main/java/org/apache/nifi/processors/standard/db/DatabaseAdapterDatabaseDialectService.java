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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transitional internal implementation of Database Dialect Service bridging to existing Database Adapters
 */
class DatabaseAdapterDatabaseDialectService extends AbstractControllerService implements DatabaseDialectService {
    private static final char SPACE_SEPARATOR = ' ';

    private static final char COMMA_SEPARATOR = ',';

    private static final int COLUMN_SIZE_IGNORED = -1;

    private static final String DOUBLE_QUOTE = "\"";

    private final DatabaseAdapter databaseAdapter;

    private final Set<StatementType> supportedStatementTypes;

    public DatabaseAdapterDatabaseDialectService(final String databaseType) {
        Objects.requireNonNull(databaseType, "Database Type required");
        databaseAdapter = DatabaseAdapterDescriptor.getDatabaseAdapter(databaseType);
        Objects.requireNonNull(databaseAdapter, "Database Adapter required");

        final Set<StatementType> statementTypes = new LinkedHashSet<>();
        statementTypes.add(StatementType.ALTER);
        statementTypes.add(StatementType.CREATE);
        statementTypes.add(StatementType.SELECT);

        if (databaseAdapter.supportsInsertIgnore()) {
            statementTypes.add(StatementType.INSERT_IGNORE);
        }
        if (databaseAdapter.supportsUpsert()) {
            statementTypes.add(StatementType.UPSERT);
        }
        supportedStatementTypes = Collections.unmodifiableSet(statementTypes);
    }

    @Override
    public StatementResponse getStatement(final StatementRequest statementRequest) {
        final StatementType statementType = statementRequest.statementType();

        final TableDefinition tableDefinition = statementRequest.tableDefinition();
        final List<String> columnNames = tableDefinition.columns()
                .stream()
                .map(ColumnDefinition::columnName)
                .toList();
        final List<String> primaryKeyColumnNames = tableDefinition.columns()
                .stream()
                .filter(ColumnDefinition::primaryKey)
                .map(ColumnDefinition::columnName)
                .toList();
        final List<ColumnDescription> columnDescriptions = getColumnDescriptions(tableDefinition);

        final String sql;

        if (StatementType.ALTER == statementType) {
            sql = databaseAdapter.getAlterTableStatement(tableDefinition.tableName(), columnDescriptions, true, true);
        } else if (StatementType.CREATE == statementType) {
            final TableSchema tableSchema = getTableSchema(tableDefinition);
            sql = databaseAdapter.getCreateTableStatement(tableSchema, false, false);
        } else if (StatementType.UPSERT == statementType) {
            sql = databaseAdapter.getUpsertStatement(tableDefinition.tableName(), columnNames, primaryKeyColumnNames);
        } else if (StatementType.INSERT_IGNORE == statementType) {
            sql = databaseAdapter.getInsertIgnoreStatement(tableDefinition.tableName(), columnNames, primaryKeyColumnNames);
        } else if (StatementType.SELECT == statementType) {
            sql = getSelectStatement(statementRequest);
        } else {
            throw new UnsupportedOperationException("Statement Type [%s] not supported".formatted(statementType));
        }

        return new StandardStatementResponse(sql);
    }

    @Override
    public Set<StatementType> getSupportedStatementTypes() {
        return supportedStatementTypes;
    }

    private String getSelectStatement(final StatementRequest statementRequest) {
        if (statementRequest instanceof QueryStatementRequest queryStatementRequest) {
            final TableDefinition tableDefinition = statementRequest.tableDefinition();
            final String qualifiedTableName = tableDefinition.tableName();
            final Optional<String> derivedTableFound = queryStatementRequest.derivedTable();

            final String selectTableSql;
            if (derivedTableFound.isPresent()) {
                final String derivedTable = derivedTableFound.get();
                final String tableAlias = databaseAdapter.getTableAliasClause(qualifiedTableName);
                selectTableSql = "SELECT * FROM (%s) %s".formatted(derivedTable, tableAlias);
            } else {
                final String tableColumns = getSelectTableColumns(tableDefinition.columns());

                final Optional<PageRequest> pageRequestFound = queryStatementRequest.pageRequest();
                final Long limit;
                final Long offset;
                final String indexColumnName;
                if (pageRequestFound.isPresent()) {
                    final PageRequest pageRequest = pageRequestFound.get();
                    limit = pageRequest.limit().isPresent() ? pageRequest.limit().getAsLong() : null;
                    offset = pageRequest.offset();
                    indexColumnName = pageRequest.indexColumnName().orElse(null);
                } else {
                    limit = null;
                    offset = null;
                    indexColumnName = null;
                }

                final String whereSql = queryStatementRequest.whereClause().orElse(null);
                final String orderBySql = queryStatementRequest.orderByClause().orElse(null);

                selectTableSql = databaseAdapter.getSelectStatement(qualifiedTableName, tableColumns, whereSql, orderBySql, limit, offset, indexColumnName);
            }

            return selectTableSql;
        } else {
            throw new IllegalArgumentException("Query Statement Request not found [%s]".formatted(statementRequest.getClass()));
        }
    }

    private String getSelectTableColumns(final List<ColumnDefinition> columnDefinitions) {
        final StringBuilder tableColumns = new StringBuilder();

        final Iterator<ColumnDefinition> columns = columnDefinitions.iterator();
        while (columns.hasNext()) {
            final ColumnDefinition columnDefinition = columns.next();
            final String columnName = columnDefinition.columnName();
            tableColumns.append(columnName);

            if (columns.hasNext()) {
                tableColumns.append(COMMA_SEPARATOR);
                tableColumns.append(SPACE_SEPARATOR);
            }
        }

        return tableColumns.toString();
    }

    private List<ColumnDescription> getColumnDescriptions(final TableDefinition tableDefinition) {
        return tableDefinition.columns().stream().map(columnDefinition ->
                new ColumnDescription(
                        columnDefinition.columnName(),
                        columnDefinition.dataType(),
                        columnDefinition.primaryKey(),
                        COLUMN_SIZE_IGNORED,
                        columnDefinition.nullable() == ColumnDefinition.Nullable.YES
                )
        ).toList();
    }

    private TableSchema getTableSchema(final TableDefinition tableDefinition) {
        final List<ColumnDescription> columnDescriptions = getColumnDescriptions(tableDefinition);
        final Set<String> primaryKeyColumnNames = tableDefinition.columns().stream()
                .filter(ColumnDefinition::primaryKey)
                .map(ColumnDefinition::columnName)
                .collect(Collectors.toUnmodifiableSet());

        return new TableSchema(
                tableDefinition.catalog().orElse(null),
                tableDefinition.schemaName().orElse(null),
                tableDefinition.tableName(),
                columnDescriptions,
                false,
                null,
                primaryKeyColumnNames,
                DOUBLE_QUOTE
        );
    }
}
