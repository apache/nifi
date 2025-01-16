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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.TableSchema;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.SQLXML;
import static java.sql.Types.VARCHAR;

public class Oracle12DatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle 12+";
    }

    @Override
    public String getDescription() {
        return "Generates Oracle compliant SQL for version 12 or greater";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause,
            Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause,
            Long limit, Long offset, String columnForPartitioning) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        final StringBuilder query = new StringBuilder("SELECT ");

        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
            if (!StringUtils.isEmpty(columnForPartitioning)) {
                query.append(" AND ");
                query.append(columnForPartitioning);
                query.append(" >= ");
                query.append(offset != null ? offset : "0");
                if (limit != null) {
                    query.append(" AND ");
                    query.append(columnForPartitioning);
                    query.append(" < ");
                    query.append((offset == null ? 0 : offset) + limit);
                }
            }
        }
        if (!StringUtils.isEmpty(orderByClause) && StringUtils.isEmpty(columnForPartitioning)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }
        if (StringUtils.isEmpty(columnForPartitioning)) {
            if (offset != null && offset > 0) {
                query.append(" OFFSET ");
                query.append(offset);
                query.append(" ROWS");
            }
            if (limit != null) {
                query.append(" FETCH NEXT ");
                query.append(limit);
                query.append(" ROWS ONLY");
            }
        }

        return query.toString();
    }

    @Override
    public String getTableAliasClause(String tableName) {
        return tableName;
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames)
            throws IllegalArgumentException {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String newValuesAlias = "n";

        String columns = columnNames.stream().collect(Collectors.joining(", ? "));

        columns = "? " + columns;

        List<String> columnsAssignment = getColumnsAssignment(columnNames, newValuesAlias, table);

        List<String> conflictColumnsClause = getConflictColumnsClause(uniqueKeyColumnNames, columnsAssignment, table,
                newValuesAlias);
        String conflictClause = "(" + conflictColumnsClause.stream().collect(Collectors.joining(" AND ")) + ")";

        String insertStatement = columnNames.stream().collect(Collectors.joining(", "));
        String insertValues = newValuesAlias + "."
                + columnNames.stream().collect(Collectors.joining(", " + newValuesAlias + "."));

        columnsAssignment.removeAll(conflictColumnsClause);
        String updateStatement = columnsAssignment.stream().collect(Collectors.joining(", "));

        StringBuilder statementStringBuilder = new StringBuilder("MERGE INTO ").append(table).append(" USING (SELECT ")
                .append(columns).append(" FROM DUAL) ").append(newValuesAlias).append(" ON ").append(conflictClause)
                .append(" WHEN NOT MATCHED THEN INSERT (").append(insertStatement).append(") VALUES (")
                .append(insertValues).append(")").append(" WHEN MATCHED THEN UPDATE SET ").append(updateStatement);

        return statementStringBuilder.toString();
    }

    private List<String> getConflictColumnsClause(Collection<String> uniqueKeyColumnNames, List<String> conflictColumns,
            String table, String newTableAlias) {
        List<String> conflictColumnsClause = conflictColumns.stream()
                .filter(column -> uniqueKeyColumnNames.stream().anyMatch(
                        uniqueKey -> column.equalsIgnoreCase(getColumnAssignment(table, uniqueKey, newTableAlias))))
                .collect(Collectors.toList());

        // Compare list sizes instead of emptyness
        if (conflictColumnsClause.size() != uniqueKeyColumnNames.size()) {

            // Try it with normalized columns
            conflictColumnsClause = conflictColumns.stream()
                    .filter((column -> uniqueKeyColumnNames.stream()
                            .anyMatch(uniqueKey -> normalizeColumnName(column).equalsIgnoreCase(
                                    normalizeColumnName(getColumnAssignment(table, uniqueKey, newTableAlias))))))
                    .collect(Collectors.toList());
        }

        return conflictColumnsClause;

    }

    private String normalizeColumnName(final String colName) {
        return colName == null ? null : colName.toUpperCase().replace("_", "");
    }

    private List<String> getColumnsAssignment(Collection<String> columnsNames, String newTableAlias, String table) {
        List<String> conflictClause = new ArrayList<>();

        for (String columnName : columnsNames) {
            conflictClause.add(getColumnAssignment(table, columnName, newTableAlias));
        }

        return conflictClause;
    }

    private String getColumnAssignment(String table, String columnName, String newTableAlias) {
        return table + "." + columnName + " = " + newTableAlias + "." + columnName;
    }

    @Override
    public String getAlterTableStatement(String tableName, List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            String dataType = getSQLForDataType(column.getDataType());
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(dataType);
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ADD (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return createTableStatement.toString();
    }

    @Override
    public String getSQLForDataType(int sqlType) {
        switch (sqlType) {
            case Types.DOUBLE:
                return "DOUBLE PRECISION";
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case CLOB:
            case NCLOB:
            case OTHER:
            case SQLXML:
                // Must have a max length specified (the Oracle docs say 2000), and use VARCHAR2 instead of VARCHAR for consistent comparison semantics
                return "VARCHAR2(2000)";
            default:
                return JDBCType.valueOf(sqlType).getName();
        }
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    /**
     * Generates a CREATE TABLE statement using the specified table schema
     * @param tableSchema The table schema including column information
     * @param quoteTableName Whether to quote the table name in the generated DDL
     * @param quoteColumnNames Whether to quote column names in the generated DDL
     * @return A String containing DDL to create the specified table
     */
    @Override
    public String getCreateTableStatement(TableSchema tableSchema, boolean quoteTableName, boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder()
                .append("DECLARE\n\tsql_stmt long;\nBEGIN\n\tsql_stmt:='CREATE TABLE ")
                .append(generateTableName(quoteTableName, tableSchema.getCatalogName(), tableSchema.getSchemaName(), tableSchema.getTableName(), tableSchema))
                .append(" (");

        List<ColumnDescription> columns = tableSchema.getColumnsAsList();
        Set<String> primaryKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescription column = columns.get(i);
            createTableStatement
                    .append((i != 0) ? ", " : "")
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()))
                    .append(column.isNullable() ? "" : " NOT NULL")
                    .append(primaryKeyColumnNames != null && primaryKeyColumnNames.contains(column.getColumnName()) ? " PRIMARY KEY" : "");
        }

        createTableStatement
                .append(")';\nEXECUTE IMMEDIATE sql_stmt;\nEXCEPTION\n\tWHEN OTHERS THEN\n\t\tIF SQLCODE = -955 THEN\n\t\t\t")
                .append("NULL;\n\t\tELSE\n\t\t\tRAISE;\n\t\tEND IF;\nEND;");

        return createTableStatement.toString();
    }
}
