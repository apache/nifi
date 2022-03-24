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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

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

        if (conflictColumnsClause.isEmpty()) {

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

            StringBuilder statementStringBuilder = new StringBuilder();

            statementStringBuilder.append(getColumnAssignment(table, columnName, newTableAlias));

            conflictClause.add(statementStringBuilder.toString());

        }

        return conflictClause;
    }

    private String getColumnAssignment(String table, String columnName, String newTableAlias) {
        return table + "." + columnName + " = " + newTableAlias + "." + columnName;
    }

}
