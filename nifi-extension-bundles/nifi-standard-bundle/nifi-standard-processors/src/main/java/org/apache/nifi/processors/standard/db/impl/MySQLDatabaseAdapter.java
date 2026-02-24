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

import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.util.StringUtils;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

/**
 * A generic database adapter that generates MySQL compatible SQL.
 */
public class MySQLDatabaseAdapter extends GenericDatabaseAdapter {
    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public String getDescription() {
        return "Generates MySQL compatible SQL";
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsInsertIgnore() {
        return true;
    }

    @Override
    public String getUpsertStatement(final String table, final List<String> columnNames, final Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        final String columns = String.join(", ", columnNames);

        final String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        final List<String> updateValues = new ArrayList<>();
        for (final String columnName : columnNames) {
            updateValues.add(columnName + " = ?");
        }
        final String parameterizedUpdateValues = String.join(", ", updateValues);

        final StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON DUPLICATE KEY UPDATE ")
                .append(parameterizedUpdateValues);
        return statementStringBuilder.toString();
    }

    @Override
    public String getInsertIgnoreStatement(final String table, final List<String> columnNames, final Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        final String columns = String.join(", ", columnNames);

        final String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        final StringBuilder statementStringBuilder = new StringBuilder("INSERT IGNORE INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")");
        return statementStringBuilder.toString();
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    @Override
    public String getAlterTableStatement(final String tableName, final List<ColumnDescription> columnsToAdd) {
        final List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (final ColumnDescription column : columnsToAdd) {
            final String dataType = getSQLForDataType(column.getDataType());
            final StringBuilder sb = new StringBuilder("ADD COLUMN ")
                    .append(column.getColumnName())
                    .append(" ")
                    .append(dataType);
            columnsAndDatatypes.add(sb.toString());
        }

        final StringBuilder alterTableStatement = new StringBuilder();
        return alterTableStatement.append("ALTER TABLE ")
                .append(tableName)
                .append(" ")
                .append(String.join(", ", columnsAndDatatypes))
                .toString();
    }

    @Override
    public String getSQLForDataType(final int sqlType) {
        return switch (sqlType) {
            case Types.DOUBLE -> "DOUBLE PRECISION";
            case CHAR, LONGNVARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, VARCHAR, CLOB, NCLOB, OTHER, SQLXML -> "TEXT";
            default -> JDBCType.valueOf(sqlType).getName();
        };
    }
}
