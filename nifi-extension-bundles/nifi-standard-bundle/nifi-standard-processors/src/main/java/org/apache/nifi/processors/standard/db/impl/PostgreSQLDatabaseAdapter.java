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
import java.util.Optional;
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

public class PostgreSQLDatabaseAdapter extends GenericDatabaseAdapter {
    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public String getDescription() {
        return "Generates PostgreSQL compatible SQL";
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
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = String.join(", ", columnNames);

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        String updateValues = columnNames.stream()
                .map(columnName -> "EXCLUDED." + columnName)
                .collect(Collectors.joining(", "));

        String conflictClause = "(" + String.join(", ", uniqueKeyColumnNames) + ")";

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON CONFLICT ")
                .append(conflictClause)
                .append(" DO UPDATE SET ")
                .append("(").append(columns).append(")")
                .append(" = ")
                .append("(").append(updateValues).append(")");

        return statementStringBuilder.toString();
    }

    @Override
    public String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = String.join(", ", columnNames);

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        String conflictClause = "(" + String.join(", ", uniqueKeyColumnNames) + ")";

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON CONFLICT ")
                .append(conflictClause)
                .append(" DO NOTHING");
        return statementStringBuilder.toString();
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    @Override
    public String getAlterTableStatement(final String tableName, final List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            String dataType = getSQLForDataType(column.getDataType());
            StringBuilder sb = new StringBuilder("ADD COLUMN ")
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(dataType);
            columnsAndDatatypes.add(sb.toString());
        }

        StringBuilder alterTableStatement = new StringBuilder();
        return alterTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ")
                .append(String.join(", ", columnsAndDatatypes))
                .toString();
    }

    /**
     * Get the auto commit mode to use for reading from this database type.
     * For PostgreSQL databases, auto commit mode must be set to false to cause a fetchSize other than 0 to take effect.
     * More Details of this behaviour in PostgreSQL driver can be found in https://jdbc.postgresql.org//documentation/head/query.html.")
     * For PostgreSQL, if autocommit is TRUE, then fetch size is treated as 0 which loads all rows of the result set to memory at once.
     * @param fetchSize The number of rows to retrieve at a time. Value of 0 means retrieve all rows at once.
     * @return Optional.empty() if auto commit mode does not matter and can be left as is.
     *         Return true or false to indicate whether auto commit needs to be true or false for this database.
     */
    @Override
    public Optional<Boolean> getAutoCommitForReads(Integer fetchSize) {
        if (fetchSize != null && fetchSize != 0) {
            return Optional.of(Boolean.FALSE);
        }
        return Optional.empty();
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
                return "TEXT";
            default:
                return JDBCType.valueOf(sqlType).getName();
        }
    }
}
