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
package org.apache.nifi.db;

import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.ROWID;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.util.StringUtils;

/**
 * A Apache Phoenix database adapter that generates ANSI SQL.
 */
@Tags({"sql", "database", "syntax", "dialect", "adapter", "phoenix"})
@CapabilityDescription("Adapter for a PhoenixSQL dialect.")
public final class PhoenixDatabaseAdapter extends AbstractControllerService implements DatabaseAdapter {
    public static final String NAME = "Phoenix";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getDescription() {
        return "Generates Phoenix compliant SQL";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
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
            if (limit != null) {
                query.append(" LIMIT ");
                query.append(limit);
            }
            if (offset != null && offset > 0) {
                query.append(" OFFSET ");
                query.append(offset);
            }
        }
        return query.toString();
    }

    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (org.apache.nifi.util.StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = String.join(", ", columnNames);

        String parameterizedUpsertValues = columnNames.stream()
                .map(columnName -> "?")
                .collect(Collectors.joining(", "));

        StringBuilder statementStringBuilder = new StringBuilder("UPSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedUpsertValues).append(")");
        return statementStringBuilder.toString();
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    @Override
    public List<String> getAlterTableStatements(final String tableName, final List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
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
        return Collections.singletonList(alterTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ")
                .append(String.join(", ", columnsAndDatatypes))
                .toString());
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

    @Override
    public String getLiteralByType(int type, String value) {
        // Format value based on column type. For example, strings and timestamps need to be quoted
        switch (type) {
            // For string-represented values, put in single quotes
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                return "'" + value + "'";
            case TIME:
                return "time '" + value + "'";
            case DATE:
            case TIMESTAMP:
                // For backwards compatibility, the type might be TIMESTAMP but the state value
                // is in DATE format. This should be a one-time occurrence as the next maximum
                // value
                // should be stored as a full timestamp. Even so, check to see if the value is
                // missing time-of-day information, and use the "date" coercion rather than the
                // "timestamp" coercion in that case
                if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return "date '" + value + "'";
                } else {
                    return "timestamp '" + value + "'";
                }
            default:
                return value;
        }
    }
}
