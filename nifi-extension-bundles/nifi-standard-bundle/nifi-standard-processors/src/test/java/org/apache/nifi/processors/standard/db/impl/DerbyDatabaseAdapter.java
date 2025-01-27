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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
 * An implementation of DatabaseAdapter for Derby (used for testing).
 */
public class DerbyDatabaseAdapter implements DatabaseAdapter {

    @Override
    public String getName() {
        return "Derby";
    }

    @Override
    public String getDescription() {
        return "Generates Derby compatible SQL (used for testing)";
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
    public boolean supportsCreateTableIfNotExists() {
        // This is not actually true, but it returns true so we can use the workaround for testing. "Real" adapters should report this accurately
        return true;
    }

    @Override
    public String getCreateTableStatement(final TableSchema tableSchema, final boolean quoteTableName, final boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<ColumnDescription> columns = tableSchema.getColumnsAsList();
        List<String> columnsAndDatatypes = new ArrayList<>(columns.size());
        Set<String> primaryKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        for (ColumnDescription column : columns) {
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()))
                    .append(column.isNullable() ? "" : " NOT NULL")
                    .append(primaryKeyColumnNames != null && primaryKeyColumnNames.contains(column.getColumnName()) ? " PRIMARY KEY" : "");
            columnsAndDatatypes.add(sb.toString());
        }

        // This will throw an exception if the table already exists, but it should only be used for tests
        createTableStatement.append("CREATE TABLE ")
                .append(generateTableName(quoteTableName, tableSchema.getCatalogName(), tableSchema.getSchemaName(), tableSchema.getTableName(), tableSchema))
                .append(" (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return createTableStatement.toString();
    }

    @Override
    public String getAlterTableStatement(final String tableName, final List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        List<String> alterTableStatements = new ArrayList<>();

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

        for (String columnAndDataType : columnsAndDatatypes) {
            StringBuilder createTableStatement = new StringBuilder();
            alterTableStatements.add(createTableStatement.append("ALTER TABLE ")
                    .append(quoteTableName ? getTableQuoteString() : "")
                    .append(tableName)
                    .append(quoteTableName ? getTableQuoteString() : "")
                    .append(" ADD COLUMN ")
                    .append(columnAndDataType).toString());
        }

        final String sql;
        if (alterTableStatements.size() == 1) {
            sql = alterTableStatements.getFirst();
        } else {
            throw new UnsupportedOperationException("Apache Derby does not support adding more than one column when altering tables");
        }

        return sql;
    }

    @Override
    public String getSQLForDataType(int sqlType) {
        switch (sqlType) {
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
                // Derby must have a max length specified
                return "VARCHAR(100)";
            default:
                return JDBCType.valueOf(sqlType).getName();
        }
    }
}