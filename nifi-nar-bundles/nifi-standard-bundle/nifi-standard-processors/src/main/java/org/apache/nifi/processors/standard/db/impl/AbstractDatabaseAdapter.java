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
import org.apache.nifi.processors.standard.db.DatabaseAdapter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A abtract database adapter that generates ANSI SQL.
 */
public abstract class AbstractDatabaseAdapter implements DatabaseAdapter {

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
    public String getInsertStatement(String tableName, List<String> columnNames) {
        final StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ")
                .append(tableName)
                .append(" (");

        IntStream.range(0, columnNames.size()).forEach(i -> {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(columnNames.get(i));
        });

        // complete the SQL statements by adding ?'s for all of the values to be escaped.
        sqlBuilder.append(") VALUES (");
        sqlBuilder.append(StringUtils.repeat("?", ",", columnNames.size()));
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    @Override
    public String getUpdateStatement(String tableName, List<String> updateColumnNames, List<String> whereColumns) {
        final StringBuilder sqlBuilder = new StringBuilder("UPDATE ")
                .append(tableName).append(" SET ");

        IntStream.range(0, updateColumnNames.size()).forEach(i -> {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(updateColumnNames.get(i)).append(" = ?");
        });

        sqlBuilder.append(" WHERE ");

        IntStream.range(0, whereColumns.size()).forEach(i -> {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(whereColumns.get(i)).append(" = ?");
        });
        return sqlBuilder.toString();
    }

    @Override
    public String getDeleteStatement(String tableName, List<String> whereColumns) {
        final StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ")
                .append(tableName)
                .append(" WHERE ");

        IntStream.range(0, whereColumns.size()).forEach(i -> {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            // Need to build a null-safe construct for the WHERE clause, since we are using PreparedStatement and won't know if the values are null. If they are null,
            // then the filter should be "column IS null" vs "column = null". Since we don't know whether the value is null, we can use the following construct (from NIFI-3742):
            //   (column = ? OR (column is null AND ? is null))
            sqlBuilder.append("(");
            sqlBuilder.append(whereColumns.get(i));
            sqlBuilder.append(" = ? OR (");
            sqlBuilder.append(whereColumns.get(i));
            sqlBuilder.append(" is null AND ? is null))");
        });
        return sqlBuilder.toString();
    }

    @Override
    public void executeDmlStatement(PreparedStatement ps, String statementType, List<List<Object>> valuesList, List<Integer> sqlTypes, List<Integer> recordSqlTypes)
            throws SQLException, IOException {

        for (List<Object> values : valuesList) {
            for (int i = 0; i < sqlTypes.size(); i = i + 1) {
                Object currentValue = values.get(i);
                int sqlType = sqlTypes.get(i);
                int recordSqlType = recordSqlTypes.get(i);

                // If DELETE type, insert the object twice because of the null check (see getDeleteStatement for details)
                if ("DELETE".equalsIgnoreCase(statementType)) {
                    psSetValue(ps, i * 2 + 1, currentValue, sqlType, recordSqlType);
                    psSetValue(ps, i * 2 + 2, currentValue, sqlType, recordSqlType);
                } else {
                    psSetValue(ps, i + 1, currentValue, sqlType, recordSqlType);
                }
            }
            ps.addBatch();
        }
        ps.executeBatch();
    }

    /**
     * This can be overridden by other adapters to handle special Objects according to sqlType and call other set methods of PreparedStatement
     */
    void psSetValue(PreparedStatement ps, int index, Object value, int sqlType, int recordSqlType) throws SQLException, IOException {
        if (null == value) {
            ps.setNull(index, sqlType);
        } else {
            switch (sqlType) {
                case Types.BLOB:
                    //resolve BLOB type for record
                    if (Types.ARRAY == recordSqlType) {
                        Object[] objects = (Object[]) value;
                        byte[] byteArray = new byte[objects.length];
                        for (int k = 0; k < objects.length; k++) {
                            Object o = objects[k];
                            if (o instanceof Number) {
                                byteArray[k] = ((Number) o).byteValue();
                            }
                        }
                        try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                            ps.setBlob(index, inputStream);
                        } catch (IOException e) {
                            throw new IOException("Unable to parse binary data " + value.toString(), e.getCause());
                        }
                    } else {
                        try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes())) {
                            ps.setBlob(index, inputStream);
                        } catch (IOException e) {
                            throw new IOException("Unable to parse binary data " + value.toString(), e.getCause());
                        }
                    }
                    break;
                 //add other Types here to resolve data
                default:
                    ps.setObject(index, value, sqlType);
            }
        }
    }

}
