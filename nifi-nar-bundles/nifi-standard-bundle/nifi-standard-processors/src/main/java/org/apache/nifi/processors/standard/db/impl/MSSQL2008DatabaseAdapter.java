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

/**
 * A database adapter that generates MS SQL Compatible SQL for version 2008.
 */
public class MSSQL2008DatabaseAdapter extends MSSQLDatabaseAdapter {
    @Override
    public String getName() {
        return "MS SQL 2008";
    }

    @Override
    public String getDescription() {
        return "Generates MS SQL Compatible SQL for version 2008";
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
        boolean useColumnForPartitioning = !StringUtils.isEmpty(columnForPartitioning);
        // If this is a limit query and not a paging query then use TOP in MS SQL
        if (limit != null && !useColumnForPartitioning) {

            if (offset != null) {
                query.append("* FROM (SELECT ");
            }
            final long effectiveOffset = (offset == null) ? 0 : offset;
            if (effectiveOffset + limit > 0) {
                query.append("TOP ");
                query.append(effectiveOffset + limit);
                query.append(" ");
            }
        }

        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }

        if (limit != null && offset != null && orderByClause != null && !useColumnForPartitioning) {
            query.append(", ROW_NUMBER() OVER(ORDER BY ");
            query.append(orderByClause);
            query.append(" asc) rnum");
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
            if (useColumnForPartitioning) {
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

        if (!StringUtils.isEmpty(orderByClause) && !useColumnForPartitioning) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }

        if (limit != null && offset != null && !useColumnForPartitioning) {
            query.append(") A WHERE rnum > ");
            query.append(offset);
            query.append(" AND rnum <= ");
            query.append(offset + limit);
        }

        return query.toString();
    }
}
