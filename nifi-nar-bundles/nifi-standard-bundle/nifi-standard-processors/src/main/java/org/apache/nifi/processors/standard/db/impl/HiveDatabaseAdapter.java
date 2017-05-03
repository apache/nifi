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

/**
 * A DatabaseAdapter that generates HIVE QL.
 */
public class HiveDatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Hive";
    }

    @Override
    public String getDescription() {
        return "Generates HIVE QL";
    }


    @Override
    public boolean getSupportsStatementTimeout() {
        return false;
    }

    @Override
    public boolean getSupportsGetTableName() {
        return false;
    }

    @Override
    public boolean getSupportsMetaDataColumnIsSigned() {
        return false;
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        if (StringUtils.isEmpty(columnNames)) {
            columnNames = "*";
        }

        final StringBuilder query = new StringBuilder("SELECT ");

        if (limit != null) {
            if (offset != null) {
                query.append("* FROM (SELECT ");
            }

        }

        query.append(columnNames);

        if (limit != null && offset != null && orderByClause != null) {
            query.append(", ROW_NUMBER() OVER(ORDER BY ");
            query.append(orderByClause);
            query.append(" ASC) rnum");
        }

        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
        }

        if (!StringUtils.isEmpty(orderByClause)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }

        if (limit != null && offset != null) {
            query.append(") A WHERE rnum > ");
            query.append(offset);
            query.append(" AND rnum <= ");
            query.append(offset + limit);
        }

        return query.toString();
    }
}
