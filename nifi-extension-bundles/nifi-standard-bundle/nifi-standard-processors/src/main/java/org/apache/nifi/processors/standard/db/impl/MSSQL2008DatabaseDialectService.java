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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardStatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;

import java.util.List;
import java.util.Optional;

@Tags({"mssql", "sqlserver", "database", "dialect"})
@CapabilityDescription("Microsoft SQL Server 2008 Database Dialect Service providing SELECT with ROW_NUMBER() paging, UPSERT using MERGE, and basic ALTER/CREATE DDL generation.")
public class MSSQL2008DatabaseDialectService extends MSSQLDatabaseDialectService {

    @Override
    public StatementResponse getStatement(final StatementRequest statementRequest) {
        if (statementRequest.statementType() == StatementType.SELECT) {
            return new StandardStatementResponse(buildSelect2008(statementRequest));
        }
        return super.getStatement(statementRequest);
    }

    private String buildSelect2008(final StatementRequest statementRequest) {
        if (!(statementRequest instanceof QueryStatementRequest query)) {
            throw new IllegalArgumentException("Query Statement Request not found [" + statementRequest.getClass() + "]");
        }

        final TableDefinition table = statementRequest.tableDefinition();
        final String qualifiedTableName = qualifyTableName(table);

        final Optional<String> derivedTable = query.derivedTable();
        if (derivedTable.isPresent()) {
            final String tableAlias = "AS " + table.tableName();
            return "SELECT * FROM (" + derivedTable.get() + ") " + tableAlias;
        }

        final String selectColumns = buildSelectColumns(table.columns());

        final Optional<PageRequest> page = query.pageRequest();
        final Long limit;
        final Long offset;
        final String indexColumnName;
        if (page.isPresent()) {
            final PageRequest p = page.get();
            limit = p.limit().isPresent() ? p.limit().getAsLong() : null;
            offset = p.offset();
            indexColumnName = p.indexColumnName().orElse(null);
        } else {
            limit = null;
            offset = null;
            indexColumnName = null;
        }

        final String whereClause = query.whereClause().orElse(null);
        final String orderByClause = query.orderByClause().orElse(null);

        final boolean partitioned = indexColumnName != null && !indexColumnName.isBlank();
        final boolean hasOrder = orderByClause != null && !orderByClause.isBlank();
        final boolean useWindowPaging = limit != null && !partitioned && offset != null && (offset > 0 || hasOrder);

        final StringBuilder sql = new StringBuilder("SELECT ");

        if (limit != null && !partitioned) {
            if (useWindowPaging) {
                sql.append("* FROM (SELECT ");
            }
            final long effectiveOffset = (offset == null) ? 0 : offset;
            if (effectiveOffset + limit > 0) {
                sql.append("TOP ").append(effectiveOffset + limit).append(' ');
            }
        }

        sql.append(selectColumns);

        if (useWindowPaging && hasOrder) {
            sql.append(", ROW_NUMBER() OVER(ORDER BY ")
               .append(orderByClause)
               .append(" asc) rnum");
        }

        sql.append(" FROM ").append(qualifiedTableName);

        if (whereClause != null && !whereClause.isBlank()) {
            sql.append(" WHERE ").append(whereClause);
            if (partitioned) {
                sql.append(" AND ")
                   .append(indexColumnName)
                   .append(" >= ")
                   .append(offset != null ? offset : 0);
                if (limit != null) {
                    sql.append(" AND ")
                       .append(indexColumnName)
                       .append(" < ")
                       .append((offset == null ? 0 : offset) + limit);
                }
            }
        }

        if (orderByClause != null && !orderByClause.isBlank() && !partitioned) {
            sql.append(" ORDER BY ").append(orderByClause);
        }

        if (useWindowPaging) {
                sql.append(") A WHERE rnum > ")
                   .append(offset)
                   .append(" AND rnum <= ")
                   .append(offset + limit);
        }

        return sql.toString();
    }

    private String buildSelectColumns(final List<ColumnDefinition> columns) {
        if (columns == null || columns.isEmpty()) {
            return "*";
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i).columnName());
        }
        return sb.toString();
    }

    private String qualifyTableName(final TableDefinition table) {
        final StringBuilder name = new StringBuilder();
        table.catalog().ifPresent(c -> name.append(c).append('.'));
        table.schemaName().ifPresent(s -> name.append(s).append('.'));
        name.append(table.tableName());
        return name.toString();
    }
}


