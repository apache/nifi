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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.util.StringUtils;

/**
 * A generic database adapter that generates Impala compatible SQL.
 */
public class ImpalaDatabaseAdapter extends GenericDatabaseAdapter {

    private static final String QUOTE_MARK = "`";

    @Override
    public String getName() {
        return "Impala";
    }

    @Override
    public String getDescription() {
        return "Generates Impala compatible SQL";
    }

    @Override
    public String unwrapIdentifier(String identifier) {
        // Removes double quotes and back-ticks.
        return identifier == null ? null : identifier.replaceAll("[\"`]", "");
    }

    @Override
    public boolean supportsUpsert() {
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

        final String columns = String.join(", ", columnNames);

        final String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        return "UPSERT INTO " + table + "(" + columns + ")" + " VALUES " + "(" + parameterizedInsertValues + ")";
    }

    @Override
    public String getTableQuoteString() {
        return QUOTE_MARK;
    }

    @Override
    public String getColumnQuoteString() {
        return QUOTE_MARK;
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    @Override
    public List<String> getAlterTableStatements(final String tableName, final List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        final List<String> columnsAndDatatypes = columnsToAdd.stream()
                .map(column->
                        (quoteColumnNames ? getColumnQuoteString() + column.getColumnName() + getColumnQuoteString() : column.getColumnName())
                                + " "
                                + getSQLForDataType(column.getDataType()))
                .collect(Collectors.toList());

        final String alterTableStatement = "ALTER TABLE "
                + (quoteTableName ?  getTableQuoteString() + tableName + getTableQuoteString() :  tableName )
                + " ADD COLUMNS " + "(" + String.join(", ", columnsAndDatatypes) + ")";

        return Collections.singletonList(alterTableStatement);
    }
}
