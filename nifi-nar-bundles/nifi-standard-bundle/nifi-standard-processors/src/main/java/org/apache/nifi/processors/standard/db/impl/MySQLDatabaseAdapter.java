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

import com.google.common.base.Preconditions;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
    public String unwrapIdentifier(String identifier) {
        // Removes double quotes and back-ticks.
        return identifier == null ? null : identifier.replaceAll("[\"`]", "");
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsInsertIgnore() {
        return true;
    }

    /**
     * Tells How many times the column values need to be inserted into the prepared statement. Some DBs (such as MySQL) need the values specified twice in the statement,
     * some need only to specify them once.
     *
     * @return An integer corresponding to the number of times to insert column values into the prepared statement for UPSERT, or -1 if upsert is not supported.
     */
    @Override
    public int getTimesToAddColumnObjectsForUpsert() {
        return 2;
    }

    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        Preconditions.checkArgument(!StringUtils.isEmpty(table), "Table name cannot be null or blank");
        Preconditions.checkArgument(columnNames != null && !columnNames.isEmpty(), "Column names cannot be null or empty");
        Preconditions.checkArgument(uniqueKeyColumnNames != null && !uniqueKeyColumnNames.isEmpty(), "Key column names cannot be null or empty");

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        List<String> updateValues = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            updateValues.add(columnNames.get(i) + " = ?");
        }
        String parameterizedUpdateValues = String.join(", ", updateValues);

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON DUPLICATE KEY UPDATE ")
                .append(parameterizedUpdateValues);
        return statementStringBuilder.toString();
    }

    @Override
    public String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        Preconditions.checkArgument(!StringUtils.isEmpty(table), "Table name cannot be null or blank");
        Preconditions.checkArgument(columnNames != null && !columnNames.isEmpty(), "Column names cannot be null or empty");
        Preconditions.checkArgument(uniqueKeyColumnNames != null && !uniqueKeyColumnNames.isEmpty(), "Key column names cannot be null or empty");

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        StringBuilder statementStringBuilder = new StringBuilder("INSERT IGNORE INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")");
        return statementStringBuilder.toString();
    }
}
