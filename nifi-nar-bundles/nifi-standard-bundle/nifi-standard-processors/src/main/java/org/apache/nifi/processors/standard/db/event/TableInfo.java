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
package org.apache.nifi.processors.standard.db.event;

import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A POJO for holding table information related to update events.
 */
public class TableInfo {

    final static String DB_TABLE_NAME_DELIMITER = "@!@";

    private String databaseName;
    private String tableName;
    private Long tableId;
    private List<ColumnDefinition> columns;

    public TableInfo(String databaseName, String tableName, Long tableId, List<ColumnDefinition> columns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.columns = columns;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getTableId() {
        return tableId;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnDefinition> columns) {
        this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableInfo tableInfo = (TableInfo) o;

        if (!databaseName.equals(tableInfo.databaseName)) return false;
        if (!tableName.equals(tableInfo.tableName)) return false;
        if (!tableId.equals(tableInfo.tableId)) return false;
        return columns != null ? columns.equals(tableInfo.columns) : tableInfo.columns == null;
    }

    @Override
    public int hashCode() {
        int result = databaseName.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + tableId.hashCode();
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }

    public static class Serializer implements org.apache.nifi.distributed.cache.client.Serializer<TableInfo> {

        @Override
        public void serialize(TableInfo value, OutputStream output) throws SerializationException, IOException {
            StringBuilder sb = new StringBuilder(value.getDatabaseName());
            sb.append(DB_TABLE_NAME_DELIMITER);
            sb.append(value.getTableName());
            sb.append(DB_TABLE_NAME_DELIMITER);
            sb.append(value.getTableId());
            List<ColumnDefinition> columnDefinitions = value.getColumns();
            if (columnDefinitions != null && !columnDefinitions.isEmpty()) {
                sb.append(DB_TABLE_NAME_DELIMITER);
                sb.append(columnDefinitions.stream().map((col) -> col.getName() + DB_TABLE_NAME_DELIMITER + col.getType()).collect(Collectors.joining(DB_TABLE_NAME_DELIMITER)));
            }
            output.write(sb.toString().getBytes());
        }
    }

    public static class Deserializer implements org.apache.nifi.distributed.cache.client.Deserializer<TableInfo> {

        @Override
        public TableInfo deserialize(byte[] input) throws DeserializationException, IOException {
            // Don't bother deserializing if empty, just return null. This usually happens when the key is not found in the cache
            if (input == null || input.length == 0) {
                return null;
            }
            String inputString = new String(input);
            String[] tokens = inputString.split(DB_TABLE_NAME_DELIMITER);
            int numTokens = tokens.length;
            if (numTokens < 3) {
                throw new IOException("Could not deserialize TableInfo from the following value: " + inputString);
            }
            String dbName = tokens[0];
            String tableName = tokens[1];
            Long tableId;
            try {
                tableId = Long.parseLong(tokens[2]);
            } catch (NumberFormatException nfe) {
                throw new IOException("Illegal table ID: " + tokens[2]);
            }
            // Parse column names and types
            List<ColumnDefinition> columnDefinitions = new ArrayList<>();
            for (int i = 0; i < numTokens - 3; i += 2) {
                try {
                    columnDefinitions.add(new ColumnDefinition(Byte.parseByte(tokens[i + 4]), tokens[i + 3]));
                } catch (NumberFormatException nfe) {
                    throw new IOException("Illegal column type value for column " + (i / 2 + 1) + ": " + tokens[i + 4]);
                }
            }

            return new TableInfo(dbName, tableName, tableId, columnDefinitions);
        }
    }
}
