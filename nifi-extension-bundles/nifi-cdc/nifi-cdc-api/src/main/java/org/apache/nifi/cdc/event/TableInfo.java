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
package org.apache.nifi.cdc.event;

import org.apache.commons.lang3.builder.EqualsBuilder;

import java.util.List;

/**
 * A POJO for holding table information related to update events.
 */
public class TableInfo {

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

        TableInfo that = (TableInfo) o;

        return new EqualsBuilder()
                .append(databaseName, that.databaseName)
                .append(tableName, that.tableName)
                .append(tableId, that.tableId)
                .append(columns, that.columns)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = databaseName.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + tableId.hashCode();
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        return result;
    }
}
