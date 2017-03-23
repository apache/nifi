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

import java.util.List;

/**
 * An abstract base class for all MySQL binlog events affecting a table.
 */
public class BaseTableEventInfo extends BaseEventInfo implements TableEventInfo {

    private String databaseName;
    private String tableName;
    private Long tableId;

    private List<ColumnDefinition> columns;

    public BaseTableEventInfo(TableInfo tableInfo, String eventType, Long timestamp) {
        super(eventType, timestamp);
        if (tableInfo != null) {
            this.databaseName = tableInfo.getDatabaseName();
            this.tableName = tableInfo.getTableName();
            this.tableId = tableInfo.getTableId();
            this.columns = tableInfo.getColumns();
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getTableId() {
        return tableId;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public ColumnDefinition getColumnByIndex(int i) {
        try {
            return columns.get(i);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            return null;
        }
    }
}
