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
package org.apache.nifi.processors.standard.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnDescription {
    private final String columnName;
    private final int dataType;
    private final boolean required;
    private final Integer columnSize;
    private final boolean nullable;

    public ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize, final boolean nullable) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.required = required;
        this.columnSize = columnSize;
        this.nullable = nullable;
    }

    public int getDataType() {
        return dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isNullable() {
        return nullable;
    }

    public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData md = resultSet.getMetaData();
        List<String> columns = new ArrayList<>();

        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            columns.add(md.getColumnName(i));
        }
        // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
        final String defaultValue = resultSet.getString("COLUMN_DEF");
        final String columnName = resultSet.getString("COLUMN_NAME");
        final int dataType = resultSet.getInt("DATA_TYPE");
        final int colSize = resultSet.getInt("COLUMN_SIZE");

        final String nullableValue = resultSet.getString("IS_NULLABLE");
        final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
        String autoIncrementValue = "NO";

        if (columns.contains("IS_AUTOINCREMENT")) {
            autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
        }

        final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
        final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

        return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize, isNullable);
    }

    public static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return colName == null ? null : (translateColumnNames ? colName.toUpperCase().replace("_", "") : colName);
    }

    @Override
    public String toString() {
        return "Column[name=" + columnName + ", dataType=" + dataType + ", required=" + required + ", columnSize=" + columnSize + "]";
    }
}
