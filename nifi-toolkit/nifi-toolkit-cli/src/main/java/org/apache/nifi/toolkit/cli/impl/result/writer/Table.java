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
package org.apache.nifi.toolkit.cli.impl.result.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Table {

    public static String EMPTY_VALUE = "(empty)";

    private final List<TableColumn> columns;
    private final List<String[]> rows = new ArrayList<>();

    private Table(final Builder builder) {
        this.columns = Collections.unmodifiableList(
                builder.columns == null ? Collections.emptyList() : new ArrayList<>(builder.columns));

        if (this.columns.isEmpty()) {
            throw new IllegalStateException("Cannot create a table with no columns");
        }
    }

    public void addRow(String ... values) {
        if (values == null) {
            throw new IllegalArgumentException("Values cannot be null");
        }

        if (values.length != columns.size()) {
            throw new IllegalArgumentException("Row has " + values.length
                    + " columns, but table has " + columns.size() + " columns");
        }

        // fill in any null values in the row with the empty value
        for (int i=0; i < values.length; i++) {
            if (values[i] == null) {
                values[i] = EMPTY_VALUE;
            }
        }

        this.rows.add(values);
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public List<String[]> getRows() {
        return rows;
    }

    /**
     * Builder for a Table.
     */
    public static class Builder {

        private final List<TableColumn> columns = new ArrayList<>();

        public Builder column(final TableColumn column) {
            if (column == null) {
                throw new IllegalArgumentException("TableColumn cannot be null");
            }
            this.columns.add(column);
            return this;
        }

        public Builder column(final String name, int minLength, int maxLength, boolean abbreviate) {
            final TableColumn column = new TableColumn(name, minLength, maxLength, abbreviate);
            return column(column);
        }

        public Table build() {
            return new Table(this);
        }

    }
}
