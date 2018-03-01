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

import org.apache.commons.lang3.StringUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DynamicTableWriter implements TableWriter {

    @Override
    public void write(final Table table, final PrintStream output) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }

        if (output == null) {
            throw new IllegalArgumentException("Output cannot be null");
        }

        if (table.getColumns().isEmpty()) {
            throw new IllegalArgumentException("Table has no columns to write");
        }

        output.println();

        final List<TableColumn> columns = table.getColumns();
        final List<String[]> rows = table.getRows();

        final int numColumns = columns.size();
        final Integer[] columnLengths = determineColumnLengths(columns, rows);
        final List<String> columnNames = columns.stream().map(c -> c.getName()).collect(Collectors.toList());

        final Object[] columnLengthsObj = Arrays.copyOf(columnLengths, numColumns, Object[].class);
        final Object[] columnNamesObj = columnNames.toArray(new Object[numColumns]);

        final String columnsPatternFormat = String.join("", Collections.nCopies(numColumns, "%%-%ds   "));
        final String columnsPattern = String.format(columnsPatternFormat, columnLengthsObj);

        // format the header line which will include the column names
        final String header = String.format(columnsPattern, columnNamesObj);
        output.println(header);

        // a little clunky way to dynamically create a nice header line, but at least no external dependency
        final Object[] headerLineValues = new Object[numColumns];
        for (int i=0; i < numColumns; i++) {
            int length = columnLengths[i];
            headerLineValues[i] =  String.join("", Collections.nCopies(length, "-"));
        }

        final String headerLine = String.format(columnsPattern, headerLineValues);
        output.println(headerLine);

        // format the rows and print them
        for (String[] row : rows) {
            // convert the row to an Object[] for the String.format and also abbreviate any values
            final Object[] rowValues = new Object[row.length];
            for (int i=0; i < row.length; i++) {
                final TableColumn column = columns.get(i);
                if (column.isAbbreviated()) {
                    rowValues[i] = StringUtils.abbreviate(row[i], columnLengths[i]);
                } else {
                    rowValues[i] = row[i];
                }
            }

            final String rowString = String.format(columnsPattern, rowValues);
            output.println(rowString);
        }

        output.println();
        output.flush();
    }

    private Integer[] determineColumnLengths(final List<TableColumn> columns, final List<String[]> rows) {
        final Integer[] columnLengths = new Integer[columns.size()];

        for (int i=0; i < columns.size(); i++) {
            final TableColumn column = columns.get(i);

            int maxLengthInColumn = -1;

            // find the max length of the values in the column
            for (String[] row : rows) {
                final String colVal = row[i];
                if (colVal != null && colVal.length() > maxLengthInColumn) {
                    maxLengthInColumn = colVal.length();
                }
            }

            // if there were values for the column, then start with the min length of the column
            if (maxLengthInColumn < 0) {
                maxLengthInColumn = column.getMinLength();
            }

            // make sure the column length is at least as long as the column name
            maxLengthInColumn = Math.max(maxLengthInColumn, column.getName().length());

            // make sure the column length is not longer than the max length
            columnLengths[i] = Math.min(maxLengthInColumn, column.getMaxLength());
        }

        return columnLengths;
    }
}
