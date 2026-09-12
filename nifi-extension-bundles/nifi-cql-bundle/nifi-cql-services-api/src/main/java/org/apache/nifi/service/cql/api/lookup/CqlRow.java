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
package org.apache.nifi.service.cql.api.lookup;

import java.util.List;
import java.util.Optional;

/**
 * One row of a statement result, with no schema imposed: its cells in selection order. Every other accessor
 * here derives from {@link #cells()}. A list rather than a name-keyed map because selection order carries
 * information, and column names are neither unique ({@code SELECT v, writetime(v), v}) nor always legal
 * identifiers.
 */
public interface CqlRow {

    /**
     * The row's cells, in selection order. Never {@code null}; empty only if the statement selected no
     * columns.
     */
    List<CqlCell> cells();

    /** The column names, in selection order; may contain duplicates or non-identifier names. */
    default List<String> columnNames() {
        return cells().stream().map(CqlCell::columnName).toList();
    }

    /**
     * The first cell with this column name, or empty if there is none. Use {@link #cells()} where a name
     * repeats.
     */
    default Optional<CqlCell> findCell(final String columnName) {
        return cells().stream().filter(cell -> cell.columnName().equals(columnName)).findFirst();
    }

    /**
     * The binary value of the named column. Throws if the row has no such column (a coding error); returns
     * {@code null} if the column holds a CQL null (data).
     *
     * @throws IllegalArgumentException if the row has no column of that name
     */
    default byte[] getBytes(final String columnName) {
        return requireCell(columnName).getBytes();
    }

    /**
     * As {@link #getBytes(String)}, for a column whose value is wanted as the backend decoded it.
     *
     * @throws IllegalArgumentException if the row has no column of that name
     */
    default Object getObject(final String columnName) {
        return requireCell(columnName).getObject();
    }

    private CqlCell requireCell(final String columnName) {
        return findCell(columnName).orElseThrow(() -> new IllegalArgumentException(
                "No column named '" + columnName + "'; row has " + columnNames()));
    }
}
