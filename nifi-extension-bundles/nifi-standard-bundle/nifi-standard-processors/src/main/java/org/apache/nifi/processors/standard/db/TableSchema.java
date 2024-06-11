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

import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TableSchema {
    private final List<String> requiredColumnNames;
    private final Set<String> primaryKeyColumnNames;
    private final Map<String, ColumnDescription> columns;
    private final String quotedIdentifierString;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public TableSchema(final String catalogName, final String schemaName, final String tableName,
                       final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
                       final NameNormalizer normalizer,
                       final Set<String> primaryKeyColumnNames, final String quotedIdentifierString) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = new LinkedHashMap<>();
        this.primaryKeyColumnNames = primaryKeyColumnNames;
        this.quotedIdentifierString = quotedIdentifierString;
        this.requiredColumnNames = new ArrayList<>();
        for (final ColumnDescription desc : columnDescriptions) {
            final String colName = normalizedName(desc.getColumnName(), translateColumnNames, normalizer);
            columns.put(colName, desc);
            if (desc.isRequired()) {
                requiredColumnNames.add(desc.getColumnName());
            }
        }
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, ColumnDescription> getColumns() {
        return columns;
    }

    public List<ColumnDescription> getColumnsAsList() {
        return new ArrayList<>(columns.values());
    }

    public List<String> getRequiredColumnNames() {
        return requiredColumnNames;
    }

    public Set<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public String getQuotedIdentifierString() {
        return quotedIdentifierString;
    }

    public static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                   final boolean translateColumnNames, final NameNormalizer normalizer,
                                   final String updateKeys, ComponentLog log) throws SQLException {
        final DatabaseMetaData dmd = conn.getMetaData();

        try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
            final List<ColumnDescription> cols = new ArrayList<>();
            while (colrs.next()) {
                final ColumnDescription col = ColumnDescription.from(colrs);
                cols.add(col);
            }
            // If no columns are found, check that the table exists
            if (cols.isEmpty()) {
                try (final ResultSet tblrs = dmd.getTables(catalog, schema, tableName, null)) {
                    List<String> qualifiedNameSegments = new ArrayList<>();
                    if (catalog != null) {
                        qualifiedNameSegments.add(catalog);
                    }
                    if (schema != null) {
                        qualifiedNameSegments.add(schema);
                    }
                    if (tableName != null) {
                        qualifiedNameSegments.add(tableName);
                    }
                    if (!tblrs.next()) {

                        throw new TableNotFoundException("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " not found, ensure the Catalog, Schema, and/or Table Names match those in the database exactly");
                    } else {
                        log.warn("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " found but no columns were found, if this is not expected then check the user permissions for getting table metadata from the database");
                    }
                }
            }

            final Set<String> primaryKeyColumns = new HashSet<>();
            if (updateKeys == null) {
                try (final ResultSet pkrs = dmd.getPrimaryKeys(catalog, schema, tableName)) {

                    while (pkrs.next()) {
                        final String colName = pkrs.getString("COLUMN_NAME");
                        primaryKeyColumns.add(colName);
                    }
                }
            } else {
                // Parse the Update Keys field and normalize the column names
                for (final String updateKey : updateKeys.split(",")) {
                    final String colName = normalizedName(updateKey, translateColumnNames, normalizer);
                    primaryKeyColumns.add(colName);

                }
            }

            return new TableSchema(catalog, schema, tableName, cols, translateColumnNames, normalizer, primaryKeyColumns, dmd.getIdentifierQuoteString());
        }
    }

    public static String normalizedName(final String name, final boolean translateNames, final NameNormalizer normalizer) {
        if (translateNames && normalizer != null) {
            return normalizer.getNormalizedName(name).trim().toUpperCase();
        }
        return name;
    }

    @Override
    public String toString() {
        return "TableSchema[columns=" + columns.values() + "]";
    }
}