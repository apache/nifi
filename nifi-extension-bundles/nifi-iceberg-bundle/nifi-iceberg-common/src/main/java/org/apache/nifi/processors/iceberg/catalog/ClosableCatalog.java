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
package org.apache.nifi.processors.iceberg.catalog;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class for Iceberg catalogs
 */
public class ClosableCatalog implements Catalog, Closeable {

    private final Catalog catalog;

    public ClosableCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public String name() {
        return catalog.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return catalog.listTables(namespace);
    }

    @Override
    public Table createTable(TableIdentifier identifier,
                             Schema schema,
                             PartitionSpec spec,
                             String location,
                             Map<String, String> properties) {
        return catalog.createTable(identifier, schema, spec, location, properties);
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            Map<String, String> properties) {
        return catalog.createTable(identifier, schema, spec, properties);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        return catalog.createTable(identifier, schema, spec);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema) {
        return catalog.createTable(identifier, schema);
    }

    @Override
    public Transaction newCreateTableTransaction(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) {
        return catalog.newCreateTableTransaction(identifier, schema, spec, location, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            Map<String, String> properties) {
        return catalog.newCreateTableTransaction(identifier, schema, spec, properties);
    }

    @Override
    public Transaction newCreateTableTransaction(
            TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        return catalog.newCreateTableTransaction(identifier, schema, spec);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
        return catalog.newCreateTableTransaction(identifier, schema);
    }

    @Override
    public Transaction newReplaceTableTransaction(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties,
            boolean orCreate) {
        return catalog.newReplaceTableTransaction(identifier, schema, spec, location, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            Map<String, String> properties,
            boolean orCreate) {
        return catalog.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
            TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate) {
        return catalog.newReplaceTableTransaction(identifier, schema, spec, orCreate);
    }

    @Override
    public Transaction newReplaceTableTransaction(
            TableIdentifier identifier, Schema schema, boolean orCreate) {
        return catalog.newReplaceTableTransaction(identifier, schema, orCreate);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return catalog.tableExists(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier) {
        return catalog.dropTable(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return catalog.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        catalog.renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        return catalog.loadTable(identifier);
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
        catalog.invalidateTable(identifier);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        return catalog.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        return catalog.buildTable(identifier, schema);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        catalog.initialize(name, properties);
    }

    @Override
    public void close() throws IOException {
        if (catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
    }
}
