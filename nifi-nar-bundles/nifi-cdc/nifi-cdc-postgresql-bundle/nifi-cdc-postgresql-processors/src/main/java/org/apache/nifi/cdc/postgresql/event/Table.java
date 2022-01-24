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

package org.apache.nifi.cdc.postgresql.event;

import java.util.HashMap;

/**
 * Table is the class for a PostgreSQL Table metadata.
 * A PostgreSQL Table is also known as Relation.
 * The Table object encapsulates the ID, namespace (or schema), name, number of
 * columns and replica identity about a specific PostgreSQL Table.
 * As may be expected, Table object also has a set of Columns objects.
 *
 * @see org.apache.nifi.cdc.postgresql.event.Column
 */
public class Table {

    private int id;
    private String namespace;
    private String name;
    private char replicaIdentity;
    private short numColumns;
    private HashMap<Integer, Column> columns = new HashMap<Integer, Column>();

    /**
     * Returns the Table ID. The OID column from pg_catalog.pg_class.
     *
     * @return int
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the Table ID.
     *
     * @param id
     *           The OID column from pg_catalog.pg_class.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Returns the Table Namespace. For example, public.
     *
     * @return String
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the Table Namespace.
     *
     * @param namespace
     *                  Table Namespace
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Returns the Table Name whithout Namespace/Schema.
     *
     * @return String
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the Table name whithout Namespace/Schema.
     *
     * @param name
     *             Table Name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the object name created by Table Namespace + '.' + Table Name.
     *
     * If Namespace is null, pg_catalog is the Namespace.
     *
     * @return String
     */
    public String getObjectName() {
        return (namespace != null) ? namespace + "." + name : "pg_catalog." + name;
    }

    /**
     * Returns the Replica Identity propriety of the Table.
     *
     * This propriety changes the information which is written to the WAL to
     * identify rows which are updated or deleted.
     *
     * The relreplident column from pg_catalog.pg_class.
     *
     * @return char
     */
    public char getReplicaIdentity() {
        return replicaIdentity;
    }

    /**
     * Sets the Replica Identity propriety of the Table.
     *
     * @param replicaIdentity
     *                        The relreplident column from pg_catalog.pg_class.
     */
    public void setReplicaIdentity(char replicaIdentity) {
        this.replicaIdentity = replicaIdentity;
    }

    /**
     * Returns the number of Table Columns.
     *
     * @return short
     */
    public short getNumColumns() {
        return numColumns;
    }

    /**
     * Sets the number of Table Columns.
     *
     * @param numColumns
     *                   Number of columns
     */
    public void setNumColumns(short numColumns) {
        this.numColumns = numColumns;
    }

    /**
     * Adds a new Column to the Table Columns set.
     *
     * @param position
     *                 The Column Position in the Table.
     * @param column
     *                 The Column object.
     */
    public void putColumn(Integer position, Column column) {
        this.columns.put(position, column);
    }

    /**
     * Returns a Table Column from position.
     *
     * @param position
     *                 Position of column.
     *
     * @return Column
     */
    public Column getColumn(Integer position) {
        return this.columns.get(position);
    }

    /**
     * Returns all Table Columns.
     *
     * @return HashMap (Interger, Column)
     */
    public HashMap<Integer, Column> getColumns() {
        return columns;
    }
}