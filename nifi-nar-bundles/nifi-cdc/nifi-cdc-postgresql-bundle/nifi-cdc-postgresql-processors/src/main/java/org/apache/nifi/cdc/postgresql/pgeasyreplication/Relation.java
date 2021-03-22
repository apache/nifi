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

package org.apache.nifi.cdc.postgresql.pgeasyreplication;

import java.util.HashMap;
import java.util.Map;

public class Relation {

    private int id;
    private String namespace;
    private String name;
    private char replicaIdentity;
    private short numColumns;
    private final Map<Integer, Column> columns = new HashMap<>();

    public void putColumn(Integer position, Column column) {
        this.columns.put(position, column);
    }

    public Column getColumn(Integer position) {
        return this.columns.get(position);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return (namespace != null) ? this.namespace + "." + this.name : "pg_catalog." + this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public char getReplicaIdentity() {
        return replicaIdentity;
    }

    public void setReplicaIdentity(char replicaIdentity) {
        this.replicaIdentity = replicaIdentity;
    }

    public short getNumColumns() {
        return numColumns;
    }

    public void setNumColumns(short numColumns) {
        this.numColumns = numColumns;
    }
}
