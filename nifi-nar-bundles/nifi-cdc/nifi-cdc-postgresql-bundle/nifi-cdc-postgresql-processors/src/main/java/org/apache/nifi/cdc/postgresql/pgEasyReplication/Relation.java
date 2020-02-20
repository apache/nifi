package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.util.HashMap;

public class Relation {

    private int id;
    private String namespace;
    private String name;
    private char replicaIdentity;
    private short numColumns;
    private HashMap<Integer, Column> columns = new HashMap<Integer, Column>();

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
