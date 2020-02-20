package org.apache.nifi.cdc.postgresql.pgEasyReplication;

public class Column {

    private int position;
    private char isKey;
    private String name;
    private int dataTypeId;
    private String dataTypeName;
    private int typeModifier;

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public char isKey() {
        return isKey;
    }

    public void setIsKey(char isKey) {
        this.isKey = isKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDataTypeId() {
        return dataTypeId;
    }

    public void setDataTypeId(int dataTypeId) {
        this.dataTypeId = dataTypeId;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public void setDataTypeName(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public int getTypeModifier() {
        return typeModifier;
    }

    public void setTypeModifier(int typeModifier) {
        this.typeModifier = typeModifier;
    }
}
